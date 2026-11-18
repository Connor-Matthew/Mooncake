#pragma once

#include <array>
#include <atomic>
#include <boost/functional/hash.hpp>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "allocation_strategy.h"
#include "count_min_sketch.h"
#include "cvm/cvm_controller.h"
#include "cvm/inter_master_rpc.h"
#include "cvm/slot_hash.h"
#include "cvm/slot_owner_heartbeat.h"
#include "deadline_scheduler.h"
#include "master_metric_manager.h"
#include "mutex.h"
#include "segment.h"
#include "tenant_quota_sharded.h"
#include "tenant_quota_policy_store.h"
#include "types.h"
#include "master_config.h"
#include "rpc_types.h"
#include "replica.h"
#include "ha/ha_types.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "task_manager.h"
#include "kv_event/kv_event_publisher.h"
#include "ha/oplog/oplog_types.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "allocator.h"
#include "metadata_store.h"
#include "vsegment/partition_quota_planner.h"
#include "vsegment/vsegment_service.h"

namespace mooncake {

namespace io_pattern {
class CfmService;
class IoPatternRuntime;
}

// Forward declaration for MasterSnapshotManager
class MasterSnapshotManager;
class MasterSnapshotRepository;

namespace ha {
class SnapshotCatalogStore;
class MasterSnapshotCodec;
struct MasterSnapshotPayloads;
class MasterSnapshotCodecTest;  // test fixture, needs private state access
}  // namespace ha

// Forward declarations
class AllocationStrategy;
class EvictionStrategy;
class HaKvBackend;
class HttpMetadataServer;
class OpLogBatchStorage;
class OrderedOpLogWriter;
struct MetadataStoragePlugin;

// Forward declarations for test classes
namespace test {
class MasterServiceSnapshotTestBase;
class SnapshotChildProcessTest;
// Friended so the promotion-on-hit tests can drive a serialize/reset/
// deserialize cycle directly via the otherwise-private
// MetadataSerializer, and inspect private clamp fields. This avoids
// standing up a full snapshot catalog + child-process harness, and
// exposing test-only accessors on MasterService itself.
class PromotionOnHitTest;
class MasterServiceTenantQuotaTest;
class MasterServiceHATest;
// Friended so the offload-on-evict tests can drive the legacy eviction fallback
// that the IO Pattern eviction handler runs when a plan under-delivers.
class OffloadOnEvictTest;
}  // namespace test
namespace benchmarks {
class BatchEvictBench;
}  // namespace benchmarks

/*
 * @brief MasterService is the main class for the master server.
 * Lock order: To avoid deadlocks, the following lock order should be followed:
 * 1. client_mutex_
 * 2. tenant_quota_policy_mutex_
 * 3. snapshot_mutex_
 * 4. metadata_shards_[shard_idx_].mutex
 * 5. tenant_quota_recompute_mutex_
 * 6. ShardedTenantQuotaTable internal mutex or segment_mutex_
 *
 * Strict tenant admission and policy mutation paths that need both
 * tenant_quota_policy_mutex_ and snapshot_mutex_ must acquire the tenant
 * policy mutex first, then snapshot_mutex_.
 * tenant_quota_recompute_mutex_ serializes the capacity snapshot and the
 * corresponding quota-table update. The segment mutex is released before
 * entering ShardedTenantQuotaTable, so these two locks are never nested.
 */
class MasterService {
    // Test friend class for snapshot/restore testing
    friend class test::MasterServiceSnapshotTestBase;
    friend class test::SnapshotChildProcessTest;
    friend class test::PromotionOnHitTest;
    friend class benchmarks::BatchEvictBench;
    friend class test::MasterServiceTenantQuotaTest;
    friend class MasterSnapshotManager;    // Allow access to internal state for
                                           // snapshot
    friend class ha::MasterSnapshotCodec;  // Allow codec to access private
                                           // members
    friend class ha::MasterSnapshotCodecTest;  // codec round-trip unit test
    friend class test::MasterServiceHATest;
    friend class test::OffloadOnEvictTest;

   public:
    using NoFProbeFn =
        std::function<bool(const std::string&, uint32_t, std::string*)>;
    using DurableFinalizeCallback =
        std::function<void(const OpLogEntry& durable_entry)>;

    MasterService();
    MasterService(const MasterServiceConfig& config);
    ~MasterService();

    void SetNoFProbeFnForTesting(NoFProbeFn fn);
    size_t GetMountedNoFSegmentCountForTesting();
    bool IsNoFSegmentMountedForTesting(const UUID& segment_id);
    std::optional<uint32_t> GetNoFHeartbeatFailureCountForTesting(
        const UUID& segment_id);
    bool IsTenantQuotaEnabled() const;
    std::vector<TenantQuotaSnapshot> ListTenantQuotaSnapshots() const;
    std::optional<TenantQuotaSnapshot> GetTenantQuotaSnapshot(
        const TenantId& tenant_id) const;
    tl::expected<TenantQuotaSnapshot, ErrorCode> UpsertTenantQuotaPolicy(
        const TenantId& tenant_id, uint64_t requested_quota_bytes);
    tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>
    DeleteTenantQuotaPolicy(const TenantId& tenant_id);
    uint64_t GetTenantQuotaAllocatableCapacityBytes();

    ErrorCode SetBatchOpLogBackendForTesting(
        std::shared_ptr<HaKvBackend> backend);

    // CVM slot ownership publishing. The CvmController now lives in the HA
    // supervisor so membership keeps running even while this node is a
    // standby; the supervisor injects the lease id it granted and drives the
    // heartbeat around serve start/stop.
    void SetCvmLeaseId(EtcdLeaseId lease_id);
    // supervisor 注入其 CvmController（同一进程内），供 slot 归属解析读
    // ring_slots 缓存（§16.15.5 缓存优先、miss 回退环推导）。与 lease
    // 注入同生命周期（serve phase 前设置，长期有效，不撤销）。
    void SetCvmController(cvm::CvmController* controller);
    ErrorCode StartSlotOwnerHeartbeat();
    void StopSlotOwnerHeartbeat();

    // CVM inter-master RPC (multi-submaster coordination). Starts the
    // etcd-driven member refresh loop so this master can reach its peers
    // (handshake now; allocation forwarding later). Same lifecycle as the
    // slot owner heartbeat: called by the supervisor around serve phases.
    ErrorCode StartInterMasterRpc();
    void StopInterMasterRpc();
    cvm::InterMasterRpcClient* inter_master_rpc() const {
        return inter_master_rpc_.get();
    }

    // Inter-master handshake data: stable identity, supervisor-granted lease
    // and current owned-slot count (read path / forwarding diagnostics).
    const std::string& master_id() const { return master_id_; }
    EtcdLeaseId cvm_lease_id() const { return cvm_lease_id_; }
    uint32_t GetOwnedSlotCount() const;

    // vsegment/psegment 术语的单行状态摘要（enabled/generation/profiles/
    // installed partitions/vsegment count/池组成），供 admin 周期日志线程
    // 输出独立的 "VSegment Status" 行。
    std::string GetVSegmentStatusLogLine() const;
    // One-line capacity summary (used / capacity (pct), available, formed,
    // quota) for the periodic "VSegment Storage" log line; returns an empty
    // string when vsegment is disabled.
    std::string GetVSegmentStorageLogLine() const;
    void SetVSegmentService(std::shared_ptr<vsegment::VSegmentService> service) {
        vsegment_service_ = std::move(service);
    }
    // supervisor 注入 vsegment auto-plan 重试策略（master --vsegment_auto_plan
    // 系 flags 解析而来，serve phase 前设置一次）。member_count==0（默认，未
    // 注入）表示关闭。注入后由 MaybeRetryVSegmentAutoPlan 在心跳与 mount
    // 端点两个低频触发点重试发布配额快照，修复「submaster 先于 client 启动
    // 时一次性 bootstrap 发现零段而永久放弃」的时序死锁。
    void SetVSegmentAutoPlanPolicy(const vsegment::VSegmentUserPolicy& policy);
    const std::vector<vsegment::PartitionVSegmentSnapshot>&
    recovered_vsegment_snapshots() const {
        return recovered_vsegment_snapshots_;
    }
    void ClearRecoveredVSegmentSnapshots() {
        recovered_vsegment_snapshots_.clear();
    }
    // 测试专用：阻塞等待 stale-evict 异步 drain 队列清空（含 1s debounce
    // 与在途请求），上限 5s。生产代码勿用；non-etcd 构建为空操作。
    void FlushStaleEvictQueueForTesting();
    // Builds a deduplicated layout table for the given (partition_id,
    // vsegment_id) pairs. Best-effort: entries that cannot be resolved are
    // omitted (hard C has no legacy fallback; callers fail for those
    // vsegments).
    std::vector<VSegmentLayoutEntry> BuildVSegmentLayoutTable(
        const std::vector<std::pair<std::string, std::string>>& vsegments);
    // Attaches the runtime layout table for the vsegment replicas contained in
    // `response`. No-op when the response has no vsegment replica or when the
    // table cannot be resolved (best-effort; hard-C clients reject a
    // vsegment transfer without the table).
    void EnrichVSegmentLayouts(GetReplicaListResponse* response);
    void EnrichVSegmentLayouts(PutStartResult* response);
    void InvalidateVSegmentLayoutCache();
    vsegment::VSegmentPutStartResult VSegmentPutStart(
        const std::string& partition_id, uint64_t route_epoch,
        const std::string& operation_id, uint64_t length,
        const std::string& profile_name = {});
    ErrorCode VSegmentPutEnd(const VSegmentDescriptor& replica,
                             uint64_t route_epoch,
                             const std::string& operation_id,
                             const std::string& object_id);
    ErrorCode VSegmentPutRevoke(const std::string& partition_id,
                                const std::string& vsegment_id,
                                uint64_t route_epoch,
                                const std::string& operation_id);

    tl::expected<std::optional<PutStartResult>, ErrorCode>
    TryVSegmentPutStart(const UUID& client_id, const std::string& key,
                        const TenantId& tenant_id, uint64_t slice_length,
                        const ReplicateConfig& config);

    // Inter-master allocation forwarding (CVM plan B phase 2). Called by
    // WrappedMasterService when a slot-owning peer asks this submaster
    // (segment owner) to allocate memory replicas. `preferred_segments`
    // is honored strictly when non-empty. Real handles are kept alive in
    // a keepalive registry until InterMasterFreeReplicas arrives.
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    InterMasterAllocateReplicas(const std::string& tenant_id,
                                const std::string& key, uint64_t slice_length,
                                uint64_t replica_num,
                                const std::vector<std::string>& preferred_segments);

    // Frees the keepalive entry (and the handles) for (tenant, key).
    // Idempotent: returns false when no entry exists.
    tl::expected<bool, ErrorCode> InterMasterFreeReplicas(
        const std::string& tenant_id, const std::string& key);

    // Inter-master read forwarding (CVM plan B phase 2). Called by
    // WrappedMasterService when a peer submaster forwards a GetReplicaList /
    // BatchGetReplicaList request to this submaster (the slot owner). Unlike
    // the client-facing GetReplicaList, these do NOT re-forward: they query
    // the local metadata directly (peer trust), so an inconsistent view
    // terminates the forward chain at the first hop instead of looping.
    tl::expected<GetReplicaListResponse, ErrorCode> InterMasterGetReplicaList(
        const std::string& key, const std::string& tenant_id);

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    InterMasterBatchGetReplicaList(const std::vector<std::string>& keys,
                                   const std::string& tenant_id);

    // Inter-master write forwarding (model B). Called by WrappedMasterService
    // when a peer submaster received a PutStart for a slot it does not own: it
    // relays the FULL PutStart to this submaster (the slot owner), which
    // executes the complete local alloc + metadata + keepalive and returns the
    // descriptors for the caller to relay to the client. Because this is the
    // slot owner, its OwnsSlot==true so the forward chain terminates here
    // (peer trust, symmetric with InterMasterGetReplicaList).
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> InterMasterPutStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, uint64_t slice_length,
        const ReplicateConfig& config);

    // Upsert variant of InterMasterPutStart: the slot owner executes the FULL
    // UpsertStart (preserving the "overwrite if exists" preemption semantics
    // that PutStart lacks), and returns the descriptors for the caller to
    // relay to the client.
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    InterMasterUpsertStart(const UUID& client_id, const std::string& key,
                           const std::string& tenant_id, uint64_t slice_length,
                           const ReplicateConfig& config);

    // Inter-master slot-metadata migration (确定性哈希方案 §15.7，RPC 直传，
    // 替代 etcd slot_meta 中转)。新 owner 通过墙向旧 owner 拉取 `slot` 的对象
    // 元数据导出；旧 owner 序列化并返回 SlotMetadataExport，但直到收到
    // InterMasterAckSlotImported 才删除本地元数据。二者均为 peer-trust，直读
    // 本地 metadata_shards_，不再经 etcd。
    tl::expected<SlotMetadataExport, ErrorCode> InterMasterExportSlot(
        uint16_t slot, const std::string& requester_master_id);

    // 新 owner 导入完成后通知旧 owner 删除本地元数据（并清空 staged 导出
    // 缓存）。幂等；返回 true 表示曾有 staged 项。
    tl::expected<bool, ErrorCode> InterMasterAckSlotImported(
        uint16_t slot, const std::string& importer_master_id);

    // ----- 段级批量迁移（§16.19.2，P4 reshard）-----

    // 源侧批量导出：[first_slot, last_slot] 闭区间快照（staged 优先，缺失时
    // 即时构建——kMigrating 冻结写后快照稳定）。与单 slot 版不同，不做
    // 「必须已失去归属」门控：reshard 期间源仍持有归属（barrier 冻结写），
    // 快照由冻结保证稳定；切 owner 后源仍持有未 ack 的本地数据，亦可导出
    //（幂等，ack 前不删）。
    tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
    InterMasterExportSlotBatch(uint16_t first_slot, uint16_t last_slot,
                               const std::string& requester_master_id);

    // 心跳交接专用的批量导出变体（slot 就绪加速）：与 InterMasterExportSlotBatch
    // 的差异在未 staged 的 slot——不做无门控即时构建，仅当本机已观察到失去该
    // slot 归属（require_not_owned 门控）时才即时构建；仍持有归属的 slot 不出现
    // 在响应中（调用方对其走单 slot 路径重试）。心跳交接路径没有 kMigrating
    // 冻结写，快照稳定性完全依赖「源已观察到 lost 并停服」。
    tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
    InterMasterExportSlotBatchGated(uint16_t first_slot, uint16_t last_slot,
                                    const std::string& requester_master_id);

    // 段级 ack：目标安装完成后通知源删除区间内全部本地元数据
    //（DropSlotMetadataRange）。源未观察到归属已切走时返回
    // SLOT_MIGRATING（目标稍后重试，重发无损）。
    tl::expected<bool, ErrorCode> InterMasterAckSlotRangeImported(
        uint16_t first_slot, uint16_t last_slot,
        const std::string& importer_master_id);

    // 写路径校验（§16.19.3）：读路径继续走 CheckSlotServiceability（源
    // kMigrating 期间仍服务读）；写路径统一走本方法——ready/expected 判定
    // 之上叠加「本段正被迁出（kMigrating 且 migrating_to 非本机）→ 冻结写」
    // 的 barrier 语义。客户端对 SLOT_MIGRATING 按总时长上界退避 + 重拉路由。
    ErrorCode CheckSlotWritable(uint16_t slot) const;

    // 幽灵写瞬态节流告警（§16.14.6 验证辅助，30s 一条防刷屏）：ring_slots
    // 模型已启用（HasSeenRingSlotsModel）但本 slot 的归属缓存未命中 →
    // 数据面判定正在走旧环回退，可能与 etcd 真实归属不一致（幽灵写）。
    // where = 调用点定位（如 "CheckSlotWritable"/"ResolveSlotOwner"）。
    void MaybeWarnGhostFallback(const char* where, uint16_t slot) const;

    /**
     * @brief Test-only wrapper around BatchEvict / NoFBatchEvict so that
     *        unit tests can drive a single eviction cycle synchronously
     *        without standing up the periodic eviction thread.
     */
    void RunBatchEvictForTesting(double evict_ratio_target,
                                 double evict_ratio_lowerbound);
    void RunNoFBatchEvictForTesting(double evict_ratio_target,
                                    double evict_ratio_lowerbound);

    /**
     * @brief Mount a memory segment for buffer allocation. This function is
     * idempotent.
     * @return ErrorCode::OK on success,
     *         ErrorCode::INVALID_PARAMS on invalid parameters,
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment cannot
     *         be mounted temporarily,
     *         ErrorCode::INTERNAL_ERROR on internal errors.
     */
    auto MountSegment(const Segment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Mount a NoF SSD segment for buffer allocation. This function is
     * idempotent.
     * @return ErrorCode::OK on success,
     *         ErrorCode::INVALID_PARAMS on invalid parameters,
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment cannot
     *         be mounted temporarily,
     *         ErrorCode::INTERNAL_ERROR on internal errors.
     */
    auto MountNoFSegment(const NoFSegment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Re-mount segments, invoked when the client is the first time to
     * connect to the master or the client Ping TTL is expired and need
     * to remount. This function is idempotent. Client should retry if the
     * return code is not ErrorCode::OK.
     * @return ErrorCode::OK means either all segments are remounted
     * successfully or the fail is not solvable by a new remount request.
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment cannot
     *         be mounted temporarily.
     *         ErrorCode::INTERNAL_ERROR if something temporary error happens.
     */
    auto ReMountSegment(const std::vector<Segment>& segments,
                        const UUID& client_id) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Re-mount NoF SSD segments, invoked when the client is the first
     * time to connect to the master or the client Ping TTL is expired and need
     * to remount. This function is idempotent. Client should retry if the
     * return code is not ErrorCode::OK.
     * @return ErrorCode::OK means either all segments are remounted
     * successfully or the fail is not solvable by a new remount request.
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment cannot
     *         be mounted temporarily.
     *         ErrorCode::INTERNAL_ERROR if something temporary error happens.
     */
    auto ReMountNoFSegment(const std::vector<NoFSegment>& segments,
                           const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Unmount a memory segment. This function is idempotent.
     * @return ErrorCode::OK on success,
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment is
     *         currently unmounting.
     */
    auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    auto GracefulUnmountSegment(const UUID& segment_id, const UUID& client_id,
                                uint64_t grace_period_ms)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Unmount a NoF ssd segment. This function is idempotent.
     * @return ErrorCode::OK on success,
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment is
     *         currently unmounting.
     */
    auto UnmountNoFSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Check if an object exists
     * @return ErrorCode::OK if exists, otherwise return other ErrorCode
     */
    auto ExistKey(const std::string& key, const TenantId& tenant_id)
        -> tl::expected<bool, ErrorCode>;

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys, const TenantId& tenant_id);

    /**
     * @brief Fetch all keys for a single tenant.
     * @return ErrorCode::OK if exists
     */
    auto GetAllKeys(const TenantId& tenant_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Fetch all segments, each node has a unique real client with fixed
     * segment name : segment name, preferred format : {ip}:{port}, bad format :
     * localhost:{port}
     * @return ErrorCode::OK if exists
     */
    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Fetch all mounted NoF segments.
     * @return std::vector<MountedNoFSegmentSnapshot> on success, error code
     * otherwise.
     */
    auto GetAllNoFSegments()
        -> tl::expected<std::vector<NoFSegment>, ErrorCode>;

    /**
     * @brief Query mounted NoF segments by segment name and return their
     * segment ids together with owner client ids.
     * @param segment_name Mounted NoF segment name.
     * @return Matching segment owner info list on success, error code
     * otherwise.
     */
    auto GetNoFSegmentsByName(const std::string& segment_name)
        -> tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode>;

    /**
     * @brief Detailed information about a single segment.
     * Keeps original types so callers can use values directly without
     * needing to parse strings back to uuid/address/enum.
     */
    struct SegmentDetailInfo {
        std::string segment_name;
        UUID segment_id{0, 0};
        UUID client_id{0, 0};
        uintptr_t base_address{0};
        uint64_t size_bytes{0};
        std::string te_endpoint;
        std::string protocol;
        SegmentStatus status{SegmentStatus::UNDEFINED};
        uint64_t allocator_used_bytes{0};
        uint64_t allocator_capacity_bytes{0};
    };

    /**
     * @brief Get detailed information of all segments, including the
     * relationships between segment_id, client_id, segment_name, status,
     * allocator used/capacity, etc.
     * @return A vector of SegmentDetailInfo on success, error code otherwise.
     */
    auto GetSegmentsDetail()
        -> tl::expected<std::vector<SegmentDetailInfo>, ErrorCode>;

    /**
     * @brief Query a segment's capacity and used size in bytes.
     * Conductor should use these information to schedule new requests.
     * @return ErrorCode::OK if exists
     */
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;

    /**
     * @brief Query IP addresses for a given client ID.
     * @param client_id The UUID of the client to query.
     * @return An expected object containing a vector of IP addresses on success
     * (empty vector if client has no IPs), or ErrorCode::CLIENT_NOT_FOUND if
     * the client doesn't exist, or another ErrorCode on other failures.
     */
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Batch query IP addresses for multiple client IDs.
     * @param client_ids Vector of client UUIDs to query.
     * @return An expected object containing a map from client_id to their IP
     * address lists on success, or an ErrorCode on failure. Non-existent
     * clients are omitted from the result map. Clients that exist but have no
     * IPs are included with empty vectors.
     */
    auto BatchQueryIp(const std::vector<UUID>& client_ids) -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>;

    bool KvEventsEnabled() const;
    KvEventPublisher::Stats GetKvEventStats() const;

    std::shared_ptr<io_pattern::CfmService> GetCfmService() const {
        return io_pattern_cfm_service_;
    }

    /**
     * @brief Batch clear KV cache replicas for specified object keys.
     * @param object_keys Vector of object key strings to clear.
     * @param client_id The UUID of the client that owns the object keys.
     * @param segment_name The name of the segment (storage device) to clear
     * from. If empty, clears replicas from all segments for the given
     * client_id.
     * @return An expected object containing a vector of successfully cleared
     * keys on success, or an ErrorCode on failure. Only successfully
     * cleared keys are included in the result.
     */
    // Existing key-only overload (signature unchanged): kept for legacy
    // callers; delegates with "default".
    auto BatchReplicaClear(const std::vector<std::string>& object_keys,
                           const UUID& client_id,
                           const std::string& segment_name)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    // New: tenant-aware overload
    auto BatchReplicaClear(const std::vector<std::string>& object_keys,
                           const UUID& client_id,
                           const std::string& segment_name,
                           const std::string& tenant_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Retrieves replica lists for object keys that match a regex
     * pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing a map from object keys to their
     * replica descriptors on success, or an ErrorCode on failure.
     */
    auto GetReplicaListByRegex(const std::string& regex_pattern,
                               const TenantId& tenant_id)
        -> tl::expected<
            std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
            ErrorCode>;

    /**
     * @brief Get list of replicas for an object
     * @param[out] replica_list Vector to store replica information
     * @return ErrorCode::OK on success, ErrorCode::REPLICA_IS_NOT_READY if not
     * ready
     */
    auto GetReplicaList(const std::string& key, const TenantId& tenant_id)
        -> tl::expected<GetReplicaListResponse, ErrorCode>;

    /**
     * @brief Read-only single-key replica list query for admin use.
     * Unlike GetReplicaList, this does not grant leases, trigger
     * promotion, or update cache-hit metrics.
     */
    auto GetReplicaListForAdmin(const std::string& key,
                                const TenantId& tenant_id)
        -> tl::expected<GetReplicaListResponse, ErrorCode>;

    /**
     * @brief Get replica lists for a batch of objects.
     */
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys,
                        const TenantId& tenant_id);

    /**
     * @brief Read-only batch replica list query for admin use.
     * Unlike BatchGetReplicaList, this does not grant leases, trigger
     * promotion, or update cache-hit metrics.
     */
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaListForAdmin(const std::vector<std::string>& keys,
                                const TenantId& tenant_id);

    /**
     * @brief Start a put operation for an object
     * @param[out] replica_list Vector to store replica information for the
     * slice
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if exists,
     *         ErrorCode::NO_AVAILABLE_HANDLE if allocation fails,
     *         ErrorCode::INVALID_PARAMS if slice size is invalid
     */
    auto PutStart(const UUID& client_id, const std::string& key,
                  const TenantId& tenant_id, const uint64_t slice_length,
                  const ReplicateConfig& config)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    /**
     * @brief Complete a put operation, replica_type indicates the type of
     * replica to complete (memory or disk)
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    auto PutEnd(const UUID& client_id, const ObjectMeta& object_meta,
                const TenantId& tenant_id, ReplicaType replica_type,
                const std::string& operation_id = "")
        -> tl::expected<void, ErrorCode>;

    auto PutEnd(const UUID& client_id, const std::string& key,
                const TenantId& tenant_id, ReplicaType replica_type,
                const std::string& operation_id = "")
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Adds a replica instance associated with the given client and key.
     */
    auto AddReplica(const UUID& client_id, const std::string& key,
                    const TenantId& tenant_id, Replica& replica)
        -> tl::expected<bool, ErrorCode>;

    /**
     * @brief Revoke a put operation, replica_type indicates the type of
     * replica to revoke (memory or disk)
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    auto PutRevoke(const UUID& client_id, const std::string& key,
                   const TenantId& tenant_id, ReplicaType replica_type,
                   const std::string& operation_id = "")
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Complete a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
        const TenantId& tenant_id, ReplicaType replica_type = ReplicaType::ALL);

    /**
     * @brief Revoke a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        const TenantId& tenant_id, ReplicaType replica_type = ReplicaType::ALL);

    /**
     * @brief Start an upsert operation. If the key does not exist, behaves
     * like PutStart. If the key exists with the same size, performs in-place
     * update (reuses existing buffers). If the key exists with a different
     * size, deletes old replicas and allocates new ones.
     * @return Replica descriptors on success, or error code on failure.
     * Possible errors: OBJECT_HAS_REPLICATION_TASK (Copy/Move/Offload in
     * progress), OBJECT_REPLICA_BUSY (replicas have non-zero refcnt).
     */
    auto UpsertStart(const UUID& client_id, const std::string& key,
                     const TenantId& tenant_id, const uint64_t slice_length,
                     const ReplicateConfig& config)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    /**
     * @brief Complete an upsert operation. Delegates to PutEnd.
     */
    auto UpsertEnd(const UUID& client_id, const ObjectMeta& object_meta,
                   const TenantId& tenant_id, ReplicaType replica_type,
                   const std::string& operation_id = "")
        -> tl::expected<void, ErrorCode>;

    auto UpsertEnd(const UUID& client_id, const std::string& key,
                   const TenantId& tenant_id, ReplicaType replica_type,
                   const std::string& operation_id = "")
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Revoke an upsert operation. Delegates to PutRevoke.
     */
    auto UpsertRevoke(const UUID& client_id, const std::string& key,
                      const TenantId& tenant_id, ReplicaType replica_type,
                      const std::string& operation_id = "")
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Start a batch of upsert operations.
     */
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchUpsertStart(const UUID& client_id,
                     const std::vector<std::string>& keys,
                     const TenantId& tenant_id,
                     const std::vector<uint64_t>& slice_lengths,
                     const ReplicateConfig& config);

    /**
     * @brief Complete a batch of upsert operations. Delegates to BatchPutEnd.
     */
    std::vector<tl::expected<void, ErrorCode>> BatchUpsertEnd(
        const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
        const TenantId& tenant_id);

    /**
     * @brief Revoke a batch of upsert operations. Delegates to BatchPutRevoke.
     */
    std::vector<tl::expected<void, ErrorCode>> BatchUpsertRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        const TenantId& tenant_id);

    /**
     * @brief Evict a disk replica for a key (triggered by client-side disk
     * eviction).
     * @param client_id The client performing the eviction
     * @param key The object key whose disk replica was evicted
     * @param replica_type DISK or LOCAL_DISK
     * @return ErrorCode::OK on success, OBJECT_NOT_FOUND if key missing
     */
    auto EvictDiskReplica(const UUID& client_id, const std::string& key,
                          const TenantId& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Batch evict disk replicas for multiple keys.
     * @param client_id The client performing the eviction
     * @param keys The object keys whose disk replicas were evicted
     * @param replica_type DISK or LOCAL_DISK
     * @return Per-key results (OK or error code)
     */
    std::vector<tl::expected<void, ErrorCode>> BatchEvictDiskReplica(
        const UUID& client_id, const std::vector<std::string>& keys,
        const TenantId& tenant_id, ReplicaType replica_type);

    /**
     * @brief Start a copy operation
     *
     * This will allocate replica buffers to copy to.
     *
     * @param client_id the client that submit the CopyStart request
     * @param key key of the object
     * @param src_segment source segment name of the replica to copy from
     * @param tgt_segments target segment names of the replicas to copy to
     *
     * @return allocated replicas on success, or ErrorCode indicating the
     * failure reason
     */
    tl::expected<CopyStartResponse, ErrorCode> CopyStart(
        const UUID& client_id, const std::string& key,
        const TenantId& tenant_id, const std::string& src_segment,
        const std::vector<std::string>& tgt_segments);

    tl::expected<void, ErrorCode> CopyEnd(const UUID& client_id,
                                          const std::string& key,
                                          const TenantId& tenant_id);

    tl::expected<void, ErrorCode> CopyRevoke(const UUID& client_id,
                                             const std::string& key,
                                             const TenantId& tenant_id);

    /**
     * @brief Start a move operation
     *
     * This will allocate replica buffer to move to
     *
     * @param client_id the client that submit the MoveStart request
     * @param key key of the object
     * @param src_segment source segment name of the replica to move from
     * @param tgt_segment target segment name of the replica to move to
     *
     * @return allocated replica on success, or ErrorCode indicating the
     * failure reason
     */
    tl::expected<MoveStartResponse, ErrorCode> MoveStart(
        const UUID& client_id, const std::string& key,
        const TenantId& tenant_id, const std::string& src_segment,
        const std::string& tgt_segment);

    tl::expected<void, ErrorCode> MoveEnd(const UUID& client_id,
                                          const std::string& key,
                                          const TenantId& tenant_id);

    tl::expected<void, ErrorCode> MoveRevoke(const UUID& client_id,
                                             const std::string& key,
                                             const TenantId& tenant_id);

    /**
     * @brief Remove an object and its replicas
     * @param key The key to remove.
     * @param force If true, skip lease and replication task checks.
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found
     */
    auto Remove(const std::string& key, const TenantId& tenant_id,
                bool force = false) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Removes objects from the master whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @param force If true, skip lease and replication task checks.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    auto RemoveByRegex(const std::string& str, const TenantId& tenant_id,
                       bool force = false) -> tl::expected<long, ErrorCode>;

    /**
     * @brief Remove all objects and their replicas across all tenants.
     * @param force If true, skip lease and replication task checks.
     * @return return the number of objects removed
     */
    long RemoveAll(bool force = false);

    /**
     * @brief Remove all objects and their replicas for a single tenant.
     * @param tenant_id The tenant whose objects should be removed.
     * @param force If true, skip lease and replication task checks.
     * @return return the number of objects removed
     */
    long RemoveAll(const TenantId& tenant_id, bool force = false);

    /**
     * @brief Batch remove objects and their replicas
     * @param keys The list of keys to remove.
     * @param force If true, skip lease and replication task checks.
     * @return Vector of expected results for each key.
     */
    auto BatchRemove(const std::vector<std::string>& keys,
                     const TenantId& tenant_id, bool force = false)
        -> std::vector<tl::expected<void, ErrorCode>>;

    /**
     * @brief Get the count of keys
     * @return The count of keys
     */
    size_t GetKeyCount() const;

    /**
     * @brief Heartbeat from client
     * @param client_id The uuid of the client
     * @return PingResponse containing view version and client status
     * @return ErrorCode::OK on success, ErrorCode::INTERNAL_ERROR if the client
     *         ping queue is full
     */
    auto Ping(const UUID& client_id) -> tl::expected<PingResponse, ErrorCode>;

    /**
     * @brief Get the master service cluster ID to use as subdirectory name
     * @return ErrorCode::OK on success, ErrorCode::INTERNAL_ERROR if cluster ID
     * is not set
     */
    tl::expected<std::string, ErrorCode> GetFsdir() const;

    /**
     * @brief Get storage backend configuration including eviction settings
     * @return GetStorageConfigResponse containing fsdir, enable_disk_eviction,
     * and quota_bytes
     */
    tl::expected<GetStorageConfigResponse, ErrorCode> GetStorageConfig() const;

    /**
     * @brief Mounts a file storage segment into the master.
     * @param enable_offloading If true, enables offloading (write-to-file).
     */
    auto MountLocalDiskSegment(const UUID& client_id, bool enable_offloading)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Unmounts a worker's LOCAL_DISK segment from this master.
     *
     * 清理该 client 的 LocalDiskSegment（ssd 容量记账、offloading 队列），
     * 与 MountLocalDiskSegment 对称。多 submaster 下由 worker 在 submaster
     * 退出集群时定向调用（RefreshSubmasterAddresses removed 分支），保证
     * 容量记账对称闭合；不清理对象元数据（LOCAL_DISK 副本由 TTL reaper
     * 与 ScanMeta 恢复路径管理）。幂等：client 无段时返回 OK。
     */
    auto UnmountLocalDiskSegment(const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief 被动坏端点上报（方案二被动层）。
     *
     * client 传输失败后上报目标 transport_endpoint，master 将其写入
     * invalid_replica_endpoints_（transport_endpoint + 对应 segment_name
     * 双形式），供分配过滤（AllocateAndInsertMetadata 的排除集）与读路径
     * （IsReplicaReadable）跳过。自愈闭环：worker 重启后 MountSegment
     * 成功即 erase，端点恢复可用。幂等；批量写与单笔写共用。
     */
    auto ReportInvalidReplicaEndpoints(const std::vector<std::string>& endpoints)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Heartbeat call to collect object-level statistics and retrieve the
     * set of non-offloaded objects.
     * @param enable_offloading Indicates whether offloading is enabled for this
     * segment.
     */
    auto OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading)
        -> tl::expected<std::vector<OffloadTaskItem>, ErrorCode>;

    /**
     * @brief Client polls whether master has requested a full SSD clear
     * (triggered by RemoveAll). Atomically checks and clears the flag.
     * @param client_id The client polling for the remove-all signal
     * @return true if client should clear all SSD files, false otherwise
     */
    auto PollRemoveAll(const UUID& client_id) -> tl::expected<bool, ErrorCode>;

    auto ReportSsdCapacity(const UUID& client_id,
                           int64_t ssd_total_capacity_bytes)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Notifies the master that offloading of specified objects has
     * succeeded.
     * @param tasks        A list of tenant-scoped objects that were
     * successfully offloaded.
     * @param metadatas    The corresponding metadata for each offloaded object,
     * including size, storage location, etc.
     */
    auto NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Returns unique RPC endpoints that own completed LOCAL_DISK
     * replicas. Used by clients to warm up offload RPC pools before traffic.
     */
    auto GetOffloadEndpoints()
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Heartbeat-driven pull of pending promotion work for a client.
     * Returns tenant-scoped promotion tasks for the holder client and clears
     * its per-client promotion_objects queue. The per-shard promotion_tasks
     * map remains populated as the source of truth until NotifyPromotionSuccess
     * commits the new MEMORY replica.
     */
    auto PromotionObjectHeartbeat(const UUID& client_id)
        -> tl::expected<std::vector<PromotionTaskItem>, ErrorCode>;

    /** Fetch pending remove tasks without removing them from the queue. */
    auto RemoveObjectHeartbeat(const UUID& client_id)
        -> tl::expected<std::vector<RemoveTaskItem>, ErrorCode>;
    auto AckRemoveObjectHeartbeat(
        const UUID& client_id, const std::vector<RemoveTaskItem>& tasks)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Stage a PROCESSING MEMORY replica for an existing key. Allocates
     * DRAM via the existing AllocationStrategy, optionally biased toward the
     * caller's local memory segment via preferred_segments. The new replica is
     * invisible to readers until NotifyPromotionSuccess flips it to COMPLETE.
     *
     * Only the holder client (the one owning the source LOCAL_DISK replica)
     * is authorized to call this. Other clients receive INVALID_PARAMS.
     * `size` must match the source replica's object_size captured at task
     * admission; mismatch returns INVALID_PARAMS to avoid allocating an
     * arbitrary buffer size from a buggy or malicious caller.
     */
    auto PromotionAllocStart(const UUID& client_id, const std::string& key,
                             const TenantId& tenant_id, uint64_t size,
                             const std::vector<std::string>& preferred_segments)
        -> tl::expected<PromotionAllocStartResponse, ErrorCode>;

    /**
     * @brief Commit a staged MEMORY replica to COMPLETE; decrement source
     * refcnt; erase per-shard and per-client task entries. Mirror of
     * NotifyOffloadSuccess.
     */
    auto NotifyPromotionSuccess(const UUID& client_id, const std::string& key,
                                const TenantId& tenant_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Holder-side failure notification: the client got past
     * PromotionAllocStart but a downstream step (local SSD read, RDMA
     * write, etc.) failed and it will not be calling
     * NotifyPromotionSuccess. Releases the master-side task state
     * immediately rather than waiting put_start_release_timeout_sec_
     * for the reaper to do it. Without this call every transient
     * client-side error (SSD throttling, RDMA flake, etc.) pins a
     * task slot and a staged DRAM buffer for the full reaper TTL,
     * which can saturate promotion_queue_limit_ on busy clusters.
     *
     * Authorization is the same as NotifyPromotionSuccess: only the
     * holder client may release a task. Effects mirror the reaper's
     * expiry path: drop source LOCAL_DISK refcnt, pop the staged
     * PROCESSING MEMORY replica if alloc_id was recorded, erase the
     * task, decrement the global in-flight counter, and clear the
     * holder's promotion_objects entry.
     */
    auto NotifyPromotionFailure(const UUID& client_id, const std::string& key,
                                const TenantId& tenant_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Create a copy task to copy an object's replicas to target segments
     * @return Copy task ID on success, ErrorCode on failure
     */
    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const TenantId& tenant_id,
        const std::vector<std::string>& targets);

    /**
     * @brief Create a move task to move an object's replica from source segment
     * to target segment
     * @return Move task ID on success, ErrorCode on failure
     */
    tl::expected<UUID, ErrorCode> CreateMoveTask(const std::string& key,
                                                 const TenantId& tenant_id,
                                                 const std::string& source,
                                                 const std::string& target);

    /**
     * @brief Create a drain job to gracefully evacuate one or more segments.
     */
    tl::expected<UUID, ErrorCode> CreateDrainJob(
        const CreateDrainJobRequest& request);

    /**
     * @brief Query the status of a drain job.
     */
    tl::expected<QueryJobResponse, ErrorCode> QueryDrainJob(const UUID& job_id);

    /**
     * @brief Cancel an in-flight drain job and restore draining segments to OK.
     */
    tl::expected<void, ErrorCode> CancelDrainJob(const UUID& job_id);

    /**
     * @brief Query current segment lifecycle state by segment name.
     */
    tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatus(
        const std::string& segment_name);

    /**
     * @brief Query current segment lifecycle state by segment id.
     */
    tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatusById(
        const UUID& segment_id);

    /**
     * @brief Restore primary state from standby promotion context.
     * Called once at promotion time before serving requests.
     */
    void RestoreFromStandbySnapshot(
        const std::vector<StandbyObjectEntry>& objects,
        uint64_t initial_oplog_sequence_id,
        const std::vector<StandbySegmentInfo>& segments,
        const std::vector<vsegment::PartitionVSegmentSnapshot>&
            vsegment_partitions = {});

    /**
     * @name OpLog 流自恢复（冷接管 / 同身份重启）
     *
     * 复用 standby 侧原语（StandbyMetadataStore + OpLogApplier +
     * OpLogBatchStandbyReader）按 source_id 重放一条 OpLog 流并导出
     * 恢复态，供 RestoreFromStandbySnapshot 物化（P4 slot 过滤自动
     * 只落本机 owned slots）。
     *
     * 适用：流主已消亡（lease 过期冷接管）或本机重启（serve 前无写入）。
     * 约束：受 etcd compaction 窗口限制（默认 5m），越窗重放失败回退
     * 旧语义（判空、客户端重建）。
     */
    ///@{
    ErrorCode RecoverStateFromOpLogStream(
        const std::string& source_id, bool& stream_present,
        std::vector<StandbyObjectEntry>& objects,
        std::vector<StandbySegmentInfo>& segments,
        std::vector<vsegment::PartitionVSegmentSnapshot>& vsegment_partitions);

    // 同身份重启失忆自愈：无 PromotionContext 时从本机流自恢复。
    // serve 启动时调用一次；成功后登记 source 防止 ImportSlotMetadata
    // 路径重复重放。
    bool TrySelfRecoverFromOpLog();

    // 死 owner（或自身旧世代）冷接管重放：ImportSlotMetadata 判空分支
    // 前调用；每个 source 定局后仅尝试一次，瞬态失败可下轮重试。
    bool TryRecoverFromDeadOwnerOpLog(const std::string& source_id);

    // 环互备领养物化（互备 1.4）：supervisor RingStandbyRuntime 在
    // ring-adopt staging（kMigrating 写屏障）期间的编排入口。与
    // TryRecoverFromDeadOwnerOpLog 同通道（重放 source_id 流 + P4
    // owned-slot 过滤 + 定局登记 + RestoreFromStandbySnapshot 物化，
    // 幂等：已定局短路返回 OK），但返回 ErrorCode 三态供编排方决策：
    //   OK        —— 已物化或已定局，可 CAS kMigrating→kStable 收尾；
    //   INCOMPLETE_OPLOG_CATCH_UP —— 30s 重放预算耗尽（etcd 抖动），
    //                瞬态：不收尾，写屏障保持，下轮重试；
    //   其余 FATAL —— 流不可恢复（越窗 / durable prefix 缺失等）或
    //                OpLog 未启用（UNAVAILABLE_IN_CURRENT_STATUS）：
    //                按既有冷接管回退语义（判空、客户端冷重建）可收尾，
    //                调用方须告警。stream_present 仅诊断（false=流不存在）。
    ErrorCode AdoptMaterializeFromDeadOwner(const std::string& source_id,
                                            bool& stream_present);
    ///@}

    /**
     * @brief Query the status of a task
     * @return Task basic info
     */
    tl::expected<QueryTaskResponse, ErrorCode> QueryTask(const UUID& task_id);

    /**
     * @brief fetch tasks assigned to a client
     * @return list of tasks
     */
    tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        const UUID& client_id, size_t batch_size);

    /**
     * @brief Mark the task as complete
     * @param client_id Client ID
     * @param request Task complete request
     * @return ErrorCode::OK on success, ErrorCode on failure
     */
    tl::expected<void, ErrorCode> MarkTaskToComplete(
        const UUID& client_id, const TaskCompleteRequest& request);

    /**
     * @brief Set the HttpMetadataServer pointer for cleanup on client timeout.
     * @param server Pointer to HttpMetadataServer. If nullptr, cleanup is
     * disabled.
     */
    void setHttpMetadataServer(HttpMetadataServer* server);

    /**
     * @brief Configure cleanup against a separately-deployed HTTP metadata
     * server (not co-located in the master process). The master sends HTTP
     * DELETE requests to this endpoint when a client times out. Only http://
     * and https:// connection strings are supported; other schemes (etcd /
     * redis / P2PHANDSHAKE) are ignored with a warning and leave cleanup
     * disabled.
     * @param metadata_connstring e.g. "http://host:8080/metadata".
     */
    void setHttpMetadataRemoteUrl(const std::string& metadata_connstring);

    /**
     * @brief Policy-driven tier-down dispatch counters. Independent of the
     * eviction counters because a demotion copies an object down and keeps its
     * MEMORY replica: it frees no bytes, so folding it into freed memory would
     * make a tier-down plan look like a plan that under-delivered.
     */
    uint64_t tier_down_attempt_count() const {
        return tier_down_attempts_.load(std::memory_order_relaxed);
    }
    uint64_t tier_down_success_count() const {
        return tier_down_successes_.load(std::memory_order_relaxed);
    }
    uint64_t tier_down_failure_count() const {
        return tier_down_failures_.load(std::memory_order_relaxed);
    }
    /// Candidates skipped because their disk copy is still in flight: a
    /// non-zero, persistent value means the tier-down driver is waiting on the
    /// client's offload queue rather than making progress.
    uint64_t tier_down_skipped_in_flight_count() const {
        return tier_down_skipped_in_flight_.load(std::memory_order_relaxed);
    }
    /// Candidates skipped because they already hold a LOCAL_DISK replica. A
    /// persistent value means selection is re-picking keys that are already
    /// paved down (each cycle's budget is being spent on nothing).
    uint64_t tier_down_skipped_paved_count() const {
        return tier_down_skipped_paved_.load(std::memory_order_relaxed);
    }

   private:
    std::unique_ptr<ha::SnapshotCatalogStore> CreateSnapshotCatalogStore();

    // Restore master state
    void RestoreState();
    void ResetStateAfterFailedRestoreAttempt();

    /**
     * @brief Apply decoded snapshot state to running master service
     * @param payloads Decoded snapshot payloads
     * @param now Current time for cleanup logic
     * @return void on success, SerializationError on failure
     */
    tl::expected<void, SerializationError> ApplySnapshotState(
        const std::chrono::system_clock::time_point& now);

    // BatchEvict evicts objects in a near-LRU way, i.e., prioritizes to evict
    // object with smaller lease timeout. It has two passes. The first pass only
    // evicts objects without soft pin. The second pass prioritizes objects
    // without soft pin, but also allows to evict soft pinned objects if
    // allow_evict_soft_pinned_objects_ is true. The first pass tries fulfill
    // evict ratio target. If the actual evicted ratio is less than
    // evict_ratio_lowerbound, the second pass will be triggered and try to
    // fulfill evict ratio lowerbound.
    void BatchEvict(double evict_ratio_target, double evict_ratio_lowerbound);
    void NoFBatchEvict(double evict_ratio_target,
                       double evict_ratio_lowerbound);
    struct TenantQuotaEvictionResult {
        uint64_t freed_bytes{0};
        uint64_t evicted_objects{0};
    };
    TenantQuotaEvictionResult EvictTenantMemoryForQuota(
        const TenantId& tenant_id, uint64_t target_bytes,
        const std::unordered_set<std::string>* candidate_keys = nullptr);

    // After an eviction pass, reports a kRemoved tier event for every candidate
    // key whose MEMORY replica is actually gone, so the IO Pattern collector
    // stops claiming an L1 replica for it. Presence is read from authoritative
    // metadata rather than inferred from access recency: an evicted key must
    // become promotable again, while a cold key that has simply not been read
    // must stay eligible for eviction.
    void ReportEvictedKeysAsTierRemovals(
        const TenantId& tenant,
        const std::unordered_set<std::string>& candidate_keys);

    // Runs the legacy lease-ordered eviction for an IO Pattern plan that could
    // not free its byte target (stale candidates, active leases, pins, or an
    // empty candidate set), so a watermark request still makes progress.
    // Returns true when the fallback ran.
    bool RunLegacyEvictionFallback(uint64_t shortfall_bytes);

    // Helper to get a snapshot of alive clients (under client_mutex_ shared
    // lock)
    std::unordered_set<UUID, boost::hash<UUID>> getAliveClientsSnapshot() const;
    void UpdateClientHostId(const UUID& client_id, const std::string& host_id);
    std::string GetClientHostId(const UUID& client_id) const;

    // Clear invalid handles in all shards
    void ClearInvalidHandles();
    void ClearInvalidHandles(
        const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients);

    std::string FormatTimestamp(
        const std::chrono::system_clock::time_point& tp);
    // We need to clean up finished tasks periodically to avoid memory leak
    // And also we can add some task ttl mechanism in the future
    void TaskCleanupThreadFunc();
    void JobDispatchThreadFunc();

    // Internal data structures
    struct ObjectIdentity {
        TenantId tenant_id;
        std::string user_key;
    };

    struct ObjectMetadata {
        // RAII-style metric management
        ~ObjectMetadata() {
            MasterMetricManager::instance().dec_key_count(1);
            if (soft_pin_timeout) {
                MasterMetricManager::instance().dec_soft_pin_key_count(1);
            }
        }

        ObjectMetadata() = delete;

        ObjectMetadata(
            const UUID& client_id_,
            const std::chrono::system_clock::time_point put_start_time_,
            size_t value_length, std::vector<Replica>&& reps,
            bool enable_soft_pin, bool enable_hard_pin = false,
            ObjectDataType data_type_ = ObjectDataType::UNKNOWN,
            std::string group_id_ = "", TenantId tenant_id_ = TenantId(),
            std::string user_key_ = {})
            : client_id(client_id_),
              put_start_time(put_start_time_),
              size(value_length),
              data_type(data_type_),
              group_id(std::move(group_id_)),
              tenant_id(std::move(tenant_id_)),
              user_key(std::move(user_key_)),
              lease_timeout(),
              soft_pin_timeout(std::nullopt),
              hard_pinned(enable_hard_pin),
              replicas_(std::move(reps)) {
            MasterMetricManager::instance().inc_key_count(1);
            if (enable_soft_pin) {
                soft_pin_timeout.emplace();
                MasterMetricManager::instance().inc_soft_pin_key_count(1);
            }
            MasterMetricManager::instance().observe_value_size(value_length);
        }

        ObjectMetadata(const ObjectMetadata&) = delete;
        ObjectMetadata& operator=(const ObjectMetadata&) = delete;
        ObjectMetadata(ObjectMetadata&&) = delete;
        ObjectMetadata& operator=(ObjectMetadata&&) = delete;

        // Updated by UpsertStart (Case B) to reflect the new writer.
        UUID client_id;
        // Updated by UpsertStart (Case B) to reset the discard timeout.
        std::chrono::system_clock::time_point put_start_time;
        const size_t size;
        std::optional<uint64_t> object_checksum;
        const ObjectDataType data_type{ObjectDataType::UNKNOWN};
        const std::string group_id;
        const TenantId tenant_id;
        const std::string user_key;

        mutable SpinLock lock;
        // Default constructor, creates a time_point representing
        // the Clock's epoch (i.e., time_since_epoch() is zero).
        mutable std::chrono::system_clock::time_point lease_timeout
            GUARDED_BY(lock);  // hard lease
        mutable std::optional<std::chrono::system_clock::time_point>
            soft_pin_timeout GUARDED_BY(lock);  // optional soft pin, only
                                                // set for vip objects
        const bool hard_pinned{false};          // immutable, set at creation
        bool memory_cache_total_accounted{false};
        bool disk_cache_total_accounted{false};
        uint64_t reserved_quota_charge_bytes{0};
        uint64_t committed_quota_charge_bytes{0};
        uint64_t pending_replaced_quota_charge_bytes{0};

        void AddReplicas(std::vector<Replica>&& replicas) {
            replicas_.insert(replicas_.end(),
                             std::move_iterator(replicas.begin()),
                             std::move_iterator(replicas.end()));
        }

        std::vector<Replica> PopReplicas(
            const std::function<bool(const Replica&)>& pred_fn) {
            auto partition_point =
                std::partition(replicas_.begin(), replicas_.end(),
                               [pred_fn](const Replica& replica) {
                                   return !pred_fn(replica);
                               });

            std::vector<Replica> popped_replicas;
            if (partition_point != replicas_.end()) {
                popped_replicas.reserve(
                    std::distance(partition_point, replicas_.end()));
                std::move(partition_point, replicas_.end(),
                          std::back_inserter(popped_replicas));
                replicas_.erase(partition_point, replicas_.end());
            }

            return popped_replicas;
        }

        std::vector<Replica> PopReplicas() { return std::move(replicas_); }

        size_t EraseReplicas(
            const std::function<bool(const Replica&)>& pred_fn) {
            auto erased_replicas = PopReplicas(pred_fn);
            return erased_replicas.size();
        }

        size_t EraseReplicas() {
            auto erased_replicas = PopReplicas();
            return erased_replicas.size();
        }

        size_t VisitReplicas(const std::function<bool(const Replica&)>& pred_fn,
                             const std::function<void(Replica&)>& visit_fn) {
            size_t num_visited = 0;

            for (auto& replica : replicas_) {
                if (pred_fn(replica)) {
                    visit_fn(replica);
                    num_visited++;
                }
            }

            return num_visited;
        }

        size_t VisitReplicas(
            const std::function<bool(const Replica&)>& pred_fn,
            const std::function<void(const Replica&)>& visit_fn) const {
            size_t num_visited = 0;

            for (auto& replica : replicas_) {
                if (pred_fn(replica)) {
                    visit_fn(replica);
                    num_visited++;
                }
            }

            return num_visited;
        }

        bool HasReplica(
            const std::function<bool(const Replica&)>& pred_fn) const {
            return std::any_of(replicas_.begin(), replicas_.end(), pred_fn);
        }

        bool AllReplicas(
            const std::function<bool(const Replica&)>& pred_fn) const {
            return std::all_of(replicas_.begin(), replicas_.end(), pred_fn);
        }

        size_t CountReplicas(
            const std::function<bool(const Replica&)>& pred_fn) const {
            return std::count_if(replicas_.begin(), replicas_.end(), pred_fn);
        }

        size_t CountReplicas() const { return replicas_.size(); }

        const std::vector<Replica>& GetAllReplicas() const { return replicas_; }

        std::optional<ReplicaStatus> HasDiffRepStatus(
            ReplicaStatus status) const {
            for (const auto& replica : replicas_) {
                if (replica.status() != status) {
                    return replica.status();
                }
            }
            return {};
        }

        Replica* GetFirstReplica(
            const std::function<bool(const Replica&)>& pred_fn) {
            const auto it =
                std::find_if(replicas_.begin(), replicas_.end(), pred_fn);
            return it != replicas_.end() ? &(*it) : nullptr;
        }

        Replica* GetReplicaByID(const ReplicaID& id) {
            return GetFirstReplica(
                [&id](const Replica& replica) { return replica.id() == id; });
        }

        bool EraseReplicaByID(const ReplicaID& id) {
            auto num_erased = EraseReplicas(
                [&id](const Replica& replica) { return replica.id() == id; });
            return num_erased > 0;
        }

        size_t EraseReplica(ReplicaType replica_type) {
            return EraseReplicas([replica_type](const Replica& replica) {
                if (replica_type == ReplicaType::ALL) {
                    return replica.is_memory_replica() ||
                           replica.is_nof_replica();
                }
                return replica.type() == replica_type;
            });
        }

        bool HasMemReplica() const {
            return HasReplica(&Replica::fn_is_memory_replica);
        }

        bool HasVSegmentReplica() const {
            return HasReplica(&Replica::fn_is_vsegment_replica);
        }

        bool HasNoFReplica() const {
            return HasReplica(&Replica::fn_is_nof_replica);
        }

        size_t GetMemReplicaCount() const {
            return CountReplicas(&Replica::fn_is_memory_replica);
        }

        size_t GetNoFReplicaCount() const {
            return CountReplicas(&Replica::fn_is_nof_replica);
        }

        Replica* GetReplicaBySegmentName(const std::string& segment_name) {
            return GetFirstReplica([&segment_name](const Replica& replica) {
                if (replica.is_vsegment_replica()) {
                    return replica.get_vsegment_descriptor().vsegment_id ==
                           segment_name;
                }
                auto names = replica.get_segment_names();
                for (auto& name_opt : names) {
                    if (name_opt == segment_name) {
                        return true;
                    }
                }
                return false;
            });
        }

        // Grant a lease with timeout as now() + ttl, only update if the new
        // timeout is larger
        void GrantLease(const uint64_t ttl, const uint64_t soft_ttl) const {
            SpinLocker locker(&lock);
            std::chrono::system_clock::time_point now =
                std::chrono::system_clock::now();
            lease_timeout =
                std::max(lease_timeout, now + std::chrono::milliseconds(ttl));
            if (soft_pin_timeout) {
                soft_pin_timeout =
                    std::max(*soft_pin_timeout,
                             now + std::chrono::milliseconds(soft_ttl));
            }
        }

        bool NeedsLeaseRefresh(const uint64_t ttl,
                               const uint64_t soft_ttl) const {
            SpinLocker locker(&lock);
            const auto now = std::chrono::system_clock::now();
            if (lease_timeout <= now + std::chrono::milliseconds(ttl / 2)) {
                return true;
            }
            return soft_pin_timeout &&
                   *soft_pin_timeout <=
                       now + std::chrono::milliseconds(soft_ttl / 2);
        }

        // Check if the lease has expired
        bool IsLeaseExpired() const {
            SpinLocker locker(&lock);
            return std::chrono::system_clock::now() >= lease_timeout;
        }

        // Check if the lease has expired
        bool IsLeaseExpired(std::chrono::system_clock::time_point& now) const {
            SpinLocker locker(&lock);
            return now >= lease_timeout;
        }

        // Check if is in soft pin status
        bool IsSoftPinned() const {
            SpinLocker locker(&lock);
            return soft_pin_timeout &&
                   std::chrono::system_clock::now() < *soft_pin_timeout;
        }

        // Check if is in soft pin status
        bool IsSoftPinned(std::chrono::system_clock::time_point& now) const {
            SpinLocker locker(&lock);
            return soft_pin_timeout && now < *soft_pin_timeout;
        }

        bool IsHardPinned() const { return hard_pinned; }

        bool IsGrouped() const { return !group_id.empty(); }

        // Check if the metadata is valid
        // Valid means it has at least one valid replica and size is greater
        // than 0
        bool IsValid() const {
            return size > 0 && HasReplica([](const Replica& replica) {
                       return !replica.is_memory_replica() ||
                              !replica.has_invalid_mem_handle();
                   });
        }

        std::vector<std::string> GetReplicaSegmentNames() const {
            std::vector<std::string> segment_names;
            for (const auto& replica : replicas_) {
                const auto& segment_name_options = replica.get_segment_names();
                for (const auto& segment_name_opt : segment_name_options) {
                    if (segment_name_opt.has_value()) {
                        segment_names.push_back(segment_name_opt.value());
                    }
                }
            }
            return segment_names;
        }

       private:
        // Use the accessors to visit and modify the replicas.
        std::vector<Replica> replicas_;
    };

    struct ReplicationTask {
        UUID client_id;
        std::chrono::system_clock::time_point start_time;
        enum class Type {
            COPY,
            MOVE,
        } type;
        ReplicaID source_id;
        std::vector<ReplicaID> replica_ids;
        uint64_t reserved_quota_charge_bytes{0};
    };

    struct OffloadingTask {
        ReplicaID source_id;
        std::chrono::system_clock::time_point start_time;
        // Client whose LOCAL_DISK segment owns the queued offload task.
        // A key may have one task per offloading node (one per MEMORY
        // replica), so tasks are matched by (key, source_client_id) when
        // workers report completion or failure.
        UUID source_client_id;
    };

    // Tracks an in-flight LOCAL_DISK -> MEMORY copy. The source
    // LOCAL_DISK replica is refcnt-pinned for the duration of the task
    // so it cannot be evicted.
    //
    // alloc_id pins down which staged PROCESSING MEMORY replica
    enum class PromotionQueueResult {
        kQueued,
        kDisabled,
        kFrequencyRejected,
        kWatermarkRejected,
        kQueueCapRejected,
        kAlreadyInFlight,
        kMemoryReplicaPresent,
        kNoLocalDiskSource,
        kNotFound,
        kPushFailed,
    };

    enum class PromotionCandidateReason {
        kWatermark,
        kQueueCap,
        kPushFailed,
    };

    struct PromotionCandidate {
        uint8_t sketch_score{0};
        std::chrono::steady_clock::time_point first_seen;
        std::chrono::steady_clock::time_point last_seen;
        std::chrono::steady_clock::time_point retry_after;
        PromotionCandidateReason last_reason{
            PromotionCandidateReason::kQueueCap};
        ErrorCode last_error{ErrorCode::OK};
        uint32_t retry_count{0};
    };

    // NotifyPromotionSuccess should commit, so a concurrent Put on the
    // same key cannot be confused with ours. 0 until
    // PromotionAllocStart records the new replica.
    //
    // start_time is the reaper deadline anchor. Set at task admission
    // and reset at PromotionAllocStart so each phase (queue-wait and
    // active-transfer) gets its own full put_start_release_timeout_sec_
    // window. Without the reset a backlogged task could enter active
    // transfer with little TTL left, and the reaper could free the
    // staged replica via EraseReplicaByID mid-RDMA-write.
    //
    // holder_id is the client owning the source LOCAL_DISK segment and
    // the only one authorized to commit (NotifyPromotionSuccess) or
    // abort (NotifyPromotionFailure) the task. Without it, any client
    // knowing the key could flip the staged PROCESSING replica to
    // COMPLETE before the holder's RDMA write landed, exposing torn
    // data to readers.
    struct PromotionTask {
        ReplicaID source_id;    // the LOCAL_DISK replica being promoted
        ReplicaID alloc_id{0};  // the new MEMORY replica staged by AllocStart
        uint64_t object_size;
        uint64_t reserved_quota_charge_bytes{0};
        std::chrono::system_clock::time_point start_time;
        UUID holder_id;  // owner of source LOCAL_DISK; only Notifier allowed
    };

    static constexpr size_t kNumShards = 1024;  // Number of metadata shards

    struct TenantState {
        std::unordered_map<std::string, ObjectMetadata> metadata;
        std::unordered_set<std::string> processing_keys;
        std::unordered_map<std::string, const ReplicationTask>
            replication_tasks;
        std::unordered_map<std::string, std::vector<OffloadingTask>>
            offloading_tasks;
        std::unordered_map<std::string, PromotionTask> promotion_tasks;
        std::unordered_map<std::string, PromotionCandidate>
            promotion_candidates;

        std::unordered_map<std::string, std::unordered_set<std::string>>
            group_members;  // group_id → set of keys

        bool Empty() const {
            return metadata.empty() && processing_keys.empty() &&
                   replication_tasks.empty() && offloading_tasks.empty() &&
                   promotion_tasks.empty() && promotion_candidates.empty() &&
                   group_members.empty();
        }
    };

    // Sharded metadata maps and their mutexes
    struct MetadataShard {
        mutable SharedMutex mutex;
        std::unordered_map<TenantId, TenantState, TenantIdHash> tenants
            GUARDED_BY(mutex);
        // Count of objects that have at least one completed LOCAL_DISK replica.
        // Used to compute eviction_base = metadata.size() - disk_object_count,
        // excluding disk-only objects from the eviction denominator.
        long disk_object_count GUARDED_BY(mutex) = 0;
    };
    std::array<MetadataShard, kNumShards> metadata_shards_;

    static bool HasCompletedMemoryCacheReplica(const ObjectMetadata& metadata);
    static bool HasCompletedDiskCacheReplica(const ObjectMetadata& metadata);
    static void SyncCacheTotalAccounting(ObjectMetadata& metadata);
    void RebuildCacheTotalAccounting();
    static void AccountCacheTotalRemoval(ObjectMetadata& metadata);
    std::vector<Replica> PopReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& pred_fn);
    std::vector<Replica> PopReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata);
    void ReleaseVSegmentReplicaState(
        const ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& pred_fn);
    size_t EraseReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& pred_fn);

    std::unordered_map<std::string, std::string> object_group_ids_
        GUARDED_BY(group_routing_mutex_);
    mutable std::unordered_set<std::string> groups_needing_lease_refresh_
        GUARDED_BY(group_routing_mutex_);
    mutable std::shared_mutex group_routing_mutex_;

    static constexpr size_t kObjectOperationLockStripes = 4096;

    struct ObjectOperationLock {
        std::unique_lock<std::mutex> lock;
    };

    ObjectOperationLock AcquireObjectOperationLock(const TenantId& tenant_id,
                                                   const std::string& key);

    std::array<std::mutex, kObjectOperationLockStripes> object_operation_locks_;

    // For accessing a metadata shard with read-write permission
    class MetadataShardAccessorRW {
       public:
        MetadataShardAccessorRW(MasterService* master_service,
                                size_t shard_index)
            : shard_(master_service->metadata_shards_[shard_index]),
              lock_(&shard_.mutex) {}

        MetadataShard* operator->() { return &shard_; }

        const MetadataShard* operator->() const { return &shard_; }

        MetadataShard& get() { return shard_; }

        const MetadataShard& get() const { return shard_; }

        // Called after adding a LOCAL_DISK replica. Increments
        // disk_object_count if this is the first completed LOCAL_DISK
        // replica for the object (i.e., exactly 1 completed disk replica now).
        void OnDiskReplicaAdded(const ObjectMetadata& metadata) {
            size_t disk_count = metadata.CountReplicas([](const Replica& r) {
                return r.is_local_disk_replica() && r.is_completed();
            });
            if (disk_count == 1) shard_.disk_object_count++;
        }

        // Called after removing a LOCAL_DISK replica, or when erasing an
        // object that had one. Pass had_completed_disk=true if the object
        // had at least one completed LOCAL_DISK replica before the removal.
        // When the entire object is being erased, call the one-arg overload.
        void OnDiskReplicaRemoved(bool had_completed_disk,
                                  const ObjectMetadata& metadata) {
            if (!had_completed_disk) return;
            bool still_has_disk = metadata.HasReplica([](const Replica& r) {
                return r.is_local_disk_replica() && r.is_completed();
            });
            if (!still_has_disk) shard_.disk_object_count--;
        }

        // Overload for full object erasure — no metadata needed.
        void OnDiskReplicaRemoved(bool had_completed_disk) {
            if (had_completed_disk) shard_.disk_object_count--;
        }

       private:
        MetadataShard& shard_;
        SharedMutexLocker lock_;
    };

    // For accessing a metadata shard with read-only permission
    class MetadataShardAccessorRO {
       public:
        MetadataShardAccessorRO(const MasterService* master_service,
                                size_t shard_index)
            : shard_(master_service->metadata_shards_[shard_index]),
              lock_(&shard_.mutex, shared_lock) {}

        const MetadataShard* operator->() const { return &shard_; }

        const MetadataShard& get() const { return shard_; }

       private:
        const MetadataShard& shard_;
        SharedMutexLocker lock_;
    };

    static ObjectIdentity MakeObjectIdentity(const std::string& user_key,
                                             TenantId tenant_id) {
        return {std::move(tenant_id), user_key};
    }
    const TenantId& ResolveRequestTenantId(const TenantId& tenant_id) const;
    ObjectIdentity MakeObjectIdentityForRequest(
        const std::string& user_key, const TenantId& tenant_id) const;
    tl::expected<TenantId, ErrorCode> ResolveTenantIdForWrite(
        const TenantId& tenant_id) const;
    tl::expected<TenantId, ErrorCode> ResolveTenantIdForWriteLocked(
        const TenantId& tenant_id) const;
    bool IsTenantRegistered(const TenantId& tenant_id) const;
    bool TenantHasObjects(const TenantId& tenant_id) const;

    // Helper to get the configured logical slot for
    // a tenant-scoped key. Slots are the unit of cross-master ownership.
    uint16_t getSlot(const TenantId& tenant_id,
                     const std::string& user_key) const {
        return cvm::KeySlot(tenant_id, user_key);
    }

    // Helper to get shard index from tenant-scoped object identity. The
    // physical shard is derived from the logical slot.
    size_t getShardIndex(const TenantId& tenant_id,
                         const std::string& user_key) const {
        return getSlot(tenant_id, user_key) % kNumShards;
    }

    // Legacy helper routes plain keys to the default tenant.
    size_t getShardIndex(const std::string& key) const {
        return getSlot(TenantId::Default(), key) % kNumShards;
    }

    size_t getMetadataShardIndex(const TenantId& tenant_id,
                                 const std::string& key) const;
    std::optional<std::string> GetGroupRoute(const TenantId& tenant_id,
                                             const std::string& key) const;
    void RegisterGroupMember(TenantState& tenant_state,
                             const TenantId& tenant_id, const std::string& key,
                             const std::string& group_id);
    void UnregisterGroupMember(TenantState& tenant_state,
                               const TenantId& tenant_id,
                               const std::string& key,
                               const std::string& group_id);
    std::unordered_map<std::string, ObjectMetadata>::iterator EraseMetadata(
        TenantState& tenant_state,
        std::unordered_map<std::string, ObjectMetadata>::iterator it,
        const TenantId& tenant_id);
    void ReleaseLocalDiskUsage(const std::vector<Replica>& replicas);
    enum class QuotaEraseMode {
        kFull,
        kPreserveOld,
        kAbortOnly,
        // slot 交接：与 kFull 相同，但跳过 ReleaseLocalDiskUsage，因为数据字节
        // 仍留在共享 segment（不应错误扣减 ssd_used_bytes）。
        kHandoff,
    };
    std::unordered_map<std::string, ObjectMetadata>::iterator EraseMetadata(
        TenantState& tenant_state,
        std::unordered_map<std::string, ObjectMetadata>::iterator it,
        const TenantId& tenant_id, QuotaEraseMode quota_mode);
    uint64_t CompletedMemoryQuotaCharge(const ObjectMetadata& metadata) const;
    uint64_t RequestedMemoryQuotaCharge(uint64_t value_length,
                                        const ReplicateConfig& config) const;
    bool ShouldProtectZeroChargeMetadataCreate(
        uint64_t requested_quota_charge) const;
    tl::expected<void, ErrorCode> ReserveTenantQuota(const TenantId& tenant_id,
                                                     uint64_t bytes);
    void CommitTenantQuota(const TenantId& tenant_id, uint64_t bytes);
    void AbortTenantQuota(const TenantId& tenant_id, uint64_t bytes);
    void ReleaseTenantQuota(const TenantId& tenant_id, uint64_t bytes);
    void ReleaseTenantQuotaPartial(const TenantId& tenant_id, uint64_t bytes);
    void CommitAdditionalTenantQuota(const TenantId& tenant_id, uint64_t bytes);
    void IncrementTenantMetadataObjectCount(const TenantId& tenant_id);
    void DecrementTenantMetadataObjectCount(const TenantId& tenant_id);
    void ReleaseCommittedQuotaCharge(ObjectMetadata& metadata, uint64_t bytes);
    void RecomputeTenantEffectiveQuotas();
    void RebuildTenantQuotaUsageFromMetadata();
    void LoadTenantQuotaPoliciesFromStoreOrThrow();
    void ApplyTenantQuotaPolicies(const TenantQuotaPolicySnapshot& snapshot);
    TenantQuotaPolicySnapshot BuildTenantQuotaPolicySnapshot() const;
    std::unordered_map<std::string, ObjectMetadata>::iterator EraseMetadata(
        TenantState& tenant_state,
        std::unordered_map<std::string, ObjectMetadata>::iterator it,
        const TenantId& tenant_id, QuotaEraseMode quota_mode,
        MetadataShardAccessorRW* shard);
    void FinalizeRemovedReplicasAfterDurable(
        const OpLogEntry& durable_entry,
        const std::vector<ReplicaID>& replica_ids, QuotaEraseMode quota_mode);
    void EnqueueRemoveTasks(const std::vector<UUID>& holder_ids,
                            const RemoveTaskItem& task);
    void FinalizeMetadataEraseAfterDurable(const OpLogEntry& durable_entry,
                                           QuotaEraseMode quota_mode);
    void FinalizeExpiredProcessingReplicasAfterDurable(
        const OpLogEntry& durable_entry,
        const std::chrono::system_clock::time_point& ttl);
    void FinalizeExpiredReplicationTaskAfterDurable(
        const OpLogEntry& durable_entry, ReplicaID source_id,
        const std::vector<ReplicaID>& target_ids,
        const std::chrono::system_clock::time_point& ttl);
    struct StaleHandleCleanupPlan {
        std::vector<ReplicaID> removed_ids;
        std::vector<Replica::Descriptor> remaining;
        bool would_invalidate{false};
    };
    StaleHandleCleanupPlan BuildStaleHandleCleanupPlan(
        const ObjectMetadata& metadata,
        const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients) const;
    tl::expected<void, ErrorCode> PersistStaleHandleCleanupForHA(
        const std::string& why, const TenantId& tenant_id,
        const std::string& key, ObjectMetadata& metadata,
        const StaleHandleCleanupPlan& plan);
    void RebuildGroupRoutingIndex();
    void GrantLeaseForGroup(const TenantState& tenant_state,
                            const std::string& key,
                            const ObjectMetadata& metadata) const;

    // Helper to clean up stale handles pointing to unmounted segments
    // or local_disk replicas whose owner client is no longer alive.
    bool CleanupStaleHandles(
        ObjectMetadata& metadata,
        const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients,
        MetadataShardAccessorRW* shard = nullptr);

    // Helper: allocate replicas, create ObjectMetadata, insert into shard,
    // and return descriptor list.  Shared by PutStart and UpsertStart.
    auto AllocateAndInsertMetadata(
        MetadataShardAccessorRW& shard, const UUID& client_id,
        const std::string& key, uint64_t value_length,
        const ReplicateConfig& config, const std::string& group_id,
        const TenantId& tenant_id,
        const std::chrono::system_clock::time_point& now)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    auto PutEndInternal(const UUID& client_id, const ObjectMeta& object_meta,
                        const TenantId& tenant_id, ReplicaType replica_type,
                        uint32_t write_batch_size, bool overwrite,
                        const std::string& operation_id = "")
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Helper to discard expired processing keys.
     */
    void DiscardExpiredProcessingReplicas(
        MetadataShardAccessorRW& shard,
        const std::chrono::system_clock::time_point& now);

    /**
     * @brief Helper to release space of expired discarded replicas.
     * @return Number of released objects that have memory replicas
     */
    uint64_t ReleaseExpiredDiscardedReplicas(
        const std::chrono::system_clock::time_point& now);

    // Eviction thread function
    void EvictionThreadFunc();
    void NofHeartbeatThreadFunc();
    bool TryUnmountNoFSegmentByHeartbeat(
        const MountedNoFSegmentSnapshot& snapshot,
        const std::string& error_reason);
    bool ProbeNoFSegment(const std::string& te_endpoint,
                         std::string* error_reason);

    // Queues an offload task for `replica` on the LOCAL_DISK segment(s)
    // mapped from the replica's MEMORY segment name(s). Returns the
    // client_id(s) of the segment(s) the task was actually enqueued on;
    // callers use them to record one protected OffloadingTask per queue.
    tl::expected<std::vector<UUID>, ErrorCode> PushOffloadingQueue(
        const ObjectIdentity& object_id, Replica& replica);

    struct GracefulUnmountDeadlineRecord {
        UUID segment_id;
        UUID client_id;
    };

    DeadlineScheduler<GracefulUnmountDeadlineRecord>
        graceful_unmount_scheduler_;

    /**
     * @brief Mirror of PushOffloadingQueue for promotion-on-hit. Inserts an
     * entry into the holder client's LocalDiskSegment::promotion_objects map.
     * Caller is responsible for refcnt-pinning the source replica and
     * recording the task in the shard's promotion_tasks map.
     */
    tl::expected<void, ErrorCode> PushPromotionQueue(
        const ObjectIdentity& object_id, Replica& source_replica);

    /**
     * @brief Helper invoked from GetReplicaList when an only-LOCAL_DISK key is
     * observed. Applies the gating chain (frequency / watermark / dedup /
     * cap), refcnt-pins the source LOCAL_DISK replica, records a
     * PromotionTask, and pushes onto the holder client's promotion_objects
     * map. Acquires its own RW shard accessor; safe to call after
     * GetReplicaList's RO accessor has been released.
     */
    PromotionQueueResult TryPushPromotionQueue(const ObjectIdentity& object_id,
                                               bool record_candidate = true);

    /**
     * @brief Queue one MEMORY replica of `object_id` for a LOCAL_DISK copy
     * (tier down) and keep the MEMORY replica in place.
     *
     * This is the demotion counterpart of an eviction: it never removes a
     * replica and never frees bytes, so the caller must not count its result as
     * reclaimed memory. Acquires its own RW shard accessor; safe to call from
     * the io_pattern eviction handler, which does not hold one while this runs.
     *
     * The outcome is reported instead of a bare bool: "nothing left to do" and
     * "queued" are operationally different, and collapsing a key that already
     * holds a LOCAL_DISK replica -- or whose copy is still in flight -- into
     * success is what hides a spinning tier-down driver.
     */
    enum class TierDownOutcome {
        kQueued,
        kAlreadyInFlight,
        kAlreadyPaved,
        kNotFound,
        kPushFailed,
    };
    TierDownOutcome TryQueueTierDown(const ObjectIdentity& object_id);
    void RecordOrUpdateCandidate(TenantState& tenant_state,
                                 const std::string& key, uint8_t sketch_score,
                                 PromotionCandidateReason reason,
                                 ErrorCode last_error);
    void EraseCandidate(TenantState& tenant_state, const std::string& key);
    void EraseCandidate(const ObjectIdentity& object_id);
    void DecrementCandidateCount();
    void BackoffCandidate(const ObjectIdentity& object_id,
                          PromotionQueueResult result);
    void ClearCandidatesForReload();
    std::chrono::milliseconds CandidateBackoff(uint32_t retry_count) const;
    bool IsTransientResult(PromotionQueueResult result) const;
    size_t RunPromotionCandidateRetry(size_t max_shards_to_scan);
    size_t RunPromotionCandidateRetry();
    size_t RunPromotionCandidateRetryForTesting();
    size_t CountCandidatesForTesting(const TenantId& tenant_id);
    void ResetCandidateBackoffsForTesting();

    // Erase any in-flight PromotionTask for `key`, abort any staged promotion
    // quota reservation, and decrement the cluster-wide in-flight counter. Safe
    // no-op if no task exists.
    void ErasePromotionTaskIfPresent(
        TenantState& tenant_state, const std::string& key,
        const TenantId& tenant_id) NO_THREAD_SAFETY_ANALYSIS {
        auto task_it = tenant_state.promotion_tasks.find(key);
        if (task_it != tenant_state.promotion_tasks.end()) {
            AbortTenantQuota(tenant_id,
                             task_it->second.reserved_quota_charge_bytes);
            tenant_state.promotion_tasks.erase(task_it);
            promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
            MasterMetricManager::instance().dec_promotion_in_flight();
            MasterMetricManager::instance().inc_promotion_cancelled();
        }
    }

    // Lease related members
    const uint64_t default_kv_lease_ttl_;     // in milliseconds
    const uint64_t default_kv_soft_pin_ttl_;  // in milliseconds
    const bool allow_evict_soft_pinned_objects_;

    // Eviction related members
    std::atomic<bool> need_mem_eviction_{
        false};  // Set to trigger memory eviction when allocation fails
    std::atomic<bool> need_nof_eviction_{
        false};  // Set to trigger NoF eviction when allocation fails
    const double eviction_ratio_;                     // in range [0.0, 1.0]
    const double eviction_high_watermark_ratio_;      // in range [0.0, 1.0]
    const double nof_eviction_ratio_;                 // in range [0.0, 1.0]
    const double nof_eviction_high_watermark_ratio_;  // in range [0.0, 1.0]

    // Eviction thread related members
    std::thread eviction_thread_;
    std::atomic<bool> eviction_running_{false};
    // Serializes legacy eviction fallbacks: the watermark thread and the
    // report-driven worker can both observe a shortfall, and BatchEvict has no
    // re-entrancy protection.
    std::mutex legacy_eviction_mutex_;
    static constexpr uint64_t kEvictionThreadSleepMs =
        10;  // 10 ms sleep between eviction checks

    // Snapshot manager handles snapshot lifecycle orchestration
    std::unique_ptr<MasterSnapshotManager> snapshot_manager_;

    // Task cleanup thread related members
    std::thread task_cleanup_thread_;
    std::atomic<bool> task_cleanup_running_{false};
    static constexpr uint64_t kTaskCleanupThreadSleepMs =
        30000;  // 30000 ms sleep between task cleanup checks

    // Used to wake task cleanup thread immediately during shutdown.
    std::mutex task_cleanup_mutex_;
    std::condition_variable task_cleanup_cv_;

    // Stale-generation eviction async drain（方案一 A 异步化）：纯 cv 唤醒
    // 无空轮询；drain 前有 1s debounce 合并挂载风暴（worker 启动连挂
    // N 段 → 一次 drain，而非 N 次全 host 扫描）。仅在 etcd 构建启动。
    struct StaleEvictRequest {
        std::string host_id;
        std::string segment_name;
        std::string te_endpoint;
        UUID mounting_client;
    };
    std::thread stale_evict_thread_;
    std::atomic<bool> stale_evict_running_{false};
    std::mutex stale_evict_queue_mutex_;
    std::condition_variable stale_evict_cv_;
    std::vector<StaleEvictRequest> stale_evict_queue_;
    // 入队未处理计数（含已 swap 出队列但在途的）：测试冲刷等待用。
    // 去重覆盖不重复计数。
    std::atomic<int> stale_evict_pending_{0};

    // Helper class for accessing metadata with automatic locking and cleanup
    class MetadataAccessorRW {
       public:
        MetadataAccessorRW(MasterService* service, ObjectIdentity object_id)
            : service_(service),
              object_id_(std::move(object_id)),
              shard_idx_(service_->getMetadataShardIndex(object_id_.tenant_id,
                                                         object_id_.user_key)),
              shard_guard_(service_, shard_idx_),
              tenant_it_(shard_guard_->tenants.find(object_id_.tenant_id)),
              tenant_state_(tenant_it_ == shard_guard_->tenants.end()
                                ? nullptr
                                : &tenant_it_->second),
              it_(tenant_state_ == nullptr
                      ? ObjectMetadataIterator{}
                      : tenant_state_->metadata.find(object_id_.user_key)),
              processing_it_(tenant_state_ == nullptr
                                 ? ProcessingIterator{}
                                 : tenant_state_->processing_keys.find(
                                       object_id_.user_key)),
              replication_task_it_(tenant_state_ == nullptr
                                       ? ReplicationTaskIterator{}
                                       : tenant_state_->replication_tasks.find(
                                             object_id_.user_key)) {
            // Automatically clean up invalid handles (memory replicas only).
            // Note: We only check memory replicas here to avoid lock order
            // violation (client_mutex_ must be acquired before metadata shard).
            // local_disk replicas are cleaned up by ClearInvalidHandles() in
            // ClientMonitorFunc.
            if (!(service_->enable_ha_ && service_->enable_oplog_) &&
                tenant_state_ != nullptr &&
                it_ != tenant_state_->metadata.end()) {
                // Erase invalid memory replicas (those with unmounted
                // segments). No client_mutex_ needed since we only check memory
                // replicas.
                const uint64_t before_charge =
                    service_->CompletedMemoryQuotaCharge(it_->second);
                service_->EraseReplicasWithCacheTotalAccounting(
                    it_->second, [](const Replica& replica) {
                        return replica.has_invalid_mem_handle();
                    });
                const uint64_t after_charge =
                    service_->CompletedMemoryQuotaCharge(it_->second);
                if (before_charge > after_charge) {
                    service_->ReleaseCommittedQuotaCharge(
                        it_->second, before_charge - after_charge);
                }
                // If no valid replicas remain, delete the whole object.
                if (!it_->second.IsValid()) {
                    const bool had_processing =
                        processing_it_ != tenant_state_->processing_keys.end();
                    this->Erase();
                    if (tenant_state_ != nullptr && had_processing) {
                        this->EraseFromProcessing();
                    }
                    if (tenant_state_ != nullptr) {
                        service_->ErasePromotionTaskIfPresent(
                            *tenant_state_, object_id_.user_key,
                            object_id_.tenant_id);
                        MaybeEraseEmptyTenant();
                    }
                }
            }
        }

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_ != nullptr &&
                   it_ != tenant_state_->metadata.end() &&
                   it_->second.IsValid();
        }

        bool InProcessing() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_ != nullptr &&
                   processing_it_ != tenant_state_->processing_keys.end();
        }

        bool HasReplicationTask() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_ != nullptr &&
                   replication_task_it_ !=
                       tenant_state_->replication_tasks.end();
        }

        MetadataShardAccessorRW& GetShard() NO_THREAD_SAFETY_ANALYSIS {
            return shard_guard_;
        }

        TenantState& GetTenantState() NO_THREAD_SAFETY_ANALYSIS {
            EnsureTenantState();
            return *tenant_state_;
        }

        // Get metadata (only call when Exists() is true)
        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return it_->second; }

        const ReplicationTask& GetReplicationTask() NO_THREAD_SAFETY_ANALYSIS {
            return replication_task_it_->second;
        }

        // Delete current metadata (for PutRevoke or Remove operations)
        void Erase() NO_THREAD_SAFETY_ANALYSIS {
            service_->EraseMetadata(*tenant_state_, it_, object_id_.tenant_id,
                                    QuotaEraseMode::kFull, &shard_guard_);
            it_ = tenant_state_->metadata.end();
            MaybeEraseEmptyTenant();
        }

        void EraseFromProcessing() NO_THREAD_SAFETY_ANALYSIS {
            tenant_state_->processing_keys.erase(processing_it_);
            processing_it_ = tenant_state_->processing_keys.end();
            MaybeEraseEmptyTenant();
        }

        void EraseReplicationTask() NO_THREAD_SAFETY_ANALYSIS {
            tenant_state_->replication_tasks.erase(replication_task_it_);
            replication_task_it_ = tenant_state_->replication_tasks.end();
            MaybeEraseEmptyTenant();
        }

        void Create(const UUID& client_id, uint64_t total_length,
                    std::vector<Replica> replicas, bool enable_soft_pin,
                    bool enable_hard_pin = false,
                    ObjectDataType data_type = ObjectDataType::UNKNOWN,
                    std::string group_id = "") {
            if (Exists()) {
                throw std::logic_error("Already exists");
            }
            const auto now = std::chrono::system_clock::now();
            EnsureTenantState();
            auto result = tenant_state_->metadata.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(object_id_.user_key),
                std::forward_as_tuple(
                    client_id, now, total_length, std::move(replicas),
                    enable_soft_pin, enable_hard_pin, data_type, group_id,
                    object_id_.tenant_id, object_id_.user_key));
            it_ = result.first;
            if (result.second) {
                service_->IncrementTenantMetadataObjectCount(
                    object_id_.tenant_id);
            }
        }

       private:
        using ObjectMetadataIterator =
            std::unordered_map<std::string, ObjectMetadata>::iterator;
        using ProcessingIterator = std::unordered_set<std::string>::iterator;
        using ReplicationTaskIterator =
            std::unordered_map<std::string, const ReplicationTask>::iterator;

        void EnsureTenantState() NO_THREAD_SAFETY_ANALYSIS {
            if (tenant_state_ != nullptr) {
                return;
            }
            auto result =
                shard_guard_->tenants.try_emplace(object_id_.tenant_id);
            tenant_it_ = result.first;
            tenant_state_ = &tenant_it_->second;
            it_ = tenant_state_->metadata.end();
            processing_it_ = tenant_state_->processing_keys.end();
            replication_task_it_ = tenant_state_->replication_tasks.end();
        }

        void MaybeEraseEmptyTenant() NO_THREAD_SAFETY_ANALYSIS {
            if (tenant_state_ == nullptr || !tenant_state_->Empty()) {
                return;
            }
            shard_guard_->tenants.erase(tenant_it_);
            tenant_state_ = nullptr;
        }

        MasterService* service_;
        ObjectIdentity object_id_;
        size_t shard_idx_;
        MetadataShardAccessorRW shard_guard_;
        std::unordered_map<TenantId, TenantState, TenantIdHash>::iterator
            tenant_it_;
        TenantState* tenant_state_;
        ObjectMetadataIterator it_;
        ProcessingIterator processing_it_;
        ReplicationTaskIterator replication_task_it_;
    };

    class MetadataSerializer {
       public:
        MetadataSerializer(MasterService* service) : service_(service) {}

        // Serialize metadata of all shards
        tl::expected<std::vector<uint8_t>, SerializationError> Serialize();

        tl::expected<void, SerializationError> Deserialize(
            const std::vector<uint8_t>& data);

        void Reset();

       private:
        MasterService* service_;

        // Serialize a single ObjectMetadata
        tl::expected<void, SerializationError> SerializeMetadata(
            const ObjectMetadata& metadata, MsgpackPacker& packer) const;

        // Deserialize a single ObjectMetadata
        [[nodiscard]] tl::expected<std::unique_ptr<ObjectMetadata>,
                                   SerializationError>
        DeserializeMetadata(const msgpack::object& obj) const;

        // Serialize a single MetadataShard
        tl::expected<void, SerializationError> SerializeShard(
            const MetadataShard& shard, MsgpackPacker& packer) const;

        // Deserialize a single MetadataShard
        tl::expected<void, SerializationError> DeserializeShard(
            const msgpack::object& obj, MetadataShard& shard);

        // Serialize discarded replicas
        tl::expected<void, SerializationError> SerializeDiscardedReplicas(
            MsgpackPacker& packer) const;

        // Deserialize discarded replicas
        tl::expected<void, SerializationError> DeserializeDiscardedReplicas(
            const msgpack::object& obj);
    };

    friend class MetadataAccessor;
    class MetadataAccessorRO {
       public:
        MetadataAccessorRO(const MasterService* service,
                           ObjectIdentity object_id)
            : service_(service),
              object_id_(std::move(object_id)),
              shard_idx_(service_->getMetadataShardIndex(object_id_.tenant_id,
                                                         object_id_.user_key)),
              shard_guard_(service_, shard_idx_),
              tenant_it_(shard_guard_->tenants.find(object_id_.tenant_id)),
              tenant_state_(tenant_it_ == shard_guard_->tenants.end()
                                ? nullptr
                                : &tenant_it_->second),
              it_(tenant_state_ == nullptr
                      ? ObjectMetadataConstIterator{}
                      : tenant_state_->metadata.find(object_id_.user_key)),
              processing_it_(tenant_state_ == nullptr
                                 ? ProcessingConstIterator{}
                                 : tenant_state_->processing_keys.find(
                                       object_id_.user_key)) {}

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_ != nullptr &&
                   it_ != tenant_state_->metadata.end() &&
                   it_->second.IsValid();
        }

        bool InProcessing() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_ != nullptr &&
                   processing_it_ != tenant_state_->processing_keys.end();
        }

        // Get metadata (only call when Exists() is true)
        const ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS {
            return it_->second;
        }

        MetadataShardAccessorRO& GetShard() NO_THREAD_SAFETY_ANALYSIS {
            return shard_guard_;
        }

        const TenantState* GetTenantState() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_;
        }

       private:
        using ObjectMetadataConstIterator =
            std::unordered_map<std::string, ObjectMetadata>::const_iterator;
        using ProcessingConstIterator =
            std::unordered_set<std::string>::const_iterator;

        const MasterService* service_;
        const ObjectIdentity object_id_;
        const size_t shard_idx_;
        MetadataShardAccessorRO shard_guard_;
        std::unordered_map<TenantId, TenantState, TenantIdHash>::const_iterator
            tenant_it_;
        const TenantState* tenant_state_;
        ObjectMetadataConstIterator it_;
        ProcessingConstIterator processing_it_;
    };

    friend class MetadataAccessorRW;
    friend class MetadataAccessorRO;

    ViewVersionId view_version_;

    // Client related members
    mutable std::shared_mutex client_mutex_;
    std::unordered_set<UUID, boost::hash<UUID>>
        ok_client_;  // client with ok status
    std::unordered_map<UUID, std::string, boost::hash<UUID>> client_host_id_;
    void ClientMonitorFunc();
    std::thread client_monitor_thread_;
    std::atomic<bool> client_monitor_running_{false};
    static constexpr uint64_t kClientMonitorSleepMs =
        1000;  // 1000 ms sleep between client monitor checks
    // boost lockfree queue requires trivial assignment operator
    struct PodUUID {
        uint64_t first;
        uint64_t second;
    };
    static constexpr size_t kClientPingQueueSize =
        128 * 1024;  // Size of the client ping queue
    boost::lockfree::queue<PodUUID> client_ping_queue_{kClientPingQueueSize};
    const int64_t client_live_ttl_sec_;
    const std::chrono::seconds nof_heartbeat_interval_sec_;
    const std::chrono::milliseconds nof_heartbeat_probe_timeout_ms_;
    const uint32_t nof_heartbeat_failures_threshold_;

    struct NoFHeartbeatState {
        UUID owner_client_id{0, 0};
        std::string segment_name;
        std::string te_endpoint;
        std::chrono::steady_clock::time_point next_probe_at{};
        std::chrono::steady_clock::time_point last_success_at{};
        uint32_t consecutive_failures{0};
        std::string last_error_reason;
    };
    std::mutex nof_heartbeat_mutex_;
    std::unordered_map<UUID, NoFHeartbeatState, boost::hash<UUID>>
        nof_heartbeat_states_;
    std::thread nof_heartbeat_thread_;
    std::atomic<bool> nof_heartbeat_running_{false};
    static constexpr uint64_t kNoFHeartbeatThreadSleepMs = 100;
    mutable std::mutex nof_probe_fn_mutex_;
    NoFProbeFn nof_probe_fn_;

    // if high availability features enabled
    const bool enable_ha_;

    const bool enable_offload_;

    // Offload-on-evict: defer disk offload to eviction time
    // (config: offload_on_evict)
    bool offload_on_evict_{false};
    // Force-evict: allow evicting MEMORY replicas without disk offload when cap
    // exceeded (config: offload_force_evict, only effective when
    // offload_on_evict_=true)
    bool offload_force_evict_{false};

    // Strict replica allocation: memory-only multi-replica requests must
    // allocate exactly replica_num replicas instead of best-effort
    // degradation (config: strict_replica_allocation)
    bool strict_replica_allocation_{false};

    // Promotion-on-hit: opt-in flag enabling LOCAL_DISK -> MEMORY promotion
    // when a Get observes a key with only LOCAL_DISK replicas.
    bool promotion_on_hit_{false};
    uint32_t promotion_admission_threshold_{2};
    uint32_t promotion_queue_limit_{50000};
    uint32_t promotion_max_per_heartbeat_{1};
    // Global in-flight task counter, checked against promotion_queue_limit_
    // as the gate cap. Promotion specifically targets skewed
    // access (hot keys re-accessed after eviction), so the global counter
    // is the correct primitive. Incremented in TryPushPromotionQueue after
    // successful enqueue; decremented in NotifyPromotionSuccess and in the
    // promotion task reaper after the task entry is erased. Relaxed memory
    // order is safe — the value is an advisory soft cap, not a barrier.
    std::atomic<uint64_t> promotion_in_flight_{0};
    // Promotion retry candidate state.
    std::atomic<uint64_t> promotion_candidate_count_{0};
    std::atomic<size_t> promotion_retry_cursor_{0};
    static constexpr size_t kPromotionCandidateLimit = 50000;
    static constexpr uint32_t kPromotionCandidateMaxRetries = 8;
    static constexpr size_t kPromotionRetryBatchSize = 128;
    static constexpr size_t kPromotionRetryShardBatch = 64;
    static constexpr std::chrono::milliseconds kPromotionCandidateTtl{60000};
    static constexpr std::chrono::milliseconds
        kPromotionCandidateInitialBackoff{10};
    static constexpr std::chrono::milliseconds kPromotionCandidateMaxBackoff{
        1000};

    // Master-side frequency sketch. Constructed only when promotion_on_hit_ is
    // true. CountMinSketch is mutex-protected internally so we can call into it
    // from any GetReplicaList caller without additional locking.
    std::unique_ptr<CountMinSketch> promotion_sketch_;

    // The IO-pattern pipeline is deliberately owned by MasterService: the
    // master has the authoritative replica map and is the only component that
    // can safely translate a policy plan into promotion/eviction operations.
    // CFM is embedded here as a component of this SubMaster: reports addressed
    // to the keys this master owns are merged into the local runtime over the
    // regular coro_rpc endpoint, so no remote reporting channel, policy poller
    // or credential is needed.
    std::shared_ptr<io_pattern::IoPatternRuntime> io_pattern_runtime_;
    std::shared_ptr<io_pattern::CfmService> io_pattern_cfm_service_;

    // Policy-driven tier down. Kept separate from the eviction counters because a
    // demotion queues a disk copy and frees no memory: folding it into freed
    // bytes would make a tier-down plan look like a plan that under-delivered.
    // Written only from the io_pattern eviction handler; relaxed order is enough
    // because these are advisory observability counters.
    std::atomic<uint64_t> tier_down_attempts_{0};
    std::atomic<uint64_t> tier_down_successes_{0};
    std::atomic<uint64_t> tier_down_failures_{0};
    // Outcomes that are neither progress nor failure. Kept separate so a stalled
    // driver is visible instead of being counted as successful demotions.
    std::atomic<uint64_t> tier_down_skipped_in_flight_{0};
    std::atomic<uint64_t> tier_down_skipped_paved_{0};

    const std::string ha_backend_type_;

    const std::string ha_backend_connstring_;
    const bool enable_oplog_;
    const uint32_t oplog_batch_max_entries_;

    // cluster id for persistent sub directory
    const std::string cluster_id_;
    // Stable master id used by the SlotOwnerHeartbeat (empty = disabled).
    const std::string master_id_;
    // CVM external HTTP API (CvmHttpServer) bind config. Port 0 keeps the
    // HTTP server disabled.
    const uint16_t cvm_http_port_;
    const std::string cvm_http_host_;
    // 集群中允许同时 serving 的 submaster 上限（CVM 名额协调）。
    const uint32_t submaster_count_;
    // 资源事实（Store 注册时上报到 SegmentDescriptor，供 vsegment 自动发现
    // 使用）：本集群 segment 的物理介质标识与「vsegment 专用且空」声明。
    // 见 MasterServiceConfig 同名字段说明。
    const std::string cvm_segment_default_medium_;
    const bool cvm_segments_vsegment_exclusive_;
    // Drives slot-metadata handoff for this submaster (deterministic ring; no
    // etcd publication). Only started in HA mode when a stable master_id is
    // configured.
    std::unique_ptr<cvm::SlotOwnerHeartbeat> slot_owner_heartbeat_;
    // slot_owner_heartbeat_ 生命周期写（Start/Stop 路径）与跨线程 Kick 读的
    // 互斥：Kick 触发点在心跳线程之外（后台首建线程、mount RPC 线程），
    // 防止对 unique_ptr 的并发读写。锁内绝不 join（Stop 在锁外执行）。
    std::mutex slot_heartbeat_kick_mutex_;
    void KickSlotHeartbeat();  // 事件驱动触发下一轮立即 reconcile（见 SlotOwnerHeartbeat::Kick）
    // Inter-master RPC client (CVM multi-submaster coordination): etcd-driven
    // member table + per-peer cached coro_rpc pools. Started/stopped by the
    // supervisor around serve phases, mirroring the heartbeat above.
    std::unique_ptr<cvm::InterMasterRpcClient> inter_master_rpc_;
    std::shared_ptr<vsegment::VSegmentService> vsegment_service_;
    // vsegment_service_ 首建互斥（双检锁内层）：同步路径（冷启动）与后台首
    // 建线程共用；临界区仅含 Load + make_shared + provider 注入（毫秒级），
    // 锁等待可忽略；稳态走外层空判快路径，零成本。
    std::mutex vsegment_service_init_mutex_;
    // Runtime-resolved vsegment layouts for inline responses, keyed by
    // vsegment_id. Cleared on segment lifecycle changes; best-effort entries
    // are omitted on resolution failure.
    mutable std::mutex vsegment_layout_cache_mutex_;
    std::unordered_map<std::string, VSegmentLayoutEntry>
        vsegment_layout_cache_;
    // ----- vsegment 首建状态机（运行期异步门控）-----
    // 心跳线程绝不内联执行分钟级的「首建 + 首次全量 reconcile sweep」：
    //   kIdle       未初始化（或后台初始化失败已复位，等待重触发）
    //   kInProgress 后台初始化线程执行 Load → 首建 → 首次全量 sweep
    //   kReady      vsegment_service_ 已发布且首次 sweep 已完成
    // 门控期间所有 RefreshVSegmentOwnership 运行期调用返回
    // VSEGMENT_SERVICE_NOT_READY——对全部调用方均为可重试语义：
    // slot_migrator 的 on_acquire pending 路径、P4 reshard 下一轮重拉、
    // 心跳周期 reconcile 跳过本周期。冷启动（StartSlotOwnerHeartbeat
    // 初始调用，gate 置 true 之前）仍走同步路径，错误硬失败启动，与历史
    // 行为一致。
    // kNoQuota: the quota snapshot is absent (vsegment disabled or not yet
    // published). Slot readiness does NOT depend on vsegment in this state —
    // the runtime gate must let slot imports proceed (mirrors the cold-start
    // semantics of RefreshVSegmentOwnership for ETCD_KEY_NOT_EXIST). The
    // background init thread keeps re-probing each heartbeat so a runtime
    // quota publish (auto-plan / POST) transitions to kReady.
    enum class VSegmentInitState { kIdle, kInProgress, kReady, kNoQuota };
    std::atomic<VSegmentInitState> vsegment_init_state_{
        VSegmentInitState::kIdle};
    // Independent re-probe in-flight flag for the background init thread.
    // Never encoded into vsegment_init_state_: encoding it (the kNoQuota ->
    // kInProgress CAS) turned every slot-admission release into a full
    // kInProgress window (init thread's etcd Get), throttling slot readiness
    // to ~1 slot per heartbeat. With this flag the spawn path is invisible
    // to the gate: kNoQuota stays kNoQuota while probing.
    std::atomic<bool> vsegment_init_probe_in_flight_{false};
    std::thread vsegment_init_thread_;
    std::mutex vsegment_init_spawn_mutex_;  // 保护线程 spawn/join 生命周期
    // StartSlotOwnerHeartbeat 初始同步调用完成后置 true：此前的调用走同步
    // 路径（构造+sweep 内联），之后的运行期调用走异步门控。
    std::atomic<bool> vsegment_runtime_gate_active_{false};
    void MaybeSpawnVSegmentInit();   // kIdle → 拉起后台首建线程（幂等）
    void VSegmentInitThreadFunc();   // 后台：Load→首建→发布→首次全量 sweep
    ErrorCode RunVSegmentOwnershipReconcile(
        const std::string& acquiring,
        vsegment::VSegmentCommitBatch* batch = nullptr);
    // 构造 + provider 注入 + 原子发布（先配置后发布）。前提：持有
    // vsegment_service_init_mutex_ 且复检 vsegment_service_ 为空。
    void ConstructVSegmentService(vsegment::PartitionPhysicalQuotaSnapshot quota);
    // ----- vsegment auto-plan 重试（--vsegment_auto_plan 发布侧自愈）-----
    // 配额快照发布侧的状态机：启动 bootstrap（master.cpp）只在进程启动时
    // 试一次，发现零段即永久放弃；本组件在快照缺失期间由心跳
    // （RefreshVSegmentOwnership 的 KEY_NOT_EXIST 分支）与 mount/remount
    // 尾部两个触发点节流重试 PlanAndPublishDiscoveredQuota，发布成功后由
    // 下一次心跳的既有 Load 路径拉起 VSegmentService。etcd Create 原子首写
    // 保证多 submaster 并发重试 first-writer-wins。
    bool MaybeRetryVSegmentAutoPlan(const char* trigger);
    // W6 静默期信号（集群级）：把最近一次 mount/remount 的墙钟毫秒写入
    // etcd 共享 key（cvm::VSegmentMountActivityKey），由
    // PlanAndPublishDiscoveredQuota 在发布前读取并等待静默——对全部触发
    // 方统一生效。
    void NoteVSegmentMountActivity();
    vsegment::VSegmentUserPolicy vsegment_auto_plan_policy_{};  // member_count==0 ⇒ 关闭
    mutable std::mutex vsegment_auto_plan_mutex_;  // 叶子锁：仅保护下方状态
    bool vsegment_auto_plan_published_{false};     // 发布成功（含并发赢家）锁存
    bool vsegment_auto_plan_gave_up_{false};       // INVALID_PARAMS 终态锁存
    std::chrono::steady_clock::time_point
        vsegment_auto_plan_last_attempt_{};
    int vsegment_auto_plan_attempts_{0};  // 日志状态变化控制（首条 INFO 后 VLOG）
    // Decoded before Partition ownership is activated. The route lifecycle
    // consumes the matching state when it creates each local manager.
    std::vector<vsegment::PartitionVSegmentSnapshot>
        recovered_vsegment_snapshots_;

    // ----- Inter-master allocation forwarding (CVM plan B phase 2) -----

    // Segment-owner side: keepalive for replicas allocated on behalf of
    // slot-owning peers, keyed by scoped key. The real handles stay alive
    // here; the peer only holds dummy-allocator replicas. Freed by
    // InterMasterFreeReplicas (broadcast by the slot owner on erase).
    std::mutex inter_master_keepalive_mutex_;
    std::unordered_map<std::string, std::vector<Replica>>
        inter_master_keepalive_;

    // Slot-owner side: scoped keys whose handles live on a peer submaster
    // (remote-allocated or migrated-in). Drives the broadcast-free on
    // EraseMetadata; kHandoff (slot migration) keeps the handles alive and
    // transfers the responsibility to the importing master.
    std::mutex remote_allocated_keys_mutex_;
    std::unordered_set<std::string> remote_allocated_keys_;

    // Slot-owner side: keeps the DummyBufferAllocator of remote replicas
    // alive (the replicas hold only weak references), keyed by endpoint.
    std::mutex remote_replica_allocator_keepalive_mutex_;
    std::unordered_map<std::string, std::shared_ptr<DummyBufferAllocator>>
        remote_replica_allocator_keepalive_;

    // Slot-owner side: local allocation failed, try peers (segment owners)
    // before giving up. Called OUTSIDE the local allocator lock to avoid
    // distributed deadlock (both masters forwarding simultaneously).
    tl::expected<std::vector<Replica>, ErrorCode> TryAllocateReplicasRemotely(
        const std::string& key, const TenantId& tenant_id,
        uint64_t value_length, size_t replica_num,
        const std::vector<std::string>& preferred_segments);

    // EraseMetadata hook: when the erased object's handles live on a peer,
    // enqueue a broadcast free (skipped for kHandoff: the data bytes must
    // survive the slot migration).
    void EnqueueRemoteFreeIfTracked(const TenantId& tenant_id,
                                    const std::string& key,
                                    QuotaEraseMode quota_mode);
    // Lease id granted by the supervisor-owned CvmController. Reused by the
    // SlotOwnerHeartbeat and segment owner records so they share the master
    // registration lifecycle (auto-removed on lease expiry). 0 until the
    // supervisor injects it via SetCvmLeaseId().
    EtcdLeaseId cvm_lease_id_{0};
    // supervisor 注入的同进程 CvmController（弱引用，supervisor 拥有）；
    // ring_slots 缓存读取（§16.15.5）。仅 serve phase 前设置一次。
    cvm::CvmController* cvm_controller_{nullptr};
    // 幽灵写告警节流（§16.14.6 验证辅助）：「模型已启用（HasSeenRing
    // SlotsModel）但当前缓存空 → 写判定走了旧环回退」是危险瞬态，
    // 数据面 QPS 高，30s 一条防刷屏。mutable：const 判定路径（CheckSlot
    // Writable / ResolveSlotOwnerMasterId）打点。0 = 未打过。
    mutable std::atomic<int64_t> last_ghost_fallback_warn_ms_{0};
    // 两级 slot 状态（确定性哈希方案 §15.6）：
    //   expected_owned_ = ring 推导「应拥有」的 slot（by ResolveOwnedSlotsForCvm）
    //   ready_owned_    = 元数据已就绪（import/replay 完成）可服务
    // 读路径以 ready_owned_ 判权；expected && !ready 即「迁移中」（Phase 4 拒
    // 绝为 SLOT_MIGRATING）。owned_slots_ready_ 表示 expected 是否已解析过，
    // 为 false 时 partition 未启用或尚未解析，读路径放行。
    mutable std::shared_mutex owned_slots_mutex_;
    std::vector<bool> expected_owned_;
    std::vector<bool> ready_owned_;
    bool owned_slots_ready_{false};
    // ready/expected 计数（owned_slots_mutex_ 保护）：饱和（ready==expected
    // 且 >0）转变时打一条「全部就绪可写入」INFO——vsegment 发布成功后写入
    // 可行性的明确观测点（此前 MarkSlotsReady 完全静默，只能从 admin
    // metrics 的 owned_slots 间接推断）。all_ready_logged_ 为饱和去重标记：
    // 进入饱和打一条，离开饱和静默复位，稳态零输出。
    uint32_t ready_slot_count_{0};
    uint32_t expected_slot_count_{0};
    bool all_ready_logged_{false};
    // 仅当 owned slot 数量变化时打印 INFO 日志（避免心跳周期刷屏）。
    bool owned_slot_count_logged_{false};
    std::size_t last_logged_owned_count_{0};
    // vsegment reconcile 周期聚合统计（防刷屏，验证辅助）：每周期累计
    // scanned/owned/foreign/no_route/load_fail/reconcile_fail/stale_cas 七项，
    // 仅当与上一周期不同才打一条 INFO（变更触发，稳态静默）；绝不逐
    // partition 打印。心跳线程与 per-slot acquire（RPC 线程）并发调用下
    // 最多漏/重一条诊断行，无害（同 last_phase_outcome_ 约定）。
    static constexpr int kVSegStatCount = 7;
    std::atomic<uint32_t> vseg_stat_last_[kVSegStatCount]{};
    // Segment view (CVM) 数据源：在 segment 挂载/卸载时把中立描述符
    // segments/<seg_id> + 每 master 挂载记录 snapshot/<master_id>/segments/
    // <seg_id> 同步到 etcd，CvmHttpServer 据此聚合出 segment 视图。仅在 etcd
    // HA backend 下生效，其余场景为空操作。
    void PublishSegmentOwnerForCvm(const Segment& segment);
    void RemoveSegmentOwnerForCvm(const UUID& segment_id);
    // 方案一 A（代际替换）：worker 每次进程启动都以新 client_id 重挂段。
    // stale_evict_thread_ drain 时调用本方法（MountSegment 尾部经
    // EnqueueStaleGenerationEvict 异步入队，见下方），在「同 segment_name
    // + 同 host_id」前提
    // 下按以下任一判据识别旧代残留（worker 重启前的死段）并立即清理：
    // a) te_endpoint 不同：P2PHANDSHAKE 协议端口随进程变化（原逻辑）；
    // b) owner client_id 不同：非 P2PHANDSHAKE（te_endpoint 为固定
    //    hostname，跨 boot 不变）快速重注册场景。segment_name=
    //    local_hostname_ 按 host 唯一，同 name 不同 client_id 即旧进程
    //    代；活 client 幂等重挂（ReMount/重复 Mount）client_id 相同，
    //    不会误杀。
    // 清理动作：内存段两阶段卸载 + etcd 挂载记录/描述符
    // （RemoveSegmentOwnerForCvm）+ invalid 集合卫生 + 同 host 旧代
    // client 的 LOCAL_DISK 段对称卸载，不等 TTL reaper 过期。
    // 设计假设：同一 host 的同 name worker 单进程部署。P2-8-2 已为
    // LocalDiskSegment 增加 segment_name（MountSegment 尾部补登）：已
    // 登记内存段挂载的 co-tenant client 按 name 过滤、不会误清；残留
    // 风险仅剩「只挂 LOCAL_DISK、从未挂内存段」的 client（无 name 上下
    // 文，退化为 host 级归并）。
    void EvictStaleGenerationForHost(const Segment& incoming,
                                     const UUID& mounting_client);
    // 方案一 A 异步化：MountSegment 尾部仅入队（µs 级），完整清理
    // （LOCAL_DISK 对称卸载 + 内存段两阶段卸载 + etcd 清理 +
    // ClearInvalidHandles）由 stale_evict_thread_ 延迟执行，把同步
    // etcd 事务与锁竞争移出 RPC 关键路径（MountSegment 30s 卡顿两大
    // 主因）。队列按 (host_id, segment_name) 去重、最新 mounting_client
    // 胜出；drain 以 shared_lock(snapshot_mutex_) 补持 helper 的锁假设
    //（snapshot → client → segment → shard），并在 helper 内前置校验
    // 参考 client 仍持有该 name 段（防快速连续重启窗口内误杀新代）；
    // 已 swap 出队的在途请求若被更新的同键请求取代则作废
    //（superseded_by_newer_mount），交由最新参考执行。
    void EnqueueStaleGenerationEvict(const Segment& incoming,
                                     const UUID& mounting_client);
    void StaleEvictThreadFunc();
    // 动态 KV slot 划分：以 etcd 注册表为唯一事实源，读取已注册的 primary
    // master 列表，按一致性哈希环（cvm::ResolveOwnedSlotsOnRing）计算本机
    // 应拥有的 slot 集合。etcd 读取失败时沿用上一轮结果（sticky），避免
    // 瞬时抖动引发全量抢夺；本机不在 primary 列表时返回空集合（不认领）。
    // 供 SlotOwnerHeartbeat 动态解析器回调调用（运行在心跳线程）。
    std::vector<uint16_t> ResolveOwnedSlotsForCvm();
    // sticky 缓存：最近一次成功解析的 owned slot 集合（etcd 抖动时沿用）。
    std::vector<uint16_t> cvm_last_resolved_owned_slots_;
    // 归属变更日志（P1）：缓存最近一次解析到的 primary 成员列表（已排序去重），
    // 用于在成员增删时打印 joined/left 根因，仅变化时打印一次。
    std::vector<std::string> cvm_last_primary_ids_;
    // Revision of the membership read used to derive this local ring.
    ViewVersionId cvm_ring_revision_{0};
    // 迁移旧环（Phase 3）：成员列表发生变化「前」的 primary 列表，供
    // ImportSlotMetadata 推导 gained slot 的上一任 owner（旧 owner 直传）。
    // 仅在 membership 变化时更新，保持不变时维持旧值以支持 acquire 重试。
    std::vector<std::string> cvm_prev_primary_ids_;
    // 当前存活的全部 master_id（含 primary 与 standby）。由
    // ResolveOwnedSlotsForCvm 在 LoadAllMasters 成功时刷新，供
    // ImportSlotMetadata 判断旧 owner 是否已消亡（消亡则无法 RPC 拉取，
    // 改走空元数据 + 客户端重建，避免对死节点无限拉取刷屏）。
    std::vector<std::string> cvm_alive_master_ids_;
    mutable std::mutex cvm_resolver_mutex_;
    // live primary → live primary 的 slot 元数据交接（确定性哈希方案 §15.7，
    // RPC 直传替代 etcd slot_meta 中转）。
    //   ExportSlotMetadata(slot)  —— on_release 钩子：收集该 slot 对象元数据
    //      并缓存到 pending_slot_exports_（不删本地，等新 owner 拉取 + ack）。
    //   ImportSlotMetadata(slot)   —— on_acquire 钩子：推导旧 owner（旧环），
    //      经 InterMasterRpc 直传拉取 export 并物化到本地，成功后回 ack。
    // 数据字节始终留在 segment，不搬移。二者均运行在心跳线程。
    ErrorCode ExportSlotMetadata(uint16_t slot);
    // Installation only; the range caller owns the durable barrier and ACK.
    ErrorCode ImportSlotMetadata(
        uint16_t slot, const SlotMetadataExport* prefetched_export,
        vsegment::VSegmentCommitBatch* batch);
    ErrorCode OnSlotRangeAcquired(uint16_t first_slot, uint16_t last_slot,
                                  std::vector<ErrorCode>& results);

    // 收集 `slot` 下所有对象的对象元数据（只读，供 stage / RPC 拉取复用）。
    // require_not_owned（默认 true，§15 单 slot 交接语义）：要求本机已失去
    // slot 归属才允许导出；P4 reshard 批量导出传 false——源仍持有归属
    //（kMigrating 冻结写保证快照稳定；切 owner 后源仍持有未 ack 数据，
    // 重拉幂等）。
    tl::expected<SlotMetadataExport, ErrorCode> BuildSlotMetadataExport(
        uint16_t slot, bool require_not_owned = true);
    tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
    BuildSlotMetadataExportBatch(uint16_t first_slot, uint16_t last_slot,
                                  bool require_not_owned);
    // ack 后删除本地 `slot` 的元数据（与 Export 侧擦除对称的释放）。
    ErrorCode DropSlotMetadataLocal(uint16_t slot);
    // 段级批量版 DropSlotMetadataLocal（§16.16.5，P4 reshard ack 后的源侧
    // 清理）：[first_slot, last_slot] 闭区间单趟扫描清理，避免逐 slot 反复
    // 全量扫描 metadata 分片（rank 段可达数千 slot）。
    ErrorCode DropSlotMetadataRange(uint16_t first_slot, uint16_t last_slot);
    // RPC 直传的 staged 导出缓存：旧 owner 在 on_release 时把导出结果放这里，
    // 等新 owner InterMasterExportSlot 拉取；收到 AckSlotImported 后删除缓存
    // 并清理本地元数据。
    std::unordered_map<uint16_t, SlotMetadataExport> pending_slot_exports_;
    mutable std::mutex pending_slot_exports_mutex_;

    // ----- P4 reshard：段级批量迁移 + 目标侧 driver（§16.16 / §16.19）-----

    // reshard driver 线程：扫描 reshard_intent 中 target==本机的意图，
    // 驱动「CAS kMigrating → CAS kStable 切 owner → 段级拉快照+安装 →
    // 段级 ack → 删 intent」五阶段（断点续传：意图持久化在 etcd，重启后
    // 重扫；任意阶段崩溃后按 ring_slots 当前状态重入，全链路幂等）。
    // 目标驱动（coordinator = target）而非设计稿的源驱动：原生认领
    //（§16.15.3 步 1，认领者是 standby、无 inter-master RPC 载体）与
    // 管理员迁移共用同一条目标侧状态机；源只承担「冻结写 + 响应批量
    // Export/Ack RPC」的被动角色。
    void ReshardDriverLoop();
    // 执行单个意图。返回 true = 已完成（含 ack）或意图失效，调用方删 intent。
    bool DriveReshardIntent(const cvm::ReshardIntent& intent);
    // 从源拉取 rank 段快照并安装（install 主体复用 ImportSlotMetadata 抽取
    // 的 InstallSlotMetadataExport）。调用前置：owner CAS 已完成（源 ack 前
    // 保留全部数据，重拉幂等）；快照稳定由 kMigrating 冻结写 + 切 owner 后
    // 源停服共同保证。安装前重读 LoadRingSlotAssign 校验 owner 仍是本机。
    ErrorCode ImportSlotRangeFrom(const std::string& source_master_id,
                                   uint32_t rank, uint16_t first_slot,
                                   uint16_t last_slot);
    // 安装单个 SlotMetadataExport（vsegment 状态 + 对象元数据 + 账务），
    // 从 ImportSlotMetadata 抽取的复用主体。effective_route 为调用方解析好的
    // 目标路由（§15 路径传成员环推导路由；rank 路径传 LoadRingSlotAssign 的
    // 显式路由，owner 必须是本机）。安装后调用 RefreshVSegmentOwnership 激活。
    ErrorCode InstallSlotMetadataExport(
        const SlotMetadataExport& export_payload,
        const partition::PartitionRoute& effective_route,
        vsegment::VSegmentCommitBatch* batch = nullptr);
    std::thread reshard_driver_thread_;
    std::atomic<bool> reshard_driver_running_{false};
    std::mutex reshard_wake_mutex_;
    std::condition_variable reshard_wake_cv_;
    bool reshard_wake_pending_{false};
    // 防同 rank 并发驱动：进行中的 (rank -> 目标 epoch)。
    std::mutex reshard_inflight_mutex_;
    std::map<uint32_t, uint64_t> reshard_inflight_;
    // A1：读路径 slot 所有权校验。UpdateExpectedSlots 由心跳 resolver 与晋升
    // 路径调用，写 expected_owned_ 位图（ring 推导的应拥有集合）；MarkSlotsReady
    // 把已完成元数据导入的 slot 置入 ready_owned_。OwnsSlot 读 ready_owned_。
    // Gained slots become ready only after object/vsegment import succeeds.
    void UpdateExpectedSlots(const std::vector<uint16_t>& slots);
    void MarkSlotsReady(const std::vector<uint16_t>& slots);
    bool OwnsSlot(uint16_t slot) const;
    // 读/写路径 slot 可服务性校验（Phase 4）：ready_owned_ → OK；expected 但
    // !ready → SLOT_MIGRATING（新 owner 正在导入元数据）；否则 → SLOT_NOT_OWNED。
    ErrorCode CheckSlotServiceability(uint16_t slot) const;
    ErrorCode CheckVSegmentServiceability(const std::string& partition_id) const;
    // 读路径转发：解析任意 slot 的 owner master_id（基于一致性哈希环与
    // etcd 中的 primary master 列表）。解析失败返回 nullopt。供 GetReplicaList
    // / BatchGetReplicaList 在 OwnsSlot 失败时向 slot owner 转发请求。
    std::optional<std::string> ResolveSlotOwnerMasterId(uint16_t slot) const;
    // GetReplicaList 的本地查询核心：不含 slot 所有权校验与转发，供
    // client-facing GetReplicaList 与 InterMasterGetReplicaList 复用。
    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaListLocal(
        const ObjectIdentity& object_id);
    // BatchGetReplicaList 的本地查询核心：不含 slot 所有权校验与转发，供
    // client-facing BatchGetReplicaList 与 InterMasterBatchGetReplicaList
    // 复用。
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaListLocal(const std::vector<std::string>& keys,
                             const TenantId& tenant_id);
    // root filesystem directory for persistent storage
    const std::string root_fs_dir_;
    // global 3fs/nfs segment size
    int64_t global_file_segment_size_;
    // storage backend eviction configuration
    const bool enable_disk_eviction_;
    const uint64_t quota_bytes_;
    const bool enable_multi_tenants_;
    const std::string tenant_quota_connector_type_;
    const std::string tenant_quota_connector_uri_;
    std::unique_ptr<TenantQuotaPolicyStore> tenant_quota_policy_store_;
    mutable std::mutex tenant_quota_policy_mutex_;
    mutable std::mutex tenant_quota_recompute_mutex_;
    ShardedTenantQuotaTable<1024> tenant_quota_table_;

    // HTTP metadata server pointer for cleanup on client timeout
    // nullptr means cleanup is disabled
    HttpMetadataServer* http_metadata_server_{nullptr};

    // Remote HTTP metadata client, used when the metadata server is deployed
    // separately. nullptr = no remote cleanup (co-located prefers the pointer).
    std::shared_ptr<MetadataStoragePlugin> http_metadata_remote_;

    // Cached HTTP metadata key prefix (initialized once at startup)
    std::string http_metadata_prefix_;

    // Async worker for remote cleanup: segments are enqueued from the client
    // monitor thread so a slow/unreachable server never blocks heartbeats.
    std::thread http_metadata_cleanup_thread_;
    std::atomic<bool> http_metadata_cleanup_running_{false};
    std::mutex http_metadata_cleanup_mutex_;
    std::condition_variable http_metadata_cleanup_cv_;
    std::vector<std::string> http_metadata_cleanup_queue_;

    void HttpMetadataCleanupThreadFunc();

    // Clean up HTTP metadata (mooncake/ram/*, mooncake/rpc_meta/*) for a
    // segment. For the co-located case this is synchronous (no network I/O);
    // for the remote case it enqueues to the async cleanup worker.
    void cleanupHttpMetadata(const std::string& segment_name);

    bool use_disk_replica_{false};

    // Segment management
    SegmentManager segment_manager_;
    NoFSegmentManager nof_segment_manager_;
    BufferAllocatorType memory_allocator_type_;
    const AllocationStrategyType allocation_strategy_type_;
    std::shared_ptr<AllocationStrategy> allocation_strategy_;

    bool enable_snapshot_restore_ = false;

    bool enable_snapshot_ = false;
    std::string snapshot_backup_dir_;
    bool use_snapshot_backup_dir_{false};
    uint64_t snapshot_interval_seconds_ = DEFAULT_SNAPSHOT_INTERVAL_SEC;
    uint64_t snapshot_child_timeout_seconds_ =
        DEFAULT_SNAPSHOT_CHILD_TIMEOUT_SEC;
    uint32_t snapshot_retention_count_ = DEFAULT_SNAPSHOT_RETENTION_COUNT;
    std::string snapshot_catalog_store_type_{};
    std::string snapshot_catalog_store_connstring_;
    std::unique_ptr<SnapshotObjectStore> snapshot_object_store_;
    std::unique_ptr<ha::SnapshotCatalogStore> snapshot_catalog_store_;
    std::unique_ptr<MasterSnapshotRepository> snapshot_repository_;
    std::unique_ptr<ha::MasterSnapshotCodec> snapshot_codec_;
    mutable std::shared_mutex snapshot_mutex_;

    // Discarded replicas management
    const std::chrono::seconds put_start_discard_timeout_sec_;
    const std::chrono::seconds put_start_release_timeout_sec_;
    const std::string cxl_path_;
    const size_t cxl_size_;
    bool enable_cxl_;

    class DiscardedReplicas {
       public:
        DiscardedReplicas() = delete;

        DiscardedReplicas(std::vector<Replica>&& replicas,
                          std::chrono::system_clock::time_point ttl)
            : replicas_(std::move(replicas)), ttl_(ttl), mem_size_(0) {
            for (auto& replica : replicas_) {
                if (replica.is_memory_replica()) {
                    mem_size_ += replica.get_memory_buffer_size();
                }
            }
            MasterMetricManager::instance().inc_put_start_discard_cnt(
                1, mem_size_);
        }

        ~DiscardedReplicas() {
            MasterMetricManager::instance().inc_put_start_release_cnt(
                1, mem_size_);
        }

        uint64_t memSize() const { return mem_size_; }

        bool isExpired(const std::chrono::system_clock::time_point& now) const {
            return ttl_ <= now;
        }

       private:
        friend class MetadataSerializer;
        std::vector<Replica> replicas_;
        std::chrono::system_clock::time_point ttl_;
        uint64_t mem_size_;
    };
    std::mutex discarded_replicas_mutex_;
    std::list<DiscardedReplicas> discarded_replicas_
        GUARDED_BY(discarded_replicas_mutex_);
    size_t offloading_queue_limit_ = 50000;
    double offload_cap_ratio_ = 0.5;

    // Task manager
    ClientTaskManager task_manager_;

    struct ActiveDrainTask {
        UUID task_id;
        TenantId tenant_id;
        std::string key;
        std::string source_segment;
        std::string target_segment;
        size_t bytes;
        std::string unit_key;
    };

    struct DrainJob {
        mutable std::mutex mutex;
        UUID id;
        JobType type{JobType::DRAIN};
        JobStatus status{JobStatus::CREATED};
        CreateDrainJobRequest request;
        std::chrono::system_clock::time_point created_at;
        std::chrono::system_clock::time_point last_updated_at;
        std::string message;
        uint64_t succeeded_units{0};
        uint64_t failed_units{0};
        uint64_t blocked_units{0};
        uint64_t migrated_bytes{0};
        std::unordered_map<UUID, ActiveDrainTask, boost::hash<UUID>>
            active_tasks;
        std::unordered_set<std::string> completed_unit_keys;
        std::unordered_map<std::string, uint32_t> retry_counts;
        std::unordered_set<std::string> terminal_failed_unit_keys;
    };

    static constexpr uint32_t kMaxDrainUnitRetries = 3;

    tl::expected<void, ErrorCode> ValidateDrainRequest(
        const CreateDrainJobRequest& request);
    tl::expected<void, ErrorCode> ValidateDrainRequestLocked(
        ScopedSegmentAccess& segment_access,
        const CreateDrainJobRequest& request);
    void ProcessDrainJobs();
    void RefreshDrainJobTasks(DrainJob& job);
    void ScheduleDrainJobTasks(DrainJob& job);
    bool MaybeCompleteDrainJob(DrainJob& job);
    std::optional<std::string> SelectDrainTargetForKey(
        const ObjectMetadata& metadata, const std::string& source_segment,
        const std::vector<std::string>& requested_targets);
    std::string MakeDrainUnitKey(const TenantId& tenant_id,
                                 const std::string& key,
                                 const std::string& source_segment) const;

    std::thread job_dispatch_thread_;
    std::atomic<bool> job_dispatch_running_{false};
    static constexpr uint64_t kJobDispatchThreadSleepMs = 500;
    std::mutex job_mutex_;
    std::unordered_map<UUID, std::shared_ptr<DrainJob>, boost::hash<UUID>>
        drain_jobs_ GUARDED_BY(job_mutex_);

    std::unique_ptr<KvEventPublisher> kv_event_publisher_;

    static KvEventConfig BuildKvEventConfig(const MasterServiceConfig& config);
    static std::string MediumForReplicaType(ReplicaType replica_type);
    static std::string MediumForMetadata(const ObjectMetadata& metadata);
    void PublishKvStored(const std::string& key, ReplicaType replica_type,
                         const ObjectMetadata& metadata,
                         const TenantId& tenant_id);
    void PublishKvRemoved(const std::string& key,
                          const ObjectMetadata& metadata,
                          const TenantId& tenant_id);
    void PublishKvRemoved(const std::string& key, const std::string& medium,
                          const TenantId& tenant_id,
                          const std::string& group_id);
    void PublishKvRemovedAfterEvict(const std::string& key,
                                    uint64_t freed_bytes,
                                    const std::string& medium,
                                    const ObjectMetadata& metadata,
                                    const TenantId& tenant_id);

    // OpLog publishing
    std::shared_ptr<HaKvBackend> batch_oplog_kv_backend_;
    std::unique_ptr<OpLogBatchStorage> batch_oplog_storage_;
    std::unique_ptr<OrderedOpLogWriter> ordered_oplog_writer_;

    // OpLog 流自恢复（冷接管 / 同身份重启）：source 定局登记与 4.2 卡死
    // 分支的分区级日志去重（防 heartbeat 周期重试刷屏）。
    std::mutex oplog_replay_mutex_;
    std::set<std::string> oplog_replay_finalized_sources_;
    std::set<std::string> missing_recovery_warned_partitions_;

    // OpLog publishing helpers
    std::string SerializeMetadataForOpLog(const ObjectMetadata& metadata) const;
    std::string SerializeMetadataForOpLogWithoutMemReplicas(
        const ObjectMetadata& metadata) const;
    std::string SerializeMetadataForOpLogFromReplicaDescriptors(
        const UUID& client_id, uint64_t size,
        const std::vector<Replica::Descriptor>& replicas,
        const std::string& group_id = "",
        ObjectDataType data_type = ObjectDataType::UNKNOWN) const;
    ErrorCode InitializeBatchOpLogWriter(std::shared_ptr<HaKvBackend> backend);
    ErrorCode RefreshVSegmentOwnership(
        const std::string& acquiring = {},
        vsegment::VSegmentCommitBatch* batch = nullptr);
    tl::expected<uint64_t, ErrorCode> AppendOpLogVisibleBeforeDurable(
        OpType type, const std::string& tenant_id, const std::string& key,
        const std::string& payload);
    tl::expected<OpLogEntry, ErrorCode> AppendOpLogWithDurableFinalize(
        OpType type, const std::string& tenant_id, const std::string& key,
        const std::string& payload, DurableFinalizeCallback callback);
    tl::expected<OrderedOpLogWriter::Reservation, ErrorCode>
    ReserveBatchOpLogSlot();
    tl::expected<OpLogEntry, ErrorCode> AppendReservedOpLogWithDurableFinalize(
        OrderedOpLogWriter::Reservation&& reservation, OpType type,
        const std::string& tenant_id, const std::string& key,
        const std::string& payload, DurableFinalizeCallback callback);

    // Invalid endpoints from standby that don't exist locally
    std::unordered_set<std::string> invalid_replica_endpoints_;
    // Guards invalid_replica_endpoints_. The read path (IsReplicaReadable, on
    // GetReplicaList) runs on many threads without snapshot_mutex_, while the
    // set is written during standby restore / remount / mount. A shared_mutex
    // lets concurrent readers proceed and serializes writers.
    mutable std::shared_mutex invalid_endpoints_mutex_;

    // Keep DummyBufferAllocator alive after standby restore.
    // Key: transport_endpoint, Value: allocator.
    std::unordered_map<std::string, std::shared_ptr<BufferAllocatorBase>>
        standby_allocator_keepalive_;
    std::vector<StandbySegmentInfo> standby_memory_segments_;
    std::unordered_map<std::string, uint64_t> standby_accounted_memory_bytes_;

    ErrorCode ValidateStandbyRemountSegment(const Segment& segment) const;

    bool IsReplicaReadable(const Replica& replica) const;
    bool IsMemoryReplicaEvictable(const Replica& replica) const;

    /**
     * Segment lifecycle persist helper. Tries to durably persist the
     * SEGMENT_MOUNT / SEGMENT_UNMOUNT entry up-front; on failure enqueues
     * the same OpLogEntry (with its already-allocated sequence_id) for
     * background retry so the standby segment registry eventually
     * converges. Suitable for paths where the local segment commit has
     * already happened (UnmountSegment) and rolling back is impossible.
     */
    void PersistSegmentOpForHAOrEnqueue(const char* why, OpType type,
                                        const std::string& key,
                                        const std::string& payload);
    void PersistSegmentOpForHAOrEnqueue(const char* why, OpType type,
                                        const TenantId& tenant_id,
                                        const std::string& key,
                                        const std::string& payload);

    /**
     * Helper to persist REMOVE OpLog for a key with strong-consistency.
     * @return OK on success, error on persist failure (caller must skip erase)
     */
    tl::expected<void, ErrorCode> PersistRemoveForHA(const char* why,
                                                     const std::string& key);
    tl::expected<void, ErrorCode> PersistRemoveForHA(const char* why,
                                                     const TenantId& tenant_id,
                                                     const std::string& key);

    /**
     * Build replica descriptors after removing replicas matching pred_fn.
     * Returns empty if no complete replicas remain.
     */
    std::vector<Replica::Descriptor> BuildRemainingReplicaDescriptors(
        const ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& should_remove) const;
};

}  // namespace mooncake
