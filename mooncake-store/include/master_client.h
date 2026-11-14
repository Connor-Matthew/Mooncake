#pragma once

#include <atomic>
#include <csignal>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>
#include <variant>
#include <boost/functional/hash.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_io/client_pool.hpp>
#ifdef YLT_ENABLE_IBV
#include <ylt/coro_io/ibverbs/ib_socket.hpp>
#endif

#include "environ.h"
#include "client_metric.h"
#include "config/rpc_protocol_config.h"
#include "config/rpc_timeout_config.h"
#include "mooncake_logging.h"
#include "replica.h"
#include "segment.h"
#include "types.h"
#include "rpc_types.h"
#include "master_metric_manager.h"
#include "store_rpc_client_io_context.h"
#include "task_manager.h"
#include "metadata_store.h"
#include "partition/partition_router.h"
#include "vsegment/vsegment.h"
#include "vsegment/vsegment_transfer.h"

namespace mooncake {

static const std::string kDefaultMasterAddress = "localhost:50051";

// 单 key 定向 RPC（InvokeRoutedWithSlotRetry）的分段计时。
// 各窗口边界由模板填充；仅调用方传入指针时才有计时开销。
// 对应日志：query_rpc_breakdown（GetReplicaList 消费）。
struct RoutedCallTiming {
    int64_t rpc_first_us = 0;     // 首次 RPC 窗（含连接池取用+协程等待）
    int64_t route_refresh_us = 0;  // SLOT_NOT_OWNED/STALE_ROUTE 重试的
                                   // etcd 路由全表重载窗
    int64_t retry_switch_us = 0;   // 重试前的 submaster 重切窗
    int64_t rpc_retry_us = 0;      // 二次 RPC 窗
    int64_t backoff_us = 0;        // SLOT_MIGRATING 有界退避总窗
                                   // （sleep+重试 RPC）
    int retry_count = 0;           // 路由刷新重试次数（0/1）
    int backoff_count = 0;         // 迁移退避重试次数（0-3）
};

namespace detail {

template <typename Variant, typename T>
struct variant_contains : std::false_type {};

template <typename... Ts, typename T>
struct variant_contains<std::variant<Ts...>, T>
    : std::bool_constant<(std::is_same_v<Ts, T> || ...)> {};

template <typename Variant, typename T>
inline constexpr bool variant_contains_v =
    variant_contains<std::decay_t<Variant>, T>::value;

template <typename SocketConfigVariant>
inline void MaybeEnableRdmaSocketConfig(SocketConfigVariant& socket_config) {
#ifdef YLT_ENABLE_IBV
    if constexpr (variant_contains_v<SocketConfigVariant,
                                     coro_io::ib_socket_t::config_t>) {
        socket_config = coro_io::ib_socket_t::config_t{};
    }
#endif
}

// Applies the RPC timeout overrides to any mooncake-store RPC client config.
// Shared by the client->master pool and the store->store offload pool so the
// two cannot drift. Absent overrides leave the caller's defaults in place.
// A negative request timeout disables the per-request timer.
template <typename ClientConfig>
inline void ApplyRpcTimeoutOverrides(ClientConfig& client_config,
                                     const RpcTimeoutConfig& config) {
    if (config.request_timeout.has_value()) {
        client_config.request_timeout_duration = *config.request_timeout;
    }
    if (config.connect_timeout.has_value()) {
        client_config.connect_timeout_duration = *config.connect_timeout;
    }
}

inline RpcClientPool::PoolConfig MakeMasterRpcClientPoolConfig(
    bool ha_enabled = false) {
    RpcClientPool::PoolConfig config;
    if (ha_enabled) {
        // HA monitor and heartbeat loops own the retry schedule. Keep each
        // attempt bounded so a deleted pod IP that silently drops SYNs cannot
        // block failover on YLT's default 4 x 30-second retry budget.
        config.connect_retry_count = 0;
        config.reconnect_wait_time = std::chrono::milliseconds{0};
        config.client_config.connect_timeout_duration = std::chrono::seconds{1};
    }

    if (RpcProtocolConfig::FromEnvironment().use_rdma) {
        MaybeEnableRdmaSocketConfig(config.client_config.socket_config);
    }

    ApplyRpcTimeoutOverrides(config.client_config,
                             RpcTimeoutConfig::FromEnvironment());
    // Pool-level sizing & idle collection (MC_YLT_RPC_POOL_*). This config
    // feeds the request-serving accessors (client_accessor_ /
    // targeted_accessor_). Without this wiring the env vars only reached the
    // heartbeat-only client_pools_, and request pools silently ran with
    // coro_io defaults (idle_timeout=30s), whose idle collector closes free
    // connections every 30s and causes periodic latency spikes when requests
    // race with the cleanup window.
    config.max_connection =
        Environ::Get().GetYltRpcPoolMaxConnection(config.max_connection);
    config.idle_timeout = std::chrono::milliseconds(
        Environ::Get().GetYltRpcPoolIdleTimeoutMs(
            config.idle_timeout.count()));
    config.short_connect_idle_timeout = std::chrono::milliseconds(
        Environ::Get().GetYltRpcPoolShortIdleTimeoutMs(
            config.short_connect_idle_timeout.count()));

    MC_LOG(INFO) << "Master RPC pool config: max_connection="
                 << config.max_connection << " idle_timeout_ms="
                 << config.idle_timeout.count()
                 << " short_connect_idle_timeout_ms="
                 << config.short_connect_idle_timeout.count();
    return config;
}

}  // namespace detail

/**
 * @brief Client for interacting with the mooncake master service
 */
class MasterClient {
   public:
    MasterClient(const UUID& client_id, MasterClientMetric* metrics = nullptr,
                 std::string tenant_id = "default")
        : client_accessor_(GetStoreRpcClientIoContextPool(), 
                           detail::MakeMasterRpcClientPoolConfig()),
          ha_control_client_accessor_(
              GetStoreRpcClientIoContextPool(),
              detail::MakeMasterRpcClientPoolConfig(/*ha_enabled=*/true)),
          ha_probe_client_accessor_(
              GetStoreRpcClientIoContextPool(),
              detail::MakeMasterRpcClientPoolConfig(/*ha_enabled=*/true)),
          targeted_accessor_(GetStoreRpcClientIoContextPool(),
                             detail::MakeMasterRpcClientPoolConfig()),
          client_id_(client_id),
          tenant_id_(std::move(tenant_id)),
          metrics_(metrics) {}
    // Drains in-flight RPCs before the pool member is released (#3909).
    ~MasterClient();

    void EnableHaConnectionPolicy();
    tl::expected<void, ErrorCode> PublishGatherEndpoint(
        const std::string& owner, const std::string& endpoint,
        bool remove = false);
    tl::expected<std::string, ErrorCode> ResolveGatherEndpoint(
        const std::string& owner);

    const std::string& tenant_id() const { return tenant_id_.value(); }

    MasterClient(const MasterClient&) = delete;
    MasterClient& operator=(const MasterClient&) = delete;

    /**
     * @brief Connects to the master service
     * @param master_addr Master service address (IP:Port)
     * @return Per-key PutStartResult values or error codes
     */
    [[nodiscard]] ErrorCode Connect(
        const std::string& master_addr = kDefaultMasterAddress);

    /**
     * @brief Loads the slot -> submaster mapping from the etcd KV view
     * snapshot into the partition router. Must be called after etcd is
     * reachable and the cluster namespace (cluster_id) is known.
     * @param etcd_endpoints Etcd endpoints, semicolon separated.
     * @param cluster_namespace Cluster namespace used to locate the snapshot.
     * @return ErrorCode indicating success/failure.
     */
    [[nodiscard]] ErrorCode LoadRoutingFromEtcd(
        const std::string& etcd_endpoints,
        const std::string& cluster_namespace);

    /**
     * @brief Resolves the submaster (primary_master_id) that owns the slot of
     * the given key, using the currently loaded partition routing table.
     * @param key Object key.
     * @return submaster id on success, std::nullopt if no mapping is loaded.
     */
    [[nodiscard]] std::optional<std::string> ResolveSubmaster(
        const std::string& key) const;

    /**
     * @brief 按指定 tenant 解析 key 的 owner submaster 地址（供 offload
     * 控制面按 task 分组定向，task 携带自身 tenant 而非 client 默认
     * tenant）。路由表未加载（单 master 模式）返回 nullopt，调用方据此
     * 回退非定向路径。miss 不打日志——批量分组场景由调用方聚合计数。
     */
    [[nodiscard]] std::optional<std::string> ResolveSubmasterFor(
        const std::string& tenant_id, const std::string& key) const;

    /**
     * @brief Checks if an object exists
     * @param object_key Key to query
     * @return tl::expected<bool, ErrorCode> indicating exist or not
     */
    [[nodiscard]] tl::expected<bool, ErrorCode> ExistKey(
        const std::string& object_key);

    /**
     * @brief Point-in-time existence check that grants no read lease
     * @param object_key Key to query
     * @return tl::expected<bool, ErrorCode> indicating exist or not at the
     * time of the call; the object may still be evicted afterwards
     */
    [[nodiscard]] tl::expected<bool, ErrorCode> ProbeKey(
        const std::string& object_key);

    /**
     * @brief Checks if multiple objects exist
     * @param object_keys Vector of keys to query
     * @return Vector containing existence status for each key
     */
    [[nodiscard]] std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& object_keys);

    /**
     * @brief Probe multiple objects, optionally leasing the last complete
     * candidate. LastHitOnly leases the last all-true candidate in the results.
     * @param object_keys Vector of keys to query
     * @return Per-key existence results in input order
     */
    [[nodiscard]] std::vector<tl::expected<bool, ErrorCode>> BatchProbeKey(
        const std::vector<std::string>& object_keys,
        const GrantLeasePolicy& policy = {});

    /**
     * @brief Calculate Store-observed cache reuse metrics
     * @param object_keys None
     * @return Map containing metrics. Legacy hit-rate keys describe cumulative
     * Store-side hits normalized by current cached object counts, not
     * end-to-end request/token hit ratios.
     */
    [[nodiscard]] tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats();

    /**
     * @brief Batch query IP addresses for multiple client IDs.
     * @param client_ids Vector of client UUIDs to query.
     * @return An expected object containing a map from client_id to their IP
     * address lists on success, or an ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    /**
     * @brief Batch clear KV cache for specified object keys on a specific
     * segment for a given client.
     * @param object_keys Vector of object key strings to clear.
     * @param client_id The UUID of the client that owns the object keys.
     * @param segment_name The name of the segment (storage device) to clear
     * from.
     * @return An expected object containing a vector of successfully cleared
     * object keys on success, or an ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<std::vector<std::string>, ErrorCode>
    BatchReplicaClear(const std::vector<std::string>& object_keys,
                      const UUID& client_id, const std::string& segment_name);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @param object_info Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] tl::expected<GetReplicaListResponse, ErrorCode>
    GetReplicaList(const std::string& object_key);
    [[nodiscard]] tl::expected<GetReplicaListResponse, ErrorCode>
    GetReplicaList(const std::string& object_key, const std::string& tenant_id);
    [[nodiscard]] vsegment::VSegmentPutStartResult VSegmentPutStart(
        const std::string& partition_id, uint64_t route_epoch,
        const std::string& operation_id, uint64_t length,
        const std::string& profile_name = {});
    [[nodiscard]] ErrorCode VSegmentPutEnd(
        const VSegmentDescriptor& replica, uint64_t route_epoch,
        const std::string& operation_id, const std::string& object_id);
    [[nodiscard]] ErrorCode VSegmentPutRevoke(
        const std::string& partition_id, const std::string& vsegment_id,
        uint64_t route_epoch, const std::string& operation_id);

    /**
     * @brief Retrieves replica lists for object keys that match a regex
     * pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing a map from object keys to their
     * replica descriptors on success, or an ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_keys Keys to query
     * @param object_infos Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& object_keys);
    [[nodiscard]] std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& object_keys,
                        const std::string& tenant_id);

    /**
     * @brief Read-only variants of GetReplicaList / BatchGetReplicaList.
     *
     * Backed by the admin read-only RPCs: the master grants no lease, does
     * not trigger promotion-on-hit, and does not update valid_get metrics.
     * Note the response still carries the master's default lease_ttl_ms
     * value; callers that need lease semantics must force it to 0 (see
     * Client::QueryReadOnly).
     *
     * Intended for best-effort background paths (e.g. SSD prefetch metadata
     * classification) that must not perturb hot-path state.
     */
    [[nodiscard]] tl::expected<GetReplicaListResponse, ErrorCode>
    GetReplicaListReadOnly(const std::string& object_key);
    [[nodiscard]] tl::expected<GetReplicaListResponse, ErrorCode>
    GetReplicaListReadOnly(const std::string& object_key,
                           const std::string& tenant_id);
    [[nodiscard]] std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaListReadOnly(const std::vector<std::string>& object_keys);
    [[nodiscard]] std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaListReadOnly(const std::vector<std::string>& object_keys,
                                const std::string& tenant_id);

    /**
     * @brief Starts a put operation
     * @param key Object key
     * @param slice_lengths Vector of slice lengths
     * @param value_length Total value length
     * @param config Replication configuration
     * @return tl::expected<PutStartResult, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<PutStartResult, ErrorCode> PutStart(
        const std::string& key, const std::vector<size_t>& slice_lengths,
        const ReplicateConfig& config);

    /**
     * @brief Starts a batch of put operations for N objects
     * @param keys Vector of object key
     * @param value_lengths Vector of total value lengths
     * @param slice_lengths Vector of vectors of slice lengths
     * @param config Replication configuration
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<tl::expected<PutStartResult, ErrorCode>>
    BatchPutStart(const std::vector<std::string>& keys,
                  const std::vector<std::vector<uint64_t>>& slice_lengths,
                  const ReplicateConfig& config);

    /**
     * @brief Ends a put operation
     * @param object_meta Object key and optional checksum
     * @param replica_type Type of replica (memory or disk)
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> PutEnd(
        const ObjectMeta& object_meta, ReplicaType replica_type,
        const std::string& operation_id = "");

    /**
     * @brief Ends a put operation for a batch of objects
     * @param keys Vector of object keys
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const std::vector<ObjectMeta>& object_metas,
        ReplicaType replica_type = ReplicaType::ALL);

    /**
     * @brief Revokes a put operation
     * @param key Object key
     * @param replica_type Type of replica (memory or disk)
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> PutRevoke(
        const std::string& key, ReplicaType replica_type,
        const std::string& operation_id = "");

    /**
     * @brief Revokes a put operation for a batch of objects
     * @param keys Vector of object keys
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const std::vector<std::string>& keys,
        ReplicaType replica_type = ReplicaType::ALL);

    /**
     * @brief Starts an upsert operation (insert or update)
     * @param key Object key
     * @param slice_lengths Vector of slice lengths
     * @param config Replication configuration
     * @return PutStartResult on success, ErrorCode on failure
     */
    [[nodiscard]] tl::expected<PutStartResult, ErrorCode> UpsertStart(
        const std::string& key, const std::vector<size_t>& slice_lengths,
        const ReplicateConfig& config);

    [[nodiscard]] std::vector<tl::expected<PutStartResult, ErrorCode>>
    BatchUpsertStart(const std::vector<std::string>& keys,
                     const std::vector<std::vector<uint64_t>>& slice_lengths,
                     const ReplicateConfig& config);

    [[nodiscard]] tl::expected<void, ErrorCode> UpsertEnd(
        const ObjectMeta& object_meta, ReplicaType replica_type,
        const std::string& operation_id = "");

    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchUpsertEnd(
        const std::vector<ObjectMeta>& object_metas);

    [[nodiscard]] tl::expected<void, ErrorCode> UpsertRevoke(
        const std::string& key, ReplicaType replica_type,
        const std::string& operation_id = "");

    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchUpsertRevoke(
        const std::vector<std::string>& keys);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @param force If true, skip lease and replication task checks
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> Remove(const std::string& key,
                                                       bool force = false);

    /**
     * @brief Removes objects from the master whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @param force If true, skip lease and replication task checks
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<long, ErrorCode> RemoveByRegex(
        const std::string& str, bool force = false);

    /**
     * @brief Removes all objects and all its replicas
     * @param force If true, skip lease and replication task checks
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    [[nodiscard]] tl::expected<long, ErrorCode> RemoveAll(bool force = false);

    /**
     * @brief Batch remove objects and all their replicas
     * @param keys List of keys to remove
     * @param force If true, skip lease and replication task checks
     * @return Vector of expected results for each key
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchRemove(
        const std::vector<std::string>& keys, bool force = false);

    /**
     * @brief Registers a segment to master for allocation
     * @param segment Segment to register
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountSegment(
        const Segment& segment);

    [[nodiscard]] tl::expected<void, ErrorCode> MountSSDSegment(
        const Segment& segment);

    /**
     * @brief Registers a NoF ssd segment to master for allocation
     * @param segment Segment to register
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountNoFSegment(
        const NoFSegment& segment);

    /**
     * @brief Re-mount segments, invoked when the client is the first time to
     * connect to the master or the client Ping TTL is expired and need
     * to remount. This function is idempotent. Client should retry if the
     * return code is not ErrorCode::OK.
     * @param segments Segments to remount
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> ReMountSegment(
        const std::vector<Segment>& segments);

    /**
     * @brief Re-mount NoF ssd segments, invoked when the client is the first
     * time to connect to the master or the client Ping TTL is expired and need
     * to remount. This function is idempotent. Client should retry if the
     * return code is not ErrorCode::OK.
     * @param segments Segments to remount
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> ReMountNoFSegment(
        const std::vector<NoFSegment>& segments);

    /**
     * @brief Unregisters a memory segment from master
     * @param segment_id ID of the segment to unmount
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> UnmountSegment(
        const UUID& segment_id);

    [[nodiscard]] tl::expected<void, ErrorCode> GracefulUnmountSegment(
        const UUID& segment_id, uint64_t grace_period_ms);

    /**
     * @brief Unregisters a NoF ssd segment from master
     * @param segment_id ID of the segment to unmount
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> UnmountNoFSegment(
        const UUID& segment_id);

    /**
     * @brief Gets all mounted NoF ssd segments from master
     * @return tl::expected<std::vector<MountedNoFSegmentSnapshot>, ErrorCode>
     * containing all mounted segments
     */
    [[nodiscard]] tl::expected<std::vector<NoFSegment>, ErrorCode>
    GetAllNoFSegments();

    /**
     * @brief Fetch all registered segment names from master.
     * Used for pre-establishing connections during client setup.
     * @return Vector of segment names (format: {ip}:{port}) on success,
     * ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<std::vector<std::string>, ErrorCode>
    GetAllSegments();

    /**
     * @brief Gets all mounted NoF segments that match a segment name together
     * with their owner client ids.
     * @param segment_name Mounted NoF segment name
     * @return Matching segment owner info list
     */
    [[nodiscard]] tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode>
    GetNoFSegmentsByName(const std::string& segment_name);

    /**
     * @brief Gets the cluster ID for the current client to use as subdirectory
     * name
     * @return GetClusterIdResponse containing the cluster ID
     */
    [[nodiscard]] tl::expected<std::string, ErrorCode> GetFsdir();

    [[nodiscard]] tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatusById(
        const UUID& segment_id);

    /**
     * @brief 定向 QuerySegmentStatusById：向指定 submaster 查询 segment 状态
     * （不切换当前连接）。用于多 submaster 优雅卸载时确认所有 primary
     * submaster 均已移除该 segment。
     */
    [[nodiscard]] tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatusByIdTo(
        const std::string& address, const UUID& segment_id);

    [[nodiscard]] tl::expected<GetStorageConfigResponse, ErrorCode>
    GetStorageConfig();

    /**
     * @brief Pings master to check its availability
     * @return tl::expected<PingResponse, ErrorCode>
     * containing view version and client status
     */
    [[nodiscard]] tl::expected<PingResponse, ErrorCode> Ping();

    /**
     * @brief 返回 client_accessor_ 当前连接的 submaster 地址（IP:Port）。
     * 供后台心跳去重使用——HeartbeatAllSubmasters 跳过当前 active 地址，
     * 避免与主循环的 Ping() 对同一 submaster 重复 ping。
     */
    std::string GetCurrentAddress() const;

    /**
     * @brief 枚举路由表中所有 primary submaster 地址（去重）。来源为
     * partition_router_（master_id 的值即 RPC 端点 address）。单 master
     * 模式下路由表为空，返回空 vector——offload 控制面调用方据此回退
     * 非定向（invoke_rpc）路径，与内存段全量 mount 的回退语义一致。
     */
    [[nodiscard]] std::vector<std::string> GetSubmasterAddresses() const;

    /**
     * @brief 定向 Ping：向指定 submaster 发送 Ping（不切换当前连接）。
     * 用于 client 侧多 submaster 心跳，防止各 submaster 因收不到 ping 而
     * 误判 client 过期并卸载其 segment。返回 NEED_REMOUNT 时调用方应对该
     * submaster 重新 mount。
     */
    [[nodiscard]] tl::expected<PingResponse, ErrorCode> PingTo(
        const std::string& address);

    /**
     * @brief 定向 MountSegment：向指定 submaster 注册 segment（不切换当前
     * 连接）。用于 client 侧全量 mount——同一 segment 挂载到所有 primary
     * submaster，使任何 slot owner 都能本地分配副本。
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountSegmentTo(
        const std::string& address, const Segment& segment);

    /**
     * @brief 定向 UnmountSegment：向指定 submaster 注销 segment（不切换当前
     * 连接）。与全量 mount 对称，保证 segment 生命周期在所有 submaster 闭合。
     */
    [[nodiscard]] tl::expected<void, ErrorCode> UnmountSegmentTo(
        const std::string& address, const UUID& segment_id);

    /**
     * @brief 定向 GracefulUnmountSegment：向指定 submaster 发起优雅卸载（不
     * 切换当前连接）。与全量 unmount 对称，保证优雅卸载在所有 submaster 生效。
     */
    [[nodiscard]] tl::expected<void, ErrorCode> GracefulUnmountSegmentTo(
        const std::string& address, const UUID& segment_id,
        uint64_t grace_period_ms);


    /**
     * @brief Mounts a local disk segment into the master.
     * @param enable_offloading If true, enables offloading (write-to-file).
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountLocalDiskSegment(
        const UUID& client_id, bool enable_offloading);

    /**
     * @brief Deregisters this client's local disk segment from the master,
     * dropping the LOCAL_DISK replicas it owns. Idempotent.
     */
    [[nodiscard]] tl::expected<void, ErrorCode> UnmountLocalDiskSegment(
        const UUID& client_id);

    /**
     * @brief 定向 MountLocalDiskSegment：向指定 submaster 注册 LOCAL_DISK 段
     * （不切换当前连接）。多 submaster 模式下 LOCAL_DISK 段与内存段对称，
     * 需挂载到所有 primary submaster，使任何 slot owner 都能本地执行
     * offload 决策；单 master 模式调用方走非定向变体。
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountLocalDiskSegmentTo(
        const std::string& address, const UUID& client_id,
        bool enable_offloading);

    /**
     * @brief 定向卸载 worker 的 LOCAL_DISK 段：向指定 submaster 清理该
     * client 的 LocalDiskSegment（ssd 容量记账、offloading 队列），与
     * MountLocalDiskSegmentTo 对称。用于 submaster 退出集群时（worker 侧
     * RefreshSubmasterAddresses removed 分支）保证容量记账对称闭合。
     * 幂等。
     */
    [[nodiscard]] tl::expected<void, ErrorCode> UnmountLocalDiskSegmentTo(
        const std::string& address, const UUID& client_id);

    /**
     * @brief Heartbeat call to collect object-level statistics and retrieve the
     * set of non-persisted objects.
     * @param enable_offloading Indicates whether persistence is enabled for
     * this segment.
     */
    [[nodiscard]] tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
    OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading);

    /**
     * @brief 定向 OffloadObjectHeartbeat：向指定 submaster 拉取该 master
     * slot 的卸载任务（不切换当前连接）。多 submaster 下各 master 的
     * offloading_objects 队列只含自己 slot 的任务，worker 心跳需逐
     * master fan-out 拉取后合并执行。
     */
    [[nodiscard]] tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
    OffloadObjectHeartbeatTo(const std::string& address, const UUID& client_id,
                             bool enable_offloading);

    /**
     * @brief Poll whether master has requested a full SSD clear.
     * @return true if client should clear all SSD files
     */
    [[nodiscard]] tl::expected<bool, ErrorCode> PollRemoveAll();

    WeightRpcResult<WeightRevisionMetadata> BeginWeightImport(
        const BeginWeightImportRequest& request);
    WeightRpcResult<WeightRevisionMetadata> CommitWeightImport(
        const CommitWeightImportRequest& request);
    WeightRpcResult<WeightRevisionMetadata> AbortWeightImport(
        const AbortWeightImportRequest& request);
    WeightRpcResult<WeightRevisionView> GetWeightRevision(
        const GetWeightRevisionRequest& request);
    WeightRpcResult<ListWeightRevisionsResponse> ListWeightRevisions(
        const ListWeightRevisionsRequest& request);
    WeightRpcResult<WeightRevisionLease> AcquireWeightRevisionLease(
        const AcquireWeightRevisionLeaseRequest& request);
    WeightRpcResult<WeightRevisionLease> RenewWeightRevisionLease(
        const RenewWeightRevisionLeaseRequest& request);
    WeightRpcResult<void> ReleaseWeightRevisionLease(
        const ReleaseWeightRevisionLeaseRequest& request);
    WeightRpcResult<WeightResidencyOperation> StartWeightResidencyOperation(
        const StartWeightResidencyOperationRequest& request);
    WeightRpcResult<WeightResidencyOperation> QueryWeightOperation(
        const QueryWeightOperationRequest& request);
    WeightRpcResult<WeightRevisionMetadata> DeleteWeightRevision(
        const DeleteWeightRevisionRequest& request);
    /**
     * @brief 定向 PollRemoveAll：向指定 submaster 查询是否要求全量清空
     * SSD（不切换当前连接）。与 LOCAL_DISK 段全量挂载对称。
     */
    [[nodiscard]] tl::expected<bool, ErrorCode> PollRemoveAllTo(
        const std::string& address, const UUID& client_id);

    [[nodiscard]] tl::expected<void, ErrorCode> ReportSsdCapacity(
        const UUID& client_id, int64_t ssd_total_capacity_bytes);

    /**
     * @brief 定向 ReportSsdCapacity：向指定 submaster 上报 SSD 容量（不切换
     * 当前连接）。多 submaster 下每个 master 各自维护 LocalDiskSegment
     * 记账，容量需广播到所有已挂载的 submaster。
     */
    [[nodiscard]] tl::expected<void, ErrorCode> ReportSsdCapacityTo(
        const std::string& address, const UUID& client_id,
        int64_t ssd_total_capacity_bytes);

    /**
     * @brief 被动坏端点上报（方案二被动层）：传输失败后上报目标
     * transport_endpoint，master 标记进 invalid_replica_endpoints_（分配
     * 过滤 + 读路径跳过；worker 重挂载自愈）。多 submaster 下内存段全量
     * 挂载于所有 submaster，坏端点影响所有 master 的分配，需广播。
     */
    [[nodiscard]] tl::expected<void, ErrorCode> ReportInvalidReplicaEndpoints(
        const std::vector<std::string>& endpoints);

    /**
     * @brief 定向 ReportInvalidReplicaEndpoints：向指定 submaster 上报
     * （不切换当前连接）。与全量挂载广播对称。
     */
    [[nodiscard]] tl::expected<void, ErrorCode> ReportInvalidReplicaEndpointsTo(
        const std::string& address, const std::vector<std::string>& endpoints);

    /**
     * @brief Adds multiple new objects to a specified client in batch.
     * @param keys         A list of object keys (names) that were successfully
     * offloaded.
     * @param metadatas    The corresponding metadata for each offloaded object,
     * including size, storage location, etc.
     */
    [[nodiscard]] tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas);
    [[nodiscard]] tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas);

    /**
     * @brief 定向 NotifyOffloadSuccess：向指定 submaster 上报卸载结果（不
     * 切换当前连接）。多 submaster 下 worker 完成落盘后须按 task 的 slot
     * 归属定向发给 owner master，避免通知因业务地址切换落到非 owner 上
     * （master 侧已加 slot 归属校验拒绝错投）。
     */
    [[nodiscard]] tl::expected<void, ErrorCode> NotifyOffloadSuccessTo(
        const std::string& address, const UUID& client_id,
        const std::vector<OffloadTaskItem>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas);

    [[nodiscard]] tl::expected<std::vector<std::string>, ErrorCode>
    GetOffloadEndpoints();

    /**
     * @brief Heartbeat-driven pull of pending L2->L1 promotion work for a
     * client. Returns tenant-scoped tasks the caller should read from local
     * SSD and stage as MEMORY replicas via PromotionAllocStart +
     * NotifyPromotionSuccess.
     */
    [[nodiscard]] tl::expected<std::vector<PromotionTaskItem>, ErrorCode>
    PromotionObjectHeartbeat(const UUID& client_id);

    /**
     * @brief Registers a prefetch (SSD->DRAM promotion) task on the master.
     *
     * Creates a promotion_tasks entry consumed by PromotionAllocStart, but
     * deliberately bypasses the promotion-on-hit admission gate and the
     * holder's heartbeat mailbox, keeping the dedicated prefetch path
     * separate from promotion-on-hit. The caller must be the holder of the
     * key's LOCAL_DISK replica (the master checks holder_id == client_id).
     * Best-effort: PROMOTION_ALREADY_EXISTS means a MEMORY replica or an
     * in-flight promotion already exists and the caller should skip quietly.
     * The client's own tenant is used.
     */
    [[nodiscard]] tl::expected<void, ErrorCode> RegisterPrefetchTask(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id);

    /**
     * @brief 定向 PromotionObjectHeartbeat：向指定 submaster 拉取该 master
     * slot 的晋升任务（不切换当前连接）。与 OffloadObjectHeartbeatTo 对称。
     */
    [[nodiscard]] tl::expected<std::vector<PromotionTaskItem>, ErrorCode>
    PromotionObjectHeartbeatTo(const std::string& address,
                               const UUID& client_id);

    /** Fetch pending remove tasks without removing them from the queue. */
    [[nodiscard]] tl::expected<std::vector<RemoveTaskItem>, ErrorCode>
    RemoveObjectHeartbeat(const UUID& client_id);
    tl::expected<void, ErrorCode> AckRemoveObjectHeartbeat(
        const UUID& client_id, const std::vector<RemoveTaskItem>& tasks);

    /**
     * @brief 定向 RemoveObjectHeartbeat / AckRemoveObjectHeartbeat：向指定
     * submaster 拉取/确认 SSD tombstone 任务（不切换当前连接）。多
     * submaster 下 removed_keys 队列按 owner 分布，需逐 master 拉取。
     */
    [[nodiscard]] tl::expected<std::vector<RemoveTaskItem>, ErrorCode>
    RemoveObjectHeartbeatTo(const std::string& address, const UUID& client_id);
    [[nodiscard]] tl::expected<void, ErrorCode> AckRemoveObjectHeartbeatTo(
        const std::string& address, const UUID& client_id,
        const std::vector<RemoveTaskItem>& tasks);

    /**
     * @brief Stage a PROCESSING MEMORY replica for an existing key during
     * promotion. Returns the new replica's descriptor that the caller writes
     * via Transfer Engine.
     */
    [[nodiscard]] tl::expected<PromotionAllocStartResponse, ErrorCode>
    PromotionAllocStart(const UUID& client_id, const std::string& key,
                        uint64_t size,
                        const std::vector<std::string>& preferred_segments);
    [[nodiscard]] tl::expected<PromotionAllocStartResponse, ErrorCode>
    PromotionAllocStart(const UUID& client_id, const std::string& key,
                        const std::string& tenant_id, uint64_t size,
                        const std::vector<std::string>& preferred_segments);

    /**
     * @brief 定向 PromotionAllocStart：向指定 submaster 申请晋升 staging
     * 副本（不切换当前连接）。promotion 任务状态（promotion_tasks 条目 +
     * staged 副本）只存在于派发该任务的 source master 内存中，执行链
     *（AllocStart→TE write→Success/Failure）必须回到 source master 闭环，
     * 故按任务来源定向而非按 slot 现算 owner。
     */
    [[nodiscard]] tl::expected<PromotionAllocStartResponse, ErrorCode>
    PromotionAllocStartTo(const std::string& address, const UUID& client_id,
                          const std::string& key, const std::string& tenant_id,
                          uint64_t size,
                          const std::vector<std::string>& preferred_segments);

    /**
     * @brief Release master-side promotion task state after a client-side
     * failure that prevents the holder from calling NotifyPromotionSuccess.
     * Idempotent; returns OK if the task was already swept by the reaper.
     */
    [[nodiscard]] tl::expected<void, ErrorCode> NotifyPromotionFailure(
        const UUID& client_id, const std::string& key);
    [[nodiscard]] tl::expected<void, ErrorCode> NotifyPromotionFailure(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id);

    /** 定向 NotifyPromotionFailure：向任务来源 master 释放任务状态。 */
    [[nodiscard]] tl::expected<void, ErrorCode> NotifyPromotionFailureTo(
        const std::string& address, const UUID& client_id,
        const std::string& key, const std::string& tenant_id);

    /**
     * @brief Commit a staged MEMORY replica to COMPLETE; called after the
     * client has written the bytes via Transfer Engine.
     */
    [[nodiscard]] tl::expected<void, ErrorCode> NotifyPromotionSuccess(
        const UUID& client_id, const std::string& key);
    [[nodiscard]] tl::expected<void, ErrorCode> NotifyPromotionSuccess(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id);

    /** 定向 NotifyPromotionSuccess：向任务来源 master 提交晋升结果。 */
    [[nodiscard]] tl::expected<void, ErrorCode> NotifyPromotionSuccessTo(
        const std::string& address, const UUID& client_id,
        const std::string& key, const std::string& tenant_id);

    /**
     * @brief Start a copy operation
     * @param key Object key
     * @param src_segment Source segment name
     * @param tgt_segments Target segment names
     * @return tl::expected<CopyStartResponse, ErrorCode> indicating
     * success/failure
     */
    [[nodiscard]] tl::expected<CopyStartResponse, ErrorCode> CopyStart(
        const std::string& key, const std::string& src_segment,
        const std::vector<std::string>& tgt_segments);
    [[nodiscard]] tl::expected<CopyStartResponse, ErrorCode> CopyStart(
        const std::string& key, const std::string& tenant_id,
        const std::string& src_segment,
        const std::vector<std::string>& tgt_segments);
    [[nodiscard]] tl::expected<CopyStartResponse, ErrorCode>
    DynamicReplicaCopyStart(const std::string& key,
                            const std::string& tenant_id,
                            const std::string& src_segment,
                            const std::vector<std::string>& tgt_segments,
                            const UUID& dynamic_replication_lease_id,
                            uint64_t dynamic_replication_version_epoch);

    /**
     * @brief End a copy operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> CopyEnd(const std::string& key);
    [[nodiscard]] tl::expected<void, ErrorCode> CopyEnd(
        const std::string& key, const std::string& tenant_id);
    [[nodiscard]] tl::expected<void, ErrorCode> DynamicReplicaCopyEnd(
        const std::string& key, const std::string& tenant_id,
        const UUID& dynamic_replication_lease_id,
        uint64_t dynamic_replication_version_epoch);

    /**
     * @brief Revoke a copy operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> CopyRevoke(
        const std::string& key);
    [[nodiscard]] tl::expected<void, ErrorCode> CopyRevoke(
        const std::string& key, const std::string& tenant_id);
    [[nodiscard]] tl::expected<void, ErrorCode> DynamicReplicaCopyRevoke(
        const std::string& key, const std::string& tenant_id,
        const UUID& dynamic_replication_lease_id,
        uint64_t dynamic_replication_version_epoch);

    /**
     * @brief Start a move operation
     * @param key Object key
     * @param src_segment Source segment name
     * @param tgt_segment Target segment name
     * @return tl::expected<MoveStartResponse, ErrorCode> indicating
     * success/failure
     */
    [[nodiscard]] tl::expected<MoveStartResponse, ErrorCode> MoveStart(
        const std::string& key, const std::string& src_segment,
        const std::string& tgt_segment);
    [[nodiscard]] tl::expected<MoveStartResponse, ErrorCode> MoveStart(
        const std::string& key, const std::string& tenant_id,
        const std::string& src_segment, const std::string& tgt_segment);

    /**
     * @brief End a move operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MoveEnd(const std::string& key);
    [[nodiscard]] tl::expected<void, ErrorCode> MoveEnd(
        const std::string& key, const std::string& tenant_id);

    /**
     * @brief Revoke a move operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MoveRevoke(
        const std::string& key);
    [[nodiscard]] tl::expected<void, ErrorCode> MoveRevoke(
        const std::string& key, const std::string& tenant_id);

    /**
     * @brief Create a task to copy an object's replica to target segments
     * @param key Object key
     * @param targets Target segments
     * @return tl::expected<UUID, ErrorCode> Copy task ID on success,
     * ErrorCode on failure
     */
    [[nodiscard]] tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::vector<std::string>& targets);
    [[nodiscard]] tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::string& tenant_id,
        const std::vector<std::string>& targets);

    /**
     * @brief Create a task to move an object's replica from source segment to
     * target segment
     * @param key Object key
     * @param source Source segment
     * @param target Target segment
     * @return tl::expected<UUID, ErrorCode> Move task ID on success,
     * ErrorCode on failure
     */
    [[nodiscard]] tl::expected<UUID, ErrorCode> CreateMoveTask(
        const std::string& key, const std::string& source,
        const std::string& target);
    [[nodiscard]] tl::expected<UUID, ErrorCode> CreateMoveTask(
        const std::string& key, const std::string& tenant_id,
        const std::string& source, const std::string& target);

    /**
     * @brief Query a task by task id
     * @param task_id Task ID to query
     * @return tl::expected<QueryTaskResponse, ErrorCode> Task basic info
     * on success, ErrorCode on failure
     */
    [[nodiscard]] tl::expected<QueryTaskResponse, ErrorCode> QueryTask(
        const UUID& task_id);

    /**
     * @brief Fetch tasks assigned to a client
     * @param batch_size Number of tasks to fetch
     * @return tl::expected<std::vector<TaskAssignment>, ErrorCode> list of
     * tasks on success, ErrorCode on failure
     */
    [[nodiscard]] tl::expected<std::vector<TaskAssignment>, ErrorCode>
    FetchTasks(size_t batch_size);

    /**
     * @brief Mark the task as complete
     * @param task_complete Task complete request
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MarkTaskToComplete(
        const TaskCompleteRequest& task_complete);

    /**
     * @brief Notify master that a disk replica was evicted locally
     * @param key The evicted object key
     * @param replica_type DISK or LOCAL_DISK
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> EvictDiskReplica(
        const std::string& key, ReplicaType replica_type);
    [[nodiscard]] tl::expected<void, ErrorCode> EvictDiskReplica(
        const std::string& key, const std::string& tenant_id,
        ReplicaType replica_type);

    /**
     * @brief Evict a disk replica only if the master still records the
     * replica the caller failed on. A master that predates this RPC answers
     * with an unknown-handler error, so the caller can tell the precondition
     * was never enforced.
     * @param key The object key whose disk replica was evicted
     * @param tenant_id The tenant scope of the object
     * @param replica_type DISK, LOCAL_DISK, or DFS. DFS is accepted only for
     * a KVCS object-storage replica through this conditional API.
     * @param expected_replica_id The replica id the caller failed on
     * @return true if evicted, false if the current record is a different
     * replica (left untouched)
     */
    [[nodiscard]] tl::expected<bool, ErrorCode> EvictDiskReplicaIfCurrent(
        const std::string& key, const std::string& tenant_id,
        ReplicaType replica_type, ReplicaID expected_replica_id);

    /**
     * @brief Batch notify master that disk replicas were evicted locally.
     * @param keys The evicted object keys
     * @param replica_type DISK or LOCAL_DISK
     * @return Per-key results (RPC_FAIL on transport error, else per-key
     * status)
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>>
    BatchEvictDiskReplica(const std::vector<std::string>& keys,
                          ReplicaType replica_type);
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>>
    BatchEvictDiskReplica(const std::vector<std::string>& keys,
                          const std::string& tenant_id,
                          ReplicaType replica_type);

   private:
    /**
     * @brief Generic RPC invocation helper for single-result operations
     * @tparam ServiceMethod Pointer to WrappedMasterService member function
     * @tparam ReturnType The expected return type of the RPC call
     * @tparam Args Parameter types for the RPC call
     * @param args Arguments to pass to the RPC call
     * @return The result of the RPC call
     */
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode> invoke_rpc(
        Args&&... args);

    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode> invoke_rpc_with_pool(
        RpcClientPool& client_accessor, Args&&... args);

    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode>
    invoke_rpc_with_client_pool(
        const std::shared_ptr<RpcClientPool::ClientPool>& client_pool,
        Args&&... args);

    /**
     * @brief 定向 RPC：向指定 submaster 发请求，使用独立的 targeted_accessor_
     * 缓存 per-address pool，不切换 client_accessor_ 的"当前地址"。供后台心跳
     * /全量 mount/unmount 使用，避免与业务请求（SwitchToSubmaster + invoke_rpc）
     * 的当前地址切换竞态。
     */
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode> invoke_rpc_to(
        const std::string& address, Args&&... args);

    /**
     * @brief Generic RPC invocation helper for batch operations
     * @tparam ServiceMethod Pointer to WrappedMasterService member function
     * @tparam ResultType The expected return type of the RPC call
     * @tparam Args Parameter types for the RPC call
     * @param input_size Size of input batch for error handling
     * @param args Arguments to pass to the RPC call
     * @return Vector of results from the batch RPC call
     */
    template <auto ServiceMethod, typename ResultType, typename... Args>
    [[nodiscard]] std::vector<tl::expected<ResultType, ErrorCode>>
    invoke_batch_rpc(size_t input_size, Args&&... args);

    void WarmupRpcPool();

    /**
     * @brief Switches the underlying RPC pool to the submaster that owns the
     * slot of the given key (single-target switching). Keeps the current
     * connection when the routing table is not loaded (single-master mode).
     * @return ErrorCode::OK on switch (or no-op), otherwise an error.
     */
    [[nodiscard]] ErrorCode SwitchToSubmaster(const std::string& tenant_id,
                                              const std::string& key);

    // Refreshes the previously configured CVM snapshot after a server reports
    // SLOT_NOT_OWNED. The caller remains responsible for a bounded retry.
    [[nodiscard]] ErrorCode RefreshSubmasterRouting();

    // Resolves a vsegment Partition to its owner. Decimal partition ids map
    // directly to the existing KV slot table; named partitions use the
    // cluster-scoped PartitionRoute record. An empty address denotes the
    // legacy single-master mode.
    [[nodiscard]] tl::expected<std::string, ErrorCode>
    ResolveVSegmentSubmaster(const std::string& partition_id);

    /**
     * @brief 带 slot 迁移重试语义的单 RPC 调用（Phase 6）。
     * 对「已 SwitchToSubmaster 的 key」执行一次 RPC，并按错误码闭环处理：
     *   - SLOT_NOT_OWNED：环已变化 → 刷新路由 + 重切 submaster，重试一次；
     *   - SLOT_MIGRATING ：owner 已 expected 但元数据未就绪 → 退避（有界）后
     *     原地重试，不刷新（环本身正确）。
     * @tparam ServiceMethod 成员函数指针；ReturnType 为 RPC 返回值类型。
     * @param tenant_id / key 仅用于错误重试时的 SwitchToSubmaster 路由。
     * @param args 原样按值转发给 invoke_rpc（多次调用安全，不会被 move 掉）。
     */
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode> InvokeRoutedWithSlotRetry(
        RoutedCallTiming* timing, const std::string& tenant_id,
        const std::string& key, uint32_t rpc_stat_key, Args... args);

    /**
     * @brief Batch 路由自愈（与 InvokeRoutedWithSlotRetry 的单 key 版互补）。
     * 首轮批量分组 RPC 收到 STALE_ROUTE / SLOT_NOT_OWNED / SLOT_MIGRATING
     * / 无 owner 空分组（UNAVAILABLE_IN_CURRENT_STATUS）的 key 子集：
     * 含 SLOT_MIGRATING 时先轻量退避一轮（60ms，等 slot import/replay
     * 就绪），再 RefreshSubmasterRouting 刷新路由后仅对失败子集重新分组
     * 重发一次（有界 1 次）；重发后仍失败的 key 保持终态错误，语义与首轮一致。
     * @param tenant_id 失败子集重新分组用租户。
     * @param results 原批结果（in/out）：send_group 写回，其余不动。
     * @param send_group(submaster, group_keys, group_orig_indices) 负责单组
     * 重发并把组内结果写回 results（group_orig_indices 为组内 key 在原批
     * 中的下标）；返回组内是否全部成功（仅用于打点）。
     */
    template <typename ResultType, typename SendGroupFn>
    void RetryBatchOnStaleRoute(
        const std::string& tenant_id, const std::vector<std::string>& keys,
        std::vector<tl::expected<ResultType, ErrorCode>>& results,
        SendGroupFn send_group);

    /**
     * @brief Switches the underlying RPC pool to the given submaster address.
     * @param address Submaster address (primary_master_id).
     */
    void SwitchToSubmasterByAddress(const std::string& address);

    /**
     * @brief Groups key indices by their owning submaster. Keys without a
     * resolved submaster are grouped under an empty string ("").
     * @return Map from submaster address to original key indices.
     */
    [[nodiscard]] std::map<std::string, std::vector<size_t>>
    GroupKeysBySubmaster(const std::vector<std::string>& keys,
                         const std::string& tenant_id);

    /**
     * @brief Accessor for the coro_rpc_client pool. Since coro_rpc_client pool
     * cannot reconnect to a different address, a new coro_rpc_client pool is
     * created if the address is different from the current one.
     */
    class RpcClientAccessor {
       public:
        void SetClientPool(
            std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
                client_pool) {
            std::lock_guard<std::shared_mutex> lock(client_mutex_);
            client_pool_ = client_pool;
        }

        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        GetClientPool() {
            std::shared_lock<std::shared_mutex> lock(client_mutex_);
            return client_pool_;
        }

       private:
        mutable std::shared_mutex client_mutex_;
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
            client_pool_;
    };

    RpcClientPool client_accessor_;
    RpcClientPool ha_control_client_accessor_;
    RpcClientPool ha_probe_client_accessor_;
    RpcDrainGuard rpc_drain_;

    // 定向 RPC 用独立 pool 访问器：后台心跳/全量 mount/unmount 用它直发指定
    // submaster，与业务请求的 client_accessor_（SwitchToSubmaster 切"当前地址"）
    // 完全隔离，避免"当前地址"竞态。invoke_rpc_to 用 GetOrCreateClientPool 的
    // 返回值发请求，不依赖该访问器的"当前地址"，因此可被多线程并发调用。
    RpcClientPool targeted_accessor_;

    // The client identification.
    const UUID client_id_;

    // Tenant identity for this client instance.
    const TenantId tenant_id_;

    // KV partition routing table (slot -> submaster). Loaded from etcd.
    partition::PartitionRouter partition_router_;
    mutable std::mutex routing_config_mutex_;
    std::string routing_cluster_namespace_;
    // Metrics for tracking RPC operations
    MasterClientMetric* metrics_;
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;
    // Mutex to insure the Connect function is atomic.
    mutable Mutex connect_mutex_;
    // The address which is passed to the coro_rpc_client
    std::string client_addr_param_ GUARDED_BY(connect_mutex_);
    std::atomic<bool> ha_connection_policy_enabled_{false};
};

}  // namespace mooncake
