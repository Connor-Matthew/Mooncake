#pragma once

#include <csignal>

#include <string>
#include <boost/functional/hash.hpp>
#include <cstdint>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_service.h"
#include "types.h"

// master_service.h 已引入 cvm/cvm_controller.h；显式声明依赖类型。
namespace mooncake::cvm {
class CvmController;
}
#include "rpc_types.h"
#include "master_config.h"
#include "kv_event/kv_event_publisher.h"
#include "io_pattern/cfm_service.h"
#include "segment.h"
#include "partition/vsegment_types.h"

namespace mooncake {

// Forward declaration
class HttpMetadataServer;
class WrappedMasterService {
   public:
    void SetBatchOpLogTerminalCallback(
        OrderedOpLogWriter::TerminalCallback callback);
    void StopBatchOpLogWriter();
    void StopHttpMetadataCleanup();
    // Restore lifecycle hooks (RFC #4700, slice 1). Serving means "committed
    // to normal operation, restore refused from here on", not "already
    // reachable": the non-HA path and EmbeddedMaster both run
    // MarkReadyForServing + ActivateServing before registering the RPC
    // service, while the HA supervisor's ActivateServingState takes the
    // Ready -> Serving edge after readiness is published, on a per-term
    // instance that is destroyed rather than reused if publication fails.
    void MarkReadyForServing();
    void ActivateServing();
    MasterService::RestoreLifecycleState GetRestoreLifecycleState() const;
    // Constructor with optional metadata-cleanup-on-timeout configuration.
    // - http_metadata_server: in-process pointer used when the HTTP metadata
    //   server is co-located in the master process (nullptr = not co-located).
    // - http_metadata_remote_url: http(s) connection string used when the
    //   metadata server is deployed separately (empty = none). Only consulted
    //   when http_metadata_server is nullptr. If both are unset, cleanup is
    //   disabled.
    WrappedMasterService(const WrappedMasterServiceConfig& config,
                         HttpMetadataServer* http_metadata_server = nullptr,
                         const std::string& http_metadata_remote_url = "");

    ~WrappedMasterService();

    tl::expected<bool, ErrorCode> ExistKey(
        const std::string& key, const std::string& tenant_id = "default");

    // Point-in-time existence check: shares the ExistKey lookup path but
    // grants no read lease, so a `true` result does not protect the object
    // from eviction.
    tl::expected<bool, ErrorCode> ProbeKey(
        const std::string& key, const std::string& tenant_id = "default");

    tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats();

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys,
        const std::string& tenant_id = "default");

    std::vector<tl::expected<bool, ErrorCode>> BatchProbeKey(
        const GrantLeasePolicy& policy, const std::vector<std::string>& keys,
        const std::string& tenant_id = "default");

    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    tl::expected<std::vector<std::string>, ErrorCode> BatchReplicaClear(
        const std::vector<std::string>& object_keys, const UUID& client_id,
        const std::string& segment_name,
        const std::string& tenant_id = "default");

    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str,
                          const std::string& tenant_id = "default");

    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& key, const std::string& tenant_id = "default",
        uint64_t client_trace_id = 0, const UUID& client_id = {});
    vsegment::VSegmentPutStartResult VSegmentPutStart(
        const std::string& partition_id, uint64_t route_epoch,
        const std::string& operation_id, uint64_t length,
        const std::string& profile_name);
    ErrorCode VSegmentPutEnd(const VSegmentDescriptor& replica,
                             uint64_t route_epoch,
                             const std::string& operation_id,
                             const std::string& object_id);
    ErrorCode VSegmentPutRevoke(const std::string& partition_id,
                                const std::string& vsegment_id,
                                uint64_t route_epoch,
                                const std::string& operation_id);

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys,
                        const std::string& tenant_id = "default",
                        uint64_t client_trace_id = 0,
                        const UUID& client_id = {});

    // Read-only admin variants: no lease grants, no promotion, no metric
    // updates.
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaListForAdmin(const std::vector<std::string>& keys,
                                const std::string& tenant_id = "default");

    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaListForAdmin(
        const std::string& key, const std::string& tenant_id = "default");

    tl::expected<PutStartResult, ErrorCode> PutStart(
        const UUID& client_id, const std::string& key,
        const uint64_t slice_length, const ReplicateConfig& config,
        const std::string& tenant_id = "default", uint64_t client_trace_id = 0);

    tl::expected<void, ErrorCode> PutEnd(
        const UUID& client_id, const ObjectMeta& object_meta,
        ReplicaType replica_type = ReplicaType::ALL,
        const std::string& tenant_id = "default", uint64_t client_trace_id = 0,
        const std::string& operation_id = "");

    tl::expected<void, ErrorCode> PutRevoke(
        const UUID& client_id, const std::string& key,
        ReplicaType replica_type = ReplicaType::ALL,
        const std::string& tenant_id = "default",
        const std::string& operation_id = "");

    std::vector<tl::expected<PutStartResult, ErrorCode>> BatchPutStart(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::vector<uint64_t>& slice_lengths,
        const ReplicateConfig& config, const std::string& tenant_id = "default",
        uint64_t client_trace_id = 0);

    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
        ReplicaType replica_type = ReplicaType::ALL,
        const std::string& tenant_id = "default", uint64_t client_trace_id = 0);

    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        ReplicaType replica_type = ReplicaType::ALL,
        const std::string& tenant_id = "default");

    tl::expected<PutStartResult, ErrorCode> UpsertStart(
        const UUID& client_id, const std::string& key,
        const uint64_t slice_length, const ReplicateConfig& config,
        const std::string& tenant_id = "default", uint64_t client_trace_id = 0);

    tl::expected<void, ErrorCode> UpsertEnd(
        const UUID& client_id, const ObjectMeta& object_meta,
        ReplicaType replica_type = ReplicaType::ALL,
        const std::string& tenant_id = "default", uint64_t client_trace_id = 0,
        const std::string& operation_id = "");

    tl::expected<void, ErrorCode> UpsertRevoke(
        const UUID& client_id, const std::string& key,
        ReplicaType replica_type = ReplicaType::ALL,
        const std::string& tenant_id = "default", uint64_t client_trace_id = 0,
        const std::string& operation_id = "");

    std::vector<tl::expected<PutStartResult, ErrorCode>> BatchUpsertStart(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::vector<uint64_t>& slice_lengths,
        const ReplicateConfig& config,
        const std::string& tenant_id = "default");

    std::vector<tl::expected<void, ErrorCode>> BatchUpsertEnd(
        const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
        const std::string& tenant_id = "default");

    std::vector<tl::expected<void, ErrorCode>> BatchUpsertRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id = "default");

    tl::expected<void, ErrorCode> Remove(
        const std::string& key, bool force = false,
        const std::string& tenant_id = "default");

    tl::expected<long, ErrorCode> RemoveByRegex(
        const std::string& str, bool force = false,
        const std::string& tenant_id = "default");

    long RemoveAll(bool force = false,
                   const std::string& tenant_id = "default");

    std::vector<tl::expected<void, ErrorCode>> BatchUpdateSoftPin(
        const std::vector<std::string>& keys, uint64_t ttl_ms,
        const std::string& tenant_id = "default");

    std::vector<tl::expected<void, ErrorCode>> BatchRemove(
        const std::vector<std::string>& keys, bool force = false,
        const std::string& tenant_id = "default");

    tl::expected<void, ErrorCode> MountSegment(const Segment& segment,
                                               const UUID& client_id);

    tl::expected<void, ErrorCode> MountNoFSegment(const NoFSegment& segment,
                                                  const UUID& client_id);

    tl::expected<void, ErrorCode> ReMountSegment(
        const std::vector<Segment>& segments, const UUID& client_id);

    tl::expected<void, ErrorCode> ReMountNoFSegment(
        const std::vector<NoFSegment>& segments, const UUID& client_id);

    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id,
                                                 const UUID& client_id);

    tl::expected<void, ErrorCode> GracefulUnmountSegment(
        const UUID& segment_id, const UUID& client_id,
        uint64_t grace_period_ms);

    tl::expected<void, ErrorCode> UnmountNoFSegment(const UUID& segment_id,
                                                    const UUID& client_id);

    [[nodiscard]] tl::expected<std::vector<NoFSegment>, ErrorCode>
    GetAllNoFSegments();

    [[nodiscard]] tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode>
    GetNoFSegmentsByName(const std::string& segment_name);

    tl::expected<std::string, ErrorCode> GetFsdir();

    tl::expected<GetStorageConfigResponse, ErrorCode> GetStorageConfig();

    tl::expected<PingResponse, ErrorCode> Ping(const UUID& client_id);

    tl::expected<std::string, ErrorCode> ServiceReady();

    [[nodiscard]] TieredStorageUsageSnapshot GetStorageUsageSnapshot() const;

    tl::expected<std::vector<TenantQuotaSnapshot>, ErrorCode>
    ListTenantQuotaSnapshots();
    tl::expected<TenantQuotaSnapshot, ErrorCode> GetTenantQuotaSnapshot(
        const std::string& tenant_id);
    tl::expected<TenantQuotaSnapshot, ErrorCode> UpsertTenantQuotaPolicy(
        const std::string& tenant_id, uint64_t requested_quota_bytes);
    tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>
    DeleteTenantQuotaPolicy(const std::string& tenant_id);
    tl::expected<uint64_t, ErrorCode> GetTenantQuotaAllocatableCapacityBytes();

    tl::expected<std::vector<std::string>, ErrorCode> GetAllKeysForAdmin();

    tl::expected<std::vector<std::string>, ErrorCode> GetAllSegmentsForAdmin();

    tl::expected<std::vector<MasterService::SegmentDetailInfo>, ErrorCode>
    GetSegmentsDetailForAdmin();

    tl::expected<std::pair<uint64_t, uint64_t>, ErrorCode> QuerySegmentForAdmin(
        const std::string& segment);

    tl::expected<void, ErrorCode> MountLocalDiskSegment(const UUID& client_id,
                                                        bool enable_offloading);

    tl::expected<void, ErrorCode> UnmountLocalDiskSegment(
        const UUID& client_id);

    /**
     * @brief 被动坏端点上报（方案二被动层）：client 传输失败后上报目标
     * transport_endpoint 列表，master 标记进 invalid_replica_endpoints_
     * 供分配过滤与读路径跳过（worker 重挂载时自愈）。
     */
    tl::expected<void, ErrorCode> ReportInvalidReplicaEndpoints(
        const std::vector<std::string>& endpoints);

    tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
    OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading);

    tl::expected<bool, ErrorCode> PollRemoveAll(const UUID& client_id);

    tl::expected<std::vector<RemoveTaskItem>, ErrorCode> RemoveObjectHeartbeat(
        const UUID& client_id);
    tl::expected<void, ErrorCode> AckRemoveObjectHeartbeat(
        const UUID& client_id, const std::vector<RemoveTaskItem>& tasks);
    tl::expected<void, ErrorCode> ReportSsdCapacity(
        const UUID& client_id, int64_t ssd_total_capacity_bytes);

    tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas);

    tl::expected<std::vector<std::string>, ErrorCode> GetOffloadEndpoints();

    // Promotion-on-hit RPCs.
    tl::expected<std::vector<PromotionTaskItem>, ErrorCode>
    PromotionObjectHeartbeat(const UUID& client_id);

    // SSD prefetch RPC. Registers a promotion task without the
    // promotion-on-hit admission gates and without the holder's heartbeat
    // mailbox; the caller drives the execution chain itself. Best-effort:
    // PROMOTION_ALREADY_EXISTS is a normal "skip" outcome. Additive RPC —
    // older masters reject it and prefetch degrades to a no-op.
    tl::expected<void, ErrorCode> RegisterPrefetchTask(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id);

    tl::expected<PromotionAllocStartResponse, ErrorCode> PromotionAllocStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, uint64_t size,
        const std::vector<std::string>& preferred_segments);

    tl::expected<void, ErrorCode> NotifyPromotionSuccess(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id);

    tl::expected<void, ErrorCode> NotifyPromotionFailure(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id);

    // Admin-only, grow-only DFS capacity management. Existing placements remain
    // valid.
    tl::expected<int, ErrorCode> GetDfsShardCount() const;
    tl::expected<int, ErrorCode> ExpandDfsShards(int shard_count);

    tl::expected<UUID, ErrorCode> CreateDrainJob(
        const CreateDrainJobRequest& request);

    tl::expected<QueryJobResponse, ErrorCode> QueryDrainJob(const UUID& job_id);

    tl::expected<void, ErrorCode> CancelDrainJob(const UUID& job_id);

    tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatus(
        const std::string& segment_name);
    tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatusById(
        const UUID& segment_id);
    tl::expected<void, ErrorCode> SetSegmentStatus(
        const std::string& segment_name, SegmentStatus status);

    WeightMetadataStore::Result<WeightRevisionMetadata> BeginWeightImport(
        const BeginWeightImportRequest& request,
        const std::string& tenant_id = "default");
    WeightMetadataStore::Result<WeightRevisionMetadata> CommitWeightImport(
        const CommitWeightImportRequest& request,
        const std::string& tenant_id = "default");
    WeightMetadataStore::Result<WeightRevisionMetadata> AbortWeightImport(
        const AbortWeightImportRequest& request,
        const std::string& tenant_id = "default");
    WeightMetadataStore::Result<WeightRevisionView> GetWeightRevision(
        const GetWeightRevisionRequest& request,
        const std::string& tenant_id = "default");
    WeightMetadataStore::Result<ListWeightRevisionsResponse>
    ListWeightRevisions(const ListWeightRevisionsRequest& request,
                        const std::string& tenant_id = "default");
    WeightMetadataStore::Result<WeightRevisionLease> AcquireWeightRevisionLease(
        const AcquireWeightRevisionLeaseRequest& request,
        const std::string& tenant_id = "default");
    WeightMetadataStore::Result<WeightRevisionLease> RenewWeightRevisionLease(
        const RenewWeightRevisionLeaseRequest& request,
        const std::string& tenant_id = "default");
    WeightMetadataStore::Result<void> ReleaseWeightRevisionLease(
        const ReleaseWeightRevisionLeaseRequest& request,
        const std::string& tenant_id = "default");
    WeightMetadataStore::Result<WeightResidencyOperation>
    StartWeightResidencyOperation(
        const StartWeightResidencyOperationRequest& request,
        const std::string& tenant_id = "default");
    WeightMetadataStore::Result<WeightResidencyOperation> QueryWeightOperation(
        const QueryWeightOperationRequest& request,
        const std::string& tenant_id = "default");
    WeightMetadataStore::Result<WeightRevisionMetadata> DeleteWeightRevision(
        const DeleteWeightRevisionRequest& request,
        const std::string& tenant_id = "default");

    // Internal method called by supervisor during promotion; NOT an RPC
    // endpoint.
    tl::expected<void, ErrorCode> RestoreFromStandby(
        const std::vector<StandbyObjectEntry>& objects,
        uint64_t initial_oplog_sequence_id,
        const std::vector<StandbySegmentInfo>& segments,
        const WeightMetadataSnapshot& weight_metadata = {},
        const std::vector<vsegment::PartitionVSegmentSnapshot>&
            vsegment_partitions = {});
    // Copies objects out of the handoff; the caller keeps ownership.
    tl::expected<void, ErrorCode> RestoreFromBatchOpLogPromotion(
        const BatchOpLogPromotionHandoff& handoff,
        size_t chunk_object_count = kDefaultBatchOpLogPromotionChunkObjects);
    tl::expected<void, ErrorCode> RestoreFromBatchOpLogPromotion(
        const StandbyMetadataStore& metadata_store,
        DurablePrefix applied_cursor,
        const std::vector<StandbySegmentInfo>& segments,
        ReplicaID max_replica_id,
        size_t chunk_object_count = kDefaultBatchOpLogPromotionChunkObjects,
        const std::vector<vsegment::PartitionVSegmentSnapshot>&
            vsegment_partitions = {});
    // 同身份重启失忆自愈（缺陷3修复）：无 PromotionContext 时从本机
    // OpLog 流自恢复。Internal（supervisor 调用），NOT an RPC endpoint.
    tl::expected<bool, ErrorCode> TrySelfRecoverFromOpLog();

    // 环互备领养物化（互备 1.4）：supervisor RingStandbyRuntime 在
    // ring-adopt staging 期间的编排入口，三态语义见
    // MasterService::AdoptMaterializeFromDeadOwner。Internal（supervisor
    // 调用），NOT an RPC endpoint.
    ErrorCode AdoptMaterializeFromDeadOwner(const std::string& source_id,
                                            bool& stream_present);

    // CVM slot ownership publishing, driven by the HA supervisor. These
    // forward to the wrapped MasterService (NOT RPC endpoints).
    void SetCvmLeaseId(EtcdLeaseId lease_id);
    void SetCvmController(cvm::CvmController* controller);
    // vsegment auto-plan 重试策略注入（--vsegment_auto_plan flags 解析），
    // 转发给内部 MasterService（见 MasterService::SetVSegmentAutoPlanPolicy）。
    void SetVSegmentAutoPlanPolicy(const vsegment::VSegmentUserPolicy& policy);
    ErrorCode StartSlotOwnerHeartbeat();
    void StopSlotOwnerHeartbeat();

    // CVM inter-master RPC lifecycle, driven by the HA supervisor (NOT an
    // RPC endpoint). Starts/stops the peer discovery + connection pools.
    ErrorCode StartInterMasterRpc();
    void StopInterMasterRpc();

    // Inter-master handshake RPC: returns this submaster's identity and
    // ownership summary so peers can verify the inter-master channel.
    tl::expected<InterMasterHandshakeResponse, ErrorCode>
    InterMasterHandshake();

    // Current number of CVM slots owned by this submaster (0 until the
    // ownership resolver has populated the local lookup). Forwarded to the
    // wrapped MasterService for use by periodic admin metrics reporting.
    uint32_t GetOwnedSlotCount() const;

    // One-line vsegment/psegment status summary (vsegment enabled state,
    // quota generation/profiles, installed partitions, vsegment count and
    // pool composition). Forwarded for the periodic VSegment Status log.
    std::string GetVSegmentStatusLogLine() const;

    // Inter-master allocation forwarding (CVM plan B): allocate memory
    // replicas in this submaster's locally mounted segments on behalf of a
    // slot-owning peer. Strictly within `preferred_segments` when given.
    // This submaster keeps the real handles alive (keepalive registry);
    // the slot owner materializes dummy-allocator replicas from the
    // returned descriptors and owns the metadata.
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    InterMasterAllocateReplicas(
        const std::string& tenant_id, const std::string& key,
        uint64_t slice_length, uint64_t replica_num,
        const std::vector<std::string>& preferred_segments);

    // Frees the keepalive entry (and thus the handles) previously created
    // by InterMasterAllocateReplicas. Returns true when an entry existed.
    tl::expected<bool, ErrorCode> InterMasterFreeReplicas(
        const std::string& tenant_id, const std::string& key);

    // Inter-master read forwarding (CVM plan B): local GetReplicaList /
    // BatchGetReplicaList query for a forwarding peer. These do NOT
    // re-forward, so an inconsistent view terminates the chain at the first
    // hop instead of looping.
    tl::expected<GetReplicaListResponse, ErrorCode> InterMasterGetReplicaList(
        const std::string& key, const std::string& tenant_id);

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    InterMasterBatchGetReplicaList(const std::vector<std::string>& keys,
                                   const std::string& tenant_id);

    // Inter-master write forwarding (model B): relays a FULL PutStart to the
    // slot-owning submaster so it allocates + writes metadata locally.
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    InterMasterPutStart(const UUID& client_id, const std::string& key,
                        const std::string& tenant_id, uint64_t slice_length,
                        const ReplicateConfig& config);

    // Upsert variant of InterMasterPutStart (owner overwrites-if-exists).
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    InterMasterUpsertStart(const UUID& client_id, const std::string& key,
                           const std::string& tenant_id, uint64_t slice_length,
                           const ReplicateConfig& config);

    // Inter-master slot-metadata migration (确定性哈希方案 §15.7，RPC 直传，
    // 替代 etcd slot_meta 中转): the new slot owner pulls the object-metadata
    // export from the previous owner. The previous owner keeps its local
    // metadata until InterMasterAckSlotImported arrives.
    tl::expected<SlotMetadataExport, ErrorCode> InterMasterExportSlot(
        uint16_t slot, const std::string& requester_master_id);

    // New owner notifies the previous owner that `slot`'s metadata has been
    // imported, so the previous owner can drop its local metadata (and clear
    // the staged export). Returns true when a staged export existed.
    tl::expected<bool, ErrorCode> InterMasterAckSlotImported(
        uint16_t slot, const std::string& importer_master_id);

    // ----- 段级批量迁移（§16.19.2，P4 reshard）-----

    // 源侧批量导出：[first_slot, last_slot] 闭区间内所有 slot 的元数据快照
    // （staged 优先，缺失时即时构建——kMigrating 冻结写后快照稳定）。幂等：
    // ack 前源不清数据，重复拉取返回相同内容。
    tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
    InterMasterExportSlotBatch(uint16_t first_slot, uint16_t last_slot,
                               const std::string& requester_master_id);

    // 心跳交接专用的批量导出变体（slot 就绪加速）：未 staged 的 slot 仅在
    // 源已观察到失去归属时即时构建（require_not_owned 门控），仍持有归属的
    // slot 缺席于响应（调用方对其走单 slot 路径重试）。新方法独立注册而非
    // 改既有签名——滚动升级期间旧 peer 无此方法，调用方收到 RPC 错误后
    // 回退单 slot 路径，兼容语义由调用方保证。
    tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
    InterMasterExportSlotBatchGated(uint16_t first_slot, uint16_t last_slot,
                                    const std::string& requester_master_id);

    // 段级 ack：目标安装完成后通知源删除区间内全部本地元数据
    // （DropSlotMetadataRange）。源未观察到归属已切走时返回
    // SLOT_MIGRATING（目标稍后重试，重发无损）。
    tl::expected<bool, ErrorCode> InterMasterAckSlotRangeImported(
        uint16_t first_slot, uint16_t last_slot,
        const std::string& importer_master_id);

    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::string& tenant_id,
        const std::vector<std::string>& targets);

    tl::expected<UUID, ErrorCode> CreateMoveTask(const std::string& key,
                                                 const std::string& tenant_id,
                                                 const std::string& source,
                                                 const std::string& target);

    tl::expected<QueryTaskResponse, ErrorCode> QueryTask(const UUID& task_id);

    tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        const UUID& client_id, size_t batch_size);

    tl::expected<void, ErrorCode> MarkTaskToComplete(
        const UUID& client_id, const TaskCompleteRequest& request);

    tl::expected<CopyStartResponse, ErrorCode> CopyStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, const std::string& src_segment,
        const std::vector<std::string>& tgt_segments);

    tl::expected<CopyStartResponse, ErrorCode> DynamicReplicaCopyStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, const std::string& src_segment,
        const std::vector<std::string>& tgt_segments,
        const UUID& dynamic_replication_lease_id,
        uint64_t dynamic_replication_version_epoch);

    tl::expected<void, ErrorCode> CopyEnd(const UUID& client_id,
                                          const std::string& key,
                                          const std::string& tenant_id);
    tl::expected<void, ErrorCode> DynamicReplicaCopyEnd(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, const UUID& dynamic_replication_lease_id,
        uint64_t dynamic_replication_version_epoch);

    tl::expected<void, ErrorCode> CopyRevoke(const UUID& client_id,
                                             const std::string& key,
                                             const std::string& tenant_id);
    tl::expected<void, ErrorCode> DynamicReplicaCopyRevoke(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, const UUID& dynamic_replication_lease_id,
        uint64_t dynamic_replication_version_epoch);

    tl::expected<MoveStartResponse, ErrorCode> MoveStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, const std::string& src_segment,
        const std::string& tgt_segment);

    tl::expected<void, ErrorCode> MoveEnd(const UUID& client_id,
                                          const std::string& key,
                                          const std::string& tenant_id);

    tl::expected<void, ErrorCode> MoveRevoke(const UUID& client_id,
                                             const std::string& key,
                                             const std::string& tenant_id);

    tl::expected<void, ErrorCode> EvictDiskReplica(const UUID& client_id,
                                                   const std::string& key,
                                                   const std::string& tenant_id,
                                                   ReplicaType replica_type);

    // Conditional variant: evicts only when the current record still carries
    // the replica id the caller failed on. Returns false when the replica was
    // replaced in the meantime; the replacement is left untouched.
    tl::expected<bool, ErrorCode> EvictDiskReplicaIfCurrent(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, ReplicaType replica_type,
        ReplicaID expected_replica_id);

    std::vector<tl::expected<void, ErrorCode>> BatchEvictDiskReplica(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id, ReplicaType replica_type);

    bool KvEventsEnabled() const;
    KvEventPublisher::Stats GetKvEventStats() const;

    io_pattern::CfmRpcService& CfmRpcEndpoint() { return cfm_rpc_service_; }

   private:
    MasterService master_service_;
    io_pattern::CfmRpcService cfm_rpc_service_;
};

class RpcWorkerPool;

// With a worker pool, every handler except Ping and ServiceReady runs on its
// worker threads; those two stay on the io threads, which the pool keeps
// free. Without one, every handler runs inline on the io threads.
void RegisterRpcService(coro_rpc::coro_rpc_server& server,
                        mooncake::WrappedMasterService& wrapped_master_service,
                        RpcWorkerPool* worker_pool = nullptr);

}  // namespace mooncake
