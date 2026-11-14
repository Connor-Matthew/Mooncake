#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "cvm/cvm_types.h"
#include "metadata_store.h"
#include "rpc_client_io_context.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

class WrappedMasterService;

namespace cvm {

// Inter-master RPC client for the CVM multi-submaster topology (plan B:
// forward allocation requests to the segment-owning submaster).
//
// Discovery is fully etcd-driven (loose coupling): a background thread
// periodically reloads the master registration table
// (/cvm/<ns>/masters/<master_id>) and maintains a master_id -> RPC address
// mapping. One coro_rpc client pool is cached per target address, so
// repeated forwarding reuses established connections.
//
// This first phase provides the handshake RPC used to verify the channel;
// forwarding methods (e.g. PutStart allocation forwarding to the segment
// owner) build on the same invoke_rpc core.
class InterMasterRpcClient {
   public:
    // Refresh period for the etcd-driven member table. Matches the CVM
    // membership/sync cadence (5s) so member add/remove is picked up with
    // comparable latency.
    static constexpr int kRefreshIntervalMs = 5000;

    // Upper bound for the pending broadcast-free queue (eviction storms
    // must not grow it unboundedly; dropped entries only leak handles until
    // the segment is unmounted).
    static constexpr size_t kMaxBroadcastFreeQueueSize = 100000;

    InterMasterRpcClient();

    ~InterMasterRpcClient();

    InterMasterRpcClient(const InterMasterRpcClient&) = delete;
    InterMasterRpcClient& operator=(const InterMasterRpcClient&) = delete;

    // Starts the member-refresh loop. `cluster_namespace` selects the CVM
    // namespace in etcd; empty disables the refresh thread (manual updates
    // only). `self_master_id` is excluded from peer handshakes. The etcd
    // client must already be connected (the supervisor's CvmController
    // connects it before serve).
    ErrorCode Start(const std::string& cluster_namespace,
                    const std::string& self_master_id = "");

    void Stop();

    // Replaces the member table (master_id -> address). Safe to call from
    // any thread; drops addresses of members that disappeared.
    void UpdateMembers(const std::vector<MasterRegistration>& members);

    // Current member table snapshot.
    std::vector<MasterRegistration> GetMembers() const;

    // Resolves the RPC address of `master_id` from the cached member table.
    std::optional<std::string> ResolveAddress(
        const std::string& master_id) const;

    // Handshakes the target submaster: returns its identity + ownership
    // summary. Used both for channel verification and liveness probing.
    tl::expected<InterMasterHandshakeResponse, ErrorCode> Handshake(
        const std::string& master_id);

    // Handshakes every known member except `self_master_id` (logging-only
    // helper for startup verification). Returns the number of successes.
    size_t HandshakeAll(const std::string& self_master_id);

    // ----- Allocation forwarding (CVM plan B phase 2) -----

    // Asks the target submaster (segment owner) to allocate memory replicas
    // in its locally mounted segments. When `preferred_segments` is
    // non-empty the allocation is strict: only those segments are used.
    // The peer keeps the real handles alive; the caller only materializes
    // dummy-allocator replicas from the returned descriptors.
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    AllocateReplicas(const std::string& master_id, const std::string& tenant_id,
                     const std::string& key, uint64_t slice_length,
                     uint64_t replica_num,
                     const std::vector<std::string>& preferred_segments);

    // Frees replicas previously allocated via AllocateReplicas on the
    // target submaster. Returns true when a keepalive entry was found.
    tl::expected<bool, ErrorCode> FreeReplicas(const std::string& master_id,
                                               const std::string& tenant_id,
                                               const std::string& key);

    // Fire-and-forget: broadcast the free to every known peer. Used when the
    // slot owner erases an object whose handles live on a segment-owning
    // peer (it does not track which peer, so all peers are asked; only the
    // owner of the keepalive entry reacts).
    void EnqueueBroadcastFree(const std::string& tenant_id,
                              const std::string& key);

    // ----- Read forwarding (CVM plan B phase 2) -----

    // Forwards a single-key GetReplicaList to the slot-owning submaster.
    // The target performs a local query only (no re-forward), so an
    // inconsistent view terminates the forward chain at the first hop.
    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& master_id, const std::string& key,
        const std::string& tenant_id);

    // Forwards a batch GetReplicaList to the slot-owning submaster. The
    // owner-built vsegment layout table rides on the first successful
    // per-key response (no batch envelope: that would change the wire type).
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::string& master_id,
                        const std::vector<std::string>& keys,
                        const std::string& tenant_id);

    // ----- Write forwarding (model B) -----

    // Relays a FULL PutStart to the slot-owning submaster. The owner executes
    // the complete local PutStart (alloc + metadata + keepalive) and returns
    // the descriptors, which the caller relays to the client. The owner's
    // OwnsSlot==true so it does not re-forward (chain stops at the first hop).
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> PutStart(
        const std::string& master_id, const UUID& client_id,
        const std::string& key, const std::string& tenant_id,
        uint64_t slice_length, const ReplicateConfig& config);

    // Upsert variant: relays a FULL UpsertStart to the slot owner so it
    // overwrites-if-exists (preemption) locally rather than via PutStart.
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> UpsertStart(
        const std::string& master_id, const UUID& client_id,
        const std::string& key, const std::string& tenant_id,
        uint64_t slice_length, const ReplicateConfig& config);

    // ----- Slot-metadata migration (确定性哈希方案 §15.7: RPC 直传) -----

    // Pulls the object-metadata export for `slot` from the previous owner
    // `master_id`. The previous owner keeps its local metadata and the staged
    // export until AckSlotImported arrives.
    tl::expected<SlotMetadataExport, ErrorCode> ExportSlot(
        const std::string& master_id, uint16_t slot,
        const std::string& requester_master_id);

    // Notifies the previous owner that `slot`'s metadata has been imported,
    // so it can drop its local metadata (and clear the staged export).
    // Returns true when the previous owner had a staged export.
    tl::expected<bool, ErrorCode> AckSlotImported(
        const std::string& master_id, uint16_t slot,
        const std::string& importer_master_id);

    // ----- 段级批量迁移（§16.19.2，P4 reshard）-----

    // 一次拉取 [first_slot, last_slot] 闭区间内所有 slot 的元数据快照。
    // 服务端优先返回 staged 缓存，缺失时即时构建（kMigrating 冻结写后
    // 快照稳定）；重复拉取幂等——ack 前源不清数据。
    tl::expected<std::vector<SlotMetadataExport>, ErrorCode> ExportSlotBatch(
        const std::string& master_id, uint16_t first_slot, uint16_t last_slot,
        const std::string& requester_master_id);

    // 心跳交接专用批量拉取（slot 就绪加速）：与 ExportSlotBatch 的差异是
    // 源侧对未 staged 的 slot 施加「必须已失去归属」门控（心跳路径无
    // kMigrating 冻结写，仍持有归属时的即时构建会读到活快照）；仍持有归属
    // 的 slot 缺席于响应，调用方回退单 slot 路径。新方法独立于既有签名——
    // 滚动升级期间旧 peer 无此方法，返回 RPC 错误由调用方兜底。
    tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
    ExportSlotBatchGated(const std::string& master_id, uint16_t first_slot,
                         uint16_t last_slot,
                         const std::string& requester_master_id);

    // 段级 ack：目标安装完成后通知源删除 [first_slot, last_slot] 内全部
    // 本地元数据（DropSlotMetadataRange）。失败由目标侧重试（源不清数据，
    // 重发无损）。
    tl::expected<bool, ErrorCode> AckSlotRangeImported(
        const std::string& master_id, uint16_t first_slot, uint16_t last_slot,
        const std::string& importer_master_id);

   private:
    // Generic sync RPC invocation against the pool of the target address.
    // Defined in the .cpp (needs the complete WrappedMasterService type).
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    tl::expected<ReturnType, ErrorCode> invoke_rpc(const std::string& address,
                                                   Args&&... args);

    // Refreshes the member table from etcd and handshakes newly joined peers
    // (channel verification + connection warmup).
    void RefreshLoop();

    // Drains the broadcast-free queue: for each task, asks every known peer
    // to free the keepalive entry for (tenant, key) if it holds one.
    void FreeLoop();

    struct FreeTask {
        std::string tenant_id;
        std::string key;
        int attempts{0};
    };

    mutable std::mutex members_mutex_;
    std::unordered_map<std::string, std::string> members_;  // id -> address

    std::string cluster_namespace_;
    std::string self_master_id_;
    std::atomic<bool> running_{false};
    std::thread refresh_thread_;
    std::thread free_thread_;
    std::mutex cv_mutex_;
    std::condition_variable cv_;

    std::mutex free_queue_mutex_;
    std::condition_variable free_cv_;
    std::deque<FreeTask> free_queue_;

    // Per-address cached coro_rpc client pools (see RpcClientPool).
    RpcClientPool pool_accessor_;
};

}  // namespace cvm
}  // namespace mooncake
