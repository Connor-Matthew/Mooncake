#pragma once

#include <functional>
#include <memory>
#include <future>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "vsegment/vsegment.h"
#include "vsegment/vsegment_runtime.h"
#include "replica.h"

namespace mooncake::vsegment {

struct VSegmentStateSnapshot {
    std::string profile_name;
    Lifecycle lifecycle{Lifecycle::PREPARING};
    VSegmentView view;
    LogicalAllocationSnapshot logical_allocation;
};
YLT_REFL(VSegmentStateSnapshot, profile_name, lifecycle, view,
         logical_allocation);

struct PartitionVSegmentSnapshot {
    std::string partition_id;
    uint64_t config_generation{0};
    uint64_t route_epoch{0};
    uint64_t metadata_revision{0};
    std::vector<VSegmentStateSnapshot> vsegments;
    std::unordered_map<std::string, std::string> operation_vsegments;
};
YLT_REFL(PartitionVSegmentSnapshot, partition_id, config_generation,
         route_epoch, metadata_revision, vsegments, operation_vsegments);

// aligned 模式：从 oplog 的 VSEGMENT_RESERVE 记录重放出来的「在飞 reservation」。
// ReconcileObjectReferences 需要把它并入重建结果，否则重启后 pending 会丢失。
struct PendingReservation {
    std::string vsegment_id;
    std::string operation_id;
    LogicalRange range;
};

// An accepted journal submission; storage durability may be awaited separately.
class VSegmentDeferredCommitHandle {
   public:
    virtual ~VSegmentDeferredCommitHandle() = default;
};

class VSegmentStateCommitter {
   public:
    virtual ~VSegmentStateCommitter() = default;
    virtual ErrorCode Commit(const PartitionVSegmentSnapshot& state,
                             const std::string& mutation,
                             std::string* detail) = 0;
    // Synchronous committers remain compatible and return an empty handle.
    virtual ErrorCode CommitDeferred(
        const PartitionVSegmentSnapshot& state, const std::string& mutation,
        std::shared_ptr<VSegmentDeferredCommitHandle>* handle,
        std::string* detail) {
        const auto error = Commit(state, mutation, detail);
        if (handle) handle->reset();
        return error;
    }
    virtual ErrorCode WaitDurable(
        const std::shared_ptr<VSegmentDeferredCommitHandle>& handle,
        std::string* /*detail*/) {
        (void)handle;
        return ErrorCode::OK;
    }
    // aligned 模式（MC_VSEGMENT_OPLOG_MODE=aligned）：写一条 O(1) 的
    // reservation 生命周期记录，替代热路径上的全量状态提交。
    // 默认实现返回 UNAVAILABLE_IN_CURRENT_MODE，因此只有支持 OpLog 的
    // committer（OrderedOpLogVSegmentCommitter）才启用该路径。
    virtual ErrorCode CommitReservation(const std::string& /*partition_id*/,
                                        const std::string& /*operation_id*/,
                                        const std::string& /*vsegment_id*/,
                                        const LogicalRange& /*range*/,
                                        uint64_t /*route_epoch*/) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }
    // aligned 模式：写一条 O(1) 的 VSEGMENT_RELEASE 记录（abort / rollback）。
    // commit 不写（对象 PUT_END 已带 operation_id，重放时可判定为已解决）。
    virtual ErrorCode CommitRelease(const std::string& /*partition_id*/,
                                    const std::string& /*operation_id*/,
                                    uint64_t /*route_epoch*/) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }
};

// Explicit ownership of accepted commits. Topology publication and metadata
// handoff must Flush(). Volatile data-path operations may discard the wait
// after journal acceptance when they intentionally match ordinary memory Put
// semantics.
class VSegmentCommitBatch {
   public:
    void Add(std::shared_ptr<VSegmentStateCommitter> committer,
             std::shared_ptr<VSegmentDeferredCommitHandle> handle) {
        if (handle) commits_[std::move(committer)] = std::move(handle);
    }
    ErrorCode Flush(std::string* detail = nullptr) {
        for (const auto& [committer, handle] : commits_) {
            const auto error = committer->WaitDurable(handle, detail);
            if (error != ErrorCode::OK) return error;
        }
        commits_.clear();
        return ErrorCode::OK;
    }
    void DiscardDurabilityWaits() { commits_.clear(); }

   private:
    std::unordered_map<std::shared_ptr<VSegmentStateCommitter>,
                       std::shared_ptr<VSegmentDeferredCommitHandle>> commits_;
};

struct VSegmentPutStartResult {
    ErrorCode error{ErrorCode::OK};
    std::string operation_id;
    VSegmentDescriptor replica;
    std::string detail;
    explicit operator bool() const { return error == ErrorCode::OK; }
};
YLT_REFL(VSegmentPutStartResult, error, operation_id, replica, detail);

struct VSegmentManagerStats {
    uint64_t logical_capacity_bytes{0};
    uint64_t logical_free_bytes{0};
    size_t preparing{0};
    size_t active{0};
    size_t draining{0};
    size_t retired{0};
    size_t reservations{0};
    size_t committed_allocations{0};
    size_t pending_creations{0};
};

// ObjectMetadata is authoritative after recovery. These references are used
// once, before serving traffic, to remove allocator state left behind by a
// crash between the vsegment-state and object-metadata durable records.
struct VSegmentObjectReference {
    VSegmentDescriptor replica;
    std::string allocation_id;
    bool committed{false};
};

// Partition-local facade intended to be owned by the Partition's SubMaster.
// Persistence transport is deliberately outside this class: HA Snapshot/OpLog
// code serializes the returned state and calls Restore after failover.
class VSegmentManager {
   public:
    VSegmentManager(PartitionVSegmentConfig config,
                    std::vector<VSegmentProfile> profiles,
                    std::shared_ptr<VSegmentStateCommitter> committer,
                    size_t max_operation_tombstones = 4096);
    VSegmentManager(const PartitionPhysicalQuotaSnapshot& quota_snapshot,
                    std::string partition_id,
                    std::shared_ptr<VSegmentStateCommitter> committer,
                    size_t max_operation_tombstones = 4096);

    VSegmentManager(std::vector<PartitionVSegmentConfig> configs,
                    const std::vector<VSegmentProfile>& profiles,
                    std::string default_profile,
                    std::shared_ptr<VSegmentStateCommitter> committer,
                    size_t max_operation_tombstones = 4096);

    VSegmentAllocationResult Create(const std::string& profile_name);
    // Hot-path API: starts at most one background creation per
    // partition/profile and asks callers to retry while it is running.
    VSegmentAllocationResult RequestCreate(const std::string& profile_name,
                                           uint64_t retry_after_ms = 50);
    VSegmentPutStartResult StartPut(const std::string& operation_id,
                                    uint64_t length,
                                    const std::string& profile_name = {},
                                    uint64_t expected_route_epoch = 0,
                                    const std::vector<std::string>&
                                        excluded_vsegments = {},
                                    VSegmentCommitBatch* batch = nullptr);
    ErrorCode SetRouteEpoch(uint64_t route_epoch);
    // A supplied batch defers the durability barrier to the caller.
    ErrorCode EnsureInitialVSegments(std::string* detail = nullptr,
                                     VSegmentCommitBatch* batch = nullptr);
    ErrorCode WaitForDurable(
        const std::shared_ptr<VSegmentDeferredCommitHandle>& handle,
        std::string* detail = nullptr);
    uint64_t RouteEpoch() const;
    ErrorCode Deactivate(uint64_t route_epoch);
    ReservationResult ReservePut(const std::string& vsegment_id,
                                 const std::string& operation_id,
                                 uint64_t length,
                                 uint64_t expected_route_epoch = 0);
    ErrorCode CommitPut(const std::string& vsegment_id,
                        const std::string& operation_id,
                        const std::string& allocation_id,
                        LogicalRange* range = nullptr,
                        uint64_t expected_route_epoch = 0,
                        const LogicalRange* expected_range = nullptr,
                        VSegmentCommitBatch* batch = nullptr);
    ErrorCode AbortPut(const std::string& vsegment_id,
                       const std::string& operation_id,
                       uint64_t expected_route_epoch = 0);
    ErrorCode RollbackPutReservation(const std::string& vsegment_id,
                                     const std::string& operation_id,
                                     uint64_t expected_route_epoch = 0);
    ErrorCode ReleaseObject(const std::string& vsegment_id,
                            const std::string& allocation_id,
                            LogicalRange range,
                            uint64_t expected_route_epoch = 0);
    ErrorCode ForgetOperation(const std::string& operation_id);
    ErrorCode TransitionLifecycle(const std::string& vsegment_id,
                                  Lifecycle target);

    PartitionVSegmentSnapshot Snapshot() const;
    ErrorCode Restore(const PartitionVSegmentSnapshot& snapshot,
                      std::string* detail = nullptr);
    ErrorCode ReconcileObjectReferences(
        const std::vector<VSegmentObjectReference>& references,
        std::string* detail = nullptr,
        const std::vector<PendingReservation>& pending_reservations = {});
    bool FindView(const std::string& vsegment_id, VSegmentView* view) const;
    VSegmentManagerStats Stats() const;
    // 注入运行时坏端点排除集提供者（te_endpoint 与 segment_name 双形式）。
    // 物理分配（CreateSingleFlight）时调用获取快照，跳过其中的段，避免把
    // 新 vsegment 的条带成员落在已判死的物理段上。锁序：manager mutex_ →
    // provider 内的 invalid 锁（与 MountSegment 自愈 erase 方向一致）。
    void SetExcludedSegmentsProvider(
        std::function<std::set<std::string>()> provider) {
        std::lock_guard<std::mutex> lock(mutex_);
        excluded_segments_provider_ = std::move(provider);
    }

   private:
    struct ManagedVSegment {
        std::string profile_name;
        Lifecycle lifecycle{Lifecycle::PREPARING};
        VSegmentView view;
        std::unique_ptr<LogicalRangeAllocator> logical_allocator;
    };

    VSegmentAllocationResult CreateSingleFlight(const std::string& profile_name,
                                                 uint64_t expected_route_epoch = 0,
                                                 bool single_phase_persist = false,
                                                 VSegmentCommitBatch* batch = nullptr);
    VSegmentAllocationResult FinishCreate(VSegmentAllocationResult result);
    PartitionVSegmentSnapshot SnapshotLocked() const;
    ErrorCode PersistLocked(const std::string& mutation,
                            std::string* detail = nullptr);
    // Submits under mutex_; the caller waits after releasing it.
    ErrorCode PersistDeferredLocked(
        const std::string& mutation, std::string* detail = nullptr,
        std::shared_ptr<VSegmentDeferredCommitHandle>* fence_out = nullptr);
    void TrimOperationTombstonesLocked(ManagedVSegment& managed,
                                       size_t max_completed);
    std::string partition_id_;
    uint64_t config_generation_{0};
    std::string default_profile_;
    std::unordered_map<std::string, VSegmentProfile> profiles_;
    std::vector<PartitionVSegmentConfig> quota_configs_;
    PartitionQuotaAllocator physical_allocator_;
    CreationCoordinator profile_creation_coordinator_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ManagedVSegment> vsegments_;
    std::unordered_map<std::string, std::string> operation_vsegments_;
    uint64_t route_epoch_{0};
    uint64_t metadata_revision_{0};
    std::shared_ptr<VSegmentStateCommitter> committer_;
    // Also fences idempotent reads of an accepted, not-yet-durable mutation.
    std::shared_ptr<VSegmentDeferredCommitHandle> latest_persist_;
    std::mutex initial_creation_mutex_;
    bool owned_{true};
    size_t max_operation_tombstones_{4096};
    // aligned 模式下分配器状态只能由 ReconcileObjectReferences 重建。reconcile
    // 失败时内存状态不可信，必须拒绝新的分配（fail-closed），否则可能重复分配
    // 已有对象的区间。读路径不受影响；下一次 reconcile 成功会自动解除。
    bool degraded_{false};
    mutable std::mutex pending_mutex_;
    std::unordered_map<std::string,
                       std::shared_future<VSegmentAllocationResult>>
        pending_creations_;
    std::function<std::set<std::string>()> excluded_segments_provider_;
};

}  // namespace mooncake::vsegment
