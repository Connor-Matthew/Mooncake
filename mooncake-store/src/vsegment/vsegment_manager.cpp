#include "vsegment/vsegment_manager.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <set>
#include <sstream>

#include "master_perf.h"
#include "vsegment/vsegment_metrics.h"
#include "vsegment/vsegment_oplog_mode.h"

namespace mooncake::vsegment {
namespace {

VSegmentAllocationResult ManagerError(ErrorCode error, std::string detail) {
    VSegmentAllocationResult result;
    result.error = error;
    result.detail = std::move(detail);
    return result;
}

ReservationResult ReservationError(ErrorCode error, std::string detail) {
    ReservationResult result;
    result.error = error;
    result.detail = std::move(detail);
    return result;
}

std::vector<PartitionVSegmentConfig> ConfigsForPartition(
    const PartitionPhysicalQuotaSnapshot& snapshot,
    const std::string& partition_id) {
    std::vector<PartitionVSegmentConfig> configs;
    for (const auto& profile : snapshot.profile_specs) {
        PartitionVSegmentConfig config;
        if (BuildPartitionConfig(snapshot, partition_id, profile.name,
                                 &config) == ErrorCode::OK)
            configs.push_back(std::move(config));
    }
    return configs;
}

}  // namespace

VSegmentManager::VSegmentManager(PartitionVSegmentConfig config,
                                 std::vector<VSegmentProfile> profiles,
                                 std::shared_ptr<VSegmentStateCommitter> committer,
                                 size_t max_operation_tombstones)
    : partition_id_(config.partition_id),
      config_generation_(config.config_generation),
      default_profile_(config.profile_name),
      quota_configs_{std::move(config)},
      physical_allocator_(quota_configs_),
      committer_(std::move(committer)),
      max_operation_tombstones_(
          std::max<size_t>(1, max_operation_tombstones)) {
    for (auto& profile : profiles) {
        profiles_.emplace(profile.name, std::move(profile));
    }
}

VSegmentManager::VSegmentManager(
    const PartitionPhysicalQuotaSnapshot& quota_snapshot,
    std::string partition_id,
    std::shared_ptr<VSegmentStateCommitter> committer,
    size_t max_operation_tombstones)
    : partition_id_(std::move(partition_id)),
      config_generation_(quota_snapshot.config_generation),
      default_profile_(quota_snapshot.default_profile),
      quota_configs_(ConfigsForPartition(quota_snapshot, partition_id_)),
      physical_allocator_(quota_configs_),
      committer_(std::move(committer)),
      max_operation_tombstones_(
          std::max<size_t>(1, max_operation_tombstones)) {
    for (const auto& profile : quota_snapshot.profile_specs)
        profiles_.emplace(profile.name, profile);
}

VSegmentManager::VSegmentManager(
    std::vector<PartitionVSegmentConfig> configs,
    const std::vector<VSegmentProfile>& profiles, std::string default_profile,
    std::shared_ptr<VSegmentStateCommitter> committer,
    size_t max_operation_tombstones)
    : partition_id_(configs.empty() ? std::string{} : configs.front().partition_id),
      config_generation_(configs.empty() ? 0 : configs.front().config_generation),
      default_profile_(std::move(default_profile)),
      quota_configs_(std::move(configs)),
      physical_allocator_(quota_configs_),
      committer_(std::move(committer)),
      max_operation_tombstones_(std::max<size_t>(1, max_operation_tombstones)) {
    for (const auto& profile : profiles) profiles_.emplace(profile.name, profile);
}

VSegmentAllocationResult VSegmentManager::Create(
    const std::string& profile_name) {
    const auto creation_key = partition_id_ + "/" + profile_name;
    auto result = profile_creation_coordinator_.GetOrCreate(
        creation_key,
        [&] { return CreateSingleFlight(profile_name, 0); });
    return FinishCreate(std::move(result));
}

VSegmentAllocationResult VSegmentManager::FinishCreate(VSegmentAllocationResult result) {
    if (result) {
        std::shared_ptr<VSegmentDeferredCommitHandle> fence;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            fence = latest_persist_;
        }
        // A concurrent initialization batch may have won the single flight.
        result.error = WaitForDurable(fence, &result.detail);
    }
    if (result) VSegmentMetrics::Instance().IncCreateSuccess();
    else VSegmentMetrics::Instance().IncCreateFailure();
    return result;
}

VSegmentAllocationResult VSegmentManager::RequestCreate(
    const std::string& profile_name, uint64_t retry_after_ms) {
    if (!profiles_.count(profile_name))
        return ManagerError(ErrorCode::INVALID_PARAMS,
                            "unknown vsegment profile: " + profile_name);
    const auto key = partition_id_ + "/" + profile_name;
    uint64_t creation_epoch = 0;
    {
        std::lock_guard<std::mutex> state_lock(mutex_);
        creation_epoch = route_epoch_;
    }
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto pending = pending_creations_.find(key);
    if (pending != pending_creations_.end()) {
        if (pending->second.wait_for(std::chrono::milliseconds(0)) ==
            std::future_status::ready) {
            auto result = pending->second.get();
            pending_creations_.erase(pending);
            return result;
        }
    } else {
        pending_creations_.emplace(
            key, std::async(std::launch::async,
                            [this, profile_name, creation_epoch] {
                                const auto creation_key =
                                    partition_id_ + "/" + profile_name;
                                auto result =
                                    profile_creation_coordinator_.GetOrCreate(
                                        creation_key, [&] {
                                            return CreateSingleFlight(
                                                profile_name, creation_epoch);
                                        });
                                return FinishCreate(std::move(result));
                            })
                     .share());
    }
    return ManagerError(ErrorCode::VSEGMENT_CREATING,
                        "vsegment creation is in progress; retry_after_ms=" +
                            std::to_string(retry_after_ms));
}

VSegmentPutStartResult VSegmentManager::StartPut(
    const std::string& operation_id, uint64_t length,
    const std::string& requested_profile, uint64_t expected_route_epoch,
    const std::vector<std::string>& excluded_vsegments,
    VSegmentCommitBatch* batch) {
    const auto& profile_name =
        requested_profile.empty() ? default_profile_ : requested_profile;
    if (operation_id.empty() || length == 0 || !profiles_.count(profile_name))
        return {ErrorCode::INVALID_PARAMS, operation_id, {},
                "invalid PutStart operation, length, or profile"};

    for (int pass = 0; pass < 2; ++pass) {
        std::shared_ptr<VSegmentDeferredCommitHandle> fence;
        bool allocated = false;
        VSegmentPutStartResult allocated_result;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
                return {ErrorCode::STALE_ROUTE, operation_id, {},
                        "PutStart route epoch is stale"};
            if (degraded_) {
                // Reconcile failed for this partition: the in-memory allocator
                // may overlap live objects, so refuse new reservations.
                return {ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, operation_id,
                        {}, "vsegment partition degraded: reconcile failed"};
            }
            auto existing_operation = operation_vsegments_.find(operation_id);
            if (existing_operation != operation_vsegments_.end()) {
                // Reuse the reservation and propagate its latest accepted
                // journal fence to the caller's durability policy.
                auto managed = vsegments_.find(existing_operation->second);
                if (managed == vsegments_.end())
                    return {ErrorCode::INTERNAL_ERROR, operation_id, {},
                            "PutStart operation references a missing vsegment"};
                auto reservation =
                    managed->second.logical_allocator->Reserve(operation_id,
                                                               length);
                if (!reservation)
                    return {reservation.error, operation_id, {},
                            std::move(reservation.detail)};
                allocated_result = {ErrorCode::OK, operation_id,
                                    {partition_id_, existing_operation->second,
                                     reservation.range.offset,
                                     reservation.range.length, operation_id}, {}};
                fence = latest_persist_;
                allocated = true;
            }
            for (auto& [id, managed] : vsegments_) {
                if (allocated) break;
                if (std::find(excluded_vsegments.begin(),
                              excluded_vsegments.end(), id) !=
                    excluded_vsegments.end())
                    continue;
                if (managed.lifecycle != Lifecycle::ACTIVE ||
                    managed.profile_name != profile_name)
                    continue;
                const auto before = managed.logical_allocator->Snapshot();
                auto reservation =
                    managed.logical_allocator->Reserve(operation_id, length);
                if (!reservation) {
                    if (reservation.error == ErrorCode::NO_AVAILABLE_HANDLE)
                        continue;
                    return {reservation.error, operation_id, {},
                            std::move(reservation.detail)};
                }
                std::string detail;
                operation_vsegments_.emplace(operation_id, id);
                ErrorCode persisted = ErrorCode::OK;
                if (VSegmentOplogAlignedMode()) {
                    // aligned：只写一条 O(1) 的 reservation 记录，
                    // 不做全量状态序列化、不产生 durable 等待。
                    persisted = committer_->CommitReservation(
                        partition_id_, operation_id, id, reservation.range,
                        route_epoch_);
                    if (persisted != ErrorCode::OK) {
                        detail = "failed to persist vsegment reservation";
                    }
                } else {
                    persisted = PersistDeferredLocked("logical_reserve",
                                                      &detail, &fence);
                }
                if (persisted != ErrorCode::OK) {
                    operation_vsegments_.erase(operation_id);
                    managed.logical_allocator->Restore(before);
                    return {persisted, operation_id, {}, std::move(detail)};
                }
                allocated_result = {ErrorCode::OK,
                                    operation_id,
                                    {partition_id_, id,
                                     reservation.range.offset,
                                     reservation.range.length, operation_id},
                                    {}};
                allocated = true;
                break;
            }
        }
        if (allocated) {
            if (batch) {
                batch->Add(committer_, fence);
                return allocated_result;
            }
            // durable fence：entry 已被 ordered writer 接受不可取消，等待
            // 语义与原同步持久化一致（etcd 故障时无限告警等待直至 durable）。
            const auto error = WaitForDurable(fence, &allocated_result.detail);
            if (error != ErrorCode::OK) allocated_result.error = error;
            return allocated_result;
        }
        auto creation = RequestCreate(profile_name);
        if (!creation) {
            return {creation.error, operation_id, {},
                    std::move(creation.detail)};
        }
    }
    return {ErrorCode::NO_AVAILABLE_HANDLE, operation_id, {},
            "new vsegment has no allocatable logical range"};
}

VSegmentAllocationResult VSegmentManager::CreateSingleFlight(
    const std::string& profile_name, uint64_t expected_route_epoch,
    bool single_phase_persist, VSegmentCommitBatch* batch) {
    auto profile = profiles_.find(profile_name);
    if (profile == profiles_.end()) {
        return ManagerError(ErrorCode::INVALID_PARAMS,
                            "unknown vsegment profile: " + profile_name);
    }
    // Preserve submission order under mutex_; wait outside the critical section.
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    VSegmentAllocationResult result;
    bool fresh_created = false;
    std::string fresh_vsegment_id;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (expected_route_epoch != 0 && route_epoch_ != expected_route_epoch)) {
            VSegmentMetrics::Instance().IncStaleRoute();
            return ManagerError(ErrorCode::STALE_ROUTE,
                                "vsegment creation route epoch is stale");
        }
        bool adopted = false;
        for (auto& [id, pending] : vsegments_) {
            if (pending.profile_name != profile_name ||
                pending.lifecycle != Lifecycle::PREPARING)
                continue;
            pending.lifecycle = Lifecycle::ACTIVE;
            std::string detail;
            auto persisted = PersistDeferredLocked("vsegment_create_commit",
                                                    &detail, &fence);
            if (persisted != ErrorCode::OK) {
                pending.lifecycle = Lifecycle::PREPARING;
                return ManagerError(persisted, std::move(detail));
            }
            result = {ErrorCode::OK, pending.view, {}};
            adopted = true;
            break;
        }
        if (!adopted) {
            const std::string vsegment_id = UuidToString(generate_uuid());
            auto logical = std::make_unique<LogicalRangeAllocator>(
                profile->second.member_extent_size * profile->second.member_count);
            // 运行时坏端点排除集快照：把新 vsegment 的条带成员避开已判死的物理段
            // （provider 为空时不过滤，行为与旧版本一致）。
            std::set<std::string> excluded_segments;
            const std::set<std::string>* excluded_segments_ptr = nullptr;
            if (excluded_segments_provider_) {
                excluded_segments = excluded_segments_provider_();
                excluded_segments_ptr = &excluded_segments;
            }
            auto allocation = physical_allocator_.Allocate(
                vsegment_id, profile->second, excluded_segments_ptr);
            if (!allocation) return allocation;
            try {
                vsegments_.emplace(
                    vsegment_id,
                    ManagedVSegment{profile_name,
                        single_phase_persist ? Lifecycle::ACTIVE : Lifecycle::PREPARING,
                        allocation.view, std::move(logical)});
            } catch (...) {
                physical_allocator_.Release(allocation.view);
                throw;
            }
            std::string detail;
            auto persisted = PersistDeferredLocked(
                single_phase_persist ? "vsegment_create" : "vsegment_create_begin",
                &detail, &fence);
            if (persisted != ErrorCode::OK) {
                vsegments_.erase(vsegment_id);
                physical_allocator_.Release(allocation.view);
                return ManagerError(persisted, std::move(detail));
            }
            if (!single_phase_persist) {
                vsegments_.at(vsegment_id).lifecycle = Lifecycle::ACTIVE;
                persisted = PersistDeferredLocked("vsegment_create_commit", &detail,
                                                   &fence);
                if (persisted != ErrorCode::OK) {
                    vsegments_.at(vsegment_id).lifecycle = Lifecycle::PREPARING;
                    return ManagerError(persisted, std::move(detail));
                }
            }
            result = allocation;
            fresh_created = true;
            fresh_vsegment_id = vsegment_id;
        }
    }
    if (batch) {
        batch->Add(committer_, fence);
    } else {
        const auto error = WaitForDurable(fence, &result.detail);
        if (error != ErrorCode::OK) result.error = error;
    }
    // vsegment/psegment 术语的创建可见性日志（锁外打印，避免拖慢提交序）：
    // 进程内首卷 INFO 全量（成员 extent 清单），此后 VLOG(1) 防刷屏
    //（16384 partition 首建期每 partition 一卷）；聚合计数见周期
    // "VSegment Status" 行与 VSegmentMetrics。adopted（PREPARING→ACTIVE
    // 续提交流程）不算新创建，不打。
    if (fresh_created && result.error == ErrorCode::OK) {
        static std::atomic<bool> s_first_vsegment_logged{false};
        const bool first = !s_first_vsegment_logged.exchange(true);
        if (first || VLOG_IS_ON(1)) {
            std::ostringstream os;
            os << "VSegment created" << (first ? " (first)" : "")
               << ": partition=" << partition_id_
               << ", id=" << fresh_vsegment_id
               << ", capacity=" << result.view.logical_capacity
               << ", members=[";
            for (size_t m = 0; m < result.view.members.size(); ++m) {
                if (m) os << ", ";
                const auto& member = result.view.members[m];
                os << "{" << member.segment_id
                   << ",offset=" << member.base_offset
                   << ",len=" << member.length << "}";
            }
            os << "]";
            if (first) {
                LOG(INFO) << os.str();
            } else {
                VLOG(1) << os.str();
            }
        }
    }
    return result;
}

ErrorCode VSegmentManager::SetRouteEpoch(uint64_t route_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!owned_ || route_epoch == 0 || route_epoch < route_epoch_) {
        VSegmentMetrics::Instance().IncStaleRoute();
        return ErrorCode::STALE_ROUTE;
    }
    route_epoch_ = route_epoch;
    return ErrorCode::OK;
}

uint64_t VSegmentManager::RouteEpoch() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return route_epoch_;
}

ErrorCode VSegmentManager::Deactivate(uint64_t route_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (route_epoch == 0 || route_epoch < route_epoch_)
        return ErrorCode::STALE_ROUTE;
    route_epoch_ = route_epoch;
    owned_ = false;
    return ErrorCode::OK;
}

ErrorCode VSegmentManager::EnsureInitialVSegments(std::string* detail,
                                                 VSegmentCommitBatch* batch) {
    std::lock_guard<std::mutex> creation_lock(initial_creation_mutex_);
    std::vector<std::pair<std::string, uint32_t>> targets;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& config : quota_configs_) {
            const auto& name = config.profile_name;
            const auto profile = profiles_.find(name);
            if (profile == profiles_.end()) {
                if (detail)
                    *detail = "Partition quota references unknown profile";
                return ErrorCode::INVALID_PARAMS;
            }
            uint32_t present = 0;
            for (const auto& [id, managed] : vsegments_) {
                (void)id;
                if (managed.profile_name == name &&
                    managed.lifecycle == Lifecycle::ACTIVE)
                    ++present;
            }
            if (present < profile->second.initial_vsegment_count)
                targets.emplace_back(name,
                                     profile->second.initial_vsegment_count -
                                         present);
        }
    }
    for (const auto& [profile_name, count] : targets) {
        for (uint32_t index = 0; index < count; ++index) {
            // P2：首建走单阶段持久化（一笔 ACTIVE entry，无 begin/commit
            // 两阶段），配合 P3 的批量 durable barrier 削减冷启动 etcd 事务。
            const auto creation_key = partition_id_ + "/" + profile_name;
            auto result = profile_creation_coordinator_.GetOrCreate(
                creation_key, [&] {
                    return CreateSingleFlight(profile_name, 0, true, batch);
                });
            if (result) VSegmentMetrics::Instance().IncCreateSuccess();
            else VSegmentMetrics::Instance().IncCreateFailure();
            if (!result) {
                if (detail) *detail = std::move(result.detail);
                return result.error;
            }
        }
    }
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        fence = latest_persist_;
    }
    if (batch) {
        batch->Add(committer_, fence);
        return ErrorCode::OK;
    }
    return WaitForDurable(fence, detail);
}

ReservationResult VSegmentManager::ReservePut(
    const std::string& vsegment_id, const std::string& operation_id,
    uint64_t length, uint64_t expected_route_epoch) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    ReservationResult reserved;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
            return ReservationError(ErrorCode::STALE_ROUTE,
                                    "PutStart route epoch is stale");
        auto managed = vsegments_.find(vsegment_id);
        if (managed == vsegments_.end() ||
            managed->second.lifecycle != Lifecycle::ACTIVE)
            return ReservationError(ErrorCode::SEGMENT_NOT_FOUND,
                                    "vsegment not found or not active");
        auto existing = operation_vsegments_.find(operation_id);
        if (existing != operation_vsegments_.end() &&
            existing->second != vsegment_id)
            return ReservationError(ErrorCode::INVALID_WRITE,
                                    "operation is reserved in another vsegment");
        const auto before = managed->second.logical_allocator->Snapshot();
        auto result = managed->second.logical_allocator->Reserve(operation_id,
                                                                  length);
        if (!result) return result;
        std::string detail;
        const bool inserted =
            operation_vsegments_.emplace(operation_id, vsegment_id).second;
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        ErrorCode persisted = ErrorCode::OK;
        if (VSegmentOplogAlignedMode()) {
            persisted = committer_->CommitReservation(
                partition_id_, operation_id, vsegment_id, result.range,
                route_epoch_);
            if (persisted != ErrorCode::OK) {
                detail = "failed to persist vsegment reservation";
            }
        } else {
            persisted =
                PersistDeferredLocked("logical_reserve", &detail, &fence);
        }
        if (persisted != ErrorCode::OK) {
            if (inserted) operation_vsegments_.erase(operation_id);
            managed->second.logical_allocator->Restore(before);
            return ReservationError(persisted, std::move(detail));
        }
        reserved = std::move(result);
    }
    const auto error = WaitForDurable(fence);
    return error == ErrorCode::OK ? reserved : ReservationError(error, {});
}

ErrorCode VSegmentManager::CommitPut(const std::string& vsegment_id,
                                     const std::string& operation_id,
                                     const std::string& allocation_id,
                                     LogicalRange* range,
                                     uint64_t expected_route_epoch,
                                     const LogicalRange* expected_range,
                                     VSegmentCommitBatch* batch) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
            return ErrorCode::STALE_ROUTE;
        auto managed = vsegments_.find(vsegment_id);
        if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        const auto before = managed->second.logical_allocator->Snapshot();
        if (expected_range) {
            auto reservation = std::find_if(
                before.reservations.begin(), before.reservations.end(),
                [&](const auto& candidate) {
                    return candidate.operation_id == operation_id;
                });
            auto completed = std::find_if(
                before.completed_operations.begin(),
                before.completed_operations.end(), [&](const auto& candidate) {
                    return candidate.operation_id == operation_id &&
                           candidate.outcome == OperationOutcome::COMMITTED;
                });
            const LogicalRange* actual =
                reservation != before.reservations.end()
                    ? &reservation->range
                    : (completed != before.completed_operations.end()
                           ? &completed->range
                           : nullptr);
            if (!actual || actual->offset != expected_range->offset ||
                actual->length != expected_range->length)
                return ErrorCode::INVALID_VERSION;
        }
        const auto result = managed->second.logical_allocator->Commit(
            operation_id, allocation_id, range);
        if (result != ErrorCode::OK) return result;
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        const auto persisted =
            VSegmentOplogAlignedMode()
                ? ErrorCode::OK
                : PersistDeferredLocked("logical_commit", nullptr, &fence);
        if (persisted != ErrorCode::OK) {
            managed->second.logical_allocator->Restore(before);
            return persisted;
        }
    }
    if (batch) {
        batch->Add(committer_, fence);
        return ErrorCode::OK;
    }
    return WaitForDurable(fence);
}

ErrorCode VSegmentManager::AbortPut(const std::string& vsegment_id,
                                    const std::string& operation_id,
                                    uint64_t expected_route_epoch) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
            return ErrorCode::STALE_ROUTE;
        auto managed = vsegments_.find(vsegment_id);
        if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        const auto before = managed->second.logical_allocator->Snapshot();
        const auto result =
            managed->second.logical_allocator->Abort(operation_id);
        if (result != ErrorCode::OK) return result;
        const auto operations_before = operation_vsegments_;
        TrimOperationTombstonesLocked(managed->second,
                                      max_operation_tombstones_);
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        ErrorCode persisted = ErrorCode::OK;
        if (VSegmentOplogAlignedMode()) {
            persisted = committer_->CommitRelease(partition_id_, operation_id,
                                                  route_epoch_);
        } else {
            persisted =
                PersistDeferredLocked("logical_abort", nullptr, &fence);
        }
        if (persisted != ErrorCode::OK) {
            managed->second.logical_allocator->Restore(before);
            operation_vsegments_ = operations_before;
            return persisted;
        }
    }
    return WaitForDurable(fence);
}

ErrorCode VSegmentManager::RollbackPutReservation(
    const std::string& vsegment_id, const std::string& operation_id,
    uint64_t expected_route_epoch) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
            return ErrorCode::STALE_ROUTE;
        auto managed = vsegments_.find(vsegment_id);
        if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        const auto before = managed->second.logical_allocator->Snapshot();
        const auto operations_before = operation_vsegments_;
        const auto result =
            managed->second.logical_allocator->CancelReservation(operation_id);
        if (result != ErrorCode::OK) return result;
        operation_vsegments_.erase(operation_id);
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        ErrorCode persisted = ErrorCode::OK;
        if (VSegmentOplogAlignedMode()) {
            persisted = committer_->CommitRelease(partition_id_, operation_id,
                                                  route_epoch_);
        } else {
            persisted =
                PersistDeferredLocked("logical_abort", nullptr, &fence);
        }
        if (persisted != ErrorCode::OK) {
            managed->second.logical_allocator->Restore(before);
            operation_vsegments_ = operations_before;
            return persisted;
        }
    }
    return WaitForDurable(fence);
}

void VSegmentManager::TrimOperationTombstonesLocked(
    ManagedVSegment& managed, size_t max_completed) {
    const auto snapshot = managed.logical_allocator->Snapshot();
    if (snapshot.completed_operations.size() <= max_completed) return;
    size_t to_remove =
        snapshot.completed_operations.size() - max_completed;
    for (const auto& operation : snapshot.completed_operations) {
        if (to_remove == 0) return;
        if (operation.outcome == OperationOutcome::COMMITTED) continue;
        if (managed.logical_allocator->ForgetCompleted(
                operation.operation_id) == ErrorCode::OK) {
            operation_vsegments_.erase(operation.operation_id);
            --to_remove;
        }
    }
}

ErrorCode VSegmentManager::ReleaseObject(const std::string& vsegment_id,
                                         const std::string& allocation_id,
                                         LogicalRange range,
                                         uint64_t expected_route_epoch) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!owned_ ||
            (route_epoch_ != 0 && expected_route_epoch != route_epoch_))
            return ErrorCode::STALE_ROUTE;
        auto managed = vsegments_.find(vsegment_id);
        if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        const auto before = managed->second.logical_allocator->Snapshot();
        const auto result = managed->second.logical_allocator->Release(
            allocation_id, range);
        if (result != ErrorCode::OK) return result;
        std::vector<std::pair<std::string, std::string>> forgotten_operations;
        for (const auto& completed : before.completed_operations) {
            if (completed.allocation_id != allocation_id) continue;
            auto operation = operation_vsegments_.find(completed.operation_id);
            if (operation != operation_vsegments_.end()) {
                forgotten_operations.push_back(*operation);
                operation_vsegments_.erase(operation);
            }
            const auto forgotten =
                managed->second.logical_allocator->ForgetCompleted(
                    completed.operation_id);
            if (forgotten != ErrorCode::OK) {
                managed->second.logical_allocator->Restore(before);
                operation_vsegments_.insert(forgotten_operations.begin(),
                                            forgotten_operations.end());
                return forgotten;
            }
        }
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        const auto persisted = VSegmentOplogAlignedMode()
                                   ? ErrorCode::OK
                                   : PersistDeferredLocked("logical_release",
                                                      nullptr, &fence);
        if (persisted != ErrorCode::OK) {
            managed->second.logical_allocator->Restore(before);
            operation_vsegments_.insert(forgotten_operations.begin(),
                                        forgotten_operations.end());
            return persisted;
        }
    }
    return WaitForDurable(fence);
}

ErrorCode VSegmentManager::ForgetOperation(const std::string& operation_id) {
    std::shared_ptr<VSegmentDeferredCommitHandle> fence;
    {
        std::lock_guard<std::mutex> lock(mutex_);
    if (!owned_) return ErrorCode::STALE_ROUTE;
        auto operation = operation_vsegments_.find(operation_id);
        if (operation == operation_vsegments_.end())
            return ErrorCode::OBJECT_NOT_FOUND;
        auto managed = vsegments_.find(operation->second);
        if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
        const auto before = managed->second.logical_allocator->Snapshot();
        auto result =
            managed->second.logical_allocator->ForgetCompleted(operation_id);
        if (result != ErrorCode::OK) return result;
        const auto vsegment_id = operation->second;
        operation_vsegments_.erase(operation);
        // 锁收窄：提交在锁内完成，durable 等待移到锁外 fence 收口（语义
        // 同原同步持久化；失败回滚不变）。
        if (!VSegmentOplogAlignedMode()) {
            result = PersistDeferredLocked("operation_gc", nullptr, &fence);
        }
        if (result != ErrorCode::OK) {
            managed->second.logical_allocator->Restore(before);
            operation_vsegments_.emplace(operation_id, vsegment_id);
            return result;
        }
    }
    return WaitForDurable(fence);
}

ErrorCode VSegmentManager::TransitionLifecycle(
    const std::string& vsegment_id, Lifecycle target) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!owned_) return ErrorCode::STALE_ROUTE;
    auto managed = vsegments_.find(vsegment_id);
    if (managed == vsegments_.end()) return ErrorCode::SEGMENT_NOT_FOUND;
    const auto current = managed->second.lifecycle;
    if (current == target) return ErrorCode::OK;
    const bool valid =
        (current == Lifecycle::PREPARING && target == Lifecycle::ACTIVE) ||
        (current == Lifecycle::ACTIVE && target == Lifecycle::DRAINING) ||
        (current == Lifecycle::DRAINING && target == Lifecycle::RETIRED);
    if (!valid) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    if (target == Lifecycle::RETIRED &&
        !managed->second.logical_allocator->Empty())
        return ErrorCode::OBJECT_REPLICA_BUSY;
    std::vector<std::pair<std::string, std::string>> retired_operations;
    if (target == Lifecycle::RETIRED) {
        for (auto operation = operation_vsegments_.begin();
             operation != operation_vsegments_.end();) {
            if (operation->second == vsegment_id) {
                retired_operations.push_back(*operation);
                operation = operation_vsegments_.erase(operation);
            } else {
                ++operation;
            }
        }
    }
    managed->second.lifecycle = target;
    bool released_extent = false;
    if (target == Lifecycle::RETIRED) {
        const auto released = physical_allocator_.Release(managed->second.view);
        if (released != ErrorCode::OK) {
            managed->second.lifecycle = current;
            operation_vsegments_.insert(retired_operations.begin(),
                                        retired_operations.end());
            return released;
        }
        released_extent = true;
    }
    const auto persisted = PersistLocked("lifecycle_transition");
    if (persisted != ErrorCode::OK) {
        managed->second.lifecycle = current;
        operation_vsegments_.insert(retired_operations.begin(),
                                    retired_operations.end());
        if (released_extent) {
            std::string ignored;
            physical_allocator_.Restore(managed->second.view,
                                         profiles_.at(
                                             managed->second.profile_name),
                                         &ignored);
        }
    }
    return persisted;
}

PartitionVSegmentSnapshot VSegmentManager::SnapshotLocked() const {
    PartitionVSegmentSnapshot snapshot;
    snapshot.partition_id = partition_id_;
    snapshot.config_generation = config_generation_;
    snapshot.route_epoch = route_epoch_;
    snapshot.metadata_revision = metadata_revision_;
    snapshot.operation_vsegments = operation_vsegments_;
    for (const auto& [id, managed] : vsegments_) {
        snapshot.vsegments.push_back(
            {managed.profile_name, managed.lifecycle, managed.view,
             managed.logical_allocator->Snapshot()});
    }
    std::sort(snapshot.vsegments.begin(), snapshot.vsegments.end(),
              [](const auto& left, const auto& right) {
                  return left.view.vsegment_id < right.view.vsegment_id;
              });
    return snapshot;
}

PartitionVSegmentSnapshot VSegmentManager::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return SnapshotLocked();
}

ErrorCode VSegmentManager::PersistLocked(const std::string& mutation,
                                         std::string* detail) {
    if (!committer_) {
        if (detail) *detail = "vsegment state committer is not configured";
        return ErrorCode::PERSISTENT_FAIL;
    }
    ++metadata_revision_;
    auto result = committer_->Commit(SnapshotLocked(), mutation, detail);
    if (result != ErrorCode::OK) {
        --metadata_revision_;
        VSegmentMetrics::Instance().IncPersistenceFailure();
    }
    return result;
}

ErrorCode VSegmentManager::PersistDeferredLocked(
    const std::string& mutation, std::string* detail,
    std::shared_ptr<VSegmentDeferredCommitHandle>* fence_out) {
    if (!committer_) {
        if (detail) *detail = "vsegment state committer is not configured";
        return ErrorCode::PERSISTENT_FAIL;
    }
    ++metadata_revision_;
    std::shared_ptr<VSegmentDeferredCommitHandle> handle;
    // committer 不支持延迟（默认回退同步 Commit）时 handle 为空且 entry
    // 已 durable，语义与 PersistLocked 等价。
    SpDiag::PerfPoint pt_snapshot(PerfKey::VSEGMENT_STATE_SNAPSHOT,
                                  SpDiag::PerfLevel::MODULE);
    pt_snapshot.Start();
    auto snapshot = SnapshotLocked();
    pt_snapshot.End(0);
    auto result =
        committer_->CommitDeferred(snapshot, mutation, &handle, detail);
    if (result != ErrorCode::OK) {
        --metadata_revision_;
        VSegmentMetrics::Instance().IncPersistenceFailure();
        return result;
    }
    latest_persist_ = handle;
    if (fence_out) *fence_out = std::move(handle);
    return ErrorCode::OK;
}

ErrorCode VSegmentManager::WaitForDurable(
    const std::shared_ptr<VSegmentDeferredCommitHandle>& handle,
    std::string* detail) {
    if (!handle) return ErrorCode::OK;
    return committer_->WaitDurable(handle, detail);
}

ErrorCode VSegmentManager::Restore(
    const PartitionVSegmentSnapshot& snapshot, std::string* detail) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (quota_configs_.empty()) {
        if (detail) *detail = "Partition has no profile quota";
        return ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT;
    }
    for (const auto& config : quota_configs_) {
        auto validation = ValidatePartitionConfig(config, detail);
        if (validation != ErrorCode::OK) return validation;
    }
    if (!vsegments_.empty()) {
        if (detail) *detail = "restore requires an empty manager";
        return ErrorCode::INVALID_PARAMS;
    }
    if (snapshot.partition_id != partition_id_ ||
        snapshot.config_generation != config_generation_) {
        if (detail) *detail = "snapshot Partition or generation mismatch";
        return ErrorCode::INVALID_VERSION;
    }
    PartitionQuotaAllocator verifier(quota_configs_);
    std::vector<std::unique_ptr<LogicalRangeAllocator>> logical_allocators;
    std::vector<VSegmentProfile> matching_profiles;
    std::vector<bool> restore_view;
    for (const auto& state : snapshot.vsegments) {
        if (state.lifecycle != Lifecycle::PREPARING &&
            state.lifecycle != Lifecycle::ACTIVE &&
            state.lifecycle != Lifecycle::DRAINING &&
            state.lifecycle != Lifecycle::RETIRED) {
            if (detail) *detail = "invalid vsegment lifecycle";
            return ErrorCode::INVALID_PARAMS;
        }
        auto profile = profiles_.find(state.profile_name);
        if (profile == profiles_.end()) {
            if (detail) *detail = "snapshot view has no matching profile";
            return ErrorCode::INVALID_PARAMS;
        }
        if (state.lifecycle == Lifecycle::PREPARING ||
            state.lifecycle == Lifecycle::RETIRED) {
            // CREATE_COMMIT was not durably observed. Its statically owned
            // extents are safe to roll back locally during recovery.
            logical_allocators.push_back(nullptr);
            matching_profiles.push_back(profile->second);
            restore_view.push_back(false);
            continue;
        }
        auto validation = verifier.Restore(state.view, profile->second, detail);
        if (validation != ErrorCode::OK) return validation;
        auto logical = std::make_unique<LogicalRangeAllocator>(
            state.view.logical_capacity);
        validation = logical->Restore(state.logical_allocation, detail);
        if (validation != ErrorCode::OK) return validation;
        logical_allocators.push_back(std::move(logical));
        matching_profiles.push_back(profile->second);
        restore_view.push_back(true);
    }

    for (size_t index = 0; index < snapshot.vsegments.size(); ++index) {
        const auto& state = snapshot.vsegments[index];
        if (!restore_view[index]) continue;
        auto restored = physical_allocator_.Restore(
            state.view, matching_profiles[index], detail);
        if (restored != ErrorCode::OK) return restored;
        vsegments_.emplace(
            state.view.vsegment_id,
            ManagedVSegment{state.profile_name, state.lifecycle, state.view,
                            std::move(logical_allocators[index])});
    }
    for (const auto& [operation, vsegment] : snapshot.operation_vsegments) {
        if (operation.empty() || !vsegments_.count(vsegment)) {
            if (detail) *detail = "operation references missing vsegment";
            return ErrorCode::INVALID_PARAMS;
        }
    }
    operation_vsegments_ = snapshot.operation_vsegments;
    route_epoch_ = snapshot.route_epoch;
    metadata_revision_ = snapshot.metadata_revision;
    return ErrorCode::OK;
}

ErrorCode VSegmentManager::ReconcileObjectReferences(
    const std::vector<VSegmentObjectReference>& references,
    std::string* detail,
    const std::vector<PendingReservation>& pending_reservations) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!owned_) return ErrorCode::STALE_ROUTE;
    std::unordered_map<std::string, std::vector<VSegmentObjectReference>>
        by_vsegment;
    for (const auto& reference : references) {
        if (reference.replica.partition_id != partition_id_ ||
            reference.replica.vsegment_id.empty() ||
            reference.allocation_id.empty() || reference.replica.length == 0) {
            if (detail) *detail = "invalid recovered object reference";
            return ErrorCode::INVALID_PARAMS;
        }
        if (!vsegments_.count(reference.replica.vsegment_id)) {
            if (detail) *detail = "object references missing vsegment";
            return ErrorCode::SEGMENT_NOT_FOUND;
        }
        if (!reference.committed && reference.replica.operation_id.empty()) {
            if (detail) *detail = "pending object has no operation identity";
            return ErrorCode::INVALID_PARAMS;
        }
        by_vsegment[reference.replica.vsegment_id].push_back(reference);
    }

    std::unordered_map<std::string, LogicalAllocationSnapshot> before;
    const auto operations_before = operation_vsegments_;
    std::unordered_map<std::string, std::string> reconciled_operations;
    std::unordered_map<std::string, std::vector<PendingReservation>>
        pending_by_vsegment;
    for (const auto& pending : pending_reservations) {
        if (!pending.vsegment_id.empty() && !pending.operation_id.empty()) {
            pending_by_vsegment[pending.vsegment_id].push_back(pending);
        }
    }
    for (auto& [vsegment_id, managed] : vsegments_) {
        before.emplace(vsegment_id, managed.logical_allocator->Snapshot());
        LogicalAllocationSnapshot desired;
        desired.logical_capacity = managed.view.logical_capacity;
        std::vector<LogicalRange> occupied;
        for (const auto& reference : by_vsegment[vsegment_id]) {
            LogicalRange range{reference.replica.logical_offset,
                               reference.replica.length};
            occupied.push_back(range);
            const std::string operation_id =
                reference.replica.operation_id.empty()
                    ? "recovered/" + reference.allocation_id
                    : reference.replica.operation_id;
            if (reference.committed) {
                desired.completed_operations.push_back(
                    {operation_id, reference.allocation_id,
                     OperationOutcome::COMMITTED, range, 0});
            } else {
                desired.reservations.push_back({operation_id, range});
            }
            if (!reconciled_operations.emplace(operation_id, vsegment_id)
                     .second) {
                if (detail) *detail = "duplicate recovered operation identity";
                for (auto& [id, state] : vsegments_) {
                    auto snapshot = before.find(id);
                    if (snapshot != before.end())
                        state.logical_allocator->Restore(snapshot->second);
                }
                return ErrorCode::INVALID_PARAMS;
            }
        }
        const auto pending_it = pending_by_vsegment.find(vsegment_id);
        if (pending_it != pending_by_vsegment.end()) {
            for (const auto& pending : pending_it->second) {
                if (reconciled_operations.count(pending.operation_id) != 0) {
                    continue;  // 已被 committed 对象覆盖
                }
                const LogicalRange range = pending.range;
                bool overlaps = (range.length == 0);
                for (const auto& taken : occupied) {
                    if (range.offset < taken.offset + taken.length &&
                        taken.offset < range.offset + range.length) {
                        overlaps = true;
                        break;
                    }
                }
                if (overlaps) {
                    LOG(WARNING) << "reconcile: dropping pending reservation "
                                 << pending.operation_id
                                 << " (overlaps committed range or empty)";
                    continue;
                }
                occupied.push_back(range);
                desired.reservations.push_back({pending.operation_id, range});
                reconciled_operations.emplace(pending.operation_id,
                                              vsegment_id);
            }
        }
        std::sort(occupied.begin(), occupied.end(), [](const auto& left,
                                                       const auto& right) {
            return left.offset < right.offset;
        });
        uint64_t cursor = 0;
        for (const auto& range : occupied) {
            if (range.offset < cursor ||
                range.offset > desired.logical_capacity ||
                range.length > desired.logical_capacity - range.offset) {
                if (detail)
                    *detail =
                        "overlapping or out-of-range object reference";
                for (auto& [id, state] : vsegments_) {
                    auto snapshot = before.find(id);
                    if (snapshot != before.end())
                        state.logical_allocator->Restore(snapshot->second);
                }
                // Rebuilt state cannot be trusted: refuse new allocations
                // (fail-closed) until a later reconcile succeeds.
                degraded_ = true;
                LOG(ERROR) << "vsegment reconcile failed for partition "
                           << partition_id_
                           << " (overlapping or out-of-range reference); "
                              "partition degraded, writes rejected";
                return ErrorCode::INVALID_PARAMS;
            }
            if (range.offset > cursor)
                desired.free_ranges.push_back({cursor, range.offset - cursor});
            cursor = range.offset + range.length;
        }
        if (cursor < desired.logical_capacity)
            desired.free_ranges.push_back(
                {cursor, desired.logical_capacity - cursor});
        auto result = managed.logical_allocator->Restore(desired, detail);
        if (result != ErrorCode::OK) {
            for (auto& [id, state] : vsegments_)
                state.logical_allocator->Restore(before.at(id));
            degraded_ = true;
            LOG(ERROR) << "vsegment reconcile failed for partition "
                       << partition_id_
                       << " (allocator restore rejected); "
                          "partition degraded, writes rejected";
            return result;
        }
    }
    operation_vsegments_ = std::move(reconciled_operations);
    const auto result = PersistLocked("recovery_reconcile", detail);
    if (result != ErrorCode::OK) {
        for (auto& [id, state] : vsegments_)
            state.logical_allocator->Restore(before.at(id));
        operation_vsegments_ = operations_before;
        degraded_ = true;
        LOG(ERROR) << "vsegment reconcile failed for partition " << partition_id_
                   << " (checkpoint persist failed); "
                      "partition degraded, writes rejected";
        return result;
    }
    if (degraded_) {
        LOG(INFO) << "vsegment partition " << partition_id_
                  << " recovered: reconcile succeeded, leaving degraded state";
        degraded_ = false;
    }
    return result;
}

bool VSegmentManager::FindView(const std::string& vsegment_id,
                               VSegmentView* view) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto vsegment = vsegments_.find(vsegment_id);
    if (!owned_ || vsegment == vsegments_.end()) return false;
    if (vsegment->second.lifecycle != Lifecycle::ACTIVE &&
        vsegment->second.lifecycle != Lifecycle::DRAINING)
        return false;
    if (view) *view = vsegment->second.view;
    return true;
}

VSegmentManagerStats VSegmentManager::Stats() const {
    VSegmentManagerStats stats;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [id, managed] : vsegments_) {
            stats.logical_capacity_bytes += managed.view.logical_capacity;
            stats.logical_free_bytes +=
                managed.logical_allocator->FreeBytes();
            switch (managed.lifecycle) {
                case Lifecycle::PREPARING:
                    ++stats.preparing;
                    break;
                case Lifecycle::ACTIVE:
                    ++stats.active;
                    break;
                case Lifecycle::DRAINING:
                    ++stats.draining;
                    break;
                case Lifecycle::RETIRED:
                    ++stats.retired;
                    break;
            }
            stats.reservations +=
                managed.logical_allocator->ReservationCount();
            stats.committed_allocations +=
                managed.logical_allocator->CommittedCount();
        }
    }
    std::lock_guard<std::mutex> pending_lock(pending_mutex_);
    for (const auto& [key, creation] : pending_creations_) {
        (void)key;
        if (creation.wait_for(std::chrono::milliseconds(0)) !=
            std::future_status::ready)
            ++stats.pending_creations;
    }
    return stats;
}

}  // namespace mooncake::vsegment
