#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "ha/oplog/ordered_oplog_writer.h"
#include "vsegment/vsegment_manager.h"

namespace mooncake::vsegment {

struct VSegmentStateDelta {
    std::vector<VSegmentStateSnapshot> upserted_vsegments;
    std::vector<std::string> removed_vsegment_ids;
    std::unordered_map<std::string, std::string> upserted_operations;
    std::vector<std::string> removed_operation_ids;
};
YLT_REFL(VSegmentStateDelta, upserted_vsegments, removed_vsegment_ids,
         upserted_operations, removed_operation_ids);

struct VSegmentOpLogRecord {
    std::string partition_id;
    uint64_t route_epoch{0};
    uint64_t metadata_revision{0};
    std::string mutation;
    PartitionVSegmentSnapshot state;
    // Legacy records contain only `state` and therefore deserialize with the
    // default true. New records use one full checkpoint per committer lifetime
    // followed by compact deltas.
    bool full_state{true};
    VSegmentStateDelta delta;
};
YLT_REFL(VSegmentOpLogRecord, partition_id, route_epoch, metadata_revision,
         mutation, state, full_state, delta);

class OrderedOpLogVSegmentCommitter final : public VSegmentStateCommitter {
   public:
    explicit OrderedOpLogVSegmentCommitter(
        OrderedOpLogWriter* writer,
        std::chrono::milliseconds durable_wait_warning_interval =
            std::chrono::milliseconds(30000))
        : writer_(writer),
          durable_wait_warning_interval_(
              durable_wait_warning_interval.count() > 0
                  ? durable_wait_warning_interval
                  : std::chrono::milliseconds(30000)) {}

    ErrorCode Commit(const PartitionVSegmentSnapshot& state,
                     const std::string& mutation,
                     std::string* detail) override;

    // Submission order determines the delta chain. WaitDurable observes the
    // writer's storage prefix and does not depend on its callback thread.
    ErrorCode CommitDeferred(
        const PartitionVSegmentSnapshot& state, const std::string& mutation,
        std::shared_ptr<VSegmentDeferredCommitHandle>* handle,
        std::string* detail) override;
    ErrorCode WaitDurable(
        const std::shared_ptr<VSegmentDeferredCommitHandle>& handle,
        std::string* detail) override;

    // aligned 模式：写一条 O(1) 的 VSEGMENT_RESERVE 记录。
    // payload = serialize(PartitionOpLogEntry{event_type=kReserve,
    //                    payload=serialize(VSegmentReservationPayload)})。
    // 不做全量状态序列化，也不等待 durable。
    ErrorCode CommitReservation(const std::string& partition_id,
                                const std::string& operation_id,
                                const std::string& vsegment_id,
                                const LogicalRange& range,
                                uint64_t route_epoch) override;

    // aligned 模式：写 VSEGMENT_RELEASE（abort / rollback 用）。
    ErrorCode CommitRelease(const std::string& partition_id,
                            const std::string& operation_id,
                            uint64_t route_epoch) override;

   private:
    OrderedOpLogWriter* writer_;
    std::chrono::milliseconds durable_wait_warning_interval_;
    std::mutex mutex_;
    std::unordered_map<std::string, PartitionVSegmentSnapshot> last_states_;
};

// Caller serializes access to states. Both standby and cold recovery use
// this path so full checkpoints and incremental records share validation.
ErrorCode ApplyVSegmentStateEntry(
    const OpLogEntry& entry,
    std::unordered_map<std::string, PartitionVSegmentSnapshot>& states,
    std::string* detail = nullptr);

// Replays legacy full-state and current delta records after a snapshot.
ErrorCode ReplayVSegmentState(
    const PartitionVSegmentSnapshot& base,
    const std::vector<OpLogEntry>& entries,
    PartitionVSegmentSnapshot* recovered,
    std::string* detail = nullptr);

}  // namespace mooncake::vsegment
