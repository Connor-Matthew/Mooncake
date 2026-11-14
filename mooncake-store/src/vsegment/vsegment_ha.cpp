#include "vsegment/vsegment_ha.h"

#include <algorithm>
#include <mutex>
#include <unordered_set>

#include "master_perf.h"
#include "mooncake_logging.h"

namespace mooncake::vsegment {
namespace {

bool SameState(const VSegmentStateSnapshot& left,
               const VSegmentStateSnapshot& right) {
    // 写路径加速：结构化字段比较（各 struct 已提供 operator==），替代原先
    // 的 JSON 双序列化比较——每次 delta 构建为每个未变更 vsegment 省两次
    // 序列化。语义与逐字段 JSON 对比一致（字段全覆盖，含顺序敏感的 vector）。
    return left.profile_name == right.profile_name &&
           left.lifecycle == right.lifecycle && left.view == right.view &&
           left.logical_allocation == right.logical_allocation;
}

VSegmentStateDelta BuildDelta(const PartitionVSegmentSnapshot& before,
                              const PartitionVSegmentSnapshot& after) {
    VSegmentStateDelta delta;
    std::unordered_map<std::string, const VSegmentStateSnapshot*> old;
    for (const auto& item : before.vsegments)
        old.emplace(item.view.vsegment_id, &item);
    std::unordered_set<std::string> present;
    for (const auto& item : after.vsegments) {
        present.insert(item.view.vsegment_id);
        auto found = old.find(item.view.vsegment_id);
        if (found == old.end() || !SameState(*found->second, item))
            delta.upserted_vsegments.push_back(item);
    }
    for (const auto& item : before.vsegments) {
        if (!present.count(item.view.vsegment_id))
            delta.removed_vsegment_ids.push_back(item.view.vsegment_id);
    }
    for (const auto& [operation, vsegment] : after.operation_vsegments) {
        auto found = before.operation_vsegments.find(operation);
        if (found == before.operation_vsegments.end() ||
            found->second != vsegment)
            delta.upserted_operations.emplace(operation, vsegment);
    }
    for (const auto& [operation, vsegment] : before.operation_vsegments) {
        (void)vsegment;
        if (!after.operation_vsegments.count(operation))
            delta.removed_operation_ids.push_back(operation);
    }
    return delta;
}

ErrorCode ApplyDelta(const VSegmentOpLogRecord& record,
                     PartitionVSegmentSnapshot* state,
                     std::string* detail) {
    std::unordered_set<std::string> removed(
        record.delta.removed_vsegment_ids.begin(),
        record.delta.removed_vsegment_ids.end());
    state->vsegments.erase(
        std::remove_if(state->vsegments.begin(), state->vsegments.end(),
                       [&](const auto& item) {
                           return removed.count(item.view.vsegment_id) != 0;
                       }),
        state->vsegments.end());
    for (const auto& replacement : record.delta.upserted_vsegments) {
        if (replacement.view.vsegment_id.empty()) {
            if (detail) *detail = "delta contains an empty vsegment id";
            return ErrorCode::INVALID_PARAMS;
        }
        auto found = std::find_if(
            state->vsegments.begin(), state->vsegments.end(),
            [&](const auto& item) {
                return item.view.vsegment_id == replacement.view.vsegment_id;
            });
        if (found == state->vsegments.end())
            state->vsegments.push_back(replacement);
        else
            *found = replacement;
    }
    for (const auto& operation : record.delta.removed_operation_ids)
        state->operation_vsegments.erase(operation);
    for (const auto& [operation, vsegment] :
         record.delta.upserted_operations)
        state->operation_vsegments[operation] = vsegment;
    state->route_epoch = record.route_epoch;
    state->metadata_revision = record.metadata_revision;
    return ErrorCode::OK;
}

}  // namespace

ErrorCode OrderedOpLogVSegmentCommitter::Commit(
    const PartitionVSegmentSnapshot& state, const std::string& mutation,
    std::string* detail) {
    std::shared_ptr<VSegmentDeferredCommitHandle> handle;
    const auto error = CommitDeferred(state, mutation, &handle, detail);
    return error == ErrorCode::OK ? WaitDurable(handle, detail) : error;
}

namespace {
// P3b：延迟 durable 句柄的具体载体（对外只暴露基类指针）。
struct DeferredDurableWait : public VSegmentDeferredCommitHandle {
    uint64_t sequence_id{0};
};
}  // namespace

ErrorCode OrderedOpLogVSegmentCommitter::CommitDeferred(
    const PartitionVSegmentSnapshot& state, const std::string& mutation,
    std::shared_ptr<VSegmentDeferredCommitHandle>* handle, std::string* detail) {
    if (!writer_ || mutation.empty() || state.partition_id.empty() ||
        state.metadata_revision == 0 || handle == nullptr) {
        if (detail) *detail = "invalid deferred vsegment OpLog commit";
        return ErrorCode::INVALID_PARAMS;
    }
    std::unique_lock<std::mutex> commit_lock(mutex_);
    // entry 构建逻辑与 Commit 一致（full_state/delta 判定 + 序列化 +
    // 校验）。last_states_ 在提交时即前移：后续 entry 的 delta 基于提交
    // 顺序，ordered writer 的全局 seq 保证重放序，崩溃时截断在任意 entry
    // 边界均得到一致状态。
    VSegmentOpLogRecord record;
    record.partition_id = state.partition_id;
    record.route_epoch = state.route_epoch;
    record.metadata_revision = state.metadata_revision;
    record.mutation = mutation;
    SpDiag::PerfPoint pt_delta(PerfKey::VSEGMENT_STATE_DELTA,
                               SpDiag::PerfLevel::MODULE);
    pt_delta.Start();
    auto previous = last_states_.find(state.partition_id);
    if (previous == last_states_.end() ||
        previous->second.metadata_revision + 1 != state.metadata_revision) {
        record.full_state = true;
        record.state = state;
    } else {
        record.full_state = false;
        record.delta = BuildDelta(previous->second, state);
    }
    pt_delta.End(0);
    SpDiag::PerfPoint pt_serialize(PerfKey::VSEGMENT_STATE_SERIALIZE,
                                   SpDiag::PerfLevel::MODULE);
    pt_serialize.Start();
    std::string payload;
    struct_json::to_json(record, payload);
    OpLogEntry entry;
    entry.op_type = OpType::VSEGMENT_STATE;
    entry.object_key = state.partition_id;
    entry.payload = std::move(payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    std::string validation_detail;
    if (!ValidateOpLogEntrySize(entry, &validation_detail)) {
        if (detail) *detail = std::move(validation_detail);
        pt_serialize.End(-1);
        return ErrorCode::INVALID_PARAMS;
    }
    pt_serialize.End(0);
    SpDiag::PerfPoint pt_enqueue(PerfKey::VSEGMENT_STATE_ENQUEUE,
                                 SpDiag::PerfLevel::MODULE);
    pt_enqueue.Start();
    auto reservation = writer_->Reserve();
    if (!reservation) {
        if (detail) *detail = "OpLog writer is not accepting vsegment state";
        pt_enqueue.End(-1);
        return reservation.error();
    }
    auto wait = std::make_shared<DeferredDurableWait>();
    auto pending = writer_->Commit(std::move(*reservation), std::move(entry), {});
    if (!pending) {
        if (detail) *detail = "failed to enqueue vsegment OpLog entry";
        pt_enqueue.End(-1);
        return pending.error();
    }
    pt_enqueue.End(0);
    wait->sequence_id = pending->sequence_id();
    last_states_[state.partition_id] = state;
    *handle = std::move(wait);
    return ErrorCode::OK;
}

ErrorCode OrderedOpLogVSegmentCommitter::CommitReservation(
    const std::string& partition_id, const std::string& operation_id,
    const std::string& vsegment_id, const LogicalRange& range,
    uint64_t route_epoch) {
    if (writer_ == nullptr || partition_id.empty() || operation_id.empty() ||
        vsegment_id.empty() || range.length == 0) {
        return ErrorCode::INVALID_PARAMS;
    }

    VSegmentReservationPayload reservation;
    reservation.vsegment_id = vsegment_id;
    reservation.offset = range.offset;
    reservation.length = range.length;

    PartitionOpLogEntry record;
    record.partition_id = partition_id;
    record.route_epoch = route_epoch;
    record.metadata_revision = 0;  // reservation 不参与 metadata_revision
    record.operation_id = operation_id;
    record.event_type = static_cast<int32_t>(PartitionOpEventType::kReserve);
    struct_json::to_json(reservation, record.payload);

    std::string payload;
    struct_json::to_json(record, payload);

    OpLogEntry entry;
    entry.op_type = OpType::VSEGMENT_RESERVE;
    entry.object_key = partition_id;
    entry.payload = std::move(payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    entry.prefix_hash = 0;

    std::string validation_detail;
    if (!ValidateOpLogEntrySize(entry, &validation_detail)) {
        LOG(WARNING) << "vsegment reservation record rejected: "
                     << validation_detail;
        return ErrorCode::INVALID_PARAMS;
    }

    auto slot = writer_->Reserve();
    if (!slot) return slot.error();
    auto pending = writer_->Commit(std::move(*slot), std::move(entry), {});
    if (!pending) return pending.error();
    return ErrorCode::OK;
}

ErrorCode OrderedOpLogVSegmentCommitter::CommitRelease(
    const std::string& partition_id, const std::string& operation_id,
    uint64_t route_epoch) {
    if (writer_ == nullptr || partition_id.empty() || operation_id.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }

    PartitionOpLogEntry record;
    record.partition_id = partition_id;
    record.route_epoch = route_epoch;
    record.metadata_revision = 0;
    record.operation_id = operation_id;
    record.event_type = static_cast<int32_t>(PartitionOpEventType::kAbort);

    std::string payload;
    struct_json::to_json(record, payload);

    OpLogEntry entry;
    entry.op_type = OpType::VSEGMENT_RELEASE;
    entry.object_key = partition_id;
    entry.payload = std::move(payload);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    entry.prefix_hash = 0;

    std::string validation_detail;
    if (!ValidateOpLogEntrySize(entry, &validation_detail)) {
        LOG(WARNING) << "vsegment release record rejected: "
                     << validation_detail;
        return ErrorCode::INVALID_PARAMS;
    }

    auto slot = writer_->Reserve();
    if (!slot) return slot.error();
    auto pending = writer_->Commit(std::move(*slot), std::move(entry), {});
    if (!pending) return pending.error();
    return ErrorCode::OK;
}

ErrorCode OrderedOpLogVSegmentCommitter::WaitDurable(
    const std::shared_ptr<VSegmentDeferredCommitHandle>& handle,
    std::string* detail) {
    if (!handle) {
        if (detail) *detail = "null deferred vsegment OpLog handle";
        return ErrorCode::INVALID_PARAMS;
    }
    auto* wait = static_cast<DeferredDurableWait*>(handle.get());
    SpDiag::PerfPoint pt_wait(PerfKey::VSEGMENT_STATE_DURABLE_WAIT,
                              SpDiag::PerfLevel::MODULE);
    pt_wait.Start();
    const auto result = writer_->WaitDurable(wait->sequence_id,
                                             durable_wait_warning_interval_);
    pt_wait.End(result == ErrorCode::OK ? 0 : -1);
    return result;
}

ErrorCode ReplayVSegmentState(
    const PartitionVSegmentSnapshot& base,
    const std::vector<OpLogEntry>& entries,
    PartitionVSegmentSnapshot* recovered, std::string* detail) {
    if (!recovered || base.partition_id.empty())
        return ErrorCode::INVALID_PARAMS;
    *recovered = base;
    uint64_t revision = base.metadata_revision;
    for (const auto& entry : entries) {
        if (entry.op_type != OpType::VSEGMENT_STATE) continue;
        if (!VerifyOpLogChecksum(entry)) {
            if (detail) *detail = "vsegment OpLog checksum mismatch";
            return ErrorCode::CHECKSUM_MISMATCH;
        }
        VSegmentOpLogRecord record;
        try {
            struct_json::from_json(record, entry.payload);
        } catch (const std::exception& error) {
            if (detail) *detail = error.what();
            return ErrorCode::INVALID_PARAMS;
        }
        if (record.partition_id != base.partition_id ||
            (record.full_state &&
             record.state.partition_id != base.partition_id)) {
            if (detail) *detail = "vsegment OpLog Partition mismatch";
            return ErrorCode::INVALID_PARAMS;
        }
        if (record.metadata_revision <= revision) continue;
        if (record.route_epoch < recovered->route_epoch ||
            (record.full_state &&
             record.state.route_epoch != record.route_epoch)) {
            if (detail) *detail = "stale vsegment route epoch";
            return ErrorCode::STALE_ROUTE;
        }
        const bool initial_checkpoint = revision == 0 && record.full_state;
        if (!initial_checkpoint && record.metadata_revision != revision + 1) {
            if (detail) *detail = "vsegment OpLog revision gap";
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        if (record.full_state &&
            record.state.metadata_revision != record.metadata_revision) {
            if (detail) *detail = "vsegment OpLog revision mismatch";
            return ErrorCode::INVALID_VERSION;
        }
        if (record.full_state) {
            *recovered = std::move(record.state);
        } else {
            const auto applied = ApplyDelta(record, recovered, detail);
            if (applied != ErrorCode::OK) return applied;
        }
        revision = record.metadata_revision;
    }
    return ErrorCode::OK;
}

ErrorCode ApplyVSegmentStateEntry(
    const OpLogEntry& entry,
    std::unordered_map<std::string, PartitionVSegmentSnapshot>& states,
    std::string* detail) {
    // Aligned mode (the default) writes one VSEGMENT_RESERVE per put and
    // VSEGMENT_RELEASE on abort. Their payload is a PartitionOpLogEntry, not a
    // VSegmentOpLogRecord: parsing it as a state record either failed (replay
    // stalled) or yielded an empty full_state that wiped the partition. The
    // replayed pending set is not consumed by ReconcileObjectReferences yet,
    // and in-flight puts with object metadata are recovered from their
    // PROCESSING replicas, so reservation records are skipped here.
    if (entry.op_type == OpType::VSEGMENT_RESERVE ||
        entry.op_type == OpType::VSEGMENT_RELEASE) {
        return ErrorCode::OK;
    }
    if (entry.op_type != OpType::VSEGMENT_STATE) {
        if (detail) *detail = "not a vsegment state record";
        return ErrorCode::INVALID_PARAMS;
    }
    VSegmentOpLogRecord record;
    try {
        struct_json::from_json(record, entry.payload);
    } catch (const std::exception& error) {
        if (detail) *detail = error.what();
        return ErrorCode::INVALID_PARAMS;
    }
    if (record.partition_id.empty()) return ErrorCode::INVALID_PARAMS;
    auto found = states.find(record.partition_id);
    PartitionVSegmentSnapshot base, recovered;
    if (found != states.end()) base = found->second;
    else base.partition_id = record.partition_id;
    const auto error = ReplayVSegmentState(base, {entry}, &recovered, detail);
    if (error == ErrorCode::OK) states[record.partition_id] = std::move(recovered);
    return error;
}

}  // namespace mooncake::vsegment
