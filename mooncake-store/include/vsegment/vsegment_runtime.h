#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "vsegment/vsegment.h"

namespace mooncake::vsegment {

struct LogicalRange {
    uint64_t offset{0};
    uint64_t length{0};
};
YLT_REFL(LogicalRange, offset, length);

// 写路径加速：结构化相等比较（供 vsegment HA delta 构建使用，替代 JSON
// 双序列化比较；语义与逐字段 JSON 对比一致）。
inline bool operator==(const LogicalRange& left, const LogicalRange& right) {
    return left.offset == right.offset && left.length == right.length;
}

struct ReservationResult {
    ErrorCode error{ErrorCode::OK};
    LogicalRange range;
    std::string detail;

    explicit operator bool() const { return error == ErrorCode::OK; }
};

struct ReservationRecord {
    std::string operation_id;
    LogicalRange range;
};
YLT_REFL(ReservationRecord, operation_id, range);

inline bool operator==(const ReservationRecord& left,
                       const ReservationRecord& right) {
    return left.operation_id == right.operation_id &&
           left.range == right.range;
}

enum class OperationOutcome : uint8_t { COMMITTED, ABORTED, RELEASED };
struct CompletedOperationRecord {
    std::string operation_id;
    std::string allocation_id;
    OperationOutcome outcome{OperationOutcome::ABORTED};
    LogicalRange range;
    struct_pack::compatible<uint64_t, 1> completion_revision;
};
YLT_REFL(CompletedOperationRecord, operation_id, allocation_id, outcome,
         range, completion_revision);

inline bool operator==(const CompletedOperationRecord& left,
                       const CompletedOperationRecord& right) {
    return left.operation_id == right.operation_id &&
           left.allocation_id == right.allocation_id &&
           left.outcome == right.outcome && left.range == right.range &&
           left.completion_revision.value_or(0) ==
               right.completion_revision.value_or(0);
}

struct LogicalAllocationSnapshot {
    uint64_t logical_capacity{0};
    std::vector<LogicalRange> free_ranges;
    std::vector<ReservationRecord> reservations;
    std::vector<CompletedOperationRecord> completed_operations;
};
YLT_REFL(LogicalAllocationSnapshot, logical_capacity, free_ranges,
         reservations, completed_operations);

inline bool operator==(const LogicalAllocationSnapshot& left,
                       const LogicalAllocationSnapshot& right) {
    return left.logical_capacity == right.logical_capacity &&
           left.free_ranges == right.free_ranges &&
           left.reservations == right.reservations &&
           left.completed_operations == right.completed_operations;
}

// Tracks only free space and in-flight PutStart reservations. Committed ranges
// are represented by ObjectMetadata and are returned through Release().
class LogicalRangeAllocator {
   public:
    explicit LogicalRangeAllocator(uint64_t logical_capacity);

    ReservationResult Reserve(const std::string& operation_id,
                              uint64_t length);
    ErrorCode Commit(const std::string& operation_id,
                     const std::string& allocation_id, LogicalRange* range);
    ErrorCode Abort(const std::string& operation_id);
    // Rolls back an internal, not-yet-returned reservation without recording
    // a terminal operation outcome, so the same operation id may be retried.
    ErrorCode CancelReservation(const std::string& operation_id);
    ErrorCode Release(const std::string& allocation_id, LogicalRange range);

    LogicalAllocationSnapshot Snapshot() const;
    ErrorCode Restore(const LogicalAllocationSnapshot& snapshot,
                      std::string* detail = nullptr);

    uint64_t FreeBytes() const;
    size_t ReservationCount() const;
    size_t CommittedCount() const;
    bool Empty() const;
    ErrorCode ForgetCompleted(const std::string& operation_id);

   private:
    static void InsertAndMerge(std::vector<LogicalRange>& ranges,
                               LogicalRange range);
    bool OverlapsFreeOrReserved(LogicalRange range) const;

    uint64_t logical_capacity_;
    mutable std::mutex mutex_;
    std::vector<LogicalRange> free_ranges_;
    std::unordered_map<std::string, LogicalRange> reservations_;
    std::unordered_map<std::string, CompletedOperationRecord>
        completed_operations_;
    std::unordered_map<std::string, LogicalRange> committed_allocations_;
    uint64_t next_completion_revision_{1};
};

struct ClientSlice {
    uint64_t address{0};
    uint64_t length{0};
};

struct TransferSubRequest {
    uint64_t sequence{0};
    uint64_t client_buffer_offset{0};
    uint64_t client_address{0};
    std::string segment_id;
    uint64_t physical_offset{0};
    uint64_t length{0};
};

struct ResolveResult {
    ErrorCode error{ErrorCode::OK};
    std::vector<TransferSubRequest> requests;
    std::string detail;

    explicit operator bool() const { return error == ErrorCode::OK; }
};

// Splits on both stripe and Client Slice boundaries. The output preserves
// logical order, while execution may complete in any order.
ResolveResult ResolveTransfer(const VSegmentView& view,
                              uint64_t logical_offset, uint64_t length,
                              const std::vector<ClientSlice>& slices);

// Serializes overlapping creation calls for the same creation_key. The owner
// stores durable results and calls Forget after publishing them.
class CreationCoordinator {
   public:
    using Factory = std::function<VSegmentAllocationResult()>;

    VSegmentAllocationResult GetOrCreate(const std::string& creation_key,
                                         Factory factory);
    void Forget(const std::string& creation_key);

   private:
    struct Entry {
        bool creating{true};
        size_t users{1};
        VSegmentAllocationResult result;
        std::condition_variable ready;
    };

    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<Entry>> entries_;
};

}  // namespace mooncake::vsegment
