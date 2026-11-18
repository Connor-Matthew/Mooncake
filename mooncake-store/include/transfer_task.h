#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <stack>
#include <unordered_set>
#include <string>
#include <thread>
#include <vector>

#include "transfer_engine.h"
#include "types.h"
#include "replica.h"
#include "storage_backend.h"
#include "client_metric.h"
#include "nof/nvmeof_initiator.h"
#include "rpc_types.h"
#include "vsegment/vsegment_transfer.h"

namespace mooncake {

/**
 * @brief Transfer strategy enumeration
 */
enum class TransferStrategy {
    LOCAL_MEMCPY = 0,     // Local memory copy using memcpy
    TRANSFER_ENGINE = 1,  // Remote transfer using transfer engine
    FILE_READ = 2,        // File read operation
    EMPTY = 3,
    SPDK_NVMF = 4  // Spdk nvmf operation
};

enum class OffloadBufferAccess {
    kTransferEngine,
    kLocalAddress,
};

enum class TransferIntent : int {
    kUnspecified = transfer_intent_values::kUnspecified,
    kForegroundGet = transfer_intent_values::kForegroundGet,
    kBackgroundPrefetch = transfer_intent_values::kBackgroundPrefetch,
    kMigration = transfer_intent_values::kMigration,
};

inline std::optional<TransferIntent> TransferIntentFromInt(int intent) {
    switch (intent) {
        case transfer_intent_values::kUnspecified:
            return TransferIntent::kUnspecified;
        case transfer_intent_values::kForegroundGet:
            return TransferIntent::kForegroundGet;
        case transfer_intent_values::kBackgroundPrefetch:
            return TransferIntent::kBackgroundPrefetch;
        case transfer_intent_values::kMigration:
            return TransferIntent::kMigration;
        default:
            return std::nullopt;
    }
}

/**
 * @brief Stream operator for TransferStrategy
 */
inline std::ostream& operator<<(std::ostream& os,
                                const TransferStrategy& strategy) noexcept {
    switch (strategy) {
        case TransferStrategy::LOCAL_MEMCPY:
            return os << "LOCAL_MEMCPY";
        case TransferStrategy::TRANSFER_ENGINE:
            return os << "TRANSFER_ENGINE";
        case TransferStrategy::SPDK_NVMF:
            return os << "SPDK_NVMF";
        case TransferStrategy::FILE_READ:
            return os << "FILE_READ";
        case TransferStrategy::EMPTY:
            return os << "EMPTY";
        default:
            return os << "UNKNOWN";
    }
}

/**
 * @brief Per-object NoF timing, captured for sampled high-frequency traces.
 *
 * The first three phases form a non-overlapping decomposition of total_us:
 * queue_to_first_submit_us + first_to_last_submit_us +
 * last_submit_to_complete_us ~= total_us.  Completion timings overlap the
 * submit span because completions may arrive while later sub-I/Os are still
 * being admitted to the qpair.
 */
struct NofTransferTiming {
    int32_t worker_index = -1;
    uint32_t lane = 0;
    uint16_t qpair_id = 0;
    uint32_t sub_io_count = 0;
    uint64_t lane_task_sequence = 0;
    uint64_t lane_completion_sequence = 0;
    uint32_t qos_tasks_ahead_at_enqueue = 0;
    int64_t dispatch_queue_us = 0;
    int64_t worker_setup_us = 0;
    int64_t qos_fifo_wait_us = 0;
    // Decompose qos_fifo_wait_us by the lane occupancy observed while this
    // task was behind another task.  These fields distinguish backpressure
    // from task-level head-of-line blocking with unused credit.
    int64_t qos_fifo_full_wait_us = 0;
    int64_t qos_fifo_partial_wait_us = 0;
    int64_t qos_fifo_zero_wait_us = 0;
    int64_t head_to_first_submit_us = 0;
    int64_t credit_wait_us = 0;
    // Credit wait accumulated while at the lane head before the first submit
    // succeeded; credit_wait_us - first_credit_wait_us is the post-first-
    // submit stall.  These values overlap head_to_first_submit_us and
    // first_to_last_submit_us respectively; they are causal annotations, not
    // additional non-overlapping phases.
    int64_t first_credit_wait_us = 0;
    int64_t qos_queue_us = 0;
    int64_t queue_to_first_submit_us = 0;
    int64_t first_to_last_submit_us = 0;
    int64_t last_submit_to_complete_us = 0;
    int64_t first_submit_to_first_completion_us = 0;
    int64_t first_completion_minus_last_submit_us = 0;
    int64_t first_completion_to_complete_us = 0;
    int64_t total_us = 0;
    int64_t complete_to_observe_us = 0;
    // The slowest sub-I/O splits aggregate drain into an initiator polling
    // gap versus a downstream completion gap.
    int64_t max_sub_io_latency_us = 0;
    uint32_t max_sub_io_index = 0;
    uint16_t max_sub_io_cid = 0;
    int64_t max_completion_gap_us = 0;
    int64_t max_poll_return_gap_us = 0;
    uint64_t poll_calls_during_task = 0;
    uint64_t empty_polls_during_task = 0;
    uint64_t polled_completions_during_task = 0;
};

/**
 * @brief Abstract base class for operation state management
 *
 * This class encapsulates the common state and behavior for async transfer
 * operations. Derived classes implement strategy-specific waiting logic.
 */
class OperationState {
   public:
    explicit OperationState(uint64_t trace_id = 0) : trace_id_(trace_id) {}
    virtual ~OperationState() = default;

    // Non-copyable, non-movable
    OperationState(const OperationState&) = delete;
    OperationState& operator=(const OperationState&) = delete;
    OperationState(OperationState&&) = delete;
    OperationState& operator=(OperationState&&) = delete;

    /**
     * @brief Check if the operation has completed
     */
    virtual bool is_completed() = 0;

    /**
     * @brief Get the operation result. Make sure to call is_completed() first.
     */
    ErrorCode get_result() const {  // lock mutex
        std::lock_guard<std::mutex> lock(mutex_);
        assert(result_.has_value() &&
               "get_result() called on an incomplete or failed-to-set "
               "operation state.");
        return result_.value_or(ErrorCode::INVALID_PARAMS);
    }

    /**
     * @brief Get the transfer strategy
     */
    virtual TransferStrategy get_strategy() const = 0;

    /**
     * @brief Wait for the operation to complete (strategy-specific
     * implementation)
     */
    virtual void wait_for_completion() = 0;

    // Available only for sampled NoF operations after completion.
    virtual std::optional<NofTransferTiming> nof_timing() const {
        return std::nullopt;
    }

    /** Trace id of the request that created this operation (0 = none). */
    uint64_t trace_id() const { return trace_id_; }

   protected:
    const uint64_t trace_id_ = 0;
    std::optional<ErrorCode> result_ = std::nullopt;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
};

/**
 * @brief Operation state for local memcpy transfers
 */
class EmptyOperationState : public OperationState {
   public:
    bool is_completed() override { return true; }

    void wait_for_completion() override {}

    TransferStrategy get_strategy() const override {
        return TransferStrategy::EMPTY;
    }
};

/**
 * @brief Operation state for local memcpy transfers
 */
class MemcpyOperationState : public OperationState {
   public:
    explicit MemcpyOperationState(uint64_t trace_id = 0)
        : OperationState(trace_id) {}

    bool is_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }

    void set_completed(ErrorCode error_code) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!result_.has_value());
            result_.emplace(error_code);
        }
        cv_.notify_all();
    }

    void wait_for_completion() override {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return result_.has_value(); });
    }

    TransferStrategy get_strategy() const override {
        return TransferStrategy::LOCAL_MEMCPY;
    }
};

/**
 * @brief Operation state for NoF (NVMe-oF) transfers
 */
class NofOperationState : public OperationState {
   public:
    explicit NofOperationState(uint64_t trace_id = 0,
                               bool track_timing = false)
        : OperationState(trace_id), track_timing_(track_timing) {}

    bool is_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }

    void set_completed(
        ErrorCode error_code,
        std::optional<NofTransferTiming> timing) {
        set_completed(error_code, std::string(), std::move(timing));
    }

    void set_completed(ErrorCode error_code, std::string error_detail,
                       std::optional<NofTransferTiming> timing) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!result_.has_value());
            if (track_timing_ && timing.has_value()) {
                timing_ = std::make_unique<NofTransferTiming>(
                    std::move(timing.value()));
                completed_at_ = std::chrono::steady_clock::now();
            }
            result_.emplace(error_code);
            error_detail_ = std::move(error_detail);
        }
        cv_.notify_all();
    }

    void set_completed(ErrorCode error_code, std::string error_detail = "") {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!result_.has_value());
            result_.emplace(error_code);
            error_detail_ = std::move(error_detail);
        }
        cv_.notify_all();
    }

    // Data-plane error detail (sc/sct/status string), empty on success.
    // Surfaced to logs today; available here for future API plumbing.
    [[nodiscard]] std::string error_detail() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return error_detail_;
    }

    void wait_for_completion() override {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return result_.has_value(); });
    }

    TransferStrategy get_strategy() const override {
        return TransferStrategy::SPDK_NVMF;
    }

    bool track_timing() const { return track_timing_; }

    std::optional<NofTransferTiming> nof_timing() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!result_.has_value() || !timing_) {
            return std::nullopt;
        }
        auto timing = *timing_;
        timing.complete_to_observe_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - completed_at_)
                .count();
        return timing;
    }

   private:
    bool track_timing_ = false;
    std::unique_ptr<NofTransferTiming> timing_;
    std::chrono::steady_clock::time_point completed_at_{};
    std::string error_detail_;
};

class FilereadOperationState : public OperationState {
   public:
    explicit FilereadOperationState(uint64_t trace_id = 0)
        : OperationState(trace_id) {}

    bool is_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }

    void set_completed(ErrorCode error_code) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!result_.has_value());
            result_.emplace(error_code);
        }
        cv_.notify_all();
    }

    void wait_for_completion() override {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return result_.has_value(); });
    }

    TransferStrategy get_strategy() const override {
        return TransferStrategy::FILE_READ;
    }
};

/**
 * @brief Operation state for transfer engine operations
 */
/**
 * @brief Store-side timing of one transfer engine batch.
 *
 * Attached only to batches of at least MC_TRANSFER_TRACE_MIN_BYTES bytes.
 */
struct TransferEngineTrace {
    uint64_t trace_id = 0;
    uint64_t bytes = 0;
    size_t requests = 0;
    bool is_read = false;
    bool is_tent = false;
    int64_t open_segment_us = 0;
    int64_t alloc_batch_us = 0;
    int64_t build_requests_us = 0;
    int64_t engine_submit_us = 0;
    std::chrono::steady_clock::time_point submit_end;
    std::chrono::steady_clock::time_point wait_start;
    uint64_t polls = 0;
};

class TransferEngineOperationState : public OperationState {
   public:
    TransferEngineOperationState(TransferEngine& engine, BatchID batch_id,
                                 size_t batch_size, uint64_t trace_id = 0)
        : OperationState(trace_id),
          engine_(engine),
          tent_engine_(engine.getTentEngine().get()),
          batch_id_(batch_id),
          batch_size_(batch_size),
          start_ts_(getCurrentTimeInMilli()) {}

    ~TransferEngineOperationState();

    bool is_completed() override;

    void wait_for_completion() override;

    TransferStrategy get_strategy() const override {
        return TransferStrategy::TRANSFER_ENGINE;
    }

    // Must be called before the state is shared with any waiter.
    void set_trace(std::unique_ptr<TransferEngineTrace> trace) {
        trace_ = std::move(trace);
    }

   private:
    /**
     * @brief Check the current completion status of the task, make sure to lock
     * the mutex before calling this function.
     * Updates the internal state and returns true if the task is completed.
     */
    void check_task_status();

    void set_result_internal(ErrorCode error_code);

    void log_trace(ErrorCode error_code) const;

    TransferEngine& engine_;
    mooncake::tent::TransferEngine* tent_engine_;
    BatchID batch_id_;
    size_t batch_size_;
    const int64_t start_ts_;
    std::unique_ptr<TransferEngineTrace> trace_;
};

/**
 * @brief Represents the future result of an asynchronous transfer operation
 *
 * This class provides a std::future-like interface for asynchronous transfer
 * operations. Users can check completion status, wait for results, or get the
 * final error code.
 */
class TransferFuture {
   public:
    explicit TransferFuture(std::shared_ptr<OperationState> state);

    // Non-copyable but movable
    TransferFuture(const TransferFuture&) = delete;
    TransferFuture& operator=(const TransferFuture&) = delete;
    TransferFuture(TransferFuture&&) = default;
    TransferFuture& operator=(TransferFuture&&) = default;

    /**
     * @brief Check if the operation has completed (non-blocking)
     * @return true if the operation is finished, false otherwise
     */
    bool isReady() const;

    /**
     * @brief Wait for the operation to complete (blocking)
     * @return ErrorCode indicating success or failure
     */
    ErrorCode wait();

    /**
     * @brief Get the result, waiting if necessary (blocking)
     * @return ErrorCode indicating success or failure
     */
    ErrorCode get();

    /**
     * @brief Get the transfer strategy used by this operation
     * @return TransferStrategy enum value
     */
    TransferStrategy strategy() const;

    /**
     * @brief Per-task NoF timing, available only for sampled operations
     * after completion (empty for every other strategy)
     */
    std::optional<NofTransferTiming> nofTiming() const;

    /**
     * @brief Create another future that observes the same operation
     *
     * Used when one submitted batch carries several keys, so each key can
     * wait on it independently.
     */
    TransferFuture share() const { return TransferFuture(state_); }

   private:
    std::shared_ptr<OperationState> state_;
};

/// Scatter transfer operation returned by
/// TransferSubmitter::submitNativeScatter().
///
/// Thread safety: NOT thread-safe. Only one thread may call wait(),
/// waitFor(), or destroy this object at a time. The caller that received
/// the operation from submitNativeScatter() owns it for its entire lifetime.
class StoreScatterTransferOperation {
   public:
    StoreScatterTransferOperation(StoreScatterTransferOperation&&) noexcept;
    StoreScatterTransferOperation& operator=(
        StoreScatterTransferOperation&&) noexcept;
    ~StoreScatterTransferOperation();

    StoreScatterTransferOperation(const StoreScatterTransferOperation&) =
        delete;
    StoreScatterTransferOperation& operator=(
        const StoreScatterTransferOperation&) = delete;

    Status wait();
    Status waitFor(std::chrono::nanoseconds timeout);

   private:
    class Impl;
    explicit StoreScatterTransferOperation(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
    friend class TransferSubmitter;
};

/**
 * @brief Memory copy operation descriptor
 */
struct MemcpyOperation {
    void* dest;
    const void* src;
    size_t size;

    MemcpyOperation(void* d, const void* s, size_t sz)
        : dest(d), src(s), size(sz) {}
};

/**
 * @brief Memcpy task for async execution
 */
struct MemcpyTask {
    std::vector<MemcpyOperation> operations;
    std::shared_ptr<MemcpyOperationState> state;
    uint64_t trace_id;
    std::chrono::steady_clock::time_point submitted_at;
    size_t queued_ahead = 0;

    MemcpyTask(std::vector<MemcpyOperation> ops,
               std::shared_ptr<MemcpyOperationState> s, uint64_t trace = 0)
        : operations(std::move(ops)),
          state(std::move(s)),
          trace_id(trace),
          submitted_at(std::chrono::steady_clock::now()) {}
};

/**
 * @brief Thread pool for asynchronous memcpy operations
 *
 * This class manages a bounded worker pool for accelerator copies and for
 * host copies when MC_STORE_MEMCPY_INLINE is disabled.
 * MC_STORE_MEMCPY_WORKERS controls the worker count.
 */
class MemcpyWorkerPool {
   public:
    explicit MemcpyWorkerPool();
    ~MemcpyWorkerPool();

    // Non-copyable, non-movable
    MemcpyWorkerPool(const MemcpyWorkerPool&) = delete;
    MemcpyWorkerPool& operator=(const MemcpyWorkerPool&) = delete;
    MemcpyWorkerPool(MemcpyWorkerPool&&) = delete;
    MemcpyWorkerPool& operator=(MemcpyWorkerPool&&) = delete;

    /**
     * @brief Submit a memcpy task for async execution
     * @param task The memcpy task to execute
     */
    void submitTask(MemcpyTask task);

   private:
    void workerThread();

    std::vector<std::thread> workers_;
    std::queue<MemcpyTask> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<bool> shutdown_;
};

struct NofQos;
struct NofMetric;

/**
 * @brief Per-task NoF timing state, allocated only for sampled tasks.
 *
 * Filled in by the NoF worker that owns the task; published to the
 * operation state as an NofTransferTiming snapshot once the task completes.
 */
struct NofTaskTiming {
    // Time points
    std::chrono::steady_clock::time_point worker_dequeued_at{};
    std::chrono::steady_clock::time_point qos_enqueued_at{};
    std::chrono::steady_clock::time_point qos_head_at{};
    std::chrono::steady_clock::time_point first_submit_at{};
    std::chrono::steady_clock::time_point last_submit_at{};
    std::chrono::steady_clock::time_point first_completion_at{};
    std::chrono::steady_clock::time_point previous_completion_at{};
    std::chrono::steady_clock::time_point credit_wait_started_at{};

    // Aggregated measurements
    uint64_t lane_task_sequence = 0;
    uint64_t poll_calls_at_first_submit = 0;
    uint64_t empty_polls_at_first_submit = 0;
    uint64_t polled_completions_at_first_submit = 0;
    int64_t max_sub_io_latency_us = 0;
    uint32_t max_sub_io_index = 0;
    uint16_t max_sub_io_cid = 0;
    int64_t max_completion_gap_us = 0;
    int64_t max_poll_return_gap_us = 0;
    int64_t credit_wait_us = 0;
    int64_t first_credit_wait_us = 0;
    int64_t qos_fifo_full_wait_us = 0;
    int64_t qos_fifo_partial_wait_us = 0;
    int64_t qos_fifo_zero_wait_us = 0;
    bool credit_wait_active = false;
    uint32_t qos_tasks_ahead_at_enqueue = 0;
    uint32_t submitted_sub_io_count = 0;
    uint32_t completed_sub_io_count = 0;
};

/**
 * @brief NoF operation descriptor (byte-oriented, opaque handle)
 */
struct NofTask {
    NofSegmentHandle* seg_handle;
    void* ptr;
    uint64_t byte_offset;
    uint64_t byte_length;
    uint64_t remaining_bytes;
    int outstanding_sub_io;
    NofIOOp op;
    int idx;  // subop idx
    bool failed;
    bool on_chain;
    std::shared_ptr<NofOperationState> state;
    // First failure's error detail (sc/sct/status string); written only on
    // the failure path, empty in steady state — zero hot-path cost.
    std::string error_detail;
    // Shared ownership of the worker's outstanding-I/O counter (formerly a
    // raw pointer to a worker-thread stack variable).
    std::shared_ptr<std::atomic<int64_t>> io_count;
    NofQos* nof_qos;
    NofTask* nxt;
    // Optional metrics sink; wired by TransferSubmitter, nullptr when the
    // client has no metrics.
    NofMetric* metric = nullptr;
    // Lane count of the task's segment, captured at dispatch. The QoS
    // inflight budget is split across lanes so a multi-lane segment keeps
    // the single-lane total.
    uint32_t lane_count = 1;
    // Set once the state has been completed. A failing sub-I/O can finish a
    // task that is still queued at the QoS head; the head is popped later,
    // so completion must be idempotent.
    bool completed = false;
    // Buffer classification for GPU Direct, resolved once per task.
    NofBufferInfo buffer_info;
    // Arrival time at the worker pool's dispatch queue.
    std::chrono::steady_clock::time_point queued_at;
    // Round-robin lane index picked at dispatch (diagnostic only).
    uint32_t lane_index = 0;
    // Owning NoF worker index (diagnostic only).
    int worker_index = -1;
    // First sub-I/O was admitted to the qpair.
    bool first_sub_io_submitted = false;
    // Populated only when the operation state samples this trace.
    std::unique_ptr<NofTaskTiming> timing;

    NofTask(NofSegmentHandle* handle, void* buf, uint64_t byte_off,
            uint64_t byte_len, NofIOOp op_code,
            std::shared_ptr<NofOperationState> s)
        : seg_handle(handle),
          ptr(buf),
          byte_offset(byte_off),
          byte_length(byte_len),
          remaining_bytes(byte_len),
          outstanding_sub_io(0),
          op(op_code),
          idx(0),
          failed(false),
          on_chain(false),
          state(std::move(s)),
          io_count(nullptr),
          nof_qos(nullptr),
          nxt(nullptr),
          queued_at(std::chrono::steady_clock::now()),
          timing(state && state->track_timing()
                     ? std::make_unique<NofTaskTiming>()
                     : nullptr) {}
};

struct NofSubTask {
    // Embedded adaptor keeps the steady-state submit/completion path free of
    // per-sub-IO heap allocation.
    NofIOAdaptor adaptor;
    NofTask* task;
    uint32_t submit_bytes;
    std::stack<NofSubTask*>* sub_task_pool;
    // Sub-I/O sequence within the task, and its submit time; populated only
    // for tasks whose timing is sampled.
    uint32_t sub_io_index = 0;
    std::chrono::steady_clock::time_point submitted_at{};
};

constexpr int kDefaultNofSubmitChunkBytes = (1 << 17);    // 128k
constexpr int kDefaultNofInflightBytesLimit = (1 << 25);  // 32M

struct NofQos {
    // Block size cached at QoS creation; the poll loop does not re-query it.
    const uint32_t block_size;
    const int blocks_per_chunk;
    const int inflight_blocks_limit;
    int inflight_blocks[static_cast<size_t>(NofIOOp::kNum)];
    NofTask* head[static_cast<size_t>(NofIOOp::kNum)];
    NofTask* tail[static_cast<size_t>(NofIOOp::kNum)];
    // Tasks with sub-I/O in flight on this lane. When the lane is isolated
    // their remaining callbacks never run, so they are failed from here.
    std::unordered_set<NofTask*> live;
    // Lane isolated: queued and in-flight tasks were failed; no more submits.
    bool dead = false;
    // Last submit-from-idle or completion on this lane. With I/O in flight
    // and no progress for MC_NOF_IO_TIMEOUT_MS the lane is isolated.
    std::chrono::steady_clock::time_point last_progress{};
    // Per-lane sequence numbers and poll counters backing the NoF timing
    // breakdown logs (diagnostic only).
    uint64_t enqueued_task_sequence = 0;
    uint64_t completed_task_sequence = 0;
    uint64_t poll_calls = 0;
    uint64_t empty_polls = 0;
    uint64_t polled_completions = 0;
    std::chrono::steady_clock::time_point last_poll_return_at{};

    // lane_count splits the inflight budget across a segment's lanes.
    explicit NofQos(uint32_t bs, uint32_t lane_count = 1);

    bool Empty() const {
        return (head[static_cast<size_t>(NofIOOp::kRead)] == nullptr &&
                head[static_cast<size_t>(NofIOOp::kWrite)] == nullptr);
    }

    void PushTask(NofTask* task) {
        size_t op = static_cast<size_t>(task->op);
        if (head[op] == nullptr) {
            head[op] = task;
            tail[op] = task;
        } else {
            tail[op]->nxt = task;
            tail[op] = task;
        }
    }

    void PopTask(NofIOOp op) {
        size_t i = static_cast<size_t>(op);
        if (head[i]) {
            head[i] = head[i]->nxt;
        }
    }
};

constexpr int kDefaultNofWorkers = 4;
class NofWorkerPool {
   public:
    explicit NofWorkerPool(std::shared_ptr<NVMeoFInitiator> initiator,
                           int numa_socket_id = 0);
    ~NofWorkerPool();

    NofWorkerPool(const NofWorkerPool&) = delete;
    NofWorkerPool& operator=(const NofWorkerPool&) = delete;
    NofWorkerPool(NofWorkerPool&&) = delete;
    NofWorkerPool& operator=(NofWorkerPool&&) = delete;

    void submitTask(NofTask task);

   private:
    void workerThread(int work_idx);

    std::shared_ptr<NVMeoFInitiator> initiator_;
    int worker_count_;
    int numa_socket_id_;
    std::vector<std::thread> workers_;
    std::unique_ptr<std::queue<NofTask>[]> task_queue_;
    std::unique_ptr<std::mutex[]> queue_mutex_;
    std::unique_ptr<std::condition_variable[]> queue_cv_;
    std::atomic<bool> shutdown_;
    // Picks a healthy lane of `segment` round-robin (nullptr when none),
    // reports the segment's lane count, and optionally the round-robin
    // index of the chosen lane (diagnostic only). Caller holds seg_mutex_.
    NofSegmentHandle* PickLaneLocked(NofSegmentHandle* segment,
                                     uint32_t* lane_count,
                                     uint32_t* lane_index = nullptr);

    std::mutex seg_mutex_;
    int seg_num = 0;
    // Keyed by lane handle: each lane is polled by exactly one worker.
    std::map<NofSegmentHandle*, int> seg_to_worker_;
    // Per segment (lane 0 handle): next lane for round-robin dispatch.
    std::map<NofSegmentHandle*, uint64_t> seg_next_lane_;
};

/**
 * @brief Fileread task for async execution
 */
struct FilereadTask {
    std::string file_path;
    size_t object_size;
    std::vector<Slice> slices;
    std::shared_ptr<FilereadOperationState> state;
    uint64_t trace_id;

    FilereadTask(const std::string& path, size_t size,
                 const std::vector<Slice>& slices_ref,
                 std::shared_ptr<FilereadOperationState> s, uint64_t trace = 0)
        : file_path(path),
          object_size(size),
          slices(slices_ref),
          state(std::move(s)),
          trace_id(trace) {}
};

/**
 * @brief Thread pool for asynchronous memcpy operations
 *
 * This class manages a single worker thread that executes memcpy operations
 * asynchronously.
 */
class FilereadWorkerPool {
   public:
    explicit FilereadWorkerPool(std::shared_ptr<StorageBackend>& backend);
    ~FilereadWorkerPool();

    // Non-copyable, non-movable
    FilereadWorkerPool(const FilereadWorkerPool&) = delete;
    FilereadWorkerPool& operator=(const FilereadWorkerPool&) = delete;
    FilereadWorkerPool(FilereadWorkerPool&&) = delete;
    FilereadWorkerPool& operator=(FilereadWorkerPool&&) = delete;

    /**
     * @brief Submit a memcpy task for async execution
     * @param task The memcpy task to execute
     */
    void submitTask(FilereadTask task);

   private:
    friend class FilereadWorkerPoolTestPeer;

    void workerThread();

    std::vector<std::thread> workers_;
    std::queue<FilereadTask> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<bool> shutdown_;
    std::shared_ptr<StorageBackend> backend_;
};

/**
 * @brief Submitter class for asynchronous transfer operations
 *
 * This class analyzes transfer requirements, selects optimal strategies, and
 * immediately submits operations returning TransferFuture objects for result
 * tracking.
 */
class TransferSubmitter {
   public:
    explicit TransferSubmitter(
        TransferEngine& engine, std::shared_ptr<StorageBackend>& backend,
        const std::string& local_hostname,
        TransferMetric* transfer_metric = nullptr, int numa_socket_id = 0,
        std::shared_ptr<NVMeoFInitiator> nof_initiator = nullptr,
        NofMetric* nof_metric = nullptr);

    /**
     * @brief Submit an asynchronous transfer operation
     *
     * Analyzes the transfer requirements, selects the optimal strategy,
     * and immediately submits the operation. Returns a TransferFuture
     * that can be used to track completion and get results.
     *
     * @param handles Buffer descriptors for the transfer
     * @param slices Memory slices for the transfer
     * @param op_code Transfer operation (READ/WRITE)
     * @return TransferFuture representing the async operation, or nullopt on
     * failure
     */
    std::optional<TransferFuture> submit(
        const Replica::Descriptor& replica, std::vector<Slice>& slices,
        TransferRequest::OpCode op_code, void* ptr = nullptr, size_t size = 0,
        TransferIntent intent = TransferIntent::kUnspecified);

    /**
     * @brief Submit a range read: read [src_offset, src_offset+size) from
     * object into slice.ptr. Slices must total exactly `size` bytes.
     */
    std::optional<TransferFuture> submitRangeRead(
        const Replica::Descriptor& replica, std::vector<Slice>& slices,
        uint64_t src_offset,
        TransferIntent intent = TransferIntent::kUnspecified);

    std::optional<TransferFuture> submitRangeWrite(
        const Replica::Descriptor& replica, std::vector<Slice>& slices,
        uint64_t dst_offset,
        TransferIntent intent = TransferIntent::kUnspecified);

    TransferEngine::ScatterTransferOperation submitScatter(
        const std::vector<TransferEngine::ScatterTransferRange>& transfers);
    StoreScatterTransferOperation submitNativeScatter(
        const std::vector<TransferEngine::ScatterTransferRange>& transfers,
        TransferIntent intent = TransferIntent::kUnspecified);

    // Co-located memory replicas are combined into one MemcpyTask when local
    // memcpy is enabled. Mixed or remote batches use TransferEngine.
    std::optional<TransferFuture> submit_batch(
        const std::vector<Replica::Descriptor>& replicas,
        std::vector<std::vector<Slice>>& all_slices,
        TransferRequest::OpCode op_code,
        TransferIntent intent = TransferIntent::kUnspecified);

    std::optional<TransferFuture> submitVSegment(
        const vsegment::VSegmentTransferPlan& plan,
        TransferRequest::OpCode op_code);

    std::optional<TransferFuture> submit_batch_get_offload_object(
        const std::string& transfer_engine_addr,
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& pointers,
        const std::unordered_map<std::string, std::vector<Slice>>&
            batched_slices,
        OffloadBufferAccess buffer_access,
        TransferIntent intent = TransferIntent::kUnspecified);

    // Push counterpart of submit_batch_get_offload_object, run on the data
    // owner. WRITEs each key's on-disk blob (staged in the owner's local
    // ClientBuffer at src_pointers[i]) directly into the requester's
    // destination slices, opening the requester's segment once.
    std::optional<TransferFuture> submit_batch_push_offload_object(
        const std::string& requester_te_addr,
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& src_pointers,
        const std::vector<std::vector<OffloadDstSlice>>& dst_slices);

    [[nodiscard]] bool canUseLocalMemcpy(const std::string& endpoint) const;

    /**
     * @brief Pure comparison helper: returns true iff both endpoints are
     * non-empty and identical. Exposed for unit testing of the locality
     * decision without instantiating a full TransferEngine.
     *
     * Two endpoints identify the same process only when their ip:port (or
     * full hostname) match exactly; same-host different-process pairs share
     * an IP but not a port and must NOT be treated as locally addressable.
     */
    static bool isSameProcessEndpoint(const std::string& handle_endpoint,
                                      const std::string& local_endpoint);

   private:
    TransferEngine& engine_;
    mooncake::tent::TransferEngine* tent_engine_ = nullptr;
    // Cached at construction: the local transport endpoint never changes for
    // the lifetime of the TransferSubmitter, so we avoid calling
    // engine_.getLocalIpAndPort() (which allocates a string) on every transfer.
    const std::string local_endpoint_;
    std::unique_ptr<MemcpyWorkerPool> memcpy_pool_;
    std::shared_ptr<NVMeoFInitiator> nof_initiator_;
    NofMetric* nof_metric_ = nullptr;
    std::unique_ptr<NofWorkerPool> nof_pool_;
    std::unique_ptr<FilereadWorkerPool> fileread_pool_;
    bool memcpy_enabled_;
    const std::string local_hostname_;
    TransferMetric* transfer_metric_;

    /**
     * @brief Select the optimal transfer strategy
     */
    TransferStrategy selectStrategy(const AllocatedBuffer::Descriptor& handle,
                                    const std::vector<Slice>& slices) const;

    /**
     * @brief Validate transfer parameters
     */
    bool validateTransferParams(const AllocatedBuffer::Descriptor& handle,
                                const std::vector<Slice>& slices) const;

    void appendMemcpyOperations(const AllocatedBuffer::Descriptor& handle,
                                const std::vector<Slice>& slices,
                                TransferRequest::OpCode op_code,
                                uint64_t buffer_offset,
                                std::vector<MemcpyOperation>& operations);

    /**
     * @brief Submit memcpy operation asynchronously
     */
    std::optional<TransferFuture> submitMemcpyOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices, const TransferRequest::OpCode op_code,
        uint64_t src_offset = 0);

    // Host-only copies may run inline (MC_STORE_MEMCPY_INLINE); accelerator
    // copies are queued on the memcpy pool.
    std::optional<TransferFuture> submitMemcpyOperations(
        std::vector<MemcpyOperation> operations, uint64_t trace_id = 0);

    /**
     * @brief Submit NoF (NVMe-oF) operation asynchronously
     */
    std::optional<TransferFuture> submitNofOperation(
        const AllocatedBuffer::Descriptor& handle, void* ptr, size_t size,
        const TransferRequest::OpCode op_code);

    /**
     * @brief Submit transfer engine operation asynchronously
     * @param src_offset Optional offset in source buffer (default 0)
     */
    std::optional<TransferFuture> submitTransferEngineOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices, const TransferRequest::OpCode op_code,
        uint64_t src_offset = 0,
        TransferIntent intent = TransferIntent::kUnspecified);

    std::optional<TransferFuture> submitMemoryReadOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices, uint64_t src_offset,
        TransferIntent intent = TransferIntent::kUnspecified);

    std::optional<TransferFuture> submitMemoryWriteOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices, uint64_t dst_offset,
        TransferIntent intent = TransferIntent::kUnspecified);

    std::optional<TransferFuture> submitFileReadOperation(
        const Replica::Descriptor& replica, std::vector<Slice>& slices,
        TransferRequest::OpCode op_code);

    /**
     * @brief Calculate total bytes for transfer operation and update metrics
     */
    void updateTransferMetrics(const std::vector<Slice>& slices,
                               TransferRequest::OpCode op);

    std::optional<TransferFuture> submitTransfer(
        std::vector<TransferRequest>& requests,
        TransferIntent intent = TransferIntent::kUnspecified,
        int64_t open_segment_us = 0);
};

}  // namespace mooncake
