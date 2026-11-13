#include "transfer_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <limits>
#include <sstream>
#include <string>
#include <vector>
#include "config/transfer_submitter_config.h"
#include "config/nof_debug_config.h"
#include "config/fileread_worker_pool_config.h"
#include "config/nof_worker_pool_config.h"
#include "device/accelerator_registry.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#ifdef USE_TENT
#include "tent/transfer_engine.h"
#endif
#include "nof/nvmeof_initiator.h"
#ifdef USE_NOF_URMA
#include "nof_gpu_direct_report.h"
#endif
#include "vsegment/vsegment_transfer.h"

namespace mooncake {

static int GetPositiveEnvOrDefault(const char* name, int default_value) {
    const char* raw_value = std::getenv(name);
    if (!raw_value || raw_value[0] == '\0') {
        return default_value;
    }

    errno = 0;
    char* end_ptr = nullptr;
    long parsed = std::strtol(raw_value, &end_ptr, 10);
    if (errno != 0 || end_ptr == raw_value ||
        (end_ptr != nullptr && *end_ptr != '\0') || parsed <= 0 ||
        parsed > std::numeric_limits<int>::max()) {
        LOG(WARNING) << "Invalid value for " << name << ": " << raw_value
                     << ", using default " << default_value;
        return default_value;
    }

    return static_cast<int>(parsed);
}

static bool ParseBoolEnv(const char* name, bool default_value) {
    const char* raw_value = std::getenv(name);
    if (!raw_value || raw_value[0] == '\0') {
        return default_value;
    }
    std::string normalized(raw_value);
    std::transform(
        normalized.begin(), normalized.end(), normalized.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (normalized == "1" || normalized == "true" || normalized == "yes" ||
        normalized == "on") {
        return true;
    }
    if (normalized == "0" || normalized == "false" || normalized == "no" ||
        normalized == "off") {
        return false;
    }
    LOG(WARNING) << "Invalid value for " << name << ": " << raw_value
                 << ", using default " << default_value;
    return default_value;
}

// Used by the polling loop, which also serves TENT batches in
// USE_EVENT_DRIVEN_COMPLETION builds, so not compiled out there.
// UB has dedicated posting/polling workers. Long Store-side busy waits can
// compete with those workers; keep a short spin for fast completions, then
// optionally back off. Range [0, 1s].
static int ReadUbPollDelayUs(const char* name, int default_value) {
    const char* value = std::getenv(name);
    if (!value) return default_value;
    char* end = nullptr;
    errno = 0;
    const long parsed = std::strtol(value, &end, 10);
    if (errno || end == value || *end || parsed < 0 || parsed > 1000000) {
        LOG(WARNING) << "Invalid " << name << "; using " << default_value;
        return default_value;
    }
    return static_cast<int>(parsed);
}

static int UbPollSpinUs() {
    static const int value = ReadUbPollDelayUs("MC_STORE_UB_POLL_SPIN_US", 50);
    return value;
}

static int UbPollSleepUs() {
    // Latency-sensitive reads busy poll by default; deployments preferring
    // lower CPU usage opt into backoff.
    static const int value = ReadUbPollDelayUs("MC_STORE_UB_POLL_SLEEP_US", 0);
    return value;
}

// A lane with I/O in flight and no completion for this long is isolated.
// 0 disables the check.
static std::chrono::milliseconds GetNofIoTimeout() {
    static const auto value = [] {
        const char* raw_value = std::getenv("MC_NOF_IO_TIMEOUT_MS");
        if (raw_value && std::string(raw_value) == "0") {
            return std::chrono::milliseconds(0);
        }
        return std::chrono::milliseconds(
            GetPositiveEnvOrDefault("MC_NOF_IO_TIMEOUT_MS", 60000));
    }();
    return value;
}

static bool IsNofDebugEnabled() {
    return mooncake::NoFDebugConfig::IsEnabledAtFirstUse();
}

static std::chrono::milliseconds GetNofDebugIntervalMs() {
    return mooncake::NoFDebugConfig::IntervalMsAtFirstUse();
}

static int GetNofSubmitChunkBytes() {
    static const int value = GetPositiveEnvOrDefault(
        "MC_NOF_SUBMIT_CHUNK_BYTES", mooncake::kDefaultNofSubmitChunkBytes);
    return value;
}

static int GetNofInflightBytesLimit() {
    static const int value = GetPositiveEnvOrDefault(
        "MC_NOF_INFLIGHT_BYTES_LIMIT", mooncake::kDefaultNofInflightBytesLimit);
    return value;
}

static int CountNofQueuedTasks(const mooncake::NofTask* head) {
    int count = 0;
    const mooncake::NofTask* cursor = head;
    while (cursor != nullptr) {
        ++count;
        cursor = cursor->nxt;
    }
    return count;
}

// Completes the task once all bytes were submitted and all sub-I/O returned.
// Idempotent: a failing sub-I/O can complete a task that is still queued at
// the QoS head (on_chain); popping the head later calls this again. The task
// is deleted only when it is off the chain.
static inline void NofTaskCompletion(mooncake::NofTask* task) {
    if (task->remaining_bytes != 0 || task->outstanding_sub_io != 0) {
        return;
    }
    if (!task->completed) {
        task->completed = true;
        if (task->nof_qos) {
            task->nof_qos->live.erase(task);
        }
        task->state->set_completed(task->failed
                                       ? mooncake::ErrorCode::TRANSFER_FAIL
                                       : mooncake::ErrorCode::OK,
                                   std::move(task->error_detail));
    }
    if (!task->on_chain) {
        delete task;
    }
}

static void nof_io_complete(void* ctx, const mooncake::NofIOCompletion& c) {
    if (!ctx) {
        LOG(ERROR) << "nof_io_complete ctx is null";
        return;
    }

    mooncake::NofSubTask* sub_task =
        reinterpret_cast<mooncake::NofSubTask*>(ctx);
    mooncake::NofTask* task = sub_task->task;
    mooncake::NofQos* nof_qos = task->nof_qos;
    size_t op = static_cast<size_t>(task->op);

    if (task->io_count &&
        task->io_count->fetch_sub(1, std::memory_order_acq_rel) <= 0) {
        LOG(ERROR) << "total outstanding io < 0";
    }

    if (--(task->outstanding_sub_io) < 0) {
        LOG(ERROR) << "task outstanding io < 0";
    }

    nof_qos->inflight_blocks[op] -=
        static_cast<int>(sub_task->submit_bytes / nof_qos->block_size);
    if (nof_qos->inflight_blocks[op] < 0) {
        LOG(ERROR) << "qos inflight blocks < 0";
    }
    nof_qos->last_progress = std::chrono::steady_clock::now();
#ifdef USE_NOF_URMA
    mooncake::RecordNofIoCompleted(task->buffer_info, c.success,
                                   task->op == mooncake::NofIOOp::kWrite,
                                   sub_task->submit_bytes);
#endif

    if (!c.success) {
#ifdef USE_NOF
        // NofMetric is only defined in USE_NOF builds (metric is always
        // nullptr elsewhere).
        if (task->metric) {
            if (task->op == NofIOOp::kWrite) {
                task->metric->RecordWriteErrors();
            } else {
                task->metric->RecordReadErrors();
            }
        }
#endif
        LOG(ERROR) << "nof task I/O failed: sc=" << c.sc << " sct=" << c.sct
                   << " " << c.error_string;
        task->remaining_bytes = 0;
        task->failed = true;
        if (task->error_detail.empty()) {
            task->error_detail = c.error_string;  // keep the first error detail
        }
#ifdef USE_NOF
    } else if (task->metric) {
        if (task->op == NofIOOp::kWrite) {
            task->metric->ObserveWrite(sub_task->submit_bytes);
        } else {
            task->metric->ObserveRead(sub_task->submit_bytes);
        }
#endif
    }

    NofTaskCompletion(task);

    sub_task->sub_task_pool->push(sub_task);
}
namespace {

#ifdef USE_TENT
int GetTentPositiveEnvOrDefault(const char* name, int default_value) {
    const char* raw_value = std::getenv(name);
    if (!raw_value || raw_value[0] == '\0') return default_value;

    errno = 0;
    char* end_ptr = nullptr;
    long parsed = std::strtol(raw_value, &end_ptr, 10);
    if (errno != 0 || end_ptr == raw_value || *end_ptr != '\0' || parsed <= 0 ||
        parsed > std::numeric_limits<int>::max()) {
        LOG(WARNING) << "Invalid value for " << name << ": " << raw_value
                     << ", using default " << default_value;
        return default_value;
    }
    return static_cast<int>(parsed);
}

std::optional<tent::Request::OpCode> ToTentOpCode(
    TransferRequest::OpCode op_code) {
    switch (op_code) {
        case TransferRequest::READ:
            return tent::Request::READ;
        case TransferRequest::WRITE:
            return tent::Request::WRITE;
        default:
            return std::nullopt;
    }
}

// NOTE: This only handles TransferIntent values (0-3). The compat-layer
// toTentIntent in transfer_engine.cpp also handles raw int values 4-6
// (CHECKPOINT, WEIGHT_LOADING, STAGING_INTERNAL) for forward compatibility.
std::optional<tent::IntentType> ToTentIntent(TransferIntent intent) {
    switch (intent) {
        case TransferIntent::kUnspecified:
            return tent::IntentType::INTENT_UNSPEC;
        case TransferIntent::kForegroundGet:
            return tent::IntentType::FOREGROUND_GET;
        case TransferIntent::kBackgroundPrefetch:
            return tent::IntentType::BACKGROUND_PREFETCH;
        case TransferIntent::kMigration:
            return tent::IntentType::MIGRATION;
        default:
            return std::nullopt;
    }
}

std::optional<TransferStatusEnum> ToClassicTransferStatus(
    tent::TransferStatusEnum status) {
    switch (status) {
        case tent::INITIAL:
        case tent::PENDING:
            return TransferStatusEnum::WAITING;
        case tent::INVALID:
            return TransferStatusEnum::INVALID;
        case tent::CANCELED:
            return TransferStatusEnum::CANCELED;
        case tent::COMPLETED:
            return TransferStatusEnum::COMPLETED;
        case tent::TIMEOUT:
            return TransferStatusEnum::TIMEOUT;
        case tent::FAILED:
            return TransferStatusEnum::FAILED;
        default:
            return std::nullopt;
    }
}
#endif

}  // namespace

NofQos::NofQos(uint32_t bs, uint32_t lane_count)
    : block_size(bs == 0 || bs == kInvalidBlockSize ? 1 : bs),
      blocks_per_chunk(
          std::max(1, GetNofSubmitChunkBytes() / static_cast<int>(block_size))),
      // Split per lane so a multi-lane segment keeps the single-lane total.
      inflight_blocks_limit(std::max(
          1, GetNofInflightBytesLimit() / static_cast<int>(block_size) /
                 static_cast<int>(std::max<uint32_t>(lane_count, 1)))) {
    for (size_t i = 0; i < static_cast<size_t>(NofIOOp::kNum); ++i) {
        inflight_blocks[i] = 0;
        head[i] = nullptr;
        tail[i] = nullptr;
    }
}

// ============================================================================
// FilereadWorkerPool Implementation
// ============================================================================
FilereadWorkerPool::FilereadWorkerPool(std::shared_ptr<StorageBackend>& backend)
    : shutdown_(false) {
    static const auto config = FilereadWorkerPoolConfig::FromEnvironment();
    const int num_workers = config.worker_count;
    VLOG(1) << "Creating FilereadWorkerPool with " << num_workers << " workers";

    // Start worker threads
    workers_.reserve(num_workers);
    for (int i = 0; i < num_workers; ++i) {
        workers_.emplace_back(&FilereadWorkerPool::workerThread, this);
    }
    backend_ = backend;
}

FilereadWorkerPool::~FilereadWorkerPool() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_.store(true);
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "FilereadWorkerPool destroyed";
}

void FilereadWorkerPool::submitTask(FilereadTask task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_.load()) {
            LOG(WARNING)
                << "Attempting to submit task to shutdown FilereadWorkerPool";
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

void FilereadWorkerPool::workerThread() {
    VLOG(2) << "FilereadWorkerPool worker thread started";

    while (true) {
        FilereadTask task("", 0, {}, nullptr);

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.state) {
            try {
                if (!backend_) {
                    LOG(ERROR)
                        << "Backend is not initialized, cannot load object";
                    task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                    continue;
                }

                auto load_result = backend_->LoadObject(
                    task.file_path, task.slices, task.object_size);
                if (load_result) {
                    VLOG(2) << "Fileread task completed successfully with "
                            << task.file_path;
                    task.state->set_completed(ErrorCode::OK);
                } else {
                    LOG(ERROR)
                        << "Fileread task failed for file: " << task.file_path
                        << " with error: " << toString(load_result.error());
                    task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async fileread: " << e.what();
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            }
        }
    }

    VLOG(2) << "FilereadWorkerPool worker thread exiting";
}

// ============================================================================
// NofWorkerPool Implementation
// ============================================================================
// to fully utilize the available ssd bandwidth, we use a default of 4 worker
// threads.

NofWorkerPool::NofWorkerPool(std::shared_ptr<NVMeoFInitiator> initiator,
                             int numa_socket_id)
    : initiator_(std::move(initiator)),
      worker_count_(NoFWorkerPoolConfig::AtFirstUse().worker_count),
      numa_socket_id_(numa_socket_id),
      task_queue_(std::make_unique<std::queue<NofTask>[]>(worker_count_)),
      queue_mutex_(std::make_unique<std::mutex[]>(worker_count_)),
      queue_cv_(std::make_unique<std::condition_variable[]>(worker_count_)),
      shutdown_(false) {
    VLOG(1) << "Creating NofWorkerPool with " << worker_count_ << " workers";

    // Start worker threads
    workers_.reserve(worker_count_);
    for (int i = 0; i < worker_count_; ++i) {
        workers_.emplace_back(&NofWorkerPool::workerThread, this, i);
    }
}

NofWorkerPool::~NofWorkerPool() {
    if (shutdown_.exchange(true)) {
        return;
    }

    for (int i = 0; i < worker_count_; ++i) {
        queue_cv_[i].notify_all();
    }

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "NofWorkerPool destroyed";
}

void NofWorkerPool::submitTask(NofTask task) {
    if (!task.state) {
        LOG(ERROR) << "Attempting to submit nof task without state";
        return;
    }

    if (shutdown_.load()) {
        LOG(WARNING) << "Attempting to submit task to shutdown NofWorkerPool";
        task.state->set_completed(ErrorCode::TRANSFER_FAIL);
        return;
    }

    int worker_idx = -1;
    {
        std::lock_guard<std::mutex> lock(seg_mutex_);
        // Each task runs on one lane of its segment; the lane handle then
        // keys worker affinity and QoS exactly like a single-lane segment.
        uint32_t lane_count = 1;
        NofSegmentHandle* lane = PickLaneLocked(task.seg_handle, &lane_count);
        if (lane == nullptr) {
            LOG(ERROR) << "No healthy NoF lane for segment "
                       << task.seg_handle;
            task.state->set_completed(ErrorCode::TRANSFER_FAIL,
                                      "no healthy NoF lane");
            return;
        }
        task.seg_handle = lane;
        task.lane_count = lane_count;
        NofSegmentHandle* seg = lane;
        bool new_binding = false;
        if (seg_to_worker_.find(seg) != seg_to_worker_.end()) {
            worker_idx = seg_to_worker_[seg];
        } else {
            worker_idx = (seg_num++ % worker_count_);
            seg_to_worker_[seg] = worker_idx;
            new_binding = true;
        }
        if (new_binding && IsNofDebugEnabled()) {
            LOG(INFO) << "nof_worker_bind seg_handle=" << seg
                      << " worker_idx=" << worker_idx;
        }
    }
    if (worker_idx < 0 || worker_idx >= worker_count_) {
        LOG(ERROR) << "seg is not bind to invalid worker " << worker_idx;
        task.state->set_completed(ErrorCode::TRANSFER_FAIL);
        return;
    }

    {
        std::lock_guard<std::mutex> lock(queue_mutex_[worker_idx]);
        task_queue_[worker_idx].push(std::move(task));
    }
    queue_cv_[worker_idx].notify_one();
}

NofSegmentHandle* NofWorkerPool::PickLaneLocked(NofSegmentHandle* segment,
                                                uint32_t* lane_count) {
    const size_t count = initiator_->GetLaneCount(segment);
    *lane_count = static_cast<uint32_t>(std::max<size_t>(count, 1));
    if (count == 0) {
        return nullptr;
    }
    uint64_t& next = seg_next_lane_[segment];
    for (size_t attempt = 0; attempt < count; ++attempt) {
        NofSegmentHandle* lane = initiator_->GetLane(segment, next++ % count);
        if (lane != nullptr && initiator_->IsLaneHealthy(lane)) {
            return lane;
        }
    }
    return nullptr;
}

static bool HasBufferedTask(
    const std::map<NofSegmentHandle*, std::unique_ptr<NofQos>>& seg_to_qos) {
    for (const auto& [_, nof_qos] : seg_to_qos) {
        if (!nof_qos->Empty()) {
            return true;
        }
    }
    return false;
}

constexpr int kDefaultNofSubTaskChunkSize = 4096;

static inline bool CheckSubTaskPool(std::stack<NofSubTask*>& sub_task_pool,
                                    std::vector<NofSubTask*>& sub_task_chunks,
                                    int work_idx) {
    if (!sub_task_pool.empty()) {
        return true;
    }
    NofSubTask* sub_tasks =
        new (std::nothrow) NofSubTask[kDefaultNofSubTaskChunkSize];
    if (!sub_tasks) {
        LOG(ERROR) << "alloc NofSubTask failed, worker " << work_idx;
        return false;
    }
    sub_task_chunks.push_back(sub_tasks);

    for (int i = 0; i < kDefaultNofSubTaskChunkSize; ++i) {
        sub_tasks[i].sub_task_pool = &sub_task_pool;
        sub_task_pool.push(&sub_tasks[i]);
    }

    return true;
}

// Isolates a lane after a fatal submit/poll error or an I/O timeout. Runs on
// the lane's worker (its only polling thread), as ReleaseFailedLane requires.
// Outstanding I/O is aborted: callbacks either run inside ReleaseFailedLane
// (failing their tasks normally) or never run, in which case the stranded
// tasks are failed here. Queued tasks fail without being submitted.
static void IsolateNofLane(NVMeoFInitiator& initiator, NofSegmentHandle* lane,
                           NofQos* qos,
                           std::atomic<int64_t>& total_outstanding_io,
                           int work_idx, const char* reason) {
    LOG(ERROR) << "Isolating NoF lane " << lane << " on worker " << work_idx
               << ": " << reason << " (in-flight tasks=" << qos->live.size()
               << ")";
    qos->dead = true;
    initiator.MarkLaneFailed(lane);
    initiator.ReleaseFailedLane(lane);

    // Collect every affected task once: a partially submitted task can be
    // both queued at the head and in flight.
    std::unordered_set<NofTask*> affected(qos->live.begin(), qos->live.end());
    for (size_t i = 0; i < static_cast<size_t>(NofIOOp::kNum); ++i) {
        while (NofTask* task = qos->head[i]) {
            qos->PopTask(static_cast<NofIOOp>(i));
            task->on_chain = false;
            affected.insert(task);
        }
        qos->inflight_blocks[i] = 0;
    }
    for (NofTask* task : affected) {
        const int lost = task->outstanding_sub_io;
        if (lost > 0) {
            total_outstanding_io.fetch_sub(lost, std::memory_order_acq_rel);
#ifdef USE_NOF_URMA
            RecordNofIoAborted(task->buffer_info, task->op == NofIOOp::kWrite,
                               static_cast<uint64_t>(lost));
#endif
        }
        task->outstanding_sub_io = 0;
        task->remaining_bytes = 0;
        task->failed = true;
        if (task->error_detail.empty()) {
            task->error_detail = reason;
        }
        NofTaskCompletion(task);  // completes once; deletes (off chain)
    }
}

void NofWorkerPool::workerThread(int work_idx) {
    bindToSocket(numa_socket_id_);
    VLOG(2) << "NofWorkerPool worker thread started";

    // No longer a stack-variable address: the worker and the task share
    // ownership.
    auto total_outstanding_io = std::make_shared<std::atomic<int64_t>>(0);
    std::map<NofSegmentHandle*, std::unique_ptr<NofQos>> seg_to_qos;
    std::stack<NofSubTask*> sub_task_pool;
    std::vector<NofSubTask*> sub_task_chunks;
    auto& task_queue = task_queue_[work_idx];
    auto& queue_cv = queue_cv_[work_idx];
    auto& queue_mutex = queue_mutex_[work_idx];
    auto last_debug_snapshot = std::chrono::steady_clock::now();

    // Best-effort preallocation: the submit loop retries on demand. The
    // worker must not exit here — a dead worker hangs teardown at join().
    (void)CheckSubTaskPool(sub_task_pool, sub_task_chunks, work_idx);

    while (true) {
        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_cv.wait(lock, [this, &task_queue, &total_outstanding_io,
                                 &seg_to_qos] {
                return shutdown_.load() || !task_queue.empty() ||
                       total_outstanding_io->load(std::memory_order_acquire) !=
                           0 ||
                       HasBufferedTask(seg_to_qos);
            });

            if (shutdown_.load() && task_queue.empty() &&
                total_outstanding_io->load(std::memory_order_acquire) == 0 &&
                !HasBufferedTask(seg_to_qos)) {
                break;
            }

            while (!task_queue.empty()) {
                NofTask* task =
                    new (std::nothrow) NofTask(std::move(task_queue.front()));
                if (task == nullptr) {
                    LOG(ERROR) << "alloc NofTask failed, worker " << work_idx;
                    // The front was not moved from: fail it and pop, or the
                    // worker spins forever and teardown hangs at join().
                    auto& front = task_queue.front();
                    if (front.state) {
                        front.state->set_completed(ErrorCode::TRANSFER_FAIL);
                    }
                    task_queue.pop();
                    continue;
                }

                NofQos* nof_qos = nullptr;
                auto it = seg_to_qos.find(task->seg_handle);
                if (it == seg_to_qos.end()) {
                    auto qos = std::make_unique<NofQos>(
                        initiator_->GetBlockSize(task->seg_handle),
                        task->lane_count);
                    if (qos == nullptr) {
                        LOG(ERROR)
                            << "alloc NofQos failed, worker " << work_idx;
                        // task owns the moved-out state: fail it, then drop
                        // the task and the moved-from queue front.
                        if (task->state) {
                            task->state->set_completed(
                                ErrorCode::TRANSFER_FAIL);
                        }
                        delete task;
                        task_queue.pop();
                        continue;
                    }
                    nof_qos = qos.get();
                    seg_to_qos[task->seg_handle] = std::move(qos);
                    if (IsNofDebugEnabled()) {
                        LOG(INFO)
                            << "nof_qos_create worker_idx=" << work_idx
                            << " seg_handle=" << task->seg_handle
                            << " blocks_per_chunk="
                            << seg_to_qos[task->seg_handle]->blocks_per_chunk
                            << " inflight_blocks_limit="
                            << seg_to_qos[task->seg_handle]
                                   ->inflight_blocks_limit;
                    }
                } else {
                    nof_qos = it->second.get();
                }
                // The lane may have been isolated after this task was routed
                // to it.
                const char* reject_reason = nullptr;
                if (nof_qos->dead ||
                    !initiator_->IsLaneHealthy(task->seg_handle)) {
                    reject_reason = "NoF lane failed";
                }
#ifdef USE_NOF_URMA
                if (reject_reason == nullptr) {
                    // GPU Direct: classify once per task. Device memory that
                    // cannot be served must fail, never silently stage.
                    task->buffer_info =
                        initiator_->ResolveBuffer(task->ptr, task->byte_length);
                    if (task->buffer_info.type == NofMemoryType::kError ||
                        task->buffer_info.type ==
                            NofMemoryType::kNotSupported) {
                        reject_reason = "NoF buffer classification failed";
                    }
                }
#endif
                if (reject_reason != nullptr) {
                    task->state->set_completed(ErrorCode::TRANSFER_FAIL,
                                               reject_reason);
                    delete task;
                    task_queue.pop();
                    continue;
                }
                task->io_count = total_outstanding_io;
                task->nof_qos = nof_qos;
                task->on_chain = true;
                nof_qos->PushTask(task);
                task_queue.pop();
            }
        }

        for (auto& [seg_handle, nof_qos] : seg_to_qos) {
            if (nof_qos->dead) {
                continue;
            }
            // block_size comes from the QoS cache; the hot loop no longer
            // queries the initiator.
            const uint32_t block_size = nof_qos->block_size;
            const uint64_t chunk_bytes =
                static_cast<uint64_t>(nof_qos->blocks_per_chunk) * block_size;
            const char* isolate_reason = nullptr;
            for (size_t i = 0; i < static_cast<size_t>(NofIOOp::kNum) &&
                               isolate_reason == nullptr;
                 ++i) {
                int avail_blocks = nof_qos->inflight_blocks_limit -
                                   nof_qos->inflight_blocks[i];
                while (nof_qos->head[i] && avail_blocks > 0) {
                    NofTask* task = nof_qos->head[i];
                    NofSubTask* sub_task;
                    while (task->remaining_bytes > 0 && avail_blocks > 0) {
                        const uint64_t avail_bytes =
                            static_cast<uint64_t>(avail_blocks) * block_size;
                        const uint64_t sent_bytes =
                            task->byte_length - task->remaining_bytes;
                        const uint32_t submit_bytes =
                            static_cast<uint32_t>(std::min<uint64_t>(
                                {avail_bytes, task->remaining_bytes,
                                 chunk_bytes}));
                        const uint64_t submit_off =
                            task->byte_offset + sent_bytes;
                        void* submit_ptr =
                            reinterpret_cast<char*>(task->ptr) + sent_bytes;

                        if (!CheckSubTaskPool(sub_task_pool, sub_task_chunks,
                                              work_idx)) {
                            task->failed = true;
                            task->remaining_bytes = 0;
                            break;
                        }
                        sub_task = sub_task_pool.top();
                        sub_task_pool.pop();
                        sub_task->task = task;
                        sub_task->submit_bytes = submit_bytes;
                        sub_task->adaptor = {&nof_io_complete, sub_task};

                        // Host buffers take the classic path inside the
                        // initiator; device buffers use GPU Direct.
                        int ret = initiator_->SubmitIOWithBuffer(
                            task->seg_handle, submit_ptr, submit_off,
                            submit_bytes, task->op, &sub_task->adaptor,
                            task->buffer_info);
                        if (ret != 0) {
                            // Return the pool slot: a sub-task taken out and
                            // then failing to submit must be returned, or
                            // repeated failures would exhaust the 4096 slots
                            // and allocate new chunks unboundedly.
                            sub_task->sub_task_pool->push(sub_task);
                            const bool has_inflight =
                                nof_qos->inflight_blocks[0] > 0 ||
                                nof_qos->inflight_blocks[1] > 0;
                            if ((ret == -EAGAIN || ret == -ENOMEM) &&
                                has_inflight) {
                                // Queue pressure: keep the task at the head,
                                // poll to return credits, retry next round.
                                avail_blocks = 0;
                                break;
                            }
#ifdef USE_NOF
                            if (task->metric) {
                                if (task->op == NofIOOp::kWrite) {
                                    task->metric->RecordWriteErrors();
                                } else {
                                    task->metric->RecordReadErrors();
                                }
                            }
#endif
                            LOG(ERROR) << "work " << work_idx << ", lane "
                                       << task->seg_handle
                                       << " submit io fail: ret " << ret;
                            if (ret == -ENXIO || ret == -ENODEV) {
                                // The lane is gone; fail everything on it.
                                isolate_reason = "NoF lane submit failed";
                                avail_blocks = 0;
                                break;
                            }
                            task->failed = true;
                            task->remaining_bytes = 0;
                        } else {
                            const int submit_blocks =
                                static_cast<int>(submit_bytes / block_size);
                            if (nof_qos->live.empty()) {
                                // Idle -> busy: start the timeout clock.
                                nof_qos->last_progress =
                                    std::chrono::steady_clock::now();
                            }
                            nof_qos->live.insert(task);
                            task->idx++;
                            task->remaining_bytes -= submit_bytes;
                            nof_qos->inflight_blocks[i] += submit_blocks;
                            avail_blocks -= submit_blocks;
                            task->outstanding_sub_io++;
                            total_outstanding_io->fetch_add(
                                1, std::memory_order_acq_rel);
#ifdef USE_NOF_URMA
                            RecordNofIoSubmitted(task->buffer_info,
                                                 task->op == NofIOOp::kWrite,
                                                 submit_bytes);
#endif
                        }
                    }
                    if (isolate_reason != nullptr) {
                        break;
                    }
                    if (task->remaining_bytes == 0) {
                        nof_qos->PopTask(static_cast<NofIOOp>(i));
                        task->on_chain = false;
                        NofTaskCompletion(task);
                    }
                }
            }
            if (isolate_reason != nullptr) {
                IsolateNofLane(*initiator_, seg_handle, nof_qos.get(),
                               *total_outstanding_io, work_idx,
                               isolate_reason);
            }
        }

        if (total_outstanding_io->load(std::memory_order_acquire) > 0) {
            const auto io_timeout = GetNofIoTimeout();
            const auto now = std::chrono::steady_clock::now();
            for (auto& [lane, nof_qos] : seg_to_qos) {
                if (nof_qos->dead) {
                    continue;
                }
                const int64_t ret = initiator_->PollCompletion(lane, 0);
                if (ret == -ENXIO || ret == -ENODEV) {
                    IsolateNofLane(*initiator_, lane, nof_qos.get(),
                                   *total_outstanding_io, work_idx,
                                   "NoF lane poll failed");
                } else if (ret < 0) {
                    LOG(ERROR) << "poll completion error: ret " << ret;
                } else if (io_timeout.count() > 0 && !nof_qos->live.empty() &&
                           now - nof_qos->last_progress > io_timeout) {
                    IsolateNofLane(*initiator_, lane, nof_qos.get(),
                                   *total_outstanding_io, work_idx,
                                   "NoF lane I/O timeout");
                }
            }
        }

        if (IsNofDebugEnabled()) {
            auto now = std::chrono::steady_clock::now();
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_debug_snapshot);
            if (elapsed >= GetNofDebugIntervalMs()) {
                for (const auto& [seg_handle, nof_qos] : seg_to_qos) {
                    LOG(INFO)
                        << "nof_qos_state worker_idx=" << work_idx
                        << " seg_handle=" << seg_handle
                        << " inflight_read=" << nof_qos->inflight_blocks[0]
                        << " inflight_write=" << nof_qos->inflight_blocks[1]
                        << " inflight_limit=" << nof_qos->inflight_blocks_limit
                        << " queued_read="
                        << CountNofQueuedTasks(nof_qos->head[0])
                        << " queued_write="
                        << CountNofQueuedTasks(nof_qos->head[1])
                        << " total_outstanding_io="
                        << total_outstanding_io->load(
                               std::memory_order_acquire);
                }
                last_debug_snapshot = now;
            }
        }
    }

    for (auto* sub_tasks : sub_task_chunks) {
        delete[] sub_tasks;
    }
    // seg_to_qos will automatically clean up NofQos objects using
    // unique_ptr

    VLOG(2) << "NofWorkerPool worker thread exiting";
}

// ============================================================================
// MemcpyWorkerPool Implementation
// ============================================================================
// A single worker serializes otherwise independent local-memory puts. Four
// workers expose several cores of memcpy bandwidth without creating an
// unbounded number of threads. Tune with MC_STORE_MEMCPY_WORKERS (1..64).
constexpr int kDefaultMemcpyWorkers = 4;
constexpr int kMaxMemcpyWorkers = 64;

static int GetMemcpyWorkerCount() {
    static const int worker_count = [] {
        const unsigned int hardware_threads =
            std::thread::hardware_concurrency();
        const int automatic_default =
            hardware_threads == 0
                ? kDefaultMemcpyWorkers
                : std::max(1, std::min(kDefaultMemcpyWorkers,
                                       static_cast<int>(hardware_threads)));
        const int parsed = GetPositiveEnvOrDefault("MC_STORE_MEMCPY_WORKERS",
                                                   automatic_default);
        if (parsed > kMaxMemcpyWorkers) {
            LOG(WARNING) << "MC_STORE_MEMCPY_WORKERS=" << parsed
                         << " exceeds " << kMaxMemcpyWorkers << ", using "
                         << automatic_default;
            return automatic_default;
        }
        return parsed;
    }();
    return worker_count;
}

// Host-to-host copies run inline on the submitting thread by default
// (MC_STORE_MEMCPY_INLINE=0 queues them on the pool instead).
static bool IsInlineHostMemcpyEnabled() {
    static const bool enabled = ParseBoolEnv("MC_STORE_MEMCPY_INLINE", true);
    return enabled;
}

MemcpyWorkerPool::MemcpyWorkerPool() : shutdown_(false) {
    const int worker_count = GetMemcpyWorkerCount();
    VLOG(1) << "Creating MemcpyWorkerPool with " << worker_count
            << " workers; tune with MC_STORE_MEMCPY_WORKERS";

    // Start worker threads
    workers_.reserve(worker_count);
    for (int i = 0; i < worker_count; ++i) {
        workers_.emplace_back(&MemcpyWorkerPool::workerThread, this);
    }
}

MemcpyWorkerPool::~MemcpyWorkerPool() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_.store(true);
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "MemcpyWorkerPool destroyed";
}

void MemcpyWorkerPool::submitTask(MemcpyTask task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_.load()) {
            LOG(WARNING)
                << "Attempting to submit task to shutdown MemcpyWorkerPool";
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

void MemcpyWorkerPool::workerThread() {
    VLOG(2) << "MemcpyWorkerPool worker thread started";

    while (true) {
        MemcpyTask task({}, nullptr);

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.state) {
            try {
                bool ok = true;
                auto runtime_accelerator =
                    device::GetAcceleratorRegistry().RuntimeAccelerators();
                for (const auto& op : task.operations) {
                    device::PointerInfo src_info;
                    device::PointerInfo dst_info;
                    auto* src_device = runtime_accelerator.FindDeviceForPointer(
                        op.src, &src_info);
                    auto* dst_device = runtime_accelerator.FindDeviceForPointer(
                        op.dest, &dst_info);

                    if (!src_device && !dst_device) {
                        std::memcpy(op.dest, op.src, op.size);
                    } else {
                        if (src_device && dst_device &&
                            src_device != dst_device) {
                            LOG(ERROR)
                                << "GPU memcpy failed: source and destination "
                                   "belong to different accelerator runtimes"
                                << " src_dev=" << src_info.device_id
                                << " dst_dev=" << dst_info.device_id
                                << " size=" << op.size;
                            ok = false;
                            break;
                        }
                        const device::AcceleratorDevice* accelerator = nullptr;
                        int32_t device_id = -1;
                        device::CopyDirection direction;
                        if (src_device) {
                            accelerator = src_device;
                            device_id = src_info.device_id;
                            direction = device::CopyDirection::kDeviceToHost;
                            if (dst_device) {
                                direction =
                                    device::CopyDirection::kDeviceToDevice;
                            }
                        } else {
                            accelerator = dst_device;
                            device_id = dst_info.device_id;
                            direction = device::CopyDirection::kHostToDevice;
                        }
                        accelerator->SetContext(device_id);
                        if (!accelerator->Copy(op.dest, op.src, op.size,
                                               direction)) {
                            LOG(ERROR) << "GPU memcpy failed: src_dev="
                                       << src_info.device_id
                                       << " dst_dev=" << dst_info.device_id
                                       << " size=" << op.size;
                            ok = false;
                            break;
                        }
                    }
                }

                VLOG(2) << "Memcpy task completed with "
                        << task.operations.size() << " operations"
                        << (ok ? "" : " (with GPU copy failure)");
                task.state->set_completed(ok ? ErrorCode::OK
                                             : ErrorCode::TRANSFER_FAIL);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async memcpy: " << e.what();
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            }
        }
    }

    VLOG(2) << "MemcpyWorkerPool worker thread exiting";
}

// ============================================================================
// TransferEngineOperationState Implementation
// ============================================================================

TransferEngineOperationState::~TransferEngineOperationState() {
#ifdef USE_TENT
    if (tent_engine_)
        tent_engine_->freeBatch(batch_id_);
    else
#endif
        engine_.freeBatchID(batch_id_);
}

bool TransferEngineOperationState::is_completed() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (result_.has_value()) {
        return true;
    }

    check_task_status();
    return result_.has_value();
}

void TransferEngineOperationState::check_task_status() {
    // Check all transfers in the batch.
    // Wait for ALL tasks to reach a terminal state before setting the result,
    // even if some have already failed. This prevents the caller from seeing
    // "completed" while background transfers are still in progress, which
    // could cause issues when freeBatchID is called in the destructor.
    bool all_terminated = true;
    std::vector<size_t> failed_task_ids;

    for (size_t i = 0; i < batch_size_; ++i) {
        TransferStatus status;
        Status s;
#ifdef USE_TENT
        if (tent_engine_) {
            tent::TransferStatus tent_status;
            auto tent_s =
                tent_engine_->getTransferStatus(batch_id_, i, tent_status);
            if (!tent_s.ok()) {
                LOG(ERROR) << "Failed to get transfer status for batch "
                           << batch_id_ << " task " << i << " with error "
                           << tent_s.message();
                set_result_internal(ErrorCode::TRANSFER_FAIL);
                return;
            }
            auto classic_status = ToClassicTransferStatus(tent_status.s);
            if (!classic_status) {
                LOG(ERROR) << "Unknown TENT transfer status for batch "
                           << batch_id_ << " task " << i << " with status "
                           << static_cast<int>(tent_status.s);
                set_result_internal(ErrorCode::TRANSFER_FAIL);
                return;
            }
            status.s = *classic_status;
            status.transferred_bytes = tent_status.transferred_bytes;
            s = Status::OK();
        } else
#endif
        {
            s = engine_.getTransferStatus(batch_id_, i, status);
            if (!s.ok()) {
                LOG(ERROR) << "Failed to get transfer status for batch "
                           << batch_id_ << " task " << i << " with error "
                           << s.message();
                set_result_internal(ErrorCode::TRANSFER_FAIL);
                return;
            }
        }

        switch (status.s) {
            case TransferStatusEnum::COMPLETED:
                // This transfer is done successfully
                break;
            case TransferStatusEnum::FAILED:
            case TransferStatusEnum::CANCELED:
            case TransferStatusEnum::INVALID:
#ifndef USE_ASCEND_DIRECT
                VLOG(1) << "Transfer failed for batch " << batch_id_ << " task "
                        << i << " with status " << static_cast<int>(status.s);
#endif
                failed_task_ids.push_back(i);
                break;
            default:
                // Transfer is still in progress (WAITING, PENDING, etc.)
                all_terminated = false;
                break;
        }
    }

    if (!all_terminated) {
        // Some tasks are still in progress; wait for next poll iteration.
        // Do NOT set result yet, even if some tasks have already failed.
        return;
    }

    // All tasks have reached a terminal state.
    ErrorCode ec = ErrorCode::OK;
    if (!failed_task_ids.empty()) {
        std::ostringstream oss;
        for (size_t j = 0; j < failed_task_ids.size(); ++j) {
            if (j > 0) oss << ", ";
            oss << failed_task_ids[j];
        }
        LOG(ERROR) << "Batch " << batch_id_
                   << " completed with task failures: task_ids=[" << oss.str()
                   << "]";
        ec = ErrorCode::TRANSFER_FAIL;
    }

    set_result_internal(ec);
}

void TransferEngineOperationState::set_result_internal(ErrorCode error_code) {
    if (result_.has_value()) {
        LOG(ERROR) << "Attempting to set result multiple times for batch "
                   << batch_id_
                   << ". Previous result: " << static_cast<int>(result_.value())
                   << ", attempted new result: " << static_cast<int>(error_code)
                   << ". This indicates a race condition or logic error.";
        return;  // Don't crash, just return early
    }

    VLOG(1) << "Setting transfer result for batch " << batch_id_ << " to "
            << static_cast<int>(error_code);
    result_.emplace(error_code);
}

void TransferEngineOperationState::wait_for_completion() {
    if (is_completed()) {
        return;
    }

    // 60 seconds
    constexpr int64_t timeout_milliseconds = 60 * 1000;

#ifdef USE_EVENT_DRIVEN_COMPLETION
    // TENT batches do not use the classic transport BatchDesc layout.
    if (!engine_.isUsingTent()) {
        VLOG(1) << "Waiting for transfer engine completion for batch "
                << batch_id_;
        // Wait directly on BatchDesc's condition variable.
        auto& batch_desc = Transport::toBatchDesc(batch_id_);
        bool completed;
        bool failed = false;

        // Fast path: if already finished, avoid taking the mutex and waiting.
        // Use acquire here to pair with the writer's release-store, because
        // this path may skip taking the mutex. It ensures all prior updates are
        // visible.
        completed = batch_desc.is_finished.load(std::memory_order_acquire);
        if (!completed) {
            // Use the same mutex as the notifier when updating the predicate to
            // avoid missed notifications. The predicate is re-checked under the
            // lock. Under the mutex, relaxed is sufficient; the mutex acquire
            // orders prior writes.
            std::unique_lock<std::mutex> lock(batch_desc.completion_mutex);
            const int64_t elapsed_milliseconds =
                getCurrentTimeInMilli() - start_ts_;
            if (elapsed_milliseconds < timeout_milliseconds) {
                completed = batch_desc.completion_cv.wait_for(
                    lock,
                    std::chrono::milliseconds(timeout_milliseconds -
                                              elapsed_milliseconds),
                    [&batch_desc] {
                        return batch_desc.is_finished.load(
                            std::memory_order_relaxed);
                    });
            }
        }  // Explicitly release completion_mutex before acquiring mutex_

        // Once completion is observed, read failure flag.
        if (completed) {
            failed = batch_desc.has_failure.load(std::memory_order_relaxed);
        }
        ErrorCode error_code =
            completed ? (failed ? ErrorCode::TRANSFER_FAIL : ErrorCode::OK)
                      : ErrorCode::TRANSFER_FAIL;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            set_result_internal(error_code);
        }
        if (completed) {
            VLOG(1) << "Transfer engine operation completed for batch "
                    << batch_id_
                    << " with result: " << static_cast<int>(error_code);
        } else {
            LOG(ERROR) << "Failed to complete transfers after "
                       << timeout_milliseconds << " milliseconds for batch "
                       << batch_id_;
        }
        return;
    }
#endif
    VLOG(1) << "Starting transfer engine polling for batch " << batch_id_;

    const bool ub_transport = engine_.getTransport("ub") != nullptr;
    const auto sleep_time =
        std::chrono::microseconds(ub_transport ? UbPollSleepUs() : 0);
    const auto spin_time =
        std::chrono::microseconds(ub_transport ? UbPollSpinUs() : 0);
    auto spin_until = std::chrono::steady_clock::now() + spin_time;
    if (ub_transport) {
        LOG_FIRST_N(INFO, 1) << "UB completion polling: spin_us="
                             << spin_time.count()
                             << " sleep_us=" << sleep_time.count()
                             << " (MC_STORE_UB_POLL_SLEEP_US=0 restores "
                                "busy polling)";
    }

    while (true) {
        {
            // Timeout and status are checked under the lock so a concurrent
            // observer never sees two results.
            std::unique_lock<std::mutex> lock(mutex_);
            if (result_.has_value()) return;
            if (getCurrentTimeInMilli() - start_ts_ > timeout_milliseconds) {
                LOG(ERROR) << "Failed to complete transfers after "
                           << timeout_milliseconds << " milliseconds for batch "
                           << batch_id_;
                set_result_internal(ErrorCode::TRANSFER_FAIL);
                return;
            }
            check_task_status();
            if (result_.has_value()) {
                VLOG(1) << "Transfer engine operation completed for batch "
                        << batch_id_ << " with result: "
                        << static_cast<int>(result_.value());
                break;
            }
        }
        // Lock released before sleeping so other observers see progress.
        if (sleep_time.count() > 0 &&
            std::chrono::steady_clock::now() >= spin_until) {
            std::this_thread::sleep_for(sleep_time);
            spin_until = std::chrono::steady_clock::now() + spin_time;
        }
    }
}

// ============================================================================
// TransferFuture Implementation
// ============================================================================

TransferFuture::TransferFuture(std::shared_ptr<OperationState> state)
    : state_(std::move(state)) {
    if (!state_) {
        LOG(ERROR) << "TransferFuture requires valid state";
        throw std::invalid_argument("TransferFuture requires valid state");
    }
}

bool TransferFuture::isReady() const { return state_->is_completed(); }

ErrorCode TransferFuture::wait() {
    if (!isReady()) {
        state_->wait_for_completion();
    }
    return state_->get_result();
}

ErrorCode TransferFuture::get() { return wait(); }

TransferStrategy TransferFuture::strategy() const {
    return state_->get_strategy();
}

// ============================================================================
// TransferSubmitter Implementation
// ============================================================================

TransferSubmitter::TransferSubmitter(
    TransferEngine& engine, std::shared_ptr<StorageBackend>& backend,
    const std::string& local_hostname, TransferMetric* transfer_metric,
    int numa_socket_id, std::shared_ptr<NVMeoFInitiator> nof_initiator,
    NofMetric* nof_metric)
    : engine_(engine),
      tent_engine_(engine.getTentEngine().get()),
      local_endpoint_(engine.getLocalIpAndPort()),
      memcpy_pool_(std::make_unique<MemcpyWorkerPool>()),
      nof_initiator_(std::move(nof_initiator)),
      nof_metric_(nof_metric),
      fileread_pool_(std::make_unique<FilereadWorkerPool>(backend)),
      local_hostname_(local_hostname),
      transfer_metric_(transfer_metric) {
    // Read MC_STORE_MEMCPY environment variable.
    // When not set, auto-detect based on transport type:
    //   - TCP-only environment: enable memcpy (avoids TCP loopback overhead)
    //   - RDMA/other transports: disable memcpy (RDMA is more efficient)
    const auto config = TransferSubmitterConfig::FromEnvironment();
    if (config.memcpy_enabled_override.has_value()) {
        memcpy_enabled_ = *config.memcpy_enabled_override;
    } else {
        memcpy_enabled_ = engine_.isTcpOnly();
        LOG(INFO) << "MC_STORE_MEMCPY not set, auto-detected: "
                  << (memcpy_enabled_ ? "TCP-only environment, memcpy enabled"
                                      : "non-TCP transport available, memcpy "
                                        "disabled");
    }

    if (nof_initiator_) {
        nof_pool_ =
            std::make_unique<NofWorkerPool>(nof_initiator_, numa_socket_id);
    }

    VLOG(1) << "TransferSubmitter initialized with memcpy_enabled="
            << memcpy_enabled_;
}

std::optional<TransferFuture> TransferSubmitter::submit(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code, void* ptr, size_t size,
    TransferIntent intent) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        if (!validateTransferParams(handle, slices)) {
            return std::nullopt;
        }

        if (op_code == TransferRequest::READ) {
            future = submitMemoryReadOperation(handle, slices, 0, intent);
        } else {
            TransferStrategy strategy = selectStrategy(handle, slices);

            switch (strategy) {
                case TransferStrategy::LOCAL_MEMCPY:
                    future = submitMemcpyOperation(handle, slices, op_code);
                    break;
                case TransferStrategy::TRANSFER_ENGINE:
                    future = submitTransferEngineOperation(handle, slices,
                                                           op_code, 0, intent);
                    break;
                default:
                    LOG(ERROR) << "Unknown transfer strategy: " << strategy;
                    return std::nullopt;
            }
        }
    } else if (replica.is_nof_replica()) {
        auto& ssd_desc = replica.get_nof_descriptor();
        auto& handle = ssd_desc.buffer_descriptor;

        if (!ptr || (size == 0)) {
            return std::nullopt;
        }

        future = submitNofOperation(handle, ptr, size, op_code);
    } else if (replica.is_vsegment_replica()) {
        // Without this branch a vsegment replica would fall through to the
        // file-read path.
        LOG(ERROR) << "VSegment transfer requires an expanded transfer plan";
        return std::nullopt;
    } else {
        future = submitFileReadOperation(replica, slices, op_code);
    }

    // Update metrics on successful submission
    if (future.has_value()) {
        updateTransferMetrics(slices, op_code);
    }

    return future;
}

// Opens a transfer segment through TENT when active, else the classic
// engine. Returns false when the endpoint cannot be opened.
static bool OpenTransferSegment(TransferEngine& engine,
                                [[maybe_unused]] void* tent_engine,
                                const std::string& endpoint,
                                SegmentHandle* out) {
#ifdef USE_TENT
    if (tent_engine) {
        tent::SegmentID seg_id;
        auto status = static_cast<tent::TransferEngine*>(tent_engine)
                          ->openSegment(seg_id, endpoint);
        if (!status.ok()) {
            return false;
        }
        *out = seg_id;
        return true;
    }
#endif
    *out = engine.openSegment(endpoint);
    return *out != static_cast<uint64_t>(ERR_INVALID_ARGUMENT);
}

std::optional<TransferFuture> TransferSubmitter::submitVSegment(
    const vsegment::VSegmentTransferPlan& plan,
    TransferRequest::OpCode op_code) {
    if (!plan || plan.requests.empty()) return std::nullopt;
    std::vector<TransferRequest> requests;
    requests.reserve(plan.requests.size());
    uint64_t bytes = 0;
    for (const auto& subrequest : plan.requests) {
        SegmentHandle segment = 0;
        if (!OpenTransferSegment(engine_, tent_engine_, subrequest.endpoint,
                                 &segment)) {
            LOG(ERROR) << "Failed to open vsegment member endpoint='"
                       << subrequest.endpoint << "'";
            return std::nullopt;
        }
        TransferRequest request;
        request.opcode = op_code;
        request.source =
            reinterpret_cast<void*>(subrequest.transfer.client_address);
        request.target_id = segment;
        request.target_offset = subrequest.transfer.physical_offset;
        request.length = subrequest.transfer.length;
        requests.push_back(request);
        bytes += subrequest.transfer.length;
    }
    auto future = submitTransfer(requests);
    if (future) {
        std::vector<Slice> metric_slices{{nullptr, bytes}};
        updateTransferMetrics(metric_slices, op_code);
    }
    return future;
}

std::optional<TransferFuture> TransferSubmitter::submit_batch(
    const std::vector<Replica::Descriptor>& replicas,
    std::vector<std::vector<Slice>>& all_slices,
    TransferRequest::OpCode op_code, TransferIntent intent) {
    if (replicas.size() != all_slices.size()) {
        LOG(ERROR) << "Mismatched replicas and slice lists";
        return std::nullopt;
    }

    bool use_local_memcpy = !replicas.empty();
    size_t operation_count = 0;
    for (size_t i = 0; i < replicas.size(); ++i) {
        if (!replicas[i].is_memory_replica()) {
            LOG(ERROR) << "Batch transfer only supports memory replicas";
            return std::nullopt;
        }
        const auto& handle =
            replicas[i].get_memory_descriptor().buffer_descriptor;
        if (!validateTransferParams(handle, all_slices[i])) {
            return std::nullopt;
        }
        use_local_memcpy =
            use_local_memcpy && canUseLocalMemcpy(handle.transport_endpoint_);
        operation_count += all_slices[i].size();
    }

    std::vector<TransferRequest> requests;
    std::vector<MemcpyOperation> memcpy_operations;
    if (use_local_memcpy)
        memcpy_operations.reserve(operation_count);
    else
        requests.reserve(operation_count);
    for (size_t i = 0; i < replicas.size(); ++i) {
        auto& slices = all_slices[i];
        const auto& handle =
            replicas[i].get_memory_descriptor().buffer_descriptor;
        if (use_local_memcpy) {
            appendMemcpyOperations(handle, slices, op_code, 0,
                                   memcpy_operations);
            continue;
        }
        uint64_t offset = 0;
        SegmentHandle seg = 0;
#ifdef USE_TENT
        if (tent_engine_) {
            tent::SegmentID seg_id;
            auto status =
                tent_engine_->openSegment(seg_id, handle.transport_endpoint_);
            if (!status.ok()) {
                LOG(ERROR) << "Failed to open segment "
                           << handle.transport_endpoint_;
                return std::nullopt;
            }
            seg = seg_id;
        } else
#endif
        {
            seg = engine_.openSegment(handle.transport_endpoint_);
            if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
                LOG(ERROR) << "Failed to open segment "
                           << handle.transport_endpoint_;
                return std::nullopt;
            }
        }
        for (const auto& slice : slices) {
            TransferRequest request;
            request.opcode = op_code;
            request.source = static_cast<char*>(slice.ptr);
            request.target_id = seg;
            request.target_offset = handle.buffer_address_ + offset;
            request.length = slice.size;
            requests.emplace_back(request);
            offset += slice.size;
        }
    }
    auto future = use_local_memcpy
                      ? submitMemcpyOperations(std::move(memcpy_operations))
                      : submitTransfer(requests, intent);
    // Update metrics on successful submission
    if (future.has_value()) {
        for (auto& slices : all_slices) {
            updateTransferMetrics(slices, op_code);
        }
    }
    return future;
}

class StoreScatterTransferOperation::Impl {
   public:
    explicit Impl(TransferEngine::ScatterTransferOperation operation)
        : classic_operation_(std::move(operation)) {}

    explicit Impl(Status status)
        : aggregate_status_(std::move(status)), completed_(true) {}

#ifdef USE_TENT
    Impl(std::shared_ptr<tent::TransferEngine> engine,
         const std::vector<TransferEngine::ScatterTransferRange>& ranges,
         TransferIntent intent)
        : tent_engine_(std::move(engine)) {
        build(ranges, intent);
    }
#endif
    ~Impl() {
        if (classic_operation_) {
            classic_operation_->wait();
            return;
        }
#ifdef USE_TENT
        if (completed_) return;
        // Destructor path: use a shorter timeout than wait() to avoid
        // blocking too long when the caller didn't explicitly wait.
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(GetTentPositiveEnvOrDefault(
                                  "MC_TENT_DESTRUCTOR_TIMEOUT_S", 5));
        while (!completed_ && std::chrono::steady_clock::now() < deadline) {
            poll();
            if (!completed_) std::this_thread::sleep_for(kPollInterval);
        }
        if (!completed_) forceCleanup();
#endif
    }

    Status wait() {
        if (classic_operation_) return classic_operation_->wait();
#ifdef USE_TENT
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(GetTentPositiveEnvOrDefault(
                                  "MC_TENT_TRANSFER_TIMEOUT_S", 60));
        while (!completed_) {
            poll();
            if (!completed_) std::this_thread::sleep_for(kPollInterval);
            if (!completed_ && std::chrono::steady_clock::now() >= deadline) {
                requestAbort(Status::Socket("TENT scatter transfer timed out"));
                const auto cleanup_deadline =
                    std::chrono::steady_clock::now() +
                    std::chrono::seconds(GetTentPositiveEnvOrDefault(
                        "MC_TENT_CANCEL_GRACE_S", 1));
                while (!completed_ &&
                       std::chrono::steady_clock::now() < cleanup_deadline) {
                    poll();
                    if (!completed_) std::this_thread::sleep_for(kPollInterval);
                }
                if (!completed_) forceCleanup();
            }
        }
#endif
        return aggregate_status_;
    }

    Status waitFor(std::chrono::nanoseconds timeout) {
        if (classic_operation_) return classic_operation_->waitFor(timeout);
#ifdef USE_TENT
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!completed_) {
            poll();
            if (completed_) break;
            if (std::chrono::steady_clock::now() >= deadline) {
                requestAbort(Status::Clock("scatter transfer wait timed out"));
                return aggregate_status_;
            }
            std::this_thread::sleep_for(kPollInterval);
        }
#else
        (void)timeout;
#endif
        return aggregate_status_;
    }

   private:
    static constexpr auto kPollInterval = std::chrono::microseconds(10);

#ifdef USE_TENT
    void remember(const Status& status) {
        if (aggregate_status_.ok() && !status.ok()) aggregate_status_ = status;
    }

    void complete(size_t index, const Status& status) {
        if (done_[index]) return;
        done_[index] = true;
        remember(status);
        const auto [range_index, fragment_index] = request_fragments_[index];
        if (callbacks_[range_index]) {
            try {
                callbacks_[range_index](fragment_index, status);
            } catch (...) {
                remember(Status::Context("scatter transfer callback failed"));
            }
        }
    }

    void finish() { completed_ = true; }

    void failPending(const Status& status) {
        for (size_t i = 0; i < done_.size(); ++i) complete(i, status);
    }

    void requestAbort(const Status& status) {
        remember(status);
        if (abort_requested_ || batch_id_ == 0) return;
        abort_requested_ = true;
        abort_deadline_ = std::chrono::steady_clock::now() +
                          std::chrono::seconds(GetTentPositiveEnvOrDefault(
                              "MC_TENT_CANCEL_GRACE_S", 1));
        for (size_t task = 0; task < tent_requests_.size(); ++task) {
            const size_t index = native_indexes_[task];
            if (done_[index]) continue;
            auto cancel_status = tent_engine_->cancelTransfer(batch_id_, task);
            if (!cancel_status.ok() && !cancel_status.IsNotImplemented())
                remember(Status::Context(cancel_status.ToString()));
        }
    }

    void forceCleanup() {
        failPending(Status::Socket("TENT scatter transfer cleanup timed out"));
        if (batch_id_ != 0) tent_engine_->freeBatch(batch_id_);
        batch_id_ = 0;
        finish();
    }

    void build(const std::vector<TransferEngine::ScatterTransferRange>& ranges,
               TransferIntent intent) {
        auto default_intent = ToTentIntent(intent);
        if (!default_intent) {
            aggregate_status_ =
                Status::InvalidArgument("invalid TENT scatter transfer intent");
            completed_ = true;
            return;
        }
        std::map<std::string, tent::SegmentID> segment_ids;
        for (size_t range_index = 0; range_index < ranges.size();
             ++range_index) {
            const auto& range = ranges[range_index];
            callbacks_.push_back(range.on_fragment_complete);
            const size_t count = range.local_offsets.size();
            if (range.remote_offsets.size() != count ||
                range.lengths.size() != count ||
                range.local_buffer == nullptr || range.remote_segment.empty()) {
                const auto error =
                    Status::InvalidArgument("invalid scatter transfer range");
                for (size_t i = 0; i < count; ++i) {
                    request_fragments_.emplace_back(range_index, i);
                    done_.push_back(false);
                    complete(done_.size() - 1, error);
                }
                continue;
            }
            auto segment_it = segment_ids.find(range.remote_segment);
            if (segment_it == segment_ids.end()) {
                tent::SegmentID segment_id;
                auto status =
                    tent_engine_->openSegment(segment_id, range.remote_segment);
                if (!status.ok()) {
                    const auto error = Status::Context(status.ToString());
                    for (size_t i = 0; i < count; ++i) {
                        request_fragments_.emplace_back(range_index, i);
                        done_.push_back(false);
                        complete(done_.size() - 1, error);
                    }
                    continue;
                }
                segment_it =
                    segment_ids.emplace(range.remote_segment, segment_id).first;
                // openSegment is idempotent, not a reference-counted lease.
                // Other Store operations can be using this same handle. Keep
                // it cached in the engine, as the ordinary transfer paths do;
                // closing it here would invalidate concurrent reads/writes.
            }
            for (size_t fragment = 0; fragment < count; ++fragment) {
                const size_t length = range.lengths[fragment];
                const size_t local_offset = range.local_offsets[fragment];
                const size_t remote_offset = range.remote_offsets[fragment];
                const bool valid =
                    local_offset <= range.local_capacity &&
                    length <= range.local_capacity - local_offset &&
                    remote_offset <= range.remote_size &&
                    length <= range.remote_size - remote_offset &&
                    range.remote_base_offset <=
                        std::numeric_limits<uint64_t>::max() - remote_offset &&
                    length <= std::numeric_limits<uint64_t>::max() -
                                  (range.remote_base_offset + remote_offset);
                request_fragments_.emplace_back(range_index, fragment);
                done_.push_back(false);
                if (!valid) {
                    complete(done_.size() - 1,
                             Status::InvalidArgument(
                                 "invalid scatter transfer fragment"));
                    continue;
                }
                if (length == 0) {
                    complete(done_.size() - 1, Status::OK());
                    continue;
                }
                TransferIntent request_store_intent = intent;
                if (range.intent_type != transfer_intent_values::kUnspecified) {
                    auto parsed_intent =
                        TransferIntentFromInt(range.intent_type);
                    if (!parsed_intent) {
                        complete(done_.size() - 1,
                                 Status::InvalidArgument(
                                     "invalid TENT scatter transfer intent"));
                        continue;
                    }
                    request_store_intent = *parsed_intent;
                }
                auto opcode = ToTentOpCode(range.opcode);
                auto request_intent = ToTentIntent(request_store_intent);
                if (!opcode || !request_intent) {
                    complete(done_.size() - 1,
                             Status::InvalidArgument(
                                 "invalid TENT scatter transfer request"));
                    continue;
                }
                tent::Request request;
                request.opcode = *opcode;
                request.source =
                    static_cast<char*>(range.local_buffer) + local_offset;
                request.target_id = segment_it->second;
                request.target_offset =
                    range.remote_base_offset + remote_offset;
                request.length = length;
                request.intent_type = *request_intent;
                // transport_hint left at default (0 = auto-select);
                // ScatterTransferRange does not carry per-fragment hints.
                tent_requests_.push_back(request);
                native_indexes_.push_back(done_.size() - 1);
            }
        }
        if (tent_requests_.empty()) {
            finish();
            return;
        }
        batch_id_ = tent_engine_->allocateBatch(tent_requests_.size());
        if (batch_id_ == 0) {
            failPending(Status::InvalidArgument(
                "failed to allocate TENT scatter transfer batch"));
            finish();
            return;
        }
        auto status = tent_engine_->submitTransfer(batch_id_, tent_requests_);
        if (!status.ok()) {
            failPending(Status::Context(status.ToString()));
            tent_engine_->freeBatch(batch_id_);
            batch_id_ = 0;
            finish();
            return;
        }
        remaining_ = tent_requests_.size();
    }

    void poll() {
        for (size_t task = 0; task < tent_requests_.size(); ++task) {
            const size_t index = native_indexes_[task];
            if (done_[index]) continue;
            tent::TransferStatus transfer_status;
            auto status = tent_engine_->getTransferStatus(batch_id_, task,
                                                          transfer_status);
            if (!status.ok()) {
                requestAbort(Status::Context(status.ToString()));
                if (!abort_requested_ ||
                    std::chrono::steady_clock::now() >= abort_deadline_) {
                    complete(index, Status::Context(status.ToString()));
                    assert(remaining_ > 0);
                    --remaining_;
                }
                continue;
            } else {
                auto state = ToClassicTransferStatus(transfer_status.s);
                if (!state) {
                    complete(
                        index,
                        Status::Socket("unknown TENT scatter transfer status"));
                    --remaining_;
                    continue;
                }
                if (*state == TransferStatusEnum::WAITING) continue;
                complete(index, *state == TransferStatusEnum::COMPLETED
                                    ? Status::OK()
                                    : Status::Socket(
                                          "scatter transfer fragment failed"));
            }
            assert(remaining_ > 0);
            --remaining_;
        }
        if (remaining_ != 0) return;
        auto status = tent_engine_->freeBatch(batch_id_);
        remember(status.ok() ? Status::OK()
                             : Status::Context(status.ToString()));
        batch_id_ = 0;
        finish();
    }
#endif

    std::optional<TransferEngine::ScatterTransferOperation> classic_operation_;
    Status aggregate_status_;
    bool completed_ = false;
#ifdef USE_TENT
    std::shared_ptr<tent::TransferEngine> tent_engine_;
    std::vector<tent::Request> tent_requests_;
    std::vector<size_t> native_indexes_;
    std::vector<std::pair<size_t, size_t>> request_fragments_;
    std::vector<std::function<void(size_t, const Status&)>> callbacks_;
    std::vector<uint8_t> done_;
    BatchID batch_id_ = 0;
    size_t remaining_ = 0;
    bool abort_requested_ = false;
    std::chrono::steady_clock::time_point abort_deadline_ =
        std::chrono::steady_clock::time_point::max();
#endif
};

StoreScatterTransferOperation::StoreScatterTransferOperation(
    std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

StoreScatterTransferOperation::StoreScatterTransferOperation(
    StoreScatterTransferOperation&&) noexcept = default;

StoreScatterTransferOperation& StoreScatterTransferOperation::operator=(
    StoreScatterTransferOperation&&) noexcept = default;

StoreScatterTransferOperation::~StoreScatterTransferOperation() = default;

Status StoreScatterTransferOperation::wait() {
    return impl_
               ? impl_->wait()
               : Status::InvalidArgument("invalid scatter transfer operation");
}

Status StoreScatterTransferOperation::waitFor(
    std::chrono::nanoseconds timeout) {
    return impl_
               ? impl_->waitFor(timeout)
               : Status::InvalidArgument("invalid scatter transfer operation");
}

TransferEngine::ScatterTransferOperation TransferSubmitter::submitScatter(
    const std::vector<TransferEngine::ScatterTransferRange>& transfers) {
    return engine_.submitScatter(transfers);
}

StoreScatterTransferOperation TransferSubmitter::submitNativeScatter(
    const std::vector<TransferEngine::ScatterTransferRange>& transfers,
    TransferIntent intent) {
#ifdef USE_TENT
    if (tent_engine_) {
        return StoreScatterTransferOperation(
            std::make_unique<StoreScatterTransferOperation::Impl>(
                engine_.getTentEngine(), transfers, intent));
    }
#else
    (void)intent;
#endif
    return StoreScatterTransferOperation(
        std::make_unique<StoreScatterTransferOperation::Impl>(
            engine_.submitScatter(transfers)));
}

std::optional<TransferFuture>
TransferSubmitter::submit_batch_get_offload_object(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys, const std::vector<uint64_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batched_slices,
    OffloadBufferAccess buffer_access, TransferIntent intent) {
    if (keys.size() != pointers.size()) {
        LOG(ERROR) << "Mismatched offload transfer argument counts";
        return std::nullopt;
    }

    const bool use_local_memcpy =
        buffer_access == OffloadBufferAccess::kLocalAddress;
    if (use_local_memcpy && !canUseLocalMemcpy(transfer_engine_addr)) {
        LOG(ERROR) << "Offload source is not locally addressable: "
                   << transfer_engine_addr;
        return std::nullopt;
    }

    std::vector<TransferRequest> requests;
    std::vector<MemcpyOperation> operations;
    constexpr uint64_t kMaxAddress = std::numeric_limits<uint64_t>::max();
    SegmentHandle seg = 0;
    if (!use_local_memcpy) {
        // Open once: all keys share the same transfer endpoint.
#ifdef USE_TENT
        if (tent_engine_) {
            tent::SegmentID seg_id;
            auto status =
                tent_engine_->openSegment(seg_id, transfer_engine_addr);
            if (!status.ok()) {
                LOG(ERROR) << "Failed to open segment " << transfer_engine_addr;
                return std::nullopt;
            }
            seg = seg_id;
        } else
#endif
        {
            seg = engine_.openSegment(transfer_engine_addr);
            if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
                LOG(ERROR) << "Failed to open segment " << transfer_engine_addr;
                return std::nullopt;
            }
        }
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        auto it = batched_slices.find(key);
        if (it == batched_slices.end()) {
            LOG(ERROR) << "Key not found in batched_slices: " << key;
            return std::nullopt;
        }
        uint64_t offset = 0;
        for (const auto& slice : it->second) {
            if (slice.size == 0) continue;
            if (!slice.ptr || pointers[i] > kMaxAddress - offset ||
                slice.size > kMaxAddress - pointers[i] - offset) {
                LOG(ERROR) << "Invalid offload transfer range for key: " << key;
                return std::nullopt;
            }
            if (use_local_memcpy) {
                operations.emplace_back(
                    slice.ptr,
                    reinterpret_cast<const void*>(pointers[i] + offset),
                    slice.size);
            } else {
                requests.emplace_back(TransferRequest{
                    .opcode = TransferRequest::READ,
                    .source = static_cast<char*>(slice.ptr),
                    .target_id = seg,
                    .target_offset = pointers[i] + offset,
                    .length = slice.size,
                });
            }
            offset += slice.size;
        }
    }
    return use_local_memcpy ? submitMemcpyOperations(std::move(operations))
                            : submitTransfer(requests, intent);
}

void TransferSubmitter::appendMemcpyOperations(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t buffer_offset,
    std::vector<MemcpyOperation>& operations) {
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    uint64_t offset = buffer_offset;

    for (const auto& slice : slices) {
        if (slice.ptr == nullptr) continue;

        void* dest;
        const void* src;
        if (op_code == TransferRequest::READ) {
            dest = slice.ptr;
            src = reinterpret_cast<const void*>(base_address + offset);
        } else {
            dest = reinterpret_cast<void*>(base_address + offset);
            src = slice.ptr;
        }
        offset += slice.size;
        operations.emplace_back(dest, src, slice.size);
    }
}

std::optional<TransferFuture>
TransferSubmitter::submit_batch_push_offload_object(
    const std::string& requester_te_addr,
    const std::vector<std::string>& keys,
    const std::vector<uint64_t>& src_pointers,
    const std::vector<std::vector<OffloadDstSlice>>& dst_slices) {
    if (keys.size() != src_pointers.size() ||
        keys.size() != dst_slices.size()) {
        LOG(ERROR) << "submit_batch_push_offload_object size mismatch: keys="
                   << keys.size() << ", src_pointers=" << src_pointers.size()
                   << ", dst_slices=" << dst_slices.size();
        return std::nullopt;
    }
    // Open the requester's segment once: all keys share requester_te_addr.
    SegmentHandle seg = 0;
    if (!OpenTransferSegment(engine_, tent_engine_, requester_te_addr, &seg)) {
        LOG(ERROR) << "Failed to open segment " << requester_te_addr;
        return std::nullopt;
    }
    constexpr uint64_t kMaxAddress = std::numeric_limits<uint64_t>::max();
    std::vector<TransferRequest> requests;
    for (size_t i = 0; i < keys.size(); ++i) {
        // src is the owner's contiguous ClientBuffer blob for this key; it
        // is split across the requester's destination slices.
        const uint64_t src = src_pointers[i];
        uint64_t offset = 0;
        for (const auto& dst : dst_slices[i]) {
            if (dst.size == 0) continue;
            if (src == 0 || src > kMaxAddress - offset ||
                dst.size > kMaxAddress - src - offset) {
                LOG(ERROR) << "Invalid offload push range for key: "
                           << keys[i];
                return std::nullopt;
            }
            TransferRequest request;
            request.opcode = TransferRequest::WRITE;
            request.source = reinterpret_cast<char*>(src + offset);
            request.target_id = seg;
            request.target_offset = dst.addr;
            request.length = dst.size;
            requests.emplace_back(request);
            offset += dst.size;
        }
    }
    return submitTransfer(requests);
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t src_offset) {
    std::vector<MemcpyOperation> operations;
    operations.reserve(slices.size());
    appendMemcpyOperations(handle, slices, op_code, src_offset, operations);
    return submitMemcpyOperations(std::move(operations));
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperations(
    std::vector<MemcpyOperation> operations, uint64_t trace_id) {
    auto state = std::make_shared<MemcpyOperationState>(trace_id);
    if (IsInlineHostMemcpyEnabled()) {
        // Host-only batches are copied right here: queueing them costs a
        // thread handoff that dominates small copies. Any accelerator
        // pointer sends the whole batch to the pool.
        try {
            bool host_only = true;
            auto runtime_accelerator =
                device::GetAcceleratorRegistry().RuntimeAccelerators();
            if (!runtime_accelerator.Devices().empty()) {
                for (const auto& op : operations) {
                    if (runtime_accelerator.FindDeviceForPointer(op.src) ||
                        runtime_accelerator.FindDeviceForPointer(op.dest)) {
                        host_only = false;
                        break;
                    }
                }
            }
            if (host_only) {
                for (const auto& op : operations) {
                    std::memcpy(op.dest, op.src, op.size);
                }
                state->set_completed(ErrorCode::OK);
                return TransferFuture(state);
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception while classifying memcpy pointers: "
                       << e.what();
            state->set_completed(ErrorCode::TRANSFER_FAIL);
            return TransferFuture(state);
        }
    }
    const size_t operation_count = operations.size();
    MemcpyTask task(std::move(operations), state, trace_id);
    memcpy_pool_->submitTask(std::move(task));

    VLOG(1) << "Memcpy transfer submitted to worker pool with "
            << operation_count << " operations";

    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransfer(
    std::vector<TransferRequest>& requests, TransferIntent intent) {
    const size_t batch_size = requests.size();

    BatchID batch_id = INVALID_BATCH_ID;
    Status s;

#ifdef USE_TENT
    if (tent_engine_) {
        batch_id = tent_engine_->allocateBatch(batch_size);
        if (batch_id == 0) {
            LOG(ERROR) << "Failed to allocate batch ID";
            return std::nullopt;
        }
        std::vector<tent::Request> tent_reqs;
        tent_reqs.reserve(requests.size());
        auto tent_intent = ToTentIntent(intent);
        if (!tent_intent) {
            LOG(ERROR) << "Invalid TENT transfer intent: "
                       << static_cast<int>(intent);
            tent_engine_->freeBatch(batch_id);
            return std::nullopt;
        }
        for (auto& r : requests) {
            auto tent_opcode = ToTentOpCode(r.opcode);
            if (!tent_opcode) {
                LOG(ERROR) << "Invalid TENT transfer opcode: "
                           << static_cast<int>(r.opcode);
                tent_engine_->freeBatch(batch_id);
                return std::nullopt;
            }
            tent::Request tr;
            tr.opcode = *tent_opcode;
            tr.source = r.source;
            tr.target_id = r.target_id;
            tr.target_offset = r.target_offset;
            tr.length = r.length;
            tr.transport_hint =
                mooncake::tent::c_to_transport_hint(r.transport_hint);
            tr.intent_type = *tent_intent;
            tent_reqs.push_back(tr);
        }
        auto tent_status = tent_engine_->submitTransfer(batch_id, tent_reqs);
        if (!tent_status.ok()) {
            LOG(ERROR) << "Failed to submit all transfers, error: "
                       << tent_status.ToString();
            tent_engine_->freeBatch(batch_id);
            return std::nullopt;
        }
    } else
#endif
    {
        batch_id = engine_.allocateBatchID(batch_size);
        if (batch_id == INVALID_BATCH_ID) {
            LOG(ERROR) << "Failed to allocate batch ID";
            return std::nullopt;
        }
        s = engine_.submitTransfer(batch_id, requests);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to submit all transfers, error code is "
                       << s.code();
            engine_.freeBatchID(batch_id);
            return std::nullopt;
        }
    }

    auto state = std::make_shared<TransferEngineOperationState>(
        engine_, batch_id, batch_size);

    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransferEngineOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t src_offset,
    TransferIntent intent) {
    if (handle.transport_endpoint_.empty()) {
        LOG(ERROR) << "Transport endpoint is empty for handle with address "
                   << handle.buffer_address_;
        return std::nullopt;
    }
    SegmentHandle seg = 0;
#ifdef USE_TENT
    if (tent_engine_) {
        tent::SegmentID seg_id;
        auto status =
            tent_engine_->openSegment(seg_id, handle.transport_endpoint_);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to open segment for endpoint='"
                       << handle.transport_endpoint_ << "'";
            return std::nullopt;
        }
        seg = seg_id;
    } else
#endif
    {
        seg = engine_.openSegment(handle.transport_endpoint_);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open segment for endpoint='"
                       << handle.transport_endpoint_ << "'";
            return std::nullopt;
        }
    }

    // Create transfer requests
    std::vector<TransferRequest> requests;
    requests.reserve(slices.size());
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    uint64_t offset = src_offset;

    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        if (slice.ptr == nullptr) continue;

        TransferRequest request;
        request.opcode = op_code;
        request.source = static_cast<char*>(slice.ptr);
        request.target_id = seg;
        request.target_offset = base_address + offset;
        request.length = slice.size;

        offset += slice.size;
        requests.emplace_back(request);
    }
    return submitTransfer(requests, intent);
}

std::optional<TransferFuture> TransferSubmitter::submitMemoryReadOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    uint64_t src_offset, TransferIntent intent) {
    TransferStrategy strategy = selectStrategy(handle, slices);

    if (strategy == TransferStrategy::LOCAL_MEMCPY) {
        return submitMemcpyOperation(handle, slices, TransferRequest::READ,
                                     src_offset);
    }
    if (strategy == TransferStrategy::TRANSFER_ENGINE) {
        return submitTransferEngineOperation(
            handle, slices, TransferRequest::READ, src_offset, intent);
    }

    LOG(ERROR) << "Read only supports LOCAL_MEMCPY or TRANSFER_ENGINE, got: "
               << strategy;
    return std::nullopt;
}

std::optional<TransferFuture> TransferSubmitter::submitMemoryWriteOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    uint64_t dst_offset, TransferIntent intent) {
    TransferStrategy strategy = selectStrategy(handle, slices);

    if (strategy == TransferStrategy::LOCAL_MEMCPY) {
        return submitMemcpyOperation(handle, slices, TransferRequest::WRITE,
                                     dst_offset);
    }
    if (strategy == TransferStrategy::TRANSFER_ENGINE) {
        return submitTransferEngineOperation(
            handle, slices, TransferRequest::WRITE, dst_offset, intent);
    }

    LOG(ERROR) << "Write only supports LOCAL_MEMCPY or TRANSFER_ENGINE, got: "
               << strategy;
    return std::nullopt;
}

std::optional<TransferFuture> TransferSubmitter::submitRangeRead(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    uint64_t src_offset, TransferIntent intent) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        size_t slices_size = 0;
        for (const auto& s : slices) slices_size += s.size;
        if (src_offset > std::numeric_limits<uint64_t>::max() - slices_size ||
            src_offset + slices_size > handle.size_) {
            LOG(ERROR) << "Range read overflow: src_offset=" << src_offset
                       << " + slices_size=" << slices_size
                       << " > handle.size_=" << handle.size_;
            return std::nullopt;
        }

        future = submitMemoryReadOperation(handle, slices, src_offset, intent);
    } else if (replica.is_nof_replica()) {
        LOG(ERROR) << "Range read not supported for NoF replicas";
        return std::nullopt;
    } else if (replica.is_disk_replica() || replica.is_local_disk_replica()) {
        LOG(ERROR)
            << "Range read not supported for disk replicas (use full read)";
        return std::nullopt;
    }

    if (future.has_value()) {
        updateTransferMetrics(slices, TransferRequest::READ);
    }

    return future;
}

std::optional<TransferFuture> TransferSubmitter::submitRangeWrite(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    uint64_t dst_offset, TransferIntent intent) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        size_t slices_size = 0;
        for (const auto& s : slices) slices_size += s.size;
        if (dst_offset > std::numeric_limits<uint64_t>::max() - slices_size ||
            dst_offset + slices_size > handle.size_) {
            LOG(ERROR) << "Range write overflow: dst_offset=" << dst_offset
                       << " + slices_size=" << slices_size
                       << " > handle.size_=" << handle.size_;
            return std::nullopt;
        }

        future = submitMemoryWriteOperation(handle, slices, dst_offset, intent);
    } else if (replica.is_nof_replica()) {
        LOG(ERROR) << "Range write not supported for NoF replicas";
        return std::nullopt;
    } else if (replica.is_disk_replica() || replica.is_local_disk_replica()) {
        LOG(ERROR)
            << "Range write not supported for disk replicas (use full write)";
        return std::nullopt;
    }

    if (future.has_value()) {
        updateTransferMetrics(slices, TransferRequest::WRITE);
    }

    return future;
}

std::optional<TransferFuture> TransferSubmitter::submitNofOperation(
    const AllocatedBuffer::Descriptor& handle, void* ptr, size_t size,
    const TransferRequest::OpCode op_code) {
    if (!nof_pool_) {
        // Runtime form of the former "NoF transfer requested while USE_NOF
        // is disabled" error.
        LOG(ERROR) << "NoF transfer requested but NoF is unavailable "
                      "(no initiator in this process)";
        return std::nullopt;
    }
    if (handle.transport_endpoint_.empty() || handle.size_ < size) {
        LOG(ERROR) << "Invalid NoF request endpoint="
                   << handle.transport_endpoint_
                   << ", buffer_size=" << handle.size_
                   << ", request_size=" << size;
        return std::nullopt;
    }

    NofSegmentHandle* seg_handle =
        nof_initiator_->OpenSegment(handle.transport_endpoint_);
    if (!seg_handle) {
        LOG(ERROR) << "Failed to open NoF segment endpoint="
                   << handle.transport_endpoint_;
        return std::nullopt;
    }

    uint32_t block_size = nof_initiator_->GetBlockSize(seg_handle);
    // Without SGL, ptr must be block-aligned; with SGL (supports_sgl) the
    // requirement relaxes to dma_alignment.
    const auto caps = nof_initiator_->GetCapabilities();
    const uint64_t ptr_alignment =
        caps.supports_sgl ? caps.dma_alignment : block_size;
    if (block_size == kInvalidBlockSize ||
        handle.buffer_address_ % block_size != 0 || size % block_size != 0 ||
        reinterpret_cast<std::uintptr_t>(ptr) % ptr_alignment != 0) {
        LOG(ERROR) << "NoF request offset=" << handle.buffer_address_
                   << ", ptr=" << ptr << ", size=" << size
                   << " is not aligned to block size " << block_size;
        return std::nullopt;
    }

    const NofIOOp op =
        (op_code == TransferRequest::READ) ? NofIOOp::kRead : NofIOOp::kWrite;
    auto state = std::make_shared<NofOperationState>();
    NofTask task(seg_handle, ptr, handle.buffer_address_, size, op, state);
    task.metric = nof_metric_;
    nof_pool_->submitTask(std::move(task));

    VLOG(1) << "NoF transfer submitted to " << handle.transport_endpoint_;
    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitFileReadOperation(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code) {
    auto state = std::make_shared<FilereadOperationState>();
    auto disk_replica = replica.get_disk_descriptor();
    std::string file_path = disk_replica.file_path;
    size_t file_length = disk_replica.object_size;

    // Submit memcpy operations to worker pool for async execution
    FilereadTask task(file_path, file_length, slices, state);
    fileread_pool_->submitTask(std::move(task));

    VLOG(1) << "Fileread transfer submitted to worker pool with " << file_path;

    return TransferFuture(state);
}

TransferStrategy TransferSubmitter::selectStrategy(
    const AllocatedBuffer::Descriptor& handle,
    const std::vector<Slice>& /* slices */) const {
    return canUseLocalMemcpy(handle.transport_endpoint_)
               ? TransferStrategy::LOCAL_MEMCPY
               : TransferStrategy::TRANSFER_ENGINE;
}

bool TransferSubmitter::canUseLocalMemcpy(const std::string& endpoint) const {
    return memcpy_enabled_ &&
           (isSameProcessEndpoint(endpoint, local_hostname_) ||
            isSameProcessEndpoint(endpoint, local_endpoint_));
}

bool TransferSubmitter::isSameProcessEndpoint(
    const std::string& handle_endpoint, const std::string& local_endpoint) {
    // Local memcpy requires that handle.buffer_address_ is a virtual address
    // valid in THIS process. Same host is not enough: two processes on the
    // same host share an IP but have distinct virtual address spaces, so a
    // memcpy on a peer process's address would segfault. Require the full
    // transport endpoint to match, which uniquely identifies the owning
    // process.
    return !handle_endpoint.empty() && handle_endpoint == local_endpoint;
}

bool TransferSubmitter::validateTransferParams(
    const AllocatedBuffer::Descriptor& handle,
    const std::vector<Slice>& slices) const {
    uint64_t all_slice_len = 0;
    for (auto slice : slices) {
        all_slice_len += slice.size;
    }
    if (handle.size_ != all_slice_len) {
        LOG(ERROR) << "handles len:" << handle.size_
                   << ", all_slice_len:" << all_slice_len;
        return false;
    }
    return true;
}

void TransferSubmitter::updateTransferMetrics(const std::vector<Slice>& slices,
                                              TransferRequest::OpCode op_code) {
    size_t total_bytes = 0;
    for (const auto& slice : slices) {
        total_bytes += slice.size;
    }

    if (transfer_metric_ == nullptr) {
        return;
    }

    if (op_code == TransferRequest::READ) {
        transfer_metric_->total_read_bytes.inc(total_bytes);

    } else if (op_code == TransferRequest::WRITE) {
        transfer_metric_->total_write_bytes.inc(total_bytes);
    }
}

}  // namespace mooncake
