// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef MOONCAKE_TENT_TRANSPORT_UB_SLICE_H_
#define MOONCAKE_TENT_TRANSPORT_UB_SLICE_H_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "tent/common/types.h"
#include "tent/runtime/topology.h"

namespace mooncake::tent::ub {

inline uint64_t steadyNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

// Identifies one local-device to remote-device posting path. Endpoint
// generation is part of the identity so a completion from a retired endpoint
// cannot be mistaken for work posted on its replacement.
struct UbPostPath {
    Topology::NicID local_topology_id{-1};
    SegmentID remote_segment_id{LOCAL_SEGMENT_ID};
    int remote_device_id{-1};
    uint64_t endpoint_generation{0};

    bool operator==(const UbPostPath&) const = default;

    [[nodiscard]] bool valid() const {
        return local_topology_id >= 0 && remote_device_id >= 0 &&
               endpoint_generation != 0;
    }
};

struct UbPostPathHash {
    size_t operator()(const UbPostPath& path) const noexcept {
        size_t seed = std::hash<int>{}(path.local_topology_id);
        auto combine = [&seed](size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        combine(std::hash<SegmentID>{}(path.remote_segment_id));
        combine(std::hash<int>{}(path.remote_device_id));
        combine(std::hash<uint64_t>{}(path.endpoint_generation));
        return seed;
    }
};

enum class UbSliceState : uint8_t {
    kInitial,
    kQueued,
    kPosting,
    kPosted,
    kRetryPending,
    kCompleted,
    kFailed,
    kTimedOut,
    kCanceled,
    kInvalid,
};

inline bool isTerminal(UbSliceState state) {
    switch (state) {
        case UbSliceState::kCompleted:
        case UbSliceState::kFailed:
        case UbSliceState::kTimedOut:
        case UbSliceState::kCanceled:
        case UbSliceState::kInvalid:
            return true;
        default:
            return false;
    }
}

inline TransferStatusEnum transferStatus(UbSliceState state) {
    switch (state) {
        case UbSliceState::kInitial:
            return INITIAL;
        case UbSliceState::kCompleted:
            return COMPLETED;
        case UbSliceState::kFailed:
            return FAILED;
        case UbSliceState::kTimedOut:
            return TIMEOUT;
        case UbSliceState::kCanceled:
            return CANCELED;
        case UbSliceState::kInvalid:
            return INVALID;
        default:
            return PENDING;
    }
}

inline bool isTerminal(TransferStatusEnum status) {
    return status == COMPLETED || status == FAILED || status == TIMEOUT ||
           status == CANCELED || status == INVALID;
}

inline UbSliceState sliceState(TransferStatusEnum status) {
    switch (status) {
        case COMPLETED:
            return UbSliceState::kCompleted;
        case FAILED:
            return UbSliceState::kFailed;
        case TIMEOUT:
            return UbSliceState::kTimedOut;
        case CANCELED:
            return UbSliceState::kCanceled;
        case INVALID:
            return UbSliceState::kInvalid;
        case INITIAL:
            return UbSliceState::kInitial;
        case PENDING:
        default:
            return UbSliceState::kQueued;
    }
}

struct UbSliceSpec {
    void* local_address{nullptr};
    uint64_t remote_address{0};
    size_t length{0};
    size_t request_offset{0};
    uint32_t max_retries{0};
};

struct UbAttemptToken {
    uint64_t slice_id{0};
    uint32_t attempt{0};
    UbPostPath path{};

    bool operator==(const UbAttemptToken&) const = default;

    [[nodiscard]] bool valid() const {
        return slice_id != 0 && attempt != 0 && path.valid();
    }
};

enum class UbAttemptResolution : uint8_t {
    kIgnored,
    kRetryScheduled,
    kTerminal,
};

struct UbSliceSnapshot {
    uint64_t id{0};
    UbSliceState state{UbSliceState::kInitial};
    UbPostPath path{};
    uint32_t attempt{0};
    uint32_t retry_count{0};
    uint32_t max_retries{0};
    bool cancel_requested{false};
    size_t transferred_bytes{0};
    uint64_t created_ns{0};
    uint64_t queued_ns{0};
    uint64_t attempt_started_ns{0};
    uint64_t posted_ns{0};
    uint64_t terminal_ns{0};
    // Recorded only for traced tasks (see UbTask::enableTrace()).
    uint64_t dequeued_ns{0};
    uint64_t post_returned_ns{0};
    uint32_t defer_count{0};
    // Prepare sub-phase checkpoints of the posted attempt (traced tasks only):
    // route resolved, endpoint/quota selected, local+remote segments ready.
    uint64_t route_ready_ns{0};
    uint64_t path_selected_ns{0};
    uint64_t segments_ready_ns{0};
    uint64_t route_metadata_ns{0};
    // Submit-side checkpoints (traced tasks only): submitBatch() entry, queue
    // mutex acquired, and enqueue finished.
    uint64_t submit_started_ns{0};
    uint64_t queue_locked_ns{0};
    uint64_t enqueue_finished_ns{0};
    // Route metadata sub-phase checkpoints (traced tasks only): segment
    // descriptor resolved from cache, remote buffer found, transport metadata
    // decoded, remote devices ordered.
    uint64_t segment_cache_ns{0};
    uint64_t find_buffer_ns{0};
    uint64_t decode_metadata_ns{0};
    uint64_t remote_order_ns{0};
    // Completion path checkpoints (traced tasks only): CQE observed by the
    // poller, inflight mutex acquired, inflight entry erased, transport
    // resources released, and rail monitor updated. The final attempt's
    // terminal_ns closes the chain.
    uint64_t cqe_seen_ns{0};
    uint64_t inflight_locked_ns{0};
    uint64_t inflight_erased_ns{0};
    uint64_t resources_released_ns{0};
    uint64_t rail_recorded_ns{0};
};

struct UbTask;
class UbSlice;

// Safe user-context payload for a posted operation. Holding this token keeps
// the slice alive until the device completion has been dispatched, while the
// slice itself keeps only a weak reference to its parent task.
struct UbCompletionToken {
    std::shared_ptr<UbSlice> slice;
    UbAttemptToken attempt{};

    [[nodiscard]] bool valid() const;
    bool markPosted(uint64_t now_ns = 0) const;
    UbAttemptResolution resolve(TransferStatusEnum outcome,
                                size_t transferred_bytes, bool retryable,
                                uint64_t now_ns = 0) const;
};

// A logical slice is owned by its UbTask and by any in-flight completion
// token. It keeps only a weak reference back to the task, avoiding a cycle and
// allowing a late completion to be discarded safely after batch teardown.
class UbSlice : public std::enable_shared_from_this<UbSlice> {
   public:
    using Ptr = std::shared_ptr<UbSlice>;

    UbSlice(const UbSlice&) = delete;
    UbSlice& operator=(const UbSlice&) = delete;

    [[nodiscard]] uint64_t id() const { return id_; }
    [[nodiscard]] const UbSliceSpec& spec() const { return spec_; }

    bool markQueued(uint64_t now_ns = 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (cancel_requested_.load(std::memory_order_acquire) ||
            isTerminal(state_)) {
            return false;
        }
        if (state_ != UbSliceState::kInitial &&
            state_ != UbSliceState::kRetryPending) {
            return false;
        }
        state_ = UbSliceState::kQueued;
        queued_ns_ = normalizedNow(now_ns);
        return true;
    }

    // Claims this slice for one posting attempt. A cancellation racing after
    // this point is best-effort: the posting thread owns the device boundary
    // and must resolve the attempt instead of reporting an immediate cancel.
    std::optional<UbAttemptToken> beginAttempt(const UbPostPath& path,
                                               uint64_t now_ns = 0) {
        if (!path.valid()) return std::nullopt;
        bool canceled = false;
        TransferStatusEnum terminal_status = PENDING;
        size_t terminal_bytes = 0;
        std::optional<UbAttemptToken> token;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (isTerminal(state_)) return std::nullopt;
            if (cancel_requested_.load(std::memory_order_acquire)) {
                if (state_ == UbSliceState::kInitial ||
                    state_ == UbSliceState::kQueued ||
                    state_ == UbSliceState::kRetryPending) {
                    setTerminalLocked(CANCELED, 0, normalizedNow(now_ns));
                    canceled = true;
                    terminal_status = CANCELED;
                }
            } else if (state_ == UbSliceState::kInitial ||
                       state_ == UbSliceState::kQueued ||
                       state_ == UbSliceState::kRetryPending) {
                state_ = UbSliceState::kPosting;
                path_ = path;
                ++attempt_;
                attempt_started_ns_ = normalizedNow(now_ns);
                posted_ns_ = 0;
                token = UbAttemptToken{id_, attempt_, path_};
            }
        }
        if (canceled) notifyTaskTerminal(terminal_status, terminal_bytes);
        return token;
    }

    [[nodiscard]] std::optional<UbCompletionToken> completionToken(
        const UbAttemptToken& token) {
        auto self = weak_from_this().lock();
        if (!self) return std::nullopt;
        std::lock_guard<std::mutex> lock(mutex_);
        if (!matchesActiveAttemptLocked(token) ||
            (state_ != UbSliceState::kPosting &&
             state_ != UbSliceState::kPosted)) {
            return std::nullopt;
        }
        return UbCompletionToken{std::move(self), token};
    }

    bool markPosted(const UbAttemptToken& token, uint64_t now_ns = 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!matchesActiveAttemptLocked(token) ||
            state_ != UbSliceState::kPosting) {
            return false;
        }
        state_ = UbSliceState::kPosted;
        posted_ns_ = normalizedNow(now_ns);
        return true;
    }

    // Linearization point immediately before crossing the native post
    // boundary. Cancellation that wins this mutex prevents the WR from being
    // posted and terminalizes the slice; cancellation after this point is
    // best-effort and waits for completion/drain.
    bool tryCommitPost(const UbAttemptToken& token, uint64_t now_ns = 0) {
        bool canceled = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!matchesActiveAttemptLocked(token) ||
                state_ != UbSliceState::kPosting) {
                return false;
            }
            if (cancel_requested_.load(std::memory_order_acquire)) {
                setTerminalLocked(CANCELED, 0, normalizedNow(now_ns));
                canceled = true;
            } else {
                state_ = UbSliceState::kPosted;
                posted_ns_ = normalizedNow(now_ns);
            }
        }
        if (canceled) notifyTaskTerminal(CANCELED, 0);
        return !canceled;
    }

    // Resolves exactly one posting attempt. Retriable errors move the logical
    // slice back to kRetryPending without notifying the task. Duplicate or
    // stale completions are ignored by matching attempt and endpoint
    // generation. Only the final resolution contributes task bytes/status.
    UbAttemptResolution resolveAttempt(const UbAttemptToken& token,
                                       TransferStatusEnum outcome,
                                       size_t transferred_bytes, bool retryable,
                                       uint64_t now_ns = 0) {
        bool terminal = false;
        TransferStatusEnum terminal_status = PENDING;
        size_t terminal_bytes = 0;
        UbAttemptResolution resolution = UbAttemptResolution::kIgnored;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!matchesActiveAttemptLocked(token) ||
                (state_ != UbSliceState::kPosting &&
                 state_ != UbSliceState::kPosted) ||
                !isTerminal(outcome)) {
                return UbAttemptResolution::kIgnored;
            }

            const bool canceled =
                cancel_requested_.load(std::memory_order_acquire);
            if (outcome != COMPLETED && retryable && !canceled &&
                retry_count_ < spec_.max_retries) {
                ++retry_count_;
                state_ = UbSliceState::kRetryPending;
                queued_ns_ = normalizedNow(now_ns);
                resolution = UbAttemptResolution::kRetryScheduled;
            } else {
                terminal_status =
                    (canceled && outcome != COMPLETED) ? CANCELED : outcome;
                setTerminalLocked(terminal_status, transferred_bytes,
                                  normalizedNow(now_ns));
                terminal_bytes = transferred_bytes_;
                terminal = true;
                resolution = UbAttemptResolution::kTerminal;
            }
        }
        if (terminal) notifyTaskTerminal(terminal_status, terminal_bytes);
        return resolution;
    }

    // Resolves work that failed (or completed locally) before any adapter post.
    // It deliberately refuses to terminalize kPosting/kPosted work; those must
    // drain through resolveAttempt().
    UbAttemptResolution resolveBeforePost(TransferStatusEnum outcome,
                                          size_t transferred_bytes,
                                          bool retryable, uint64_t now_ns = 0) {
        bool terminal = false;
        TransferStatusEnum terminal_status = outcome;
        size_t terminal_bytes = 0;
        UbAttemptResolution resolution = UbAttemptResolution::kIgnored;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!isTerminal(outcome) || isTerminal(state_) ||
                state_ == UbSliceState::kPosting ||
                state_ == UbSliceState::kPosted) {
                return UbAttemptResolution::kIgnored;
            }
            const bool canceled =
                cancel_requested_.load(std::memory_order_acquire);
            if (outcome != COMPLETED && retryable && !canceled &&
                retry_count_ < spec_.max_retries) {
                ++retry_count_;
                state_ = UbSliceState::kRetryPending;
                queued_ns_ = normalizedNow(now_ns);
                resolution = UbAttemptResolution::kRetryScheduled;
            } else {
                if (canceled && outcome != COMPLETED) {
                    terminal_status = CANCELED;
                }
                setTerminalLocked(terminal_status, transferred_bytes,
                                  normalizedNow(now_ns));
                terminal_bytes = transferred_bytes_;
                terminal = true;
                resolution = UbAttemptResolution::kTerminal;
            }
        }
        if (terminal) notifyTaskTerminal(terminal_status, terminal_bytes);
        return resolution;
    }

    bool tryResolveBeforePost(TransferStatusEnum outcome,
                              size_t transferred_bytes = 0,
                              uint64_t now_ns = 0) {
        return resolveBeforePost(outcome, transferred_bytes, false, now_ns) ==
               UbAttemptResolution::kTerminal;
    }

    // Best-effort cancellation. Unclaimed work becomes terminal immediately;
    // posting or posted work only observes the flag and must still be resolved
    // by its device completion.
    bool requestCancellation(uint64_t now_ns = 0) {
        cancel_requested_.store(true, std::memory_order_release);
        bool terminal = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (state_ == UbSliceState::kInitial ||
                state_ == UbSliceState::kQueued ||
                state_ == UbSliceState::kRetryPending) {
                setTerminalLocked(CANCELED, 0, normalizedNow(now_ns));
                terminal = true;
            }
        }
        if (terminal) notifyTaskTerminal(CANCELED, 0);
        return terminal;
    }

    [[nodiscard]] bool cancellationRequested() const {
        return cancel_requested_.load(std::memory_order_acquire);
    }

    // Diagnostics for traced tasks; they never affect slice state.
    void noteDequeued(uint64_t now_ns) {
        dequeued_ns_.store(now_ns, std::memory_order_relaxed);
    }
    void noteRouteMetadata(uint64_t now_ns) {
        route_metadata_ns_.store(now_ns, std::memory_order_relaxed);
    }
    void notePostReturned(uint64_t now_ns) {
        post_returned_ns_.store(now_ns, std::memory_order_relaxed);
    }
    void noteDeferred() {
        defer_count_.fetch_add(1, std::memory_order_relaxed);
    }
    void notePrepareCheckpoints(uint64_t route_ready_ns,
                                uint64_t path_selected_ns,
                                uint64_t segments_ready_ns) {
        route_ready_ns_.store(route_ready_ns, std::memory_order_relaxed);
        path_selected_ns_.store(path_selected_ns, std::memory_order_relaxed);
        segments_ready_ns_.store(segments_ready_ns, std::memory_order_relaxed);
    }
    // Submit-side checkpoints; see UbSliceSnapshot.
    void noteSubmitStages(uint64_t submit_started_ns, uint64_t queue_locked_ns,
                          uint64_t enqueue_finished_ns) {
        submit_started_ns_.store(submit_started_ns,
                                  std::memory_order_relaxed);
        queue_locked_ns_.store(queue_locked_ns, std::memory_order_relaxed);
        enqueue_finished_ns_.store(enqueue_finished_ns,
                                   std::memory_order_relaxed);
    }
    // Route metadata sub-phase checkpoints; see UbSliceSnapshot.
    void noteRouteStages(uint64_t segment_cache_ns, uint64_t find_buffer_ns,
                         uint64_t decode_metadata_ns,
                         uint64_t remote_order_ns) {
        segment_cache_ns_.store(segment_cache_ns, std::memory_order_relaxed);
        find_buffer_ns_.store(find_buffer_ns, std::memory_order_relaxed);
        decode_metadata_ns_.store(decode_metadata_ns,
                                  std::memory_order_relaxed);
        remote_order_ns_.store(remote_order_ns, std::memory_order_relaxed);
    }
    // Completion path checkpoints; see UbSliceSnapshot.
    void noteCqeSeen(uint64_t now_ns) {
        cqe_seen_ns_.store(now_ns, std::memory_order_relaxed);
    }
    void noteInflightLocked(uint64_t now_ns) {
        inflight_locked_ns_.store(now_ns, std::memory_order_relaxed);
    }
    void noteInflightErased(uint64_t now_ns) {
        inflight_erased_ns_.store(now_ns, std::memory_order_relaxed);
    }
    void noteResourcesReleased(uint64_t now_ns) {
        resources_released_ns_.store(now_ns, std::memory_order_relaxed);
    }
    void noteRailRecorded(uint64_t now_ns) {
        rail_recorded_ns_.store(now_ns, std::memory_order_relaxed);
    }

    [[nodiscard]] UbSliceSnapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return UbSliceSnapshot{
            id_,
            state_,
            path_,
            attempt_,
            retry_count_,
            spec_.max_retries,
            cancel_requested_.load(std::memory_order_acquire),
            transferred_bytes_,
            created_ns_,
            queued_ns_,
            attempt_started_ns_,
            posted_ns_,
            terminal_ns_,
            dequeued_ns_.load(std::memory_order_relaxed),
            post_returned_ns_.load(std::memory_order_relaxed),
            defer_count_.load(std::memory_order_relaxed),
            route_ready_ns_.load(std::memory_order_relaxed),
            path_selected_ns_.load(std::memory_order_relaxed),
            segments_ready_ns_.load(std::memory_order_relaxed),
            route_metadata_ns_.load(std::memory_order_relaxed),
            submit_started_ns_.load(std::memory_order_relaxed),
            queue_locked_ns_.load(std::memory_order_relaxed),
            enqueue_finished_ns_.load(std::memory_order_relaxed),
            segment_cache_ns_.load(std::memory_order_relaxed),
            find_buffer_ns_.load(std::memory_order_relaxed),
            decode_metadata_ns_.load(std::memory_order_relaxed),
            remote_order_ns_.load(std::memory_order_relaxed),
            cqe_seen_ns_.load(std::memory_order_relaxed),
            inflight_locked_ns_.load(std::memory_order_relaxed),
            inflight_erased_ns_.load(std::memory_order_relaxed),
            resources_released_ns_.load(std::memory_order_relaxed),
            rail_recorded_ns_.load(std::memory_order_relaxed)};
    }

   private:
    friend struct UbTask;

    UbSlice(uint64_t id, UbSliceSpec spec, std::weak_ptr<UbTask> task,
            uint64_t created_ns)
        : id_(id),
          spec_(std::move(spec)),
          task_(std::move(task)),
          created_ns_(normalizedNow(created_ns)) {}

    static uint64_t normalizedNow(uint64_t now_ns) {
        return now_ns == 0 ? steadyNowNs() : now_ns;
    }

    bool matchesActiveAttemptLocked(const UbAttemptToken& token) const {
        return token.slice_id == id_ && token.attempt == attempt_ &&
               token.path == path_;
    }

    void setTerminalLocked(TransferStatusEnum status, size_t bytes,
                           uint64_t now_ns) {
        state_ = sliceState(status);
        transferred_bytes_ = std::min(bytes, spec_.length);
        terminal_ns_ = now_ns;
    }

    void notifyTaskTerminal(TransferStatusEnum status, size_t bytes);

    const uint64_t id_;
    const UbSliceSpec spec_;
    std::weak_ptr<UbTask> task_;

    mutable std::mutex mutex_;
    UbSliceState state_{UbSliceState::kInitial};
    UbPostPath path_{};
    uint32_t attempt_{0};
    uint32_t retry_count_{0};
    std::atomic<bool> cancel_requested_{false};
    size_t transferred_bytes_{0};
    uint64_t created_ns_{0};
    uint64_t queued_ns_{0};
    uint64_t attempt_started_ns_{0};
    uint64_t posted_ns_{0};
    uint64_t terminal_ns_{0};
    std::atomic<uint64_t> dequeued_ns_{0};
    std::atomic<uint64_t> post_returned_ns_{0};
    std::atomic<uint32_t> defer_count_{0};
    std::atomic<uint64_t> route_ready_ns_{0};
    std::atomic<uint64_t> path_selected_ns_{0};
    std::atomic<uint64_t> segments_ready_ns_{0};
    std::atomic<uint64_t> route_metadata_ns_{0};
    std::atomic<uint64_t> submit_started_ns_{0};
    std::atomic<uint64_t> queue_locked_ns_{0};
    std::atomic<uint64_t> enqueue_finished_ns_{0};
    std::atomic<uint64_t> segment_cache_ns_{0};
    std::atomic<uint64_t> find_buffer_ns_{0};
    std::atomic<uint64_t> decode_metadata_ns_{0};
    std::atomic<uint64_t> remote_order_ns_{0};
    std::atomic<uint64_t> cqe_seen_ns_{0};
    std::atomic<uint64_t> inflight_locked_ns_{0};
    std::atomic<uint64_t> inflight_erased_ns_{0};
    std::atomic<uint64_t> resources_released_ns_{0};
    std::atomic<uint64_t> rail_recorded_ns_{0};
};

struct UbTaskSnapshot {
    TransferStatus status{PENDING, 0};
    size_t total_bytes{0};
    size_t total_slices{0};
    size_t resolved_slices{0};
    size_t remaining_slices{0};
    size_t successful_slices{0};
    bool sealed{false};
    bool cancel_requested{false};
    uint64_t created_ns{0};
    uint64_t deadline_ns{0};
    uint64_t terminal_ns{0};
};

struct UbTask : public std::enable_shared_from_this<UbTask> {
   public:
    using Ptr = std::shared_ptr<UbTask>;
    using TerminalCallback = std::function<void(const TransferStatus&)>;

    static Ptr create(Request request, TerminalCallback terminal_callback = {},
                      uint64_t created_ns = 0) {
        return Ptr(new UbTask(std::move(request), std::move(terminal_callback),
                              created_ns));
    }

    UbTask(const UbTask&) = delete;
    UbTask& operator=(const UbTask&) = delete;

    [[nodiscard]] const Request& request() const { return request_; }

    // Must be called before the task is submitted to the workers.
    void enableTrace() { trace_enabled_ = true; }
    [[nodiscard]] bool traceEnabled() const { return trace_enabled_; }
    void setLogContext(uint64_t trace_id, bool sampled) {
        log_trace_id_ = trace_id;
        diagnostics_sampled_ = sampled;
    }
    uint64_t logTraceId() const { return log_trace_id_; }
    bool diagnosticsSampled() const { return diagnostics_sampled_; }

    // Keeps the first reported memory locations of this traced task.
    void noteTraceLocations(const std::string& local,
                            const std::string& remote) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (trace_local_location_.empty()) trace_local_location_ = local;
        if (trace_remote_location_.empty()) trace_remote_location_ = remote;
    }
    [[nodiscard]] std::pair<std::string, std::string> traceLocations() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return {trace_local_location_, trace_remote_location_};
    }

    // Slices must be added before seal(). Workers may begin processing only
    // after sealing, which prevents an early completion from finalizing a task
    // while its remaining slices are still being constructed.
    UbSlice::Ptr addSlice(UbSliceSpec spec, uint64_t created_ns = 0) {
        UbSlice::Ptr slice;
        bool cancel = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (sealed_) return nullptr;
            slice = UbSlice::Ptr(new UbSlice(next_slice_id_++, std::move(spec),
                                             weak_from_this(), created_ns));
            slices_.push_back(slice);
            cancel = cancel_requested_.load(std::memory_order_acquire);
        }
        if (cancel) slice->requestCancellation(created_ns);
        return slice;
    }

    bool seal() {
        TerminalCallback callback;
        TransferStatus final_status{};
        bool notify = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (sealed_) return false;
            sealed_ = true;
            notify = maybeFinalizeLocked(final_status, callback);
        }
        if (notify && callback) callback(final_status);
        return true;
    }

    // Returns the number of slices canceled before reaching the adapter. Any
    // posting/posted slices remain pending until their attempts resolve.
    size_t requestCancellation(uint64_t now_ns = 0) {
        cancel_requested_.store(true, std::memory_order_release);
        std::vector<UbSlice::Ptr> slices;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (isTerminal(status_.s)) return 0;
            slices = slices_;
        }
        size_t canceled = 0;
        for (const auto& slice : slices) {
            if (slice->requestCancellation(now_ns)) ++canceled;
        }
        return canceled;
    }

    [[nodiscard]] bool cancellationRequested() const {
        return cancel_requested_.load(std::memory_order_acquire);
    }

    [[nodiscard]] TransferStatus transferStatus() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return status_;
    }

    [[nodiscard]] UbTaskSnapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return UbTaskSnapshot{status_,
                              request_.length,
                              slices_.size(),
                              resolved_slice_ids_.size(),
                              slices_.size() - resolved_slice_ids_.size(),
                              successful_slices_,
                              sealed_,
                              cancel_requested_.load(std::memory_order_acquire),
                              created_ns_,
                              request_.deadline_ns,
                              terminal_ns_};
    }

    [[nodiscard]] std::vector<UbSlice::Ptr> slices() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return slices_;
    }

   private:
    friend class UbSlice;

    UbTask(Request request, TerminalCallback terminal_callback,
           uint64_t created_ns)
        : request_(std::move(request)),
          created_ns_(created_ns == 0 ? steadyNowNs() : created_ns),
          terminal_callback_(std::move(terminal_callback)) {}

    static int terminalSeverity(TransferStatusEnum status) {
        switch (status) {
            case INVALID:
                return 4;
            case FAILED:
                return 3;
            case TIMEOUT:
                return 2;
            case CANCELED:
                return 1;
            default:
                return 0;
        }
    }

    void onSliceTerminal(uint64_t slice_id, TransferStatusEnum status,
                         size_t bytes) {
        TerminalCallback callback;
        TransferStatus final_status{};
        bool notify = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!resolved_slice_ids_.insert(slice_id).second) return;
            if (std::numeric_limits<size_t>::max() - status_.transferred_bytes <
                bytes) {
                status_.transferred_bytes = std::numeric_limits<size_t>::max();
            } else {
                status_.transferred_bytes += bytes;
            }
            if (status == COMPLETED) {
                ++successful_slices_;
            } else if (terminalSeverity(status) >
                       terminalSeverity(aggregate_error_)) {
                aggregate_error_ = status;
            }
            notify = maybeFinalizeLocked(final_status, callback);
        }
        if (notify && callback) callback(final_status);
    }

    bool maybeFinalizeLocked(TransferStatus& final_status,
                             TerminalCallback& callback) {
        if (!sealed_ || terminal_notified_ ||
            resolved_slice_ids_.size() != slices_.size()) {
            return false;
        }

        if (slices_.empty() || successful_slices_ == slices_.size()) {
            status_.s = COMPLETED;
        } else {
            status_.s = aggregate_error_ == PENDING ? FAILED : aggregate_error_;
        }
        terminal_notified_ = true;
        terminal_ns_ = steadyNowNs();
        final_status = status_;
        callback = std::move(terminal_callback_);
        return true;
    }

    const Request request_;
    const uint64_t created_ns_;
    mutable std::mutex mutex_;
    std::vector<UbSlice::Ptr> slices_;
    std::unordered_set<uint64_t> resolved_slice_ids_;
    uint64_t next_slice_id_{1};
    size_t successful_slices_{0};
    TransferStatusEnum aggregate_error_{PENDING};
    TransferStatus status_{PENDING, 0};
    std::atomic<bool> cancel_requested_{false};
    bool sealed_{false};
    bool terminal_notified_{false};
    uint64_t terminal_ns_{0};
    TerminalCallback terminal_callback_;
    bool trace_enabled_{false};
    uint64_t log_trace_id_{0};
    bool diagnostics_sampled_{false};
    std::string trace_local_location_;
    std::string trace_remote_location_;
};

inline bool UbCompletionToken::valid() const {
    return slice != nullptr && attempt.valid();
}

inline bool UbCompletionToken::markPosted(uint64_t now_ns) const {
    return valid() && slice->markPosted(attempt, now_ns);
}

inline UbAttemptResolution UbCompletionToken::resolve(
    TransferStatusEnum outcome, size_t transferred_bytes, bool retryable,
    uint64_t now_ns) const {
    if (!valid()) return UbAttemptResolution::kIgnored;
    return slice->resolveAttempt(attempt, outcome, transferred_bytes, retryable,
                                 now_ns);
}

inline void UbSlice::notifyTaskTerminal(TransferStatusEnum status,
                                        size_t bytes) {
    if (auto task = task_.lock()) task->onSliceTerminal(id_, status, bytes);
}

}  // namespace mooncake::tent::ub

#endif  // MOONCAKE_TENT_TRANSPORT_UB_SLICE_H_
