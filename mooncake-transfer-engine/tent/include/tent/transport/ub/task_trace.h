// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_TASK_TRACE_H_
#define TENT_TRANSPORT_UB_TASK_TRACE_H_

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <functional>
#include <iomanip>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include "mooncake_logging.h"

#include "tent/runtime/topology.h"
#include "tent/transport/ub/slice.h"

namespace mooncake::tent::ub {

// Per-task UB tracing is off unless MC_TRANSFER_TRACE_MIN_BYTES is a positive
// byte count; requests at least that long are traced.
inline uint64_t ubTaskTraceMinBytes() {
    static const uint64_t value = [] {
        const char* raw = std::getenv("MC_TRANSFER_TRACE_MIN_BYTES");
        if (raw == nullptr || *raw == '\0') return uint64_t{0};
        char* end = nullptr;
        const unsigned long long parsed = std::strtoull(raw, &end, 10);
        return *end == '\0' ? static_cast<uint64_t>(parsed) : uint64_t{0};
    }();
    return value;
}

// Process-wide gate for the trace checkpoint clock reads on hot paths.
// It mirrors the conditions that construct the task trace reporter (and
// thus enable any task tracing), so while it is false no task can be
// traced and the checkpoints are pure overhead. In particular
// MC_HIFREQ_LOG_SAMPLE_RATE=0 disables every added clock read and log.
inline bool ubTaskTraceClocksEnabled() {
    static const bool enabled =
        ubTaskTraceMinBytes() > 0 &&
        mooncake::logging::HiFreqLogSampleRate() > 0 &&
        mooncake::logging::ShouldLog(google::INFO);
    return enabled;
}

// Slice-level latency distribution, in nanoseconds.
struct UbTraceLatency {
    uint64_t avg_ns{0};
    uint64_t p50_ns{0};
    uint64_t p99_ns{0};
    uint64_t max_ns{0};
};

inline UbTraceLatency summarizeUbTraceLatency(std::vector<uint64_t> samples) {
    UbTraceLatency latency;
    if (samples.empty()) return latency;
    std::sort(samples.begin(), samples.end());
    uint64_t total = 0;
    for (uint64_t sample : samples) total += sample;
    const auto nearest_rank = [&samples](size_t percent) {
        const size_t rank = (samples.size() * percent + 99) / 100;
        return samples[std::max<size_t>(rank, 1) - 1];
    };
    latency.avg_ns = total / samples.size();
    latency.p50_ns = nearest_rank(50);
    latency.p99_ns = nearest_rank(99);
    latency.max_ns = samples.back();
    return latency;
}

// Formats one finished task as a single key=value line. Phases of a slice:
//   queue_us   enqueued -> taken by a posting worker (includes quota deferrals)
//   prepare_us taken -> ready to post (route, endpoint, quota, segment lookup)
//     prep_route_us    taken -> route and local location resolved
//       prep_route_metadata_us  taken -> remote metadata resolved; split into
//           prep_seg_cache_us   segment descriptor resolved from cache
//           prep_find_buf_us   remote buffer lookup
//           prep_decode_us     transport metadata decode
//           prep_remote_order_us remote device ordering
//       prep_local_location_us  -> local NUMA location resolved
//     prep_select_us   -> endpoint, rail and quota selected
//     prep_segments_us -> local segment found and remote segment imported
//     prep_commit_us   -> attempt committed, ready to post
//   post_us    duration of the URMA post call
//   device_us  post returned -> completion observed by the poller (device
//              time plus poller wake-up delay)
//   cqh_*_us   completion handling stages observed by the poller:
//     cqh_wait_us    CQE seen -> inflight mutex acquired (lock wait)
//     cqh_inflight_us -> entry erased under the lock (lock hold)
//     cqh_release_us -> endpoint outstanding/quota released and waiters
//                      notified; sub-split (rel_*_us):
//       rel_endpoint_us -> endpoint releaseOutstanding() returned
//       rel_quota_us    -> quota reservation released
//       rel_notify_us   -> release tail: queue waiters notified (closed by
//                         the resources-released checkpoint)
//     cqh_rail_us    -> rail monitor updated (global rail lock)
//     cqh_resolve_us -> slice resolved, task finalized, callback run
//   Submit-side phases (sub_*_us) split the enqueue path:
//     sub_build_us       task created -> submitBatch entered
//     sub_lock_wait_us   submitBatch entered -> queue mutex acquired
//     sub_enqueue_us     queue mutex acquired -> enqueue finished
// Retried slices report their final attempt.
inline std::string formatUbTaskTrace(
    const UbTask& task, BatchID batch_id,
    const std::function<std::string(Topology::NicID)>& nic_name) {
    const auto delta = [](uint64_t from, uint64_t to) {
        return to > from ? to - from : uint64_t{0};
    };
    const auto us = [](uint64_t ns) {
        return static_cast<double>(ns) / 1000.0;
    };

    const auto snapshot = task.snapshot();
    std::vector<uint64_t> queue, prepare, post, device;
    std::vector<uint64_t> prep_route, prep_select, prep_segments, prep_commit;
    std::vector<uint64_t> route_metadata, local_location;
    std::vector<uint64_t> seg_cache, find_buffer, decode, remote_order;
    std::vector<uint64_t> submit_build, submit_lock_wait, submit_enqueue;
    std::vector<uint64_t> cqh_wait, cqh_inflight, cqh_release, cqh_rail,
        cqh_resolve;
    std::vector<uint64_t> rel_endpoint, rel_quota, rel_notify;
    uint64_t first_queued = 0, first_post = 0, last_post = 0;
    uint64_t first_complete = 0, last_complete = 0;
    uint64_t retries = 0, defers = 0;
    size_t slice_size = 0;
    std::map<std::string, size_t> rails;
    for (const auto& slice : task.slices()) {
        const auto item = slice->snapshot();
        slice_size = std::max(slice_size, slice->spec().length);
        retries += item.retry_count;
        defers += item.defer_count;
        if (item.state != UbSliceState::kCompleted) continue;
        queue.push_back(delta(item.queued_ns, item.dequeued_ns));
        prepare.push_back(delta(item.dequeued_ns, item.posted_ns));
        if (item.submit_started_ns != 0) {
            submit_build.push_back(
                delta(item.created_ns, item.submit_started_ns));
            submit_lock_wait.push_back(
                delta(item.submit_started_ns, item.queue_locked_ns));
            submit_enqueue.push_back(
                delta(item.queue_locked_ns, item.enqueue_finished_ns));
        }
        if (item.segments_ready_ns != 0) {
            if (item.route_metadata_ns != 0) {
                route_metadata.push_back(
                    delta(item.dequeued_ns, item.route_metadata_ns));
                local_location.push_back(
                    delta(item.route_metadata_ns, item.route_ready_ns));
                if (item.segment_cache_ns != 0) {
                    seg_cache.push_back(
                        delta(item.dequeued_ns, item.segment_cache_ns));
                    find_buffer.push_back(
                        delta(item.segment_cache_ns, item.find_buffer_ns));
                    decode.push_back(
                        delta(item.find_buffer_ns, item.decode_metadata_ns));
                    remote_order.push_back(
                        delta(item.decode_metadata_ns, item.remote_order_ns));
                }
            }
            prep_route.push_back(delta(item.dequeued_ns, item.route_ready_ns));
            prep_select.push_back(
                delta(item.route_ready_ns, item.path_selected_ns));
            prep_segments.push_back(
                delta(item.path_selected_ns, item.segments_ready_ns));
            prep_commit.push_back(
                delta(item.segments_ready_ns, item.posted_ns));
        }
        post.push_back(delta(item.posted_ns, item.post_returned_ns));
        device.push_back(delta(item.post_returned_ns, item.terminal_ns));
        if (item.cqe_seen_ns != 0) {
            // Device time ends where the poller observed the CQE; the rest is
            // completion handling, measured stage by stage.
            device.back() = delta(item.post_returned_ns, item.cqe_seen_ns);
            cqh_wait.push_back(delta(item.cqe_seen_ns,
                                     item.inflight_locked_ns));
            cqh_inflight.push_back(delta(item.inflight_locked_ns,
                                         item.inflight_erased_ns));
            cqh_release.push_back(delta(item.inflight_erased_ns,
                                       item.resources_released_ns));
            // Release sub-split: endpoint accounting, quota release, and the
            // notify tail. A missing checkpoint (e.g. no endpoint or quota on
            // an error path) folds its stage into the next window boundary
            // so every reported delta stays well-defined.
            uint64_t release_stage_ns = item.inflight_erased_ns;
            if (item.endpoint_released_ns != 0) {
                rel_endpoint.push_back(
                    delta(release_stage_ns, item.endpoint_released_ns));
                release_stage_ns = item.endpoint_released_ns;
            }
            if (item.quota_released_ns != 0) {
                rel_quota.push_back(
                    delta(release_stage_ns, item.quota_released_ns));
                release_stage_ns = item.quota_released_ns;
            }
            rel_notify.push_back(
                delta(release_stage_ns, item.resources_released_ns));
            cqh_rail.push_back(delta(item.resources_released_ns,
                                     item.rail_recorded_ns));
            cqh_resolve.push_back(delta(item.rail_recorded_ns,
                                        item.terminal_ns));
        }
        first_queued = first_queued == 0
                           ? item.queued_ns
                           : std::min(first_queued, item.queued_ns);
        first_post = first_post == 0 ? item.posted_ns
                                     : std::min(first_post, item.posted_ns);
        last_post = std::max(last_post, item.posted_ns);
        first_complete = first_complete == 0
                             ? item.terminal_ns
                             : std::min(first_complete, item.terminal_ns);
        last_complete = std::max(last_complete, item.terminal_ns);
        ++rails[nic_name(item.path.local_topology_id) + ">" +
                std::to_string(item.path.remote_device_id)];
    }

    const uint64_t total_ns = delta(snapshot.created_ns, snapshot.terminal_ns);
    const double mbps = total_ns == 0
                            ? 0.0
                            : static_cast<double>(snapshot.total_bytes) *
                                  1000.0 / static_cast<double>(total_ns);
    const auto since_start = [&](uint64_t ns) {
        return us(delta(snapshot.created_ns, ns));
    };
    const auto [local_mem, remote_mem] = task.traceLocations();

    std::ostringstream out;
    out << std::fixed << std::setprecision(1);
    const auto write_latency = [&](const char* name,
                                   const std::vector<uint64_t>& samples) {
        const auto latency = summarizeUbTraceLatency(samples);
        out << ' ' << name << "[avg/p50/p99/max]=" << us(latency.avg_ns) << '/'
            << us(latency.p50_ns) << '/' << us(latency.p99_ns) << '/'
            << us(latency.max_ns);
    };
    out << "ub_task_trace batch=0x" << std::hex << batch_id << std::dec
        << " engine=tent trace_id=" << task.logTraceId()
        << " op=" << (task.request().opcode == Request::READ ? "read" : "write")
        << " bytes=" << snapshot.total_bytes
        << " slices=" << snapshot.total_slices << " slice_size=" << slice_size
        << " status=" << static_cast<int>(snapshot.status.s)
        << " total_us=" << us(total_ns) << " MBps=" << mbps
        << " created_ns=" << snapshot.created_ns
        << " terminal_ns=" << snapshot.terminal_ns
        << " first_post_ns=" << first_post
        << " last_complete_ns=" << last_complete << " completion_finalize_us="
        << us(delta(last_complete, snapshot.terminal_ns))
        << " build_us=" << since_start(first_queued)
        << " first_post_us=" << since_start(first_post)
        << " last_post_us=" << since_start(last_post)
        << " first_complete_us=" << since_start(first_complete)
        << " last_complete_us=" << since_start(last_complete);
    write_latency("queue_us", queue);
    write_latency("prepare_us", prepare);
    write_latency("sub_build_us", submit_build);
    write_latency("sub_lock_wait_us", submit_lock_wait);
    write_latency("sub_enqueue_us", submit_enqueue);
    write_latency("prep_route_us", prep_route);
    write_latency("prep_route_metadata_us", route_metadata);
    write_latency("prep_seg_cache_us", seg_cache);
    write_latency("prep_find_buf_us", find_buffer);
    write_latency("prep_decode_us", decode);
    write_latency("prep_remote_order_us", remote_order);
    write_latency("prep_local_location_us", local_location);
    write_latency("prep_select_us", prep_select);
    write_latency("prep_segments_us", prep_segments);
    write_latency("prep_commit_us", prep_commit);
    write_latency("post_us", post);
    write_latency("device_us", device);
    write_latency("cqh_wait_us", cqh_wait);
    write_latency("cqh_inflight_us", cqh_inflight);
    write_latency("cqh_release_us", cqh_release);
    write_latency("rel_endpoint_us", rel_endpoint);
    write_latency("rel_quota_us", rel_quota);
    write_latency("rel_notify_us", rel_notify);
    write_latency("cqh_rail_us", cqh_rail);
    write_latency("cqh_resolve_us", cqh_resolve);
    out << " retries=" << retries << " defers=" << defers << " rails=";
    bool first_rail = true;
    for (const auto& [rail, count] : rails) {
        out << (first_rail ? "" : ",") << rail << ':' << count;
        first_rail = false;
    }
    out << " local_mem=" << (local_mem.empty() ? "-" : local_mem)
        << " remote_mem=" << (remote_mem.empty() ? "-" : remote_mem);
    return out.str();
}

// Formats and writes finished tasks on its own thread, so tracing adds only a
// queue push to the transfer path.
class UbTaskTraceReporter final {
   public:
    explicit UbTaskTraceReporter(std::shared_ptr<Topology> topology)
        : topology_(std::move(topology)), thread_([this] { run(); }) {}

    ~UbTaskTraceReporter() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        cv_.notify_all();
        thread_.join();
    }

    UbTaskTraceReporter(const UbTaskTraceReporter&) = delete;
    UbTaskTraceReporter& operator=(const UbTaskTraceReporter&) = delete;

    void submit(const std::vector<UbTask::Ptr>& tasks, BatchID batch_id) {
        bool queued = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (const auto& task : tasks) {
                if (!task || !task->traceEnabled() ||
                    !isTerminal(task->transferStatus().s)) {
                    continue;
                }
                if (pending_.size() >= kMaxPending) {
                    ++dropped_;
                    continue;
                }
                pending_.emplace_back(task, batch_id);
                queued = true;
            }
        }
        if (queued) cv_.notify_one();
    }

   private:
    static constexpr size_t kMaxPending = 4096;

    void run() {
        const auto nic_name = [this](Topology::NicID id) {
            return topology_ ? topology_->getNicName(id) : std::to_string(id);
        };
        std::unique_lock<std::mutex> lock(mutex_);
        while (true) {
            cv_.wait(lock, [this] { return stopping_ || !pending_.empty(); });
            if (pending_.empty()) return;
            auto [task, batch_id] = std::move(pending_.front());
            pending_.pop_front();
            const uint64_t dropped = std::exchange(dropped_, 0);
            lock.unlock();
            mooncake::logging::ScopedTraceId log_context(task->logTraceId());
            MC_LOG(INFO) << formatUbTaskTrace(*task, batch_id, nic_name);
            if (dropped != 0) {
                LOG(WARNING) << "ub_task_trace dropped " << dropped
                             << " task(s): reporter queue is full";
            }
            task.reset();
            lock.lock();
        }
    }

    std::shared_ptr<Topology> topology_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::pair<UbTask::Ptr, BatchID>> pending_;
    uint64_t dropped_{0};
    bool stopping_{false};
    std::thread thread_;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_TASK_TRACE_H_
