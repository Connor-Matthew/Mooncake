// Copyright 2024 KVCache.AI
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

#include <glog/logging.h>
#include <atomic>
#include <memory>
#include <utility>
#include <cassert>
#include <cstdlib>
#include <cerrno>
#include <sys/epoll.h>
#include "config.h"
#include "mooncake_logging.h"
#include "transport/kunpeng_transport/ub_context.h"
#include "transport/kunpeng_transport/ub_endpoint.h"
#include "memory_location.h"
#include "transport/kunpeng_transport/ub_diagnostics.h"
#define SPDIAG_PERF_DEF_FILE "mooncake_perf_points.def"
#define SPDIAG_PROGRAM_NAME "mooncake_store"
#include "spdiag/auto_perf.h"

namespace mooncake {
namespace {
constexpr uint64_t kNumaAffinitySampleInterval = 10000;
thread_local UbWorkerMetrics* current_worker_metrics = nullptr;

uint64_t WorkerDiagnosticIntervalNs() {
    static const uint64_t interval = [] {
        const char* value = std::getenv("MC_UB_WORKER_DIAG_INTERVAL_MS");
        if (!value) return uint64_t{1000000000};
        char* end = nullptr;
        errno = 0;
        const auto ms = std::strtoull(value, &end, 10);
        if (errno || end == value || *end || *value == '-' ||
            ms == 0 || ms > 60000) {
            LOG(WARNING) << "Invalid MC_UB_WORKER_DIAG_INTERVAL_MS; using 1000";
            return uint64_t{1000000000};
        }
        return static_cast<uint64_t>(ms) * 1000000;
    }();
    return interval;
}

uint64_t WorkerIdleSpinNs() {
    static const uint64_t idle_spin_ns = [] {
        constexpr uint64_t kDefaultIdleSpinUs = 100000;  // 100 ms
        const char* value = std::getenv("MC_UB_WORKER_IDLE_SPIN_US");
        if (!value) return kDefaultIdleSpinUs * 1000;
        char* end = nullptr;
        errno = 0;
        const auto us = std::strtoull(value, &end, 10);
        if (errno || end == value || *end || *value == '-' || us > 1000000) {
            LOG(WARNING) << "Invalid MC_UB_WORKER_IDLE_SPIN_US; using "
                         << kDefaultIdleSpinUs;
            return kDefaultIdleSpinUs * 1000;
        }
        return static_cast<uint64_t>(us) * 1000;
    }();
    return idle_spin_ns;
}

std::unique_ptr<mooncake::logging::ScopedTraceId> RestoreTraceIfMissing(
    uint64_t trace_id) {
    if (trace_id == 0 || mooncake::logging::CurrentTraceId() != 0) {
        return nullptr;
    }
    return std::make_unique<mooncake::logging::ScopedTraceId>(trace_id);
}
}  // namespace

bool UbWorkerDiagnosticsEnabled() {
    static const bool enabled = [] {
        const char* value = std::getenv("MC_UB_WORKER_DIAG");
        return value && std::string(value) == "1";
    }();
    return enabled;
}

UbWorkerMetrics* CurrentUbWorkerMetrics() { return current_worker_metrics; }

std::shared_ptr<UbEndPoint> UbSIEVEEndpointStore::getEndpoint(
    const std::string& peer_nic_path) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(peer_nic_path);
    if (iter != endpoint_map_.end()) {
        iter->second.second.store(
            true, std::memory_order_relaxed);  // This is safe within read lock
        // because of idempotence
        return iter->second.first;
    }
    return nullptr;
}

std::shared_ptr<UbEndPoint> UbSIEVEEndpointStore::insertEndpoint(
    const std::string& peer_nic_path, UbContext* context) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    if (endpoint_map_.find(peer_nic_path) != endpoint_map_.end()) {
        LOG(INFO) << "Endpoint " << peer_nic_path
                  << " already exists in SIEVEEndpointStore";
        return endpoint_map_[peer_nic_path].first;
    }
    auto endpoint = context->makeEndpoint();
    if (!endpoint) {
        LOG(ERROR) << "Failed to allocate memory for UbEndPoint";
        return nullptr;
    }
    auto& config = globalConfig();
    int ret = endpoint->construct(config);
    if (ret) return nullptr;

    while (this->getSize() >= max_size_) evictEndpoint();

    endpoint->setPeerNicPath(peer_nic_path);
    endpoint_map_[peer_nic_path] = std::make_pair(endpoint, false);
    fifo_list_.push_front(peer_nic_path);
    fifo_map_[peer_nic_path] = fifo_list_.begin();
    return endpoint;
}

int UbSIEVEEndpointStore::deleteEndpoint(const std::string& peer_nic_path) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(peer_nic_path);
    if (iter != endpoint_map_.end()) {
        waiting_list_len_++;
        waiting_list_.insert(iter->second.first);
        endpoint_map_.erase(iter);
        auto fifo_iter = fifo_map_[peer_nic_path];
        if (hand_.has_value() && hand_.value() == fifo_iter) {
            fifo_iter == fifo_list_.begin() ? hand_ = std::nullopt
                                            : hand_ = std::prev(fifo_iter);
        }
        fifo_list_.erase(fifo_iter);
        fifo_map_.erase(peer_nic_path);
    }
    return 0;
}

int UbSIEVEEndpointStore::deleteEndpointByPtr(UbEndPoint* point_ptr) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    for (auto iter = endpoint_map_.begin(); iter != endpoint_map_.end();
         iter++) {
        if (iter->second.first.get() == point_ptr) {
            std::string peer_nic_path = iter->first;
            iter->second.first->deconstruct();
            waiting_list_len_++;
            waiting_list_.insert(iter->second.first);
            auto fifo_iter = fifo_map_[peer_nic_path];
            if (hand_.has_value() && hand_.value() == fifo_iter) {
                fifo_iter == fifo_list_.begin() ? hand_ = std::nullopt
                                                : hand_ = std::prev(fifo_iter);
            }
            fifo_list_.erase(fifo_iter);
            fifo_map_.erase(peer_nic_path);
            endpoint_map_.erase(iter);
            return 0;
        }
    }
    return 0;
}

void UbSIEVEEndpointStore::evictEndpoint() {
    if (fifo_list_.empty()) {
        return;
    }
    auto o = hand_.has_value() ? hand_.value() : --fifo_list_.end();
    std::string victim;
    while (true) {
        victim = *o;
        if (endpoint_map_[victim].second.load(std::memory_order_relaxed)) {
            endpoint_map_[victim].second.store(false,
                                               std::memory_order_relaxed);
            o = (o == fifo_list_.begin() ? --fifo_list_.end() : std::prev(o));
        } else {
            break;
        }
    }
    hand_ = (o == fifo_list_.begin() ? --fifo_list_.end() : std::prev(o));
    fifo_list_.erase(o);
    fifo_map_.erase(victim);
    auto victim_instance = endpoint_map_[victim].first;
    victim_instance->set_active(false);
    waiting_list_len_++;
    waiting_list_.insert(victim_instance);
    endpoint_map_.erase(victim);
    return;
}

void UbSIEVEEndpointStore::reclaimEndpoint() {
    if (waiting_list_len_.load(std::memory_order_relaxed) == 0) return;
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::shared_ptr<UbEndPoint>> to_delete;
    for (auto& endpoint : waiting_list_)
        if (!endpoint->hasOutstandingSlice()) to_delete.push_back(endpoint);
    for (auto& endpoint : to_delete) waiting_list_.erase(endpoint);
    waiting_list_len_ -= to_delete.size();
}

int UbSIEVEEndpointStore::destroy() {
    for (auto& endpoint : waiting_list_) endpoint->deconstruct();
    for (auto& kv : endpoint_map_) kv.second.first->deconstruct();
    return 0;
}

int UbSIEVEEndpointStore::disconnect() {
    for (auto& endpoint : waiting_list_) endpoint->disconnect();
    for (auto& kv : endpoint_map_) kv.second.first->disconnect();
    return 0;
}

size_t UbSIEVEEndpointStore::getSize() { return endpoint_map_.size(); }

const static int kTransferWorkerCount = globalConfig().workers_per_ctx;

UbWorkerPool::UbWorkerPool(UbContext& context, int numa_socket_id)
    : context_(context),
      numa_socket_id_(numa_socket_id),
      workers_running_(true),
      redispatch_counter_(0),
      worker_signals_(std::make_unique<UbWorkerSignal[]>(kTransferWorkerCount)),
      submitted_slice_count_(0),
      processed_slice_count_(0) {
    for (auto& i : slice_queue_count_) i.store(0, std::memory_order_relaxed);
    collective_slice_queue_.resize(kTransferWorkerCount);
    LOG(INFO) << "UB worker scheduling: per-worker wait, workers="
              << kTransferWorkerCount << " jfcs=" << context_.jfcCount()
              << " idle_spin_us=" << WorkerIdleSpinNs() / 1000;
    for (int i = 0; i < kTransferWorkerCount; ++i) {
        worker_thread_.emplace_back([this, i] { transferWorker(i); });
    }
    worker_thread_.emplace_back([this] { monitorWorker(); });
}

UbWorkerPool::~UbWorkerPool() {
    workers_running_.store(false, std::memory_order_release);
    for (int i = 0; i < kTransferWorkerCount; ++i) {
        worker_signals_[i].notify();
    }
    for (auto& entry : worker_thread_) entry.join();
}

int UbWorkerPool::submitPostSend(
    const std::vector<Transport::Slice*>& slice_list) {
#ifdef CONFIG_CACHE_SEGMENT_DESC
    thread_local uint64_t tl_last_cache_ts = getCurrentTimeInNano();
    thread_local std::unordered_map<SegmentID,
                                    std::shared_ptr<UbTransport::SegmentDesc>>
        segment_desc_map;
    uint64_t current_ts = getCurrentTimeInNano();

    if (current_ts - tl_last_cache_ts > 1000000000) {
        segment_desc_map.clear();
        tl_last_cache_ts = current_ts;
    }

    for (auto& slice : slice_list) {
        auto target_id = slice->target_id;
        if (!segment_desc_map.count(target_id)) {
            segment_desc_map[target_id] =
                context_.engine().meta()->getSegmentDescByID(target_id);
            if (!segment_desc_map[target_id]) {
                segment_desc_map.clear();
                LOG(ERROR) << "Cannot get target segment description #"
                           << target_id;
                return ERR_INVALID_ARGUMENT;
            }
        }
    }
#else
    std::unordered_map<UbTransport::SegmentID,
                       std::shared_ptr<UbTransport::SegmentDesc>>
        segment_desc_map;
    for (auto& slice : slice_list) {
        auto target_id = slice->target_id;
        if (!segment_desc_map.count(target_id))
            segment_desc_map[target_id] =
                context_.engine().meta()->getSegmentDescByID(target_id);
    }
#endif  // CONFIG_CACHE_SEGMENT_DESC
    SliceList slice_list_map[kShardCount];
    uint64_t submitted_slice_count = 0;
    thread_local std::unordered_map<int, uint64_t> failed_target_ids;
    for (auto& slice : slice_list) {
        if (failed_target_ids.count(slice->target_id)) {
            auto ts = failed_target_ids[slice->target_id];
            if (getCurrentTimeInNano() - ts < 100000000ull) {
                slice->markFailed();
                continue;
            } else {
                failed_target_ids.erase(slice->target_id);
            }
        }
        auto& peer_segment_desc = segment_desc_map[slice->target_id];
        int buffer_id, device_id;
        auto hint = globalConfig().enable_dest_device_affinity
                        ? context_.deviceName()
                        : "";
        if (UbTransport::selectDevice(peer_segment_desc.get(),
                                      slice->ub.dest_addr, slice->length, hint,
                                      buffer_id, device_id)) {
            peer_segment_desc = context_.engine().meta()->getSegmentDescByID(
                slice->target_id, true);
            if (!peer_segment_desc) {
                LOG(ERROR) << "Cannot reload target segment #"
                           << slice->target_id;
                slice->markFailed();
                failed_target_ids[slice->target_id] = getCurrentTimeInNano();
                continue;
            }

            if (UbTransport::selectDevice(peer_segment_desc.get(),
                                          slice->ub.dest_addr, slice->length,
                                          hint, buffer_id, device_id)) {
                slice->markFailed();
                failed_target_ids[slice->target_id] = getCurrentTimeInNano();
                // The per-device list used to be logged here per event,
                // which floods under failure storms; the throttled
                // fixed-size record inside dumpMetadataContent covers it.
                context_.engine().meta()->dumpMetadataContent(
                    peer_segment_desc, slice->ub.dest_addr, slice->length);
                continue;
            }
        }
        if (!peer_segment_desc) {
            slice->markFailed();
            continue;
        }
        auto targetSegment =
            peer_segment_desc->buffers[buffer_id].tseg[device_id];
        slice->ub.r_seg = context_.retrieveRemoteSeg(targetSegment);
        if (!slice->ub.r_seg) {
            LOG(ERROR) << "[UB] retrieveRemoteSeg failed for target_id="
                       << slice->target_id << " buffer_id=" << buffer_id
                       << " device_id=" << device_id
                       << " dest_addr=" << slice->ub.dest_addr;
            slice->markFailed();
            failed_target_ids[slice->target_id] = getCurrentTimeInNano();
            continue;
        }
        if (context_.numa_affinity()) {
            const auto& peer_buf = peer_segment_desc->buffers[buffer_id];
            int data_numa = parseCpuNumaNode(peer_buf.name);
            if (peer_buf.chip_id >= 0) {
                slice->ub.dst_chip_id = (uint8_t)peer_buf.chip_id;
            } else {
                slice->ub.dst_chip_id = numaNodeToChipId(data_numa);
            }
            static std::atomic<uint64_t> numa_log_counter{0};
            if (VLOG_IS_ON(2) &&
                numa_log_counter.fetch_add(1, std::memory_order_relaxed) %
                        kNumaAffinitySampleInterval ==
                    0) {
                VLOG(2) << "[numa_affinity] remote_sample trace_id="
                        << slice->trace_id << " target_id=" << slice->target_id
                        << " opcode="
                        << (slice->opcode == Transport::TransferRequest::READ
                                ? "READ"
                                : "WRITE")
                        << " remote_data_numa=" << data_numa
                        << " dst_chip=" << (int)slice->ub.dst_chip_id
                        << " remote_name=" << peer_buf.name;
            }
        }
        auto peer_nic_path =
            MakeNicPath(peer_segment_desc->nicPathServerName(),
                        peer_segment_desc->devices[device_id].name);
        slice->peer_nic_path = peer_nic_path;
        int shard_id = (slice->target_id * 10007 + device_id) % kShardCount;
        // Until POSTED, diagnostics reuse ts for the latest queue entry.
        // The endpoint replaces it with the post timestamp before submission.
        if (UbWorkerDiagnosticsEnabled()) slice->ts = getCurrentTimeInNano();
        slice_list_map[shard_id].push_back(slice);
        submitted_slice_count++;
    }
    // Account before publishing: a woken worker may complete immediately.
    submitted_slice_count_.fetch_add(submitted_slice_count,
                                     std::memory_order_relaxed);
    for (int shard_id = 0; shard_id < kShardCount; ++shard_id) {
        if (slice_list_map[shard_id].empty()) continue;
        slice_queue_lock_[shard_id].lock();
        for (auto& slice : slice_list_map[shard_id])
            slice_queue_[shard_id][slice->peer_nic_path].push_back(slice);
        slice_queue_count_[shard_id].fetch_add(slice_list_map[shard_id].size(),
                                               std::memory_order_release);
        slice_queue_lock_[shard_id].unlock();
        worker_signals_[shard_id % kTransferWorkerCount].notify();
    }
    return 0;
}

void UbWorkerPool::performPostSend(int thread_id) {
    auto* metrics = CurrentUbWorkerMetrics();
    const auto dequeue_start = metrics ? getCurrentTimeInNano() : 0;
    auto& local_slice_queue = collective_slice_queue_[thread_id];
    for (int shard_id = thread_id; shard_id < kShardCount;
         shard_id += kTransferWorkerCount) {
        if (slice_queue_count_[shard_id].load(std::memory_order_relaxed) == 0)
            continue;

        slice_queue_lock_[shard_id].lock();
        for (auto& entry : slice_queue_[shard_id]) {
            for (auto& slice : entry.second)
                local_slice_queue[entry.first].push_back(slice);
            if (auto* metrics = CurrentUbWorkerMetrics()) {
                metrics->dequeued_slices += entry.second.size();
            }
            entry.second.clear();
        }
        slice_queue_count_[shard_id].store(0, std::memory_order_relaxed);
        slice_queue_lock_[shard_id].unlock();
    }
    if (metrics) metrics->dequeue_ns += getCurrentTimeInNano() - dequeue_start;

    // Redispatch slices to other endpoints, for temporary failures
    thread_local int tl_redispatch_counter = 0;
    if (tl_redispatch_counter <
        redispatch_counter_.load(std::memory_order_relaxed)) {
        tl_redispatch_counter =
            redispatch_counter_.load(std::memory_order_relaxed);
        auto local_slice_queue_clone = local_slice_queue;
        local_slice_queue.clear();
        for (auto& entry : local_slice_queue_clone)
            redispatch(entry.second, thread_id);
        return;
    }

#ifdef CONFIG_CACHE_ENDPOINT
    thread_local uint64_t tl_last_cache_ts = getCurrentTimeInNano();
    thread_local std::unordered_map<std::string, std::shared_ptr<UbEndPoint>>
        endpoint_map;
    uint64_t current_ts = getCurrentTimeInNano();
    if (current_ts - tl_last_cache_ts > 1000000000) {
        endpoint_map.clear();
        tl_last_cache_ts = current_ts;
    }
#endif

    SliceList failed_slice_list;
    bool advanced = false;
    for (auto& entry : local_slice_queue) {
        if (entry.second.empty()) continue;
        [[maybe_unused]] const auto prepare_start =
            metrics ? getCurrentTimeInNano() : 0;
        [[maybe_unused]] auto trace =
            RestoreTraceIfMissing(entry.second.front()->trace_id);

#ifdef USE_FAKE_POST_SEND
        for (auto& slice : entry.second) slice->markSuccess();
        processed_slice_count_.fetch_add(entry.second.size());
        entry.second.clear();
#else
#ifdef CONFIG_CACHE_ENDPOINT
        auto& endpoint = endpoint_map[entry.first];
        if (endpoint == nullptr || !endpoint->active())
            endpoint = context_.endpoint(entry.first);
#else
        auto endpoint = context_.endpoint(entry.first);
#endif
        if (!endpoint) {
            for (auto& slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
        if (!endpoint->active()) {
            if (endpoint->inactiveTime() > 1.0)
                context_.deleteEndpointByPtr(endpoint.get());
            // enable for re-establishation
            for (auto& slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
        if (!endpoint->connected() && endpoint->setupConnectionsByActive()) {
            LOG(ERROR) << "Worker: Cannot make connection for endpoint: "
                       << entry.first << ", mark it inactive";
            for (auto& slice : entry.second) failed_slice_list.push_back(slice);
            endpoint->set_active(false);
            failed_nr_polls++;
            if (context_.active() && failed_nr_polls > 32 &&
                !success_nr_polls) {
                LOG(WARNING)
                    << "Failed to establish peer endpoints on local UB device "
                    << context_.nicPath() << ", mark it inactive";
                context_.set_active(false);
            }
            entry.second.clear();
            continue;
        }
        // Set endpoint pointer for each slice before submitting
        for (auto& slice : entry.second) {
            slice->ub.endpoint = endpoint.get();
        }
        if (metrics) {
            const auto elapsed = getCurrentTimeInNano() - prepare_start;
            metrics->endpoint_prepare_ns += elapsed;
            metrics->endpoint_prepare_max_ns = std::max(
                metrics->endpoint_prepare_max_ns,
                static_cast<uint64_t>(elapsed));
        }
        const auto before = entry.second.size();
        if (auto* metrics = CurrentUbWorkerMetrics()) {
            ++metrics->post_calls;
            SpDiag::PerfPoint point(PerfKey::UB_WORKER_POST_SEND,
                                    SpDiag::PerfLevel::MODULE);
            point.Start();
            const auto start = getCurrentTimeInNano();
            const auto failed_before = metrics->post_failed_slices;
            const int rc = endpoint->submitPostSend(entry.second,
                                                    failed_slice_list);
            const auto elapsed = getCurrentTimeInNano() - start;
            metrics->post_call_ns += elapsed;
            metrics->post_call_max_ns = std::max(
                metrics->post_call_max_ns, static_cast<uint64_t>(elapsed));
            const bool failed =
                rc != 0 || metrics->post_failed_slices > failed_before;
            point.End(failed ? -1 : 0);
        } else {
            endpoint->submitPostSend(entry.second, failed_slice_list);
        }
        advanced |= entry.second.size() < before;
#endif
    }

    // Posting publishes JFC outstanding credits. Wake the owners after that
    // publication, even when they observed zero credits and went to sleep.
    if (advanced) wakePollingWorkers(thread_id);

    if (!failed_slice_list.empty()) {
        for (auto& slice : failed_slice_list) slice->ub.retry_cnt++;
        redispatch(failed_slice_list, thread_id);
    }
}

void UbWorkerPool::performPoll(int thread_id) {
    int processed_slice_count = 0;
    const static size_t kPollCount = 64;
    // context_.poll() aggregates each completion's jetty_depth here. Normal
    // successful slices are published in place; staged READ successes are
    // returned in deferred_success_slices so H2D can finish before publishing.
    std::unordered_map<volatile int*, int> jetty_depth_set;
    std::vector<UbTransport::Slice*> failed_slices;
    std::vector<UbTransport::Slice*> deferred_success_slices;
    for (int jfc_index = thread_id; jfc_index < context_.jfcCount();
         jfc_index += kTransferWorkerCount) {
        if (__atomic_load_n(context_.outstandingCount(jfc_index),
                            __ATOMIC_ACQUIRE) <= 0) {
            continue;
        }
        UbTransport::Slice* failed[kPollCount];
        int num_failed = 0;
        int nr_poll;
        if (auto* metrics = CurrentUbWorkerMetrics()) {
            ++metrics->poll_calls;
            SpDiag::PerfPoint point(PerfKey::UB_WORKER_POLL,
                                    SpDiag::PerfLevel::MODULE);
            point.Start();
            const auto start = getCurrentTimeInNano();
            nr_poll = context_.poll(kPollCount, failed, num_failed,
                                    deferred_success_slices, jetty_depth_set,
                                    jfc_index);
            metrics->poll_call_ns += getCurrentTimeInNano() - start;
            metrics->poll_errors += nr_poll < 0;
            point.End(nr_poll < 0 || num_failed > 0 ? -1 : 0);
        } else {
            nr_poll = context_.poll(kPollCount, failed, num_failed,
                                    deferred_success_slices, jetty_depth_set,
                                    jfc_index);
        }
        if (nr_poll < 0) {
            LOG(ERROR) << "Worker: Failed to poll jetty for complete";
            continue;
        }
        int num_success = nr_poll - num_failed;
        success_nr_polls += num_success;
        processed_slice_count += num_success;
        for (int i = 0; i < num_failed; ++i) {
            UbTransport::Slice* slice = failed[i];
            assert(slice);
            failed_nr_polls++;
            if (context_.active() && failed_nr_polls > 32 &&
                !success_nr_polls) {
                LOG(WARNING) << "Too many errors found on local UB device "
                             << context_.nicPath() << ", mark it inactive";
                context_.set_active(false);
            }
            slice->ub.retry_cnt++;
            if (slice->ub.retry_cnt >= slice->ub.max_retry_cnt) {
                context_.deleteEndpoint(slice->peer_nic_path);
                failed_slices.push_back(slice);
            } else {
                collective_slice_queue_[thread_id][slice->peer_nic_path]
                    .push_back(slice);
                if (auto* metrics = CurrentUbWorkerMetrics()) {
                    slice->ts = getCurrentTimeInNano();
                    ++metrics->retry_enqueues;
                }
                redispatch_counter_++;
            }
        }
        if (nr_poll)
            __sync_fetch_and_sub(context_.outstandingCount(jfc_index), nr_poll);
    }

    for (auto& entry : jetty_depth_set)
        __sync_fetch_and_sub(entry.first, entry.second);

    for (auto& slice : deferred_success_slices) {
        context_.engine().onStagedSliceSuccess(slice);
    }

    // Slices that hit max_retry: final markFailed() after all reads (and the
    // jetty depth returns above) are done. Failed slices were never published
    // by poll(), so they remained safe to deref up to this point.
    for (auto& slice : failed_slices) {
        if (slice->ub.endpoint) {
            auto ptr = static_cast<UbEndPoint*>(slice->ub.endpoint);
            context_.deleteEndpointByPtr(ptr);
        }
        if (context_.engine().isStagedSlice(slice)) {
            context_.engine().onStagedSliceFinalFailure(slice);
        } else {
            slice->markFailed();
        }
        processed_slice_count_++;
    }

    if (processed_slice_count)
        processed_slice_count_.fetch_add(processed_slice_count);
}

void UbWorkerPool::redispatch(std::vector<Transport::Slice*>& slice_list,
                              int thread_id) {
    std::unordered_map<Transport::SegmentID,
                       std::shared_ptr<Transport::SegmentDesc>>
        segment_desc_map;
    for (auto& slice : slice_list) {
        auto target_id = slice->target_id;
        if (!segment_desc_map.count(target_id)) {
            segment_desc_map[target_id] =
                context_.engine().meta()->getSegmentDescByID(target_id, true);
        }
    }
    for (auto& slice : slice_list) {
        if (slice->ub.retry_cnt >= slice->ub.max_retry_cnt) {
            slice->markFailed();
            processed_slice_count_++;
        } else {
            auto& peer_segment_desc = segment_desc_map[slice->target_id];
            int buffer_id, device_id;
            if (!peer_segment_desc ||
                UbTransport::selectDevice(
                    peer_segment_desc.get(), slice->ub.dest_addr, slice->length,
                    buffer_id, device_id, slice->ub.retry_cnt)) {
                slice->markFailed();
                processed_slice_count_++;
                continue;
            }
            auto targetSegment =
                peer_segment_desc->buffers[buffer_id].tseg[device_id];
            slice->ub.r_seg = context_.retrieveRemoteSeg(targetSegment);
            if (!slice->ub.r_seg) {
                LOG(ERROR) << "[UB] retrieveRemoteSeg failed while "
                              "redispatching target_id="
                           << slice->target_id << " buffer_id=" << buffer_id
                           << " device_id=" << device_id
                           << " dest_addr=" << slice->ub.dest_addr;
                slice->markFailed();
                processed_slice_count_++;
                continue;
            }
            if (context_.numa_affinity()) {
                const auto& peer_buf = peer_segment_desc->buffers[buffer_id];
                int data_numa = parseCpuNumaNode(peer_buf.name);
                if (peer_buf.chip_id >= 0) {
                    slice->ub.dst_chip_id = (uint8_t)peer_buf.chip_id;
                } else {
                    slice->ub.dst_chip_id = numaNodeToChipId(data_numa);
                }
                static std::atomic<uint64_t> numa_log_counter{0};
                if (VLOG_IS_ON(2) &&
                    numa_log_counter.fetch_add(1, std::memory_order_relaxed) %
                            kNumaAffinitySampleInterval ==
                        0) {
                    VLOG(2)
                        << "[numa_affinity] remote_redispatch_sample trace_id="
                        << slice->trace_id << " target_id=" << slice->target_id
                        << " opcode="
                        << (slice->opcode == Transport::TransferRequest::READ
                                ? "READ"
                                : "WRITE")
                        << " remote_data_numa=" << data_numa
                        << " dst_chip=" << (int)slice->ub.dst_chip_id
                        << " remote_name=" << peer_buf.name;
                }
            }
            auto peer_nic_path =
                MakeNicPath(peer_segment_desc->nicPathServerName(),
                            peer_segment_desc->devices[device_id].name);
            slice->peer_nic_path = peer_nic_path;
            collective_slice_queue_[thread_id][peer_nic_path].push_back(slice);
            if (auto* metrics = CurrentUbWorkerMetrics()) {
                slice->ts = getCurrentTimeInNano();
                ++metrics->retry_enqueues;
            }
        }
    }
}

bool UbWorkerPool::hasWorkerWork(int thread_id) {
    for (int shard = thread_id; shard < kShardCount;
         shard += kTransferWorkerCount) {
        if (slice_queue_count_[shard].load(std::memory_order_acquire) != 0)
            return true;
    }
    // Only this worker accesses its private retry/post queues.
    for (const auto& entry : collective_slice_queue_[thread_id]) {
        if (!entry.second.empty()) return true;
    }
    for (int jfc = thread_id; jfc < context_.jfcCount();
         jfc += kTransferWorkerCount) {
        if (__atomic_load_n(context_.outstandingCount(jfc),
                            __ATOMIC_ACQUIRE) > 0)
            return true;
    }
    return false;
}

void UbWorkerPool::wakePollingWorkers(int posting_thread_id) {
    for (int jfc = 0; jfc < context_.jfcCount(); ++jfc) {
        const int owner = jfc % kTransferWorkerCount;
        if (owner != posting_thread_id &&
            __atomic_load_n(context_.outstandingCount(jfc),
                            __ATOMIC_ACQUIRE) > 0) {
            worker_signals_[owner].notify();
        }
    }
}

void UbWorkerPool::transferWorker(int thread_id) {
    bindToSocket(numa_socket_id_);
    const bool diagnostics = UbWorkerDiagnosticsEnabled();
    const uint64_t idle_spin_ns = WorkerIdleSpinNs();
    UbWorkerMetrics metrics;
    current_worker_metrics = diagnostics ? &metrics : nullptr;
    uint64_t last_report_ns = diagnostics ? getCurrentTimeInNano() : 0;
    uint64_t last_active_ns = 0;
    uint64_t idle_since_ns = getCurrentTimeInNano();
    const auto report = [&](uint64_t now_ns, bool final) {
        size_t assigned_shards = 0, assigned_jfcs = 0;
        for (int s = thread_id; s < kShardCount; s += kTransferWorkerCount) {
            ++assigned_shards;
        }
        for (int q = thread_id; q < context_.jfcCount();
             q += kTransferWorkerCount) {
            ++assigned_jfcs;
        }
        const auto completions =
            metrics.completed_slices + metrics.failed_completions;
        const auto queue_wait_avg_us = metrics.queue_wait_count
            ? metrics.queue_wait_ns / metrics.queue_wait_count / 1000 : 0;
        const auto post_to_cq_avg_us = completions
            ? metrics.completion_wait_ns / completions / 1000 : 0;
        MC_LOG(INFO) << "ub_worker_breakdown nic=" << context_.nicPath()
                     << " scope=interval"
                     << " worker=" << thread_id
                     << " configured_workers=" << kTransferWorkerCount
                     << " assigned_shards=" << assigned_shards
                     << " assigned_jfcs=" << assigned_jfcs
                     << " interval_us=" << (now_ns - last_report_ns) / 1000
                     << " iterations=" << metrics.iterations
                     << " dequeued_slices=" << metrics.dequeued_slices
                     << " post_calls=" << metrics.post_calls
                     << " post_attempt_slices=" << metrics.post_attempt_slices
                     << " post_attempt_bytes=" << metrics.post_attempt_bytes
                     << " posted_slices=" << metrics.posted_slices
                     << " post_failed_slices=" << metrics.post_failed_slices
                     << " queue_blocked_calls=" << metrics.queue_blocked_calls
                     << " jetty_full_calls=" << metrics.jetty_full_calls
                     << " jfc_full_calls=" << metrics.jfc_full_calls
                     << " queue_wait_samples=" << metrics.queue_wait_count
                     << " queue_wait_sum_us=" << metrics.queue_wait_ns / 1000
                     << " queue_wait_avg_us=" << queue_wait_avg_us
                     << " queue_wait_max_us="
                     << metrics.queue_wait_max_ns / 1000
                     << " poll_calls=" << metrics.poll_calls
                     << " poll_errors=" << metrics.poll_errors
                     << " completed_slices=" << metrics.completed_slices
                     << " completed_bytes=" << metrics.completed_bytes
                     << " failed_completions=" << metrics.failed_completions
                     << " post_to_cq_avg_us=" << post_to_cq_avg_us
                     << " post_to_cq_sum_us="
                     << metrics.completion_wait_ns / 1000
                     << " post_to_cq_max_us="
                     << metrics.completion_wait_max_ns / 1000
                     << " retry_enqueues=" << metrics.retry_enqueues
                     << " post_call_us=" << metrics.post_call_ns / 1000
                     << " post_call_max_us=" << metrics.post_call_max_ns / 1000
                     << " poll_call_us=" << metrics.poll_call_ns / 1000
                     << " dequeue_us=" << metrics.dequeue_ns / 1000
                     << " endpoint_prepare_us="
                     << metrics.endpoint_prepare_ns / 1000
                     << " endpoint_prepare_max_us="
                     << metrics.endpoint_prepare_max_ns / 1000
                     << " active_loop_gap_max_us="
                     << metrics.active_loop_gap_max_ns / 1000
                     << " idle_wait_calls=" << metrics.idle_wait_calls
                     << " idle_wait_us=" << metrics.idle_wait_ns / 1000
                     << " pending_context_slices=" << pendingSliceCount()
                     << " final=" << final;
        metrics = {};
        last_report_ns = now_ns;
    };
    while (workers_running_.load(std::memory_order_relaxed)) {
        if (diagnostics) {
            ++metrics.iterations;
            const auto now = getCurrentTimeInNano();
            if (last_active_ns && now >= last_active_ns) {
                metrics.active_loop_gap_max_ns = std::max(
                    metrics.active_loop_gap_max_ns, now - last_active_ns);
            }
            if (now - last_report_ns >= WorkerDiagnosticIntervalNs()) {
                report(now, false);
            }
        }
        if (!hasWorkerWork(thread_id)) {
            const auto now = getCurrentTimeInNano();
            // Keep recently active workers polling so sparse foreground reads
            // do not pay a scheduler wakeup before their first post/poll.
            if (now >= idle_since_ns && now - idle_since_ns < idle_spin_ns) {
                continue;
            }
            last_active_ns = 0;
            const auto ready = [&] {
                return !workers_running_.load(std::memory_order_relaxed) ||
                       hasWorkerWork(thread_id);
            };
            if (diagnostics) {
                ++metrics.idle_wait_calls;
                const auto start = getCurrentTimeInNano();
                // Timed waits keep idle-worker interval reports available.
                worker_signals_[thread_id].waitFor(
                    ready,
                    std::chrono::nanoseconds(WorkerDiagnosticIntervalNs()));
                metrics.idle_wait_ns += getCurrentTimeInNano() - start;
            } else {
                worker_signals_[thread_id].wait(ready);
            }
            idle_since_ns = getCurrentTimeInNano();
            continue;
        }
        if (diagnostics) last_active_ns = getCurrentTimeInNano();
        performPostSend(thread_id);
#ifndef USE_FAKE_POST_SEND
        performPoll(thread_id);
#endif
        idle_since_ns = getCurrentTimeInNano();
    }
    if (diagnostics) report(getCurrentTimeInNano(), true);
    current_worker_metrics = nullptr;
}

void UbWorkerPool::monitorWorker() {
    bindToSocket(numa_socket_id_);
    auto last_reset_ts = getCurrentTimeInNano();
    while (workers_running_) {
        auto current_ts = getCurrentTimeInNano();
        if (current_ts - last_reset_ts > 1000000000ll) {
            context_.set_active(true);
            last_reset_ts = current_ts;
        }
        struct epoll_event event{};
        int num_events = epoll_wait(context_.eventFd(), &event, 1, 100);
        if (num_events < 0) {
            if (errno != EWOULDBLOCK && errno != EINTR)
                PLOG(ERROR) << "Worker: epoll_wait()";
            continue;
        }

        if (num_events == 0) continue;

        if (!(event.events & EPOLLIN)) continue;

        if (event.data.fd == context_.getAsyncFd()) doProcessContextEvents();
    }
}

int UbWorkerPool::doProcessContextEvents() {
    return context_.doProcessContextEvents();
}
}  // namespace mooncake
