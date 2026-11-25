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

#include "transport/transport.h"

#include "error.h"
#include "transfer_engine.h"
#include "mooncake_logging.h"

#include <algorithm>
#include <iomanip>
#include <sstream>

namespace mooncake {

void Transport::logClassicUbTaskTrace(const TransferTask &task) {
    const auto &trace = *task.ub_trace;
    std::vector<uint64_t> device, post, cqe_handle;
    uint64_t last_complete = 0, first_post = 0;
    uint64_t retries = 0;
    size_t succeeded = 0, terminal = 0;
    uint64_t slice_size = 0;
    for (const auto &slice : trace.slices) {
        slice_size = std::max(slice_size, slice->bytes);
        const auto done = slice->terminal_ns.load(std::memory_order_acquire);
        const auto returned =
            slice->post_returned_ns.load(std::memory_order_acquire);
        const auto seen = slice->cqe_seen_ns.load(std::memory_order_acquire);
        const auto started = slice->post_ns.load(std::memory_order_relaxed);
        const auto attempts = slice->attempts.load(std::memory_order_relaxed);
        retries += attempts > 0 ? attempts - 1 : 0;
        if (started != 0)
            first_post =
                first_post == 0 ? started : std::min(first_post, started);
        last_complete = std::max(last_complete, done);
        if (done == 0) continue;
        ++terminal;
        if (!slice->success.load(std::memory_order_relaxed)) continue;
        ++succeeded;
        // A concurrent completion can precede post return. Do not clamp that
        // race to zero or mistake an unposted/failed slice for a DMA sample.
        // A missing cqe_seen stamp (slice never went through the poller)
        // likewise drops the sample instead of corrupting the split.
        if (started != 0 && returned != 0 && seen != 0 && seen >= returned &&
            returned >= started) {
            device.push_back(seen - returned);
            post.push_back(returned - started);
            if (done >= seen) cqe_handle.push_back(done - seen);
        }
    }
    auto end = trace.terminal_ns.load(std::memory_order_acquire);
    const bool task_terminal = end != 0;
    if (!task_terminal) end = last_complete;
    const bool complete =
        !trace.slices.empty() && terminal == trace.slices.size();
    const double total_us =
        complete && end >= trace.created_ns
            ? static_cast<double>(end - trace.created_ns) / 1000.0
            : -1.0;
    const auto summarize = [](const char *name, std::vector<uint64_t> samples) {
        std::ostringstream out;
        out << std::fixed << std::setprecision(3);
        out << ' ' << name << "[avg/p50/p99/max]=";
        if (samples.empty()) {
            out << "-1/-1/-1/-1";
            return out.str();
        }
        std::sort(samples.begin(), samples.end());
        double sum = 0;
        for (auto value : samples) sum += value;
        const auto rank = [&](size_t percent) {
            return static_cast<double>(
                       samples[(samples.size() * percent + 99) / 100 - 1]) /
                   1000.0;
        };
        out << sum / samples.size() / 1000.0 << '/' << rank(50) << '/'
            << rank(99) << '/' << static_cast<double>(samples.back()) / 1000.0;
        return out.str();
    };
    MC_LOG(INFO)
        << "ub_task_trace engine=classic trace_id=" << trace.trace_id
        << " batch=0x" << std::hex << task.batch_id << std::dec
        << " task=" << &task << " op=" << (trace.is_read ? "read" : "write")
        << " bytes=" << trace.bytes << " slices=" << trace.slices.size()
        << " slice_size=" << slice_size << " status="
        << (!complete               ? "incomplete"
            : succeeded == terminal ? "ok"
                                    : "failed")
        << " total_us=" << total_us << " ub_task_trace_total=" << total_us
        << " MBps=" << (total_us > 0 ? trace.bytes / total_us : -1.0)
        << " created_ns=" << trace.created_ns << " terminal_ns=" << end
        << " terminal_source=" << (task_terminal ? "task" : "last_slice")
        << " first_post_ns=" << first_post
        << " last_complete_ns=" << last_complete
        << " device_samples=" << device.size()
        << " device_samples_missing=" << trace.slices.size() - device.size()
        << " retries=" << retries << summarize("post_us", std::move(post))
        << summarize("device_us", std::move(device))
        << summarize("cqe_handle_us", std::move(cqe_handle));
}
#ifdef MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL
void Transport::releaseCongestionPermit(Slice *slice) {
    adaptive_congestion_control::complete(
        slice->rdma_congestion_control_permit,
        adaptive_congestion_control::OutcomeClass::kDerivedFlush,
        adaptive_congestion_control::FailureScope::kOperation);
}
#endif

thread_local static Transport::ThreadLocalSliceCache tl_slice_cache;

Transport::ThreadLocalSliceCache &Transport::getSliceCache() {
    return tl_slice_cache;
}

Transport::BatchID Transport::allocateBatchID(size_t batch_size) {
    auto batch_desc = new BatchDesc();
    if (!batch_desc) return ERR_MEMORY;
    batch_desc->id = BatchID(batch_desc);
    batch_desc->batch_size = batch_size;
    batch_desc->task_list.reserve(batch_size);
    batch_desc->context = NULL;
#ifdef CONFIG_USE_BATCH_DESC_SET
    batch_desc_lock_.lock();
    batch_desc_set_[batch_desc->id] = batch_desc;
    batch_desc_lock_.unlock();
#endif
    return batch_desc->id;
}

Status Transport::freeBatchID(BatchID batch_id) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        if (!batch_desc.task_list[task_id].is_finished) {
            LOG(ERROR) << "BatchID cannot be freed until all tasks are done";
            return Status::BatchBusy(
                "BatchID cannot be freed until all tasks are done");
        }
    }
    // Terminal tasks may still have completion callbacks unwinding while
    // holding a pin on this BatchDesc; wait for the pin count to drain.
    if (batch_desc.active_completion_callbacks.load(
            std::memory_order_acquire) != 0) {
        return Status::BatchBusy(
            "BatchID cannot be freed until completion callbacks are quiescent");
    }
    delete &batch_desc;
#ifdef CONFIG_USE_BATCH_DESC_SET
    RWSpinlock::WriteGuard guard(batch_desc_lock_);
    batch_desc_set_.erase(batch_id);
#endif
    return Status::OK();
}

int Transport::install(std::string &local_server_name,
                       std::shared_ptr<TransferMetadata> meta,
                       std::shared_ptr<Topology> topo) {
    local_server_name_ = local_server_name;
    metadata_ = meta;
    return 0;
}
}  // namespace mooncake
