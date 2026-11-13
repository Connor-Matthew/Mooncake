// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <algorithm>
#include <cstdint>

namespace mooncake {

// Per-worker interval counters, owned and updated only by that worker. No
// slice pointers are retained after a completion is published to the caller.
// Queue wait is measured to each post attempt (including retries). Post-to-CQ
// includes software polling delay, and is not pure hardware DMA latency.
struct UbWorkerMetrics {
    uint64_t iterations = 0;
    uint64_t dequeued_slices = 0;
    uint64_t post_calls = 0;
    uint64_t post_attempt_slices = 0;
    uint64_t post_attempt_bytes = 0;
    uint64_t posted_slices = 0;
    uint64_t post_failed_slices = 0;
    uint64_t queue_blocked_calls = 0;
    uint64_t jetty_full_calls = 0;
    uint64_t jfc_full_calls = 0;
    uint64_t queue_wait_count = 0;
    uint64_t queue_wait_ns = 0;
    uint64_t queue_wait_max_ns = 0;
    uint64_t poll_calls = 0;
    uint64_t poll_errors = 0;
    uint64_t completed_slices = 0;
    uint64_t completed_bytes = 0;
    uint64_t failed_completions = 0;
    uint64_t completion_wait_ns = 0;
    uint64_t completion_wait_max_ns = 0;
    uint64_t retry_enqueues = 0;
    uint64_t post_call_ns = 0;
    uint64_t poll_call_ns = 0;
    uint64_t post_call_max_ns = 0;
    uint64_t dequeue_ns = 0;
    uint64_t endpoint_prepare_ns = 0;
    uint64_t endpoint_prepare_max_ns = 0;
    uint64_t active_loop_gap_max_ns = 0;
    uint64_t idle_wait_calls = 0;
    uint64_t idle_wait_ns = 0;

    void RecordQueueWait(uint64_t queued_ns, uint64_t now_ns) {
        if (!queued_ns || now_ns < queued_ns) return;
        const auto elapsed = now_ns - queued_ns;
        ++queue_wait_count;
        queue_wait_ns += elapsed;
        queue_wait_max_ns = std::max(queue_wait_max_ns, elapsed);
    }
};

// Diagnostics are opt-in via MC_UB_WORKER_DIAG=1. A null current pointer makes
// endpoint instrumentation a cheap branch when diagnostics are disabled.
bool UbWorkerDiagnosticsEnabled();
UbWorkerMetrics* CurrentUbWorkerMetrics();

}  // namespace mooncake
