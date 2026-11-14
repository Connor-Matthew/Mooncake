#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <sys/syscall.h>
#include <unistd.h>

// Copied beside the optional, build-local coro_rpc client header. This state is
// owned by one sampled Mooncake invocation and passed explicitly across awaits;
// it must never be saved in the background recv coroutine or in thread locals.
#define MOONCAKE_RPC_SUBMIT_TRACE_VERSION 1

namespace coro_rpc {

struct mooncake_submit_trace_t {
    using Clock = std::chrono::steady_clock;
    enum Stage : size_t {
        Begin,
        HandlerLockBegin,
        HandlerLockAcquired,
        HandlerRegistered,
        SerializeBegin,
        Serialized,
        WriteLockBegin,
        WriteLockAcquired,
        WriteReady,
        WriteStarted,
        WriteCallback,
        WriteDone,
        SendReturned,
        RecvBegin,
        RecvDone,
        Done,
        Count
    };

    std::array<Clock::time_point, Count> times{};
    uint32_t reached = 0;
    uint32_t request_id = 0;
    uint64_t write_lock_retries = 0;
    size_t request_bytes = 0;
    size_t written_bytes = 0;
    int write_error = 0;
    bool recv_started = false;
    uint64_t write_begin_tid = 0;
    uint64_t write_callback_tid = 0;
    uint64_t write_resume_tid = 0;

    void Mark(Stage stage, Clock::time_point now = Clock::now()) {
        times[stage] = now;
        reached |= uint32_t{1} << stage;
    }

    bool Has(Stage stage) const { return reached & (uint32_t{1} << stage); }

    int64_t Us(Stage begin, Stage end) const {
        if (!Has(begin) || !Has(end)) return -1;
        return std::chrono::duration_cast<std::chrono::microseconds>(
                   times[end] - times[begin])
            .count();
    }

    int64_t Ns(Stage stage) const {
        if (!Has(stage)) return -1;
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   times[stage].time_since_epoch())
            .count();
    }

    static uint64_t Tid() {
        return static_cast<uint64_t>(::syscall(SYS_gettid));
    }
};

inline std::ostream& operator<<(std::ostream& out,
                                const mooncake_submit_trace_t& trace) {
    using T = mooncake_submit_trace_t;
    // Adjacent intervals partition B (apart from the Mooncake wrapper before
    // Begin). -1 means a boundary was not reached, not a zero-duration stage.
    // WriteCallback is available only for the TCP path. Its timestamp is when
    // Asio invokes the callback, NOT when the kernel finished transmitting.
    constexpr const char* names[] = {
        "b_handler_prepare_us", "b_handler_lock_us", "b_handler_insert_us",
        "b_timer_setup_us",     "b_serialize_us",    "b_send_setup_us",
        "b_write_lock_us",      "b_send_slot_us",    "b_write_dispatch_us",
        "b_write_callback_us",  "b_write_resume_us", "b_send_finish_us",
        "b_recv_setup_us",      "b_recv_start_us",   "b_return_us"};
    static_assert(sizeof(names) / sizeof(*names) == T::Count - 1);
    out << " b_diag=submit_v1 b_stage_mask=" << trace.reached
        << " b_complete=" << (trace.reached == (uint32_t{1} << T::Count) - 1)
        << " b_total_us=" << trace.Us(T::Begin, T::Done);
    for (size_t i = 0; i < T::Count - 1; ++i) {
        out << ' ' << names[i] << '='
            << trace.Us(static_cast<T::Stage>(i), static_cast<T::Stage>(i + 1));
    }
    return out << " b_async_write_us=" << trace.Us(T::WriteReady, T::WriteDone)
               << " b_write_lock_retries=" << trace.write_lock_retries
               << " b_request_id=" << trace.request_id
               << " b_request_bytes=" << trace.request_bytes
               << " b_written_bytes=" << trace.written_bytes
               << " b_write_error=" << trace.write_error
               << " b_recv_started=" << trace.recv_started
               << " b_write_begin_tid=" << trace.write_begin_tid
               << " b_write_callback_tid=" << trace.write_callback_tid
               << " b_write_resume_tid=" << trace.write_resume_tid
               << " b_begin_steady_ns=" << trace.Ns(T::Begin)
               << " b_write_begin_steady_ns=" << trace.Ns(T::WriteStarted)
               << " b_write_callback_steady_ns=" << trace.Ns(T::WriteCallback)
               << " b_write_resume_steady_ns=" << trace.Ns(T::WriteDone)
               << " b_end_steady_ns=" << trace.Ns(T::Done);
}

}  // namespace coro_rpc
