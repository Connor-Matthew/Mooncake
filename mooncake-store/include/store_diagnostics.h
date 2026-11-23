#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <array>
#include <chrono>
#include <sstream>
#include <string>

#include "mooncake_logging.h"

namespace mooncake {

// Same opt-in as transfer_trace and ub_task_trace. No clock reads or log
// formatting when disabled. Nanosecond stamps are comparable within a process.
inline uint64_t StoreTransferTraceMinBytes() {
    static const uint64_t threshold = [] {
        const char* raw = std::getenv("MC_TRANSFER_TRACE_MIN_BYTES");
        if (!raw || !*raw || *raw == '-') return uint64_t{0};
        char* end = nullptr;
        auto value = std::strtoull(raw, &end, 10);
        return *end == '\0' ? static_cast<uint64_t>(value) : uint64_t{0};
    }();
    return threshold;
}

class ScopedTransferPathTrace {
   public:
    ScopedTransferPathTrace(const char* scope, uint64_t bytes)
        : scope_(scope),
          bytes_(bytes),
          enabled_(StoreTransferTraceMinBytes() > 0 &&
                   bytes >= StoreTransferTraceMinBytes()) {
        if (enabled_) {
            trace_id_ = logging::CurrentTraceId();
            start_ = last_ = Clock::now();
        }
    }
    void mark(const char* phase) {
        if (!enabled_ || count_ == phases_.size()) return;
        auto now = Clock::now();
        phases_[count_++] = {phase, now - last_};
        last_ = now;
    }
    ~ScopedTransferPathTrace() {
        if (!enabled_) return;
        const auto end = Clock::now();
        std::ostringstream fields;
        for (size_t i = 0; i < count_; ++i) {
            fields << ' ' << phases_[i].name << "_us="
                   << std::chrono::duration<double, std::micro>(
                          phases_[i].duration)
                          .count();
        }
        MC_LOG(INFO)
            << "transfer_path_trace trace_id=" << trace_id_
            << " scope=" << scope_ << " bytes=" << bytes_ << " start_ns="
            << std::chrono::duration_cast<std::chrono::nanoseconds>(
                   start_.time_since_epoch())
                   .count()
            << " end_ns="
            << std::chrono::duration_cast<std::chrono::nanoseconds>(
                   end.time_since_epoch())
                   .count()
            << fields.str() << " tail_us="
            << std::chrono::duration<double, std::micro>(end - last_).count()
            << " total_us="
            << std::chrono::duration<double, std::micro>(end - start_).count();
    }
    ScopedTransferPathTrace(const ScopedTransferPathTrace&) = delete;
    ScopedTransferPathTrace& operator=(const ScopedTransferPathTrace&) = delete;

   private:
    using Clock = std::chrono::steady_clock;
    struct Phase {
        const char* name = nullptr;
        Clock::duration duration{};
    };
    const char* scope_;
    uint64_t bytes_, trace_id_ = 0;
    bool enabled_;
    Clock::time_point start_{}, last_{};
    std::array<Phase, 12> phases_{};
    size_t count_ = 0;
};

// Store transfer and RPC breakdown diagnostics run on request hot paths.
// Require an explicit opt-in, then apply the process-wide high-frequency
// sampling rate as a second gate.
inline bool ShouldSampleStoreTransferDiagnostics(uint64_t trace_id) {
    static const bool enabled = [] {
        const char* value = std::getenv("MC_STORE_TRANSFER_DIAG");
        if (!value) return false;

        std::string normalized(value);
        std::transform(
            normalized.begin(), normalized.end(), normalized.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return normalized == "1" || normalized == "true" ||
               normalized == "yes" || normalized == "on";
    }();
    return enabled && mooncake::logging::ShouldSampleHiFreqLog(trace_id);
}

}  // namespace mooncake
