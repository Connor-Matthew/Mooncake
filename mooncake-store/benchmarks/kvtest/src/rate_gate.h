#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <random>

namespace mooncake::kvtest {

inline int64_t SteadyNowUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

class RateGate {
   public:
    void Configure(int target_qps, int thread_id, int thread_count,
                   bool jitter) {
        if (target_qps <= 0) {
            interval_us_ = 0;
            return;
        }
        const int threads = std::max(1, thread_count);
        const int base = target_qps / threads;
        const int remainder = target_qps % threads;
        const int thread_qps = base + (thread_id < remainder ? 1 : 0);
        interval_us_ = thread_qps > 0
                           ? std::max<int64_t>(1, 1000000LL / thread_qps)
                           : 1000000000LL;
        const int64_t lane_width =
            std::max<int64_t>(1, interval_us_ / threads);
        lane_offset_us_ = lane_width * thread_id;
        jitter_ = std::uniform_int_distribution<int64_t>(
            0, jitter ? lane_width - 1 : 0);
    }

    void Reset(int64_t now_us) { next_slot_us_ = now_us; }
    bool Active() const { return interval_us_ > 0; }

    int64_t Next(int64_t now_us, std::mt19937 &rng) {
        if (!Active()) return now_us;
        for (;;) {
            const int64_t fire = next_slot_us_ + lane_offset_us_ + jitter_(rng);
            if (fire > now_us) return fire;
            next_slot_us_ += interval_us_;
        }
    }

    void Advance() {
        if (Active()) next_slot_us_ += interval_us_;
    }

   private:
    int64_t interval_us_{0};
    int64_t lane_offset_us_{0};
    int64_t next_slot_us_{0};
    std::uniform_int_distribution<int64_t> jitter_{0, 0};
};

}  // namespace mooncake::kvtest
