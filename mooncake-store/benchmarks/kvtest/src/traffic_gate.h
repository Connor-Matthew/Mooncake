#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <random>
#include <thread>

namespace mooncake::kvtest {

// Injectable clock so pacing logic can be unit-tested without real sleeps.
class Clock {
   public:
    virtual ~Clock() = default;
    virtual int64_t NowUs() const = 0;
    virtual void SleepUntilUs(int64_t fire_us) = 0;
};

class SteadyClock final : public Clock {
   public:
    int64_t NowUs() const override {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }
    void SleepUntilUs(int64_t fire_us) override {
        std::this_thread::sleep_until(std::chrono::steady_clock::time_point(
            std::chrono::microseconds(fire_us)));
    }
};

inline int64_t SteadyNowUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

struct TrafficStats {
    uint64_t emitted{0};
    uint64_t windows{0};
    uint64_t bursts{0};
    int64_t behind_us{0};
    int64_t burst_us_max{0};
    int64_t burst_us_last{0};
    int64_t idle_us_sum{0};
    int64_t idle_us_last{0};
};

// Traffic generation shared by the pipeline writer and the basic runner.
//
// balanced: absolute grid, one slot per interval. With catch_up=true a late
//   slot is sent immediately (the schedule keeps its backlog, matching the
//   Python client). With catch_up=false missed slots are skipped, matching the
//   original RateGate used by YuanRong kvtest (rate never exceeds target).
//
// pulse: per-window burst. A window budget accumulates the fractional
//   per-thread qps, the burst then runs back-to-back, and the next call waits
//   until the window closes. Fractional rates (e.g. 52.5/s) stay exact over
//   time because the remainder carries into the next window.
class TrafficGate {
   public:
    enum class Mode { kBalanced, kPulse };

    struct Options {
        Mode mode{Mode::kBalanced};
        double qps{0.0};
        int thread_id{0};
        int thread_count{1};
        int window_ms{1000};
        bool jitter{true};
        bool catch_up{true};
        int instance_id{0};
    };

    TrafficGate() = default;
    TrafficGate(const Options &options, Clock *clock) { Configure(options, clock); }

    void Configure(const Options &options, Clock *clock) {
        options_ = options;
        clock_ = clock;
        const int threads = std::max(1, options.thread_count);
        per_thread_qps_ = options.qps > 0.0 ? options.qps / threads : 0.0;
        window_ns_ = static_cast<int64_t>(std::max(1, options.window_ms)) *
                     1000000LL;
        interval_ns_ = per_thread_qps_ > 0.0
                           ? 1000000000.0 / per_thread_qps_
                           : 0.0;
        const double lane_width_ns =
            threads > 0 ? interval_ns_ / static_cast<double>(threads) : 0.0;
        lane_offset_ns_ =
            lane_width_ns * static_cast<double>(std::max(0, options.thread_id));
        const int64_t jitter_bound =
            options.jitter && lane_width_ns >= 1.0
                ? static_cast<int64_t>(lane_width_ns) - 1
                : 0;
        jitter_dist_.reset();
        jitter_dist_.param(
            std::uniform_int_distribution<int64_t>::param_type(0,
                                                               jitter_bound));
        rng_.seed(static_cast<uint32_t>(options.instance_id * 1000 +
                                        std::max(0, options.thread_id)));
        // Stagger fractional pulse budgets so threads with a sub-1/s share do
        // not all fire in the same window.
        const int lane = threads > 0
                             ? std::max(0, options.thread_id) % threads
                             : 0;
        initial_budget_ =
            threads > 0 ? static_cast<double>(lane) / threads : 0.0;
        Reset(clock_ != nullptr ? clock_->NowUs() : 0);
    }

    // Re-anchor the schedule. Called at process start and at phase changes so
    // warmup never leaks a backlog into the measured phase.
    void Reset(int64_t now_us) {
        const int64_t now_ns = now_us * 1000;
        next_fire_ns_ = static_cast<double>(now_ns);
        next_window_ns_ = now_ns;
        window_start_ns_ = now_ns;
        window_end_ns_ = now_ns;
        burst_start_ns_ = now_ns;
        budget_ = initial_budget_;
        burst_remaining_ = 0;
        stats_ = TrafficStats{};
    }

    bool Active() const { return per_thread_qps_ > 0.0 && clock_ != nullptr; }

    const TrafficStats &Stats() const { return stats_; }

    void WaitTurn() {
        if (!Active()) return;
        if (options_.mode == Mode::kPulse) {
            WaitPulse();
        } else {
            WaitBalanced();
        }
    }

   private:
    int64_t NowUs() const { return clock_->NowUs(); }

    void WaitBalanced() {
        const int64_t now_us = NowUs();
        const int64_t jitter =
            options_.jitter ? jitter_dist_(rng_) : static_cast<int64_t>(0);
        double fire_ns = next_fire_ns_ + lane_offset_ns_ +
                         static_cast<double>(jitter);
        const double now_ns = static_cast<double>(now_us) * 1000.0;

        if (fire_ns < now_ns) {
            const int64_t behind_us =
                now_us - static_cast<int64_t>(fire_ns / 1000.0);
            if (behind_us > 0) stats_.behind_us += behind_us;
            if (!options_.catch_up) {
                // Skip missed slots until the next future slot. The jitter is
                // redrawn per skipped slot, mirroring RateGate.
                size_t guard = 0;
                do {
                    next_fire_ns_ += interval_ns_;
                    const int64_t skipped_jitter =
                        options_.jitter ? jitter_dist_(rng_)
                                        : static_cast<int64_t>(0);
                    fire_ns = next_fire_ns_ + lane_offset_ns_ +
                              static_cast<double>(skipped_jitter);
                } while (fire_ns < now_ns && ++guard < 1000000);
            }
        }

        const int64_t fire_us =
            static_cast<int64_t>((fire_ns + 999.0) / 1000.0);
        if (fire_us > now_us) clock_->SleepUntilUs(fire_us);
        next_fire_ns_ += interval_ns_;
        ++stats_.emitted;
    }

    void WaitPulse() {
        int64_t now_ns = NowUs() * 1000;
        for (;;) {
            if (burst_remaining_ <= 0) {
                if (next_window_ns_ > now_ns) {
                    clock_->SleepUntilUs((next_window_ns_ + 999) / 1000);
                    now_ns = NowUs() * 1000;
                }
                budget_ += per_thread_qps_ *
                           (static_cast<double>(window_ns_) / 1000000000.0);
                burst_remaining_ = static_cast<int>(budget_);
                budget_ -= static_cast<double>(burst_remaining_);
                window_start_ns_ = now_ns;
                window_end_ns_ = window_start_ns_ + window_ns_;
                if (burst_remaining_ > 0) {
                    burst_start_ns_ = now_ns;
                    ++stats_.windows;
                }
            }
            if (burst_remaining_ > 0) break;
            // Fractional per-thread rate below 1/s: nothing to send in this
            // window, wait for the next one and accumulate again.
            next_window_ns_ = window_end_ns_;
            clock_->SleepUntilUs((window_end_ns_ + 999) / 1000);
            now_ns = NowUs() * 1000;
        }

        --burst_remaining_;
        ++stats_.emitted;
        if (burst_remaining_ <= 0) {
            // Do NOT sleep here: the caller still has to send this last item.
            // The next WaitTurn() waits for the window to close.
            next_window_ns_ = window_end_ns_;
            const int64_t burst_us =
                std::max<int64_t>(0, now_ns - burst_start_ns_) / 1000;
            stats_.burst_us_last = burst_us;
            stats_.burst_us_max = std::max(stats_.burst_us_max, burst_us);
            const int64_t idle_us =
                std::max<int64_t>(0, window_end_ns_ - now_ns) / 1000;
            stats_.idle_us_last = idle_us;
            stats_.idle_us_sum += idle_us;
            ++stats_.bursts;
        }
    }

    Options options_{};
    Clock *clock_{nullptr};
    double per_thread_qps_{0.0};
    double interval_ns_{0.0};
    double lane_offset_ns_{0.0};
    int64_t window_ns_{1000000000};
    double next_fire_ns_{0.0};
    int64_t next_window_ns_{0};
    int64_t window_start_ns_{0};
    int64_t window_end_ns_{0};
    int64_t burst_start_ns_{0};
    double budget_{0.0};
    double initial_budget_{0.0};
    int burst_remaining_{0};
    std::mt19937 rng_{0};
    std::uniform_int_distribution<int64_t> jitter_dist_{0, 0};
    TrafficStats stats_{};
};

}  // namespace mooncake::kvtest
