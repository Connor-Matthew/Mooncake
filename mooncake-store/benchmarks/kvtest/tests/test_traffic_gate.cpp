#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "traffic_gate.h"

namespace mooncake::kvtest {
namespace {

class FakeClock final : public Clock {
   public:
    int64_t now_us{0};
    int64_t NowUs() const override { return now_us; }
    void SleepUntilUs(int64_t fire_us) override {
        if (fire_us > now_us) now_us = fire_us;
    }
};

TrafficGate::Options BalancedOptions(double qps, int thread_id,
                                     int thread_count, bool catch_up) {
    TrafficGate::Options options;
    options.mode = TrafficGate::Mode::kBalanced;
    options.qps = qps;
    options.thread_id = thread_id;
    options.thread_count = thread_count;
    options.jitter = false;
    options.catch_up = catch_up;
    options.instance_id = 1;
    return options;
}

TrafficGate::Options PulseOptions(double qps, int thread_id = 0,
                                  int thread_count = 1) {
    TrafficGate::Options options;
    options.mode = TrafficGate::Mode::kPulse;
    options.qps = qps;
    options.thread_id = thread_id;
    options.thread_count = thread_count;
    options.window_ms = 1000;
    options.jitter = false;
    options.catch_up = true;
    options.instance_id = 1;
    return options;
}

}  // namespace

TEST(TrafficGateTest, BalancedConstantRate) {
    FakeClock clock;
    TrafficGate gate(BalancedOptions(100.0, 0, 1, true), &clock);
    gate.Reset(0);

    int fired = 0;
    while (clock.now_us < 5000000) {
        gate.WaitTurn();
        ++fired;
    }
    EXPECT_GE(fired, 495);
    EXPECT_LE(fired, 505);
}

TEST(TrafficGateTest, BalancedCatchUpBeatsSkip) {
    auto emitted_in_two_seconds = [](bool catch_up) {
        FakeClock clock;
        TrafficGate gate(BalancedOptions(100.0, 0, 1, catch_up), &clock);
        gate.Reset(0);
        int fired = 0;
        while (clock.now_us < 2000000) {
            gate.WaitTurn();
            ++fired;
            // Simulate a slow operation every 10 calls.
            if (fired % 10 == 0) clock.now_us += 50000;
        }
        return fired;
    };
    const int caught_up = emitted_in_two_seconds(true);
    const int skipped = emitted_in_two_seconds(false);
    EXPECT_GT(caught_up, skipped);
}

TEST(TrafficGateTest, BalancedRecordsBehindTime) {
    FakeClock clock;
    TrafficGate gate(BalancedOptions(100.0, 0, 1, true), &clock);
    gate.Reset(0);
    for (int i = 0; i < 20; ++i) {
        gate.WaitTurn();
        if (i == 5) clock.now_us += 300000;  // 300ms stall
    }
    EXPECT_GT(gate.Stats().behind_us, 0);
}

TEST(TrafficGateTest, PulseBurstsPerWindow) {
    FakeClock clock;
    TrafficGate gate(PulseOptions(100.0), &clock);
    gate.Reset(0);

    std::vector<int> per_window(5, 0);
    while (clock.now_us < 5000000) {
        gate.WaitTurn();
        const int window = static_cast<int>(clock.now_us / 1000000);
        if (window < 5) ++per_window[window];
        clock.now_us += 100;  // simulated service time
    }

    for (int window = 0; window < 4; ++window) {
        EXPECT_EQ(per_window[window], 100) << "window=" << window;
    }
    EXPECT_EQ(gate.Stats().bursts, 5u);
    EXPECT_GT(gate.Stats().burst_us_max, 0);
}

TEST(TrafficGateTest, PulseFractionalRate) {
    FakeClock clock;
    TrafficGate gate(PulseOptions(0.5), &clock);
    gate.Reset(0);

    int fired = 0;
    while (clock.now_us < 20000000) {
        gate.WaitTurn();
        // WaitTurn may cross the caller's deadline while waiting for the next
        // window; the phase runner must re-check after it returns.
        if (clock.now_us >= 20000000) break;
        ++fired;
        clock.now_us += 100;
    }
    EXPECT_EQ(fired, 10);
}

TEST(TrafficGateTest, PulseTwoThreadsShareQps) {
    FakeClock clock_a;
    FakeClock clock_b;
    TrafficGate gate_a(PulseOptions(105.0, 0, 2), &clock_a);
    TrafficGate gate_b(PulseOptions(105.0, 1, 2), &clock_b);
    gate_a.Reset(0);
    gate_b.Reset(0);

    int fired_a = 0;
    int fired_b = 0;
    while (clock_a.now_us < 4000000) {
        gate_a.WaitTurn();
        ++fired_a;
        clock_a.now_us += 100;
    }
    while (clock_b.now_us < 4000000) {
        gate_b.WaitTurn();
        ++fired_b;
        clock_b.now_us += 100;
    }
    EXPECT_NEAR(fired_a + fired_b, 420, 6);
}

TEST(TrafficGateTest, ZeroQpsRunsUnlimited) {
    FakeClock clock;
    TrafficGate gate(BalancedOptions(0.0, 0, 1, true), &clock);
    gate.Reset(0);
    EXPECT_FALSE(gate.Active());
    gate.WaitTurn();
    EXPECT_EQ(clock.now_us, 0);
}




TEST(TrafficGateTest, PulseWindowShorterThanOneSecondScalesBudget) {
    FakeClock clock;
    auto options = PulseOptions(100.0);
    options.window_ms = 100;
    TrafficGate gate(options, &clock);
    gate.Reset(0);

    int fired = 0;
    while (clock.now_us < 1000000) {
        gate.WaitTurn();
        if (clock.now_us >= 1000000) break;
        ++fired;
        clock.now_us += 100;
    }
    // 10 windows x 100/s x 0.1s = 100 emissions, not 100 per window.
    EXPECT_NEAR(fired, 100, 2);
}

}  // namespace mooncake::kvtest
