#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "notify_sender.h"

namespace mooncake::kvtest {
namespace {

bool WaitFor(const std::atomic<bool> &flag, int timeout_ms = 2000) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!flag.load()) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

NotifyBatch Batch() {
    NotifyBatch batch;
    batch.keys = {"k"};
    batch.sender = 1;
    batch.size = 64;
    return batch;
}

}  // namespace

TEST(NotifySenderTest, SyncDispatchCompletesAllTargets) {
    std::atomic<int> sent{0};
    std::atomic<int> recorded{0};
    NotifySender sender(
        2, 16, NotifySender::Overflow::kDrop, false,
        [&](const std::string &, const NotifyBatch &, double *latency) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if (latency) *latency = 1.0;
            ++sent;
            return true;
        },
        [&](bool, double) { ++recorded; });

    ASSERT_TRUE(sender.Dispatch({"peer1", "peer2"}, Batch()));
    EXPECT_EQ(sent.load(), 2);
    EXPECT_EQ(recorded.load(), 2);
    const auto stats = sender.GetStats();
    EXPECT_EQ(stats.submitted, 2u);
    EXPECT_EQ(stats.ok, 2u);
    EXPECT_EQ(stats.fail, 0u);
}

TEST(NotifySenderTest, AsyncDispatchDrains) {
    std::atomic<bool> release{false};
    NotifySender sender(
        2, 16, NotifySender::Overflow::kDrop, true,
        [&](const std::string &, const NotifyBatch &, double *) {
            while (!release.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return true;
        },
        nullptr);
    ASSERT_TRUE(sender.Dispatch({"p1", "p2", "p3"}, Batch()));
    EXPECT_LT(sender.GetStats().ok, 3u);
    release.store(true);
    sender.Drain();
    EXPECT_EQ(sender.GetStats().ok, 3u);
}

TEST(NotifySenderTest, DropWhenQueueFull) {
    std::atomic<bool> started{false};
    std::atomic<bool> release{false};
    NotifySender sender(
        1, 1, NotifySender::Overflow::kDrop, true,
        [&](const std::string &, const NotifyBatch &, double *) {
            started.store(true);
            while (!release.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return true;
        },
        nullptr);
    ASSERT_TRUE(sender.Dispatch({"p1"}, Batch()));
    ASSERT_TRUE(WaitFor(started));
    ASSERT_TRUE(sender.Dispatch({"p2"}, Batch()));
    EXPECT_FALSE(sender.Dispatch({"p3"}, Batch()));
    EXPECT_GE(sender.GetStats().dropped, 1u);
    release.store(true);
    sender.Drain();
}

TEST(NotifySenderTest, BlockAppliesBackpressure) {
    std::atomic<bool> started{false};
    std::atomic<bool> release{false};
    std::atomic<bool> third_done{false};
    NotifySender sender(
        1, 1, NotifySender::Overflow::kBlock, true,
        [&](const std::string &, const NotifyBatch &, double *) {
            started.store(true);
            while (!release.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return true;
        },
        nullptr);
    ASSERT_TRUE(sender.Dispatch({"p1"}, Batch()));
    ASSERT_TRUE(WaitFor(started));
    ASSERT_TRUE(sender.Dispatch({"p2"}, Batch()));
    std::thread third([&] {
        sender.Dispatch({"p3"}, Batch());
        third_done.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_FALSE(third_done.load());
    release.store(true);
    third.join();
    EXPECT_TRUE(third_done.load());
    sender.Drain();
    EXPECT_GT(sender.GetStats().backpressure_ms, 0.0);
}

}  // namespace mooncake::kvtest
