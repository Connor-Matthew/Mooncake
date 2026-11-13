// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.
// Standalone, hardware-free regression checks for the UB wakeup protocol.
// Build with C++20, -pthread and -I mooncake-transfer-engine/include.
#include "transport/kunpeng_transport/ub_worker_signal.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <iostream>
#include <latch>
#include <thread>

using mooncake::UbWorkerSignal;
using namespace std::chrono_literals;

void NotifyBeforeWait() {
    UbWorkerSignal signal;
    std::atomic<bool> work{true};
    signal.notify();
    signal.wait([&] { return work.load(); });
}

void PublishWhileEnteringWait() {
    // Force the precise race: the waiter has read "no work", the producer
    // publishes, and notify must not slip between that read and cv.wait().
    for (int i = 0; i < 100; ++i) {
        UbWorkerSignal signal;
        std::atomic<bool> work{false};
        std::latch checked_empty(1), published(1);
        std::promise<void> finished;
        auto done = finished.get_future();
        std::thread consumer([&] {
            bool first_check = true;
            signal.wait([&] {
                if (first_check) {
                    first_check = false;
                    checked_empty.count_down();
                    published.wait();
                    return false;
                }
                return work.load();
            });
            finished.set_value();
        });
        checked_empty.wait();
        work.store(true);
        published.count_down();
        signal.notify();
        assert(done.wait_for(2s) == std::future_status::ready);
        consumer.join();
    }
}

void SeparateOwnersAndShutdown() {
    UbWorkerSignal poster, poller, idle;
    std::atomic<bool> queued{false}, outstanding{false}, stopped{false};
    std::latch waiting(3);
    std::promise<void> posted, completed;
    auto posted_future = posted.get_future();
    auto completed_future = completed.get_future();
    std::atomic<unsigned> idle_checks{0};
    std::thread poster_thread([&] {
        waiting.count_down();
        poster.wait([&] { return stopped.load() || queued.load(); });
        outstanding.store(true);
        poller.notify();
        posted.set_value();
    });
    std::thread poller_thread([&] {
        waiting.count_down();
        poller.wait([&] { return stopped.load() || outstanding.load(); });
        completed.set_value();
    });
    std::thread idle_thread([&] {
        waiting.count_down();
        idle.wait([&] {
            ++idle_checks;
            return stopped.load();
        });
    });
    waiting.wait();
    queued.store(true);
    poster.notify();
    assert(posted_future.wait_for(2s) == std::future_status::ready);
    assert(completed_future.wait_for(2s) == std::future_status::ready);
    stopped.store(true);
    poster.notify();
    poller.notify();
    idle.notify();
    poster_thread.join();
    poller_thread.join();
    idle_thread.join();
    assert(idle_checks.load() > 0);
}

void DiagnosticTimeout() {
    UbWorkerSignal signal;
    signal.waitFor([] { return false; }, 1ms);
}

int main() {
    NotifyBeforeWait();
    PublishWhileEnteringWait();
    SeparateOwnersAndShutdown();
    DiagnosticTimeout();
    std::cout << "UB worker signals: 4 checks passed\n";
}
