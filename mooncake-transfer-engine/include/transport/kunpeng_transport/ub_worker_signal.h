// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <utility>

namespace mooncake {

// One waiter per signal. Publish work before notify(); checking the predicate
// under this same mutex closes the gap between finding no work and sleeping.
class UbWorkerSignal {
   public:
    void notify() {
        std::lock_guard<std::mutex> lock(mutex_);
        cv_.notify_one();
    }

    template <typename Predicate>
    void wait(Predicate ready) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, std::move(ready));
    }

    template <typename Predicate>
    void waitFor(Predicate ready, std::chrono::nanoseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, timeout, std::move(ready));
    }

   private:
    std::mutex mutex_;
    std::condition_variable cv_;
};

}  // namespace mooncake
