#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake::kvtest {

class ThreadPool {
   public:
    ThreadPool(size_t thread_count, size_t max_queue)
        : max_queue_(max_queue) {
        workers_.reserve(thread_count);
        for (size_t i = 0; i < thread_count; ++i) {
            workers_.emplace_back([this] { WorkerLoop(); });
        }
    }

    ~ThreadPool() { Stop(); }

    ThreadPool(const ThreadPool &) = delete;
    ThreadPool &operator=(const ThreadPool &) = delete;

    bool Submit(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopped_) return false;
            if (max_queue_ != 0 && tasks_.size() >= max_queue_) {
                dropped_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            tasks_.push(std::move(task));
        }
        cv_.notify_one();
        return true;
    }

    size_t QueueSize() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return tasks_.size();
    }

    uint64_t DroppedCount() const {
        return dropped_.load(std::memory_order_relaxed);
    }

    // Wait until the queue is empty and every running task finished. Used to
    // drain reader queues on /stop without dropping accepted notifications.
    bool WaitIdleFor(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return idle_cv_.wait_for(lock, timeout, [this] {
            return tasks_.empty() && active_ == 0;
        });
    }

    // Upstream-compatible name used by the YLT RPC writer path.
    bool WaitIdle(std::chrono::milliseconds timeout) {
        return WaitIdleFor(timeout);
    }

    void StopNow() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopped_ = true;
            std::queue<std::function<void()>> empty;
            tasks_.swap(empty);
        }
        cv_.notify_all();
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopped_ = true;
        }
        cv_.notify_all();
        for (auto &worker : workers_) {
            if (worker.joinable()) worker.join();
        }
        workers_.clear();
    }

   private:
    void WorkerLoop() {
        for (;;) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return stopped_ || !tasks_.empty(); });
                if (stopped_) return;
                task = std::move(tasks_.front());
                tasks_.pop();
                ++active_;
            }
            try {
                task();
            } catch (...) {
                task_failures_.fetch_add(1, std::memory_order_relaxed);
            }
            {
                std::lock_guard<std::mutex> lock(mutex_);
                --active_;
            }
            idle_cv_.notify_all();
        }
    }

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable idle_cv_;
    std::queue<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
    size_t max_queue_;
    size_t active_{0};
    std::atomic<uint64_t> dropped_{0};
    std::atomic<uint64_t> task_failures_{0};
    bool stopped_{false};
};

}  // namespace mooncake::kvtest
