#include "notify_sender.h"

#include <algorithm>
#include <chrono>
#include <utility>

namespace mooncake::kvtest {

int64_t NotifySender::NowUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

NotifySender::NotifySender(size_t threads, size_t queue_max,
                           Overflow overflow, bool async, SendFn send_fn,
                           RecordFn record_fn)
    : queue_max_(queue_max),
      overflow_(overflow),
      async_(async),
      send_fn_(std::move(send_fn)),
      record_fn_(std::move(record_fn)) {
    const size_t count = std::max<size_t>(1, threads);
    workers_.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        workers_.emplace_back([this] { Worker(); });
    }
}

NotifySender::~NotifySender() { Stop(); }

bool NotifySender::Dispatch(const std::vector<std::string> &targets,
                            const NotifyBatch &batch) {
    if (targets.empty()) return true;
    const uint64_t base = completed_.load(std::memory_order_acquire);
    size_t enqueued = 0;
    bool all_submitted = true;

    for (const auto &peer : targets) {
        int64_t blocked_us = 0;
        bool dropped = false;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            while (queue_max_ != 0 && queue_.size() >= queue_max_) {
                if (overflow_ == Overflow::kDrop) {
                    dropped_.fetch_add(1, std::memory_order_relaxed);
                    dropped = true;
                    all_submitted = false;
                    break;
                }
                if (stopping_) return false;
                const int64_t start = NowUs();
                cv_space_.wait(lock, [this] {
                    return queue_.size() < queue_max_ || stopping_;
                });
                if (stopping_) return false;
                blocked_us += NowUs() - start;
            }
            if (!dropped) {
                if (stopping_) return false;
                Job job;
                job.peer = peer;
                job.batch = batch;
                job.enqueued_us = NowUs();
                queue_.push_back(std::move(job));
                ++pending_;
                ++enqueued;
                submitted_.fetch_add(1, std::memory_order_relaxed);
                stats_.queue_peak =
                    std::max<uint64_t>(stats_.queue_peak, queue_.size());
            }
            if (blocked_us > 0) {
                stats_.backpressure_ms +=
                    static_cast<double>(blocked_us) / 1000.0;
            }
        }
        if (!dropped) cv_work_.notify_one();
    }

    if (!async_ && enqueued > 0) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_idle_.wait(lock, [&] {
            return completed_.load(std::memory_order_acquire) >=
                       base + enqueued ||
                   stopping_;
        });
    }
    return all_submitted;
}

void NotifySender::Drain() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_idle_.wait(lock, [this] { return pending_ == 0; });
}

void NotifySender::Stop() {
    if (stopping_.load(std::memory_order_acquire)) return;
    Drain();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) return;
        stopping_ = true;
    }
    cv_work_.notify_all();
    for (auto &worker : workers_) {
        if (worker.joinable()) worker.join();
    }
    workers_.clear();
}

NotifySender::Stats NotifySender::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    Stats stats = stats_;
    stats.submitted = submitted_.load(std::memory_order_relaxed);
    stats.ok = ok_.load(std::memory_order_relaxed);
    stats.fail = fail_.load(std::memory_order_relaxed);
    stats.dropped = dropped_.load(std::memory_order_relaxed);
    return stats;
}

void NotifySender::Worker() {
    for (;;) {
        Job job;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_work_.wait(lock, [this] {
                return stopping_ || !queue_.empty();
            });
            if (queue_.empty()) return;
            job = std::move(queue_.front());
            queue_.pop_front();
            ++active_;
            stats_.inflight_peak = std::max<uint64_t>(
                stats_.inflight_peak, active_);
            const double wait_ms =
                static_cast<double>(NowUs() - job.enqueued_us) / 1000.0;
            stats_.queue_wait_sum_ms += wait_ms;
            stats_.queue_wait_max_ms =
                std::max(stats_.queue_wait_max_ms, wait_ms);
            cv_space_.notify_one();
        }

        double latency_ms = 0.0;
        bool ok = false;
        if (send_fn_) {
            try {
                ok = send_fn_(job.peer, job.batch, &latency_ms);
            } catch (...) {
                ok = false;
            }
        }
        if (record_fn_) {
            try {
                record_fn_(ok, latency_ms);
            } catch (...) {
            }
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            --active_;
            --pending_;
            if (ok) {
                ok_.fetch_add(1, std::memory_order_relaxed);
            } else {
                fail_.fetch_add(1, std::memory_order_relaxed);
            }
            stats_.elapsed_sum_ms += latency_ms;
            stats_.elapsed_max_ms =
                std::max(stats_.elapsed_max_ms, latency_ms);
        }
        completed_.fetch_add(1, std::memory_order_release);
        cv_idle_.notify_all();
    }
}

}  // namespace mooncake::kvtest
