#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "notify_codec.h"

namespace mooncake::kvtest {

// Bounded, fixed-worker notification dispatcher shared by the pipeline writer.
//
// - async=false: Dispatch() returns only after every target of the batch has
//   been sent (or rejected); async=true: Dispatch() only enqueues.
// - overflow=block: a full queue blocks the writer thread (backpressure),
//   never drops; overflow=drop: reject and count the drop.
// - Queue capacity counts queued jobs, not active sends (Python semantics).
class NotifySender {
   public:
    enum class Overflow { kBlock, kDrop };

    struct Stats {
        uint64_t submitted{0};
        uint64_t ok{0};
        uint64_t fail{0};
        uint64_t dropped{0};
        uint64_t inflight_peak{0};
        uint64_t queue_peak{0};
        double backpressure_ms{0.0};
        double queue_wait_sum_ms{0.0};
        double queue_wait_max_ms{0.0};
        double elapsed_sum_ms{0.0};
        double elapsed_max_ms{0.0};
        double drain_ms{0.0};
    };

    using SendFn = std::function<bool(const std::string &peer,
                                      const NotifyBatch &batch,
                                      double *latency_ms)>;
    using RecordFn =
        std::function<void(bool ok, double latency_ms)>;

    NotifySender(size_t threads, size_t queue_max, Overflow overflow,
                 bool async, SendFn send_fn, RecordFn record_fn);
    ~NotifySender();

    NotifySender(const NotifySender &) = delete;
    NotifySender &operator=(const NotifySender &) = delete;

    // Returns false when at least one target was dropped or the sender is
    // stopping. Targets are attempted in order.
    bool Dispatch(const std::vector<std::string> &targets,
                  const NotifyBatch &batch);

    // Wait until every submitted job (queue + in-flight) completes.
    void Drain();

    void Stop();

    Stats GetStats() const;
    uint64_t Submitted() const {
        return submitted_.load(std::memory_order_relaxed);
    }

   private:
    struct Job {
        std::string peer;
        NotifyBatch batch;
        int64_t enqueued_us{0};
    };

    void Worker();
    static int64_t NowUs();

    const size_t queue_max_;
    const Overflow overflow_;
    const bool async_;
    SendFn send_fn_;
    RecordFn record_fn_;

    mutable std::mutex mutex_;
    std::condition_variable cv_work_;
    std::condition_variable cv_space_;
    std::condition_variable cv_idle_;
    std::deque<Job> queue_;
    size_t active_{0};
    size_t pending_{0};
    std::atomic<bool> stopping_{false};
    Stats stats_;
    std::vector<std::thread> workers_;

    std::atomic<uint64_t> submitted_{0};
    std::atomic<uint64_t> ok_{0};
    std::atomic<uint64_t> fail_{0};
    std::atomic<uint64_t> dropped_{0};
    std::atomic<uint64_t> completed_{0};
};

}  // namespace mooncake::kvtest
