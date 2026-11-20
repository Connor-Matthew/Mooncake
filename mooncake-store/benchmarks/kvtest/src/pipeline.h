#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "buffers.h"
#include "config.h"
#include "metrics.h"
#include "notify_codec.h"
#include "notify_sender.h"
#include "operations.h"
#include "store_adapter.h"
#include "thread_pool.h"

namespace mooncake::kvtest {

// Pipeline writer: warmup -> measured -> drain, with duration-bounded phases,
// key-space wrap (delete-before-write for put-family writers) and notification
// fan-out through a NotifySender. Start() blocks until the measured phase ends
// (one duration window or /stop).
class PipelineWriter {
   public:
    struct Stats {
        uint64_t measured_ops{0};
        uint64_t measured_failures{0};
        uint64_t warmup_ops{0};
        uint64_t maintenance_ops{0};
        uint64_t notifications{0};
        uint64_t notify_failures{0};
        std::string phase;
    };

    PipelineWriter(const Config &config, StoreAdapter &store,
                   MetricsCollector &metrics, NotifySender *sender);
    ~PipelineWriter();

    bool Prepare(std::string *error);
    bool Start(std::string *error);
    void RequestStop();
    void Stop();
    Stats GetStats() const;

   private:
    struct ThreadState {
        OpScratch scratch;
        std::unique_ptr<RegisteredBuffer> buffer;
        std::unique_ptr<RegisteredBuffer> read_buffer;
    };

    enum class PhaseState { kIdle = 0, kWarming = 1, kMeasuring = 2, kDraining = 3, kStopped = 4 };

    bool PrepareOperations(std::string *error);
    bool PrepareBuffers(std::string *error);
    void RunPhase(bool warmup, int64_t deadline_us);
    void Worker(int thread_id, bool warmup, int64_t deadline_us);
    uint64_t KeyIndex(uint64_t sequence, int thread_id, int key_index) const;
    std::vector<std::string> KeysFor(uint64_t sequence, int thread_id,
                                     bool warmup) const;
    uint64_t SizeFor(uint64_t sequence, int thread_id, std::mt19937 &rng) const;
    bool DeleteBeforeWrite(uint64_t sequence) const;
    void Notify(const NotifyBatch &batch, std::mt19937 &rng);
    std::string PhaseName() const;

    const Config &config_;
    StoreAdapter &store_;
    MetricsCollector &metrics_;
    NotifySender *sender_;

    ValueSource value_;
    VerifyOptions verify_;
    std::vector<const OpSpec *> ops_;
    std::vector<std::string> peers_;
    uint64_t max_size_{0};
    std::vector<ThreadState> states_;
    std::vector<std::thread> workers_;

    std::atomic<bool> running_{false};
    std::atomic<bool> started_{false};
    std::atomic<int> phase_state_{0};
    std::atomic<uint64_t> measured_ops_{0};
    std::atomic<uint64_t> measured_failures_{0};
    std::atomic<uint64_t> warmup_ops_{0};
    std::atomic<uint64_t> maintenance_ops_{0};
    std::atomic<uint64_t> notifications_{0};
    std::atomic<uint64_t> notify_failures_{0};
};

// Pipeline reader: executes the configured notify_pipeline for one batch.
// `exist`/`batch_exist` steps are untimed pre-checks; if any key is missing the
// data operation is skipped and the batch is recorded as MISS.
class PipelineReader {
   public:
    struct Stats {
        uint64_t batches{0};
        uint64_t warmup_batches{0};
        uint64_t precheck_misses{0};
    };

    PipelineReader(const Config &config, StoreAdapter &store,
                   MetricsCollector &metrics);
    ~PipelineReader();

    bool Prepare(std::string *error);
    void Execute(const NotifyBatch &batch);
    Stats GetStats() const;

   private:
    struct BufferSlot {
        std::unique_ptr<RegisteredBuffer> buffer;
        bool in_use{false};
    };

    bool Precheck(const NotifyBatch &batch);
    static OpScratch &Scratch();
    RegisteredBuffer *AcquireBuffer();
    void ReleaseBuffer(RegisteredBuffer *buffer);

    const Config &config_;
    StoreAdapter &store_;
    MetricsCollector &metrics_;
    VerifyOptions verify_;
    const OpSpec *data_op_{nullptr};
    std::vector<const OpSpec *> prechecks_;
    std::mutex buffer_mutex_;
    std::condition_variable buffer_cv_;
    std::vector<BufferSlot> buffers_;
    std::atomic<uint64_t> batches_{0};
    std::atomic<uint64_t> warmup_batches_{0};
    std::atomic<uint64_t> precheck_misses_{0};
};

// Bounded reader queue: HTTP handlers only decode + enqueue; worker threads
// record notify_queue_wait and run the reader pipeline. `notify_overflow`
// selects between blocking (backpressure) and dropping.
class PipelineDispatcher {
   public:
    PipelineDispatcher(const Config &config, PipelineReader &reader,
                       MetricsCollector &metrics);
    ~PipelineDispatcher();

    bool Dispatch(NotifyBatch batch);
    bool Drain(std::chrono::milliseconds timeout);
    size_t QueueSize() const { return pool_.QueueSize(); }
    uint64_t Dropped() const {
        return dropped_.load(std::memory_order_relaxed);
    }
    void StopNow();
    void Stop();

   private:
    const Config &config_;
    PipelineReader &reader_;
    MetricsCollector &metrics_;
    ThreadPool pool_;
    std::atomic<uint64_t> dropped_{0};
    std::atomic<bool> stopping_{false};
};

}  // namespace mooncake::kvtest
