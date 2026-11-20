#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "buffers.h"
#include "config.h"
#include "metrics.h"
#include "operations.h"
#include "store_adapter.h"

namespace mooncake::kvtest {

// Validates `config.operation` (single interface or "<put>/<get>") without
// touching a store. Used by --check-config and BasicRunner::Prepare.
bool ValidateBasicOperation(const Config &config, std::string *error);

// Single-interface benchmark: one process behaves like one Python client.
// key range -> [key_start, effective key_end); threads split contiguously and
// run groups of batch_keys_count keys (batch interfaces) or single keys.
class BasicRunner {
   public:
    struct Stats {
        uint64_t measured_ops{0};
        uint64_t measured_failures{0};
        uint64_t warmup_ops{0};
        int threads{0};
    };

    BasicRunner(const Config &config, StoreAdapter &store,
                MetricsCollector &metrics);
    ~BasicRunner();

    bool Prepare(std::string *error);
    // Runs warmup (if configured) then the measured phase. Blocks until the
    // measured phase ends (one pass, or `duration` seconds). Safe to call once.
    bool Start(std::string *error);
    void RequestStop();
    // Joins internal worker threads (Start already joins them; Stop is a
    // no-op unless a start attempt failed mid-flight).
    void Stop();

    Stats GetStats() const;

   private:
    struct ThreadState {
        OpScratch scratch;
        std::unique_ptr<RegisteredBuffer> write_buffer;
        std::unique_ptr<RegisteredBuffer> read_buffer;
    };
    struct PhaseSlice {
        const std::vector<std::string> *keys{nullptr};
        bool warmup{false};
        int64_t deadline_us{0};  // 0 = one pass
    };

    bool BuildKeys();
    bool PrepareThreadBuffers(std::string *error);
    bool RunPhase(bool warmup);
    void RunThread(int thread_id);
    void ExecuteGroup(const OpSpec &spec, const std::vector<std::string> &keys,
                      ThreadState &state, bool warmup);
    void RemoveKeys(const std::vector<std::string> &keys);

    const Config &config_;
    StoreAdapter &store_;
    MetricsCollector &metrics_;

    ValueSource value_;
    VerifyOptions verify_;
    uint64_t data_size_{0};
    bool mixed_{false};
    const OpSpec *put_spec_{nullptr};
    const OpSpec *get_spec_{nullptr};
    const OpSpec *single_spec_{nullptr};
    bool needs_prefill_{false};

    std::vector<std::string> measured_keys_;
    std::vector<std::string> warmup_keys_;
    std::vector<ThreadState> thread_states_;
    std::vector<std::thread> threads_;

    PhaseSlice phase_;
    std::atomic<bool> running_{false};
    std::atomic<bool> started_{false};
    std::atomic<uint64_t> measured_ops_{0};
    std::atomic<uint64_t> measured_failures_{0};
    std::atomic<uint64_t> warmup_ops_{0};
};

}  // namespace mooncake::kvtest
