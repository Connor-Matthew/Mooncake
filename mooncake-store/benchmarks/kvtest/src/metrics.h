#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mooncake::kvtest {

enum class RecordStatus : uint8_t {
    kOk = 0,
    kMiss,
    kMismatch,
    kError,
    kWarmupSkip,
};

const char *StatusName(RecordStatus status);

struct MetricSnapshot {
    std::string name;
    uint64_t total{0};
    uint64_t success{0};
    uint64_t failure{0};
    uint64_t bytes{0};
    uint64_t miss{0};
    uint64_t mismatch{0};
    uint64_t error{0};
};

class MetricsCollector {
   public:
    // samples_max caps retained latency samples per operation using streaming
    // decimation (keep every 2nd sample when the cap is hit). 0 = unlimited.
    MetricsCollector(int instance_id, int interval_ms, std::string output_dir,
                     uint64_t samples_max = 0);
    ~MetricsCollector();

    void Start();
    void Reset();
    void ResetPaused();
    void BeginMeasurement();
    void Freeze();
    void Stop();
    // Backwards-compatible two-state recording (used by the legacy pipeline
    // path): failure is reported as kError.
    void Record(const std::string &operation, double latency_ms, bool success,
                uint64_t bytes);
    void Record(const std::string &operation, double latency_ms,
                RecordStatus status, uint64_t bytes, int32_t code = 0,
                bool warmup = false);
    void RecordVerifyFailure();
    std::vector<MetricSnapshot> SnapshotCounts() const;
    std::string StatsJson() const;
    void WriteSummary();
    // Extra top-level JSON members (e.g. "\"mode\":\"basic\",\"threads\":4")
    // merged into /stats and summary.json. Used for scenario metadata without
    // teaching this header about JSON libraries.
    void SetSummaryExtra(std::string extra);
    double ElapsedSeconds() const;
    uint64_t VerifyFailures() const {
        return verify_failures_.load(std::memory_order_relaxed);
    }

   private:
    struct RequestSample {
        int64_t elapsed_us{0};
        double latency_ms{0};
        RecordStatus status{RecordStatus::kOk};
        uint64_t bytes{0};
        int32_t code{0};
    };

    struct OperationMetrics {
        std::atomic<uint64_t> total{0};
        std::atomic<uint64_t> success{0};
        std::atomic<uint64_t> failure{0};
        std::atomic<uint64_t> bytes{0};
        std::atomic<uint64_t> miss{0};
        std::atomic<uint64_t> mismatch{0};
        std::atomic<uint64_t> error{0};
        std::atomic<uint64_t> warmup{0};
        mutable std::mutex latency_mutex;
        // Continuous sum of successful interface latencies (not sampled), used
        // for MCP-style busy throughput.
        double success_latency_ms_sum{0};
        // One in-memory row per measured operation. The request path never
        // writes a file; Stop() emits the complete sample set in one batch.
        std::vector<RequestSample> samples;
        // Samples since the last window flush (metrics_<id>.csv).
        std::vector<RequestSample> window_samples;
        uint64_t stride{1};
    };

    std::shared_ptr<OperationMetrics> GetOrCreate(const std::string &operation);
    void WriteSamples();
    void MaybeDecimate(OperationMetrics *metric);
    void FlushLoop();
    void FlushWindows();

    int instance_id_;
    std::string output_dir_;
    uint64_t samples_max_{0};
    std::string summary_extra_;
    uint64_t generation_{0};
    int interval_ms_{3000};
    int64_t process_start_us_{0};
    std::atomic<int64_t> last_flush_us_{0};
    std::atomic<bool> flush_stop_{false};
    std::thread flush_thread_;
    std::mutex flush_mutex_;
    std::condition_variable flush_cv_;
    std::ofstream window_file_;
    mutable std::mutex operations_mutex_;
    std::map<std::string, std::shared_ptr<OperationMetrics>> operations_;
    std::atomic<uint64_t> verify_failures_{0};
    std::atomic<bool> running_{false};
    std::atomic<bool> recording_{false};
    std::atomic<int64_t> start_time_us_{0};
    std::atomic<int64_t> end_time_us_{0};
};

}  // namespace mooncake::kvtest
