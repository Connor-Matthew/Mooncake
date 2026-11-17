#include "metrics.h"

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <utility>

namespace mooncake::kvtest {
namespace {

int64_t SteadyNowUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::atomic<uint64_t> g_collector_generation{1};

uint64_t NextGeneration() {
    return g_collector_generation.fetch_add(1, std::memory_order_relaxed);
}

double Percentile(const std::vector<double> &sorted, double percentile) {
    if (sorted.empty()) return 0;
    const auto rank = static_cast<size_t>(std::llround(
        percentile * static_cast<double>(sorted.size() - 1)));
    return sorted[rank];
}

Json::Value LatencyJson(std::vector<double> values) {
    Json::Value result(Json::objectValue);
    if (values.empty()) {
        result["count"] = Json::UInt64(0);
        return result;
    }
    std::sort(values.begin(), values.end());
    double sum = 0;
    for (double value : values) sum += value;
    result["count"] = static_cast<Json::UInt64>(values.size());
    result["avg_ms"] = sum / values.size();
    result["min_ms"] = values.front();
    result["p50_ms"] = Percentile(values, 0.50);
    result["p90_ms"] = Percentile(values, 0.90);
    result["p99_ms"] = Percentile(values, 0.99);
    result["p999_ms"] = Percentile(values, 0.999);
    result["p9999_ms"] = Percentile(values, 0.9999);
    result["max_ms"] = values.back();
    return result;
}

std::string JsonString(const Json::Value &value) {
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, value);
}

void ApplySummaryExtra(const std::string &extra, Json::Value *root) {
    if (root == nullptr || extra.empty()) return;
    Json::CharReaderBuilder builder;
    Json::Value parsed;
    std::string error;
    std::istringstream input("{" + extra + "}");
    if (!Json::parseFromStream(builder, input, &parsed, &error)) return;
    if (!parsed.isObject()) return;
    for (const auto &name : parsed.getMemberNames()) {
        (*root)[name] = parsed[name];
    }
}

}  // namespace

const char *StatusName(RecordStatus status) {
    switch (status) {
        case RecordStatus::kOk:
            return "OK";
        case RecordStatus::kMiss:
            return "MISS";
        case RecordStatus::kMismatch:
            return "MISMATCH";
        case RecordStatus::kError:
            return "ERR";
        case RecordStatus::kWarmupSkip:
            return "WARMUP";
    }
    return "ERR";
}

MetricsCollector::MetricsCollector(int instance_id, int interval_ms,
                                   std::string output_dir,
                                   uint64_t samples_max)
    : instance_id_(instance_id),
      output_dir_(std::move(output_dir)),
      samples_max_(samples_max),
      generation_(NextGeneration()),
      interval_ms_(std::max(0, interval_ms)) {}

MetricsCollector::~MetricsCollector() { Stop(); }

void MetricsCollector::SetSummaryExtra(std::string extra) {
    std::lock_guard<std::mutex> lock(operations_mutex_);
    summary_extra_ = std::move(extra);
}

void MetricsCollector::Start() {
    std::filesystem::create_directories(output_dir_);
    process_start_us_ = SteadyNowUs();
    last_flush_us_.store(process_start_us_, std::memory_order_release);
    if (interval_ms_ > 0) {
        {
            std::lock_guard<std::mutex> lock(flush_mutex_);
            window_file_.open(
                std::filesystem::path(output_dir_) /
                    ("metrics_" + std::to_string(instance_id_) + ".csv"),
                std::ios::out | std::ios::trunc);
            window_file_
                << "elapsed_ms,operation,count,success,failure,miss,mismatch,"
                   "error,avg_ms,p90_ms,p99_ms,p999_ms,p9999_ms,max_ms,qps,"
                   "throughput_mb_s\n";
            flush_stop_.store(false, std::memory_order_release);
        }
        flush_thread_ = std::thread([this] { FlushLoop(); });
    }
    start_time_us_.store(SteadyNowUs(), std::memory_order_release);
    end_time_us_.store(0, std::memory_order_release);
    recording_.store(true, std::memory_order_release);
    running_.store(true, std::memory_order_release);
}

void MetricsCollector::Reset() {
    ResetPaused();
    BeginMeasurement();
}

void MetricsCollector::ResetPaused() {
    recording_.store(false, std::memory_order_release);
    std::lock_guard<std::mutex> operations_lock(operations_mutex_);
    for (const auto &[name, metric] : operations_) {
        (void)name;
        std::lock_guard<std::mutex> latency_lock(metric->latency_mutex);
        metric->total.store(0, std::memory_order_relaxed);
        metric->success.store(0, std::memory_order_relaxed);
        metric->failure.store(0, std::memory_order_relaxed);
        metric->bytes.store(0, std::memory_order_relaxed);
        metric->miss.store(0, std::memory_order_relaxed);
        metric->mismatch.store(0, std::memory_order_relaxed);
        metric->error.store(0, std::memory_order_relaxed);
        metric->warmup.store(0, std::memory_order_relaxed);
        metric->success_latency_ms_sum = 0.0;
        metric->samples.clear();
        metric->window_samples.clear();
        metric->stride = 1;
    }
    verify_failures_.store(0, std::memory_order_relaxed);
    start_time_us_.store(0, std::memory_order_release);
    end_time_us_.store(0, std::memory_order_release);
}

void MetricsCollector::BeginMeasurement() {
    last_flush_us_.store(SteadyNowUs(), std::memory_order_release);
    start_time_us_.store(SteadyNowUs(), std::memory_order_release);
    end_time_us_.store(0, std::memory_order_release);
    recording_.store(true, std::memory_order_release);
}

void MetricsCollector::Freeze() {
    if (!recording_.exchange(false, std::memory_order_acq_rel)) return;
    // Wait for records that passed the pre-lock recording check so the frozen
    // elapsed time and counters describe the same completed interval.
    std::lock_guard<std::mutex> operations_lock(operations_mutex_);
    for (const auto &[name, metric] : operations_) {
        (void)name;
        std::lock_guard<std::mutex> latency_lock(metric->latency_mutex);
    }
    end_time_us_.store(SteadyNowUs(), std::memory_order_release);
}

double MetricsCollector::ElapsedSeconds() const {
    const int64_t start = start_time_us_.load(std::memory_order_acquire);
    const int64_t end = end_time_us_.load(std::memory_order_acquire);
    const int64_t now = end == 0 ? SteadyNowUs() : end;
    return start == 0
               ? 0.0
               : std::max<int64_t>(0, now - start) / 1000000.0;
}

void MetricsCollector::Stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) return;
    if (flush_thread_.joinable()) {
        {
            std::lock_guard<std::mutex> lock(flush_mutex_);
            flush_stop_.store(true, std::memory_order_release);
        }
        flush_cv_.notify_all();
        flush_thread_.join();
    }
    Freeze();
    WriteSamples();
    WriteSummary();
    std::lock_guard<std::mutex> lock(flush_mutex_);
    if (window_file_.is_open()) {
        window_file_.flush();
        window_file_.close();
    }
}

std::shared_ptr<MetricsCollector::OperationMetrics>
MetricsCollector::GetOrCreate(const std::string &operation) {
    // The thread-local cache is keyed by collector address, but a destroyed
    // collector can be replaced at the same address (notably in tests), so
    // each collector carries a generation and stale caches are dropped.
    thread_local std::unordered_map<
        const MetricsCollector *,
        std::pair<uint64_t,
                  std::unordered_map<std::string,
                                     std::shared_ptr<OperationMetrics>>>>
        cache;
    auto &collector_cache = cache[this];
    if (collector_cache.first != generation_) {
        collector_cache.first = generation_;
        collector_cache.second.clear();
    }
    auto cached = collector_cache.second.find(operation);
    if (cached != collector_cache.second.end()) return cached->second;
    std::lock_guard<std::mutex> lock(operations_mutex_);
    auto &entry = operations_[operation];
    if (!entry) entry = std::make_shared<OperationMetrics>();
    collector_cache.second.emplace(operation, entry);
    return entry;
}

void MetricsCollector::MaybeDecimate(OperationMetrics *metric) {
    if (samples_max_ < 2 || metric->samples.size() < samples_max_) return;
    std::vector<RequestSample> compact;
    compact.reserve((metric->samples.size() + 1) / 2);
    for (size_t i = 0; i < metric->samples.size(); i += 2) {
        compact.push_back(metric->samples[i]);
    }
    metric->samples.swap(compact);
    metric->stride *= 2;
}

void MetricsCollector::FlushLoop() {
    std::unique_lock<std::mutex> lock(flush_mutex_);
    while (!flush_stop_.load(std::memory_order_acquire)) {
        flush_cv_.wait_for(lock, std::chrono::milliseconds(interval_ms_),
                           [this] {
                               return flush_stop_.load(
                                   std::memory_order_acquire);
                           });
        lock.unlock();
        FlushWindows();
        lock.lock();
    }
    lock.unlock();
    FlushWindows();
}

void MetricsCollector::FlushWindows() {
    if (interval_ms_ <= 0) return;
    const int64_t now = SteadyNowUs();
    const int64_t last = last_flush_us_.exchange(now);
    const double seconds =
        std::max(0.001, static_cast<double>(now - last) / 1000000.0);

    struct WindowRow {
        std::string operation;
        uint64_t total{0};
        uint64_t success{0};
        uint64_t failure{0};
        uint64_t miss{0};
        uint64_t mismatch{0};
        uint64_t error{0};
        uint64_t bytes{0};
        double avg_ms{0.0};
        double p90_ms{0.0};
        double p99_ms{0.0};
        double p999_ms{0.0};
        double p9999_ms{0.0};
        double max_ms{0.0};
    };

    std::vector<WindowRow> rows;
    {
        std::lock_guard<std::mutex> operations_lock(operations_mutex_);
        for (auto &[name, metric] : operations_) {
            std::vector<RequestSample> window;
            {
                std::lock_guard<std::mutex> latency_lock(
                    metric->latency_mutex);
                window.swap(metric->window_samples);
            }
            if (window.empty()) continue;
            WindowRow row;
            row.operation = name;
            std::vector<double> latencies;
            for (const auto &sample : window) {
                ++row.total;
                switch (sample.status) {
                    case RecordStatus::kOk:
                        ++row.success;
                        row.bytes += sample.bytes;
                        latencies.push_back(sample.latency_ms);
                        break;
                    case RecordStatus::kMiss:
                        ++row.failure;
                        ++row.miss;
                        break;
                    case RecordStatus::kMismatch:
                        ++row.failure;
                        ++row.mismatch;
                        break;
                    case RecordStatus::kError:
                        ++row.failure;
                        ++row.error;
                        break;
                    case RecordStatus::kWarmupSkip:
                        break;
                }
            }
            std::sort(latencies.begin(), latencies.end());
            if (!latencies.empty()) {
                double sum = 0;
                for (double value : latencies) sum += value;
                row.avg_ms = sum / latencies.size();
                row.p90_ms = Percentile(latencies, 0.90);
                row.p99_ms = Percentile(latencies, 0.99);
                row.p999_ms = Percentile(latencies, 0.999);
                row.p9999_ms = Percentile(latencies, 0.9999);
                row.max_ms = latencies.back();
            }
            rows.push_back(std::move(row));
        }
    }
    if (rows.empty()) return;

    std::lock_guard<std::mutex> flush_lock(flush_mutex_);
    if (!window_file_.is_open()) return;
    const double elapsed_ms =
        static_cast<double>(now - process_start_us_) / 1000.0;
    for (const auto &row : rows) {
        const double qps =
            seconds > 0 ? static_cast<double>(row.total) / seconds : 0.0;
        const double throughput_mb =
            seconds > 0
                ? static_cast<double>(row.bytes) / seconds / (1024.0 * 1024.0)
                : 0.0;
        window_file_ << std::fixed << std::setprecision(3) << elapsed_ms << ','
                     << row.operation << ',' << row.total << ',' << row.success
                     << ',' << row.failure << ',' << row.miss << ','
                     << row.mismatch << ',' << row.error << ','
                     << std::setprecision(6) << row.avg_ms << ',' << row.p90_ms
                     << ',' << row.p99_ms << ',' << row.p999_ms << ','
                     << row.p9999_ms << ',' << row.max_ms << ','
                     << std::setprecision(2) << qps << ',' << throughput_mb
                     << '\n';
    }
    window_file_.flush();
}

void MetricsCollector::Record(const std::string &operation, double latency_ms,
                              bool success, uint64_t bytes) {
    Record(operation, latency_ms,
           success ? RecordStatus::kOk : RecordStatus::kError, bytes, 0,
           false);
}

void MetricsCollector::Record(const std::string &operation, double latency_ms,
                              RecordStatus status, uint64_t bytes,
                              int32_t code, bool warmup) {
    if (!recording_.load(std::memory_order_acquire)) return;
    const int64_t recorded_us = SteadyNowUs();
    auto metric = GetOrCreate(operation);
    std::lock_guard<std::mutex> lock(metric->latency_mutex);
    if (!recording_.load(std::memory_order_acquire)) return;

    if (warmup || status == RecordStatus::kWarmupSkip) {
        metric->warmup.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    metric->total.fetch_add(1, std::memory_order_relaxed);
    switch (status) {
        case RecordStatus::kOk:
            metric->success.fetch_add(1, std::memory_order_relaxed);
            metric->bytes.fetch_add(bytes, std::memory_order_relaxed);
            metric->success_latency_ms_sum += latency_ms;
            break;
        case RecordStatus::kMiss:
            metric->failure.fetch_add(1, std::memory_order_relaxed);
            metric->miss.fetch_add(1, std::memory_order_relaxed);
            break;
        case RecordStatus::kMismatch:
            metric->failure.fetch_add(1, std::memory_order_relaxed);
            metric->mismatch.fetch_add(1, std::memory_order_relaxed);
            break;
        case RecordStatus::kError:
            metric->failure.fetch_add(1, std::memory_order_relaxed);
            metric->error.fetch_add(1, std::memory_order_relaxed);
            break;
        case RecordStatus::kWarmupSkip:
            break;
    }

    const int64_t start = start_time_us_.load(std::memory_order_acquire);
    MaybeDecimate(metric.get());
    RequestSample sample{std::max<int64_t>(0, recorded_us - start),
                         latency_ms, status, bytes, code};
    metric->samples.push_back(sample);
    if (interval_ms_ > 0) metric->window_samples.push_back(sample);
}

void MetricsCollector::RecordVerifyFailure() {
    if (!recording_.load(std::memory_order_acquire)) return;
    verify_failures_.fetch_add(1, std::memory_order_relaxed);
}

std::vector<MetricSnapshot> MetricsCollector::SnapshotCounts() const {
    std::vector<MetricSnapshot> snapshots;
    std::lock_guard<std::mutex> lock(operations_mutex_);
    snapshots.reserve(operations_.size());
    for (const auto &[name, metric] : operations_) {
        snapshots.push_back(
            {name,
             metric->total.load(std::memory_order_relaxed),
             metric->success.load(std::memory_order_relaxed),
             metric->failure.load(std::memory_order_relaxed),
             metric->bytes.load(std::memory_order_relaxed),
             metric->miss.load(std::memory_order_relaxed),
             metric->mismatch.load(std::memory_order_relaxed),
             metric->error.load(std::memory_order_relaxed)});
    }
    return snapshots;
}

std::string MetricsCollector::StatsJson() const {
    Json::Value root(Json::objectValue);
    root["instance_id"] = instance_id_;
    root["verify_failures"] = static_cast<Json::UInt64>(
        verify_failures_.load(std::memory_order_relaxed));
    Json::Value operations(Json::objectValue);
    std::lock_guard<std::mutex> lock(operations_mutex_);
    uint64_t warmup_total = 0;
    for (const auto &[name, metric] : operations_) {
        Json::Value op(Json::objectValue);
        op["total"] = static_cast<Json::UInt64>(
            metric->total.load(std::memory_order_relaxed));
        op["success"] = static_cast<Json::UInt64>(
            metric->success.load(std::memory_order_relaxed));
        op["failure"] = static_cast<Json::UInt64>(
            metric->failure.load(std::memory_order_relaxed));
        const uint64_t bytes =
            metric->bytes.load(std::memory_order_relaxed);
        op["bytes"] = static_cast<Json::UInt64>(bytes);
        double busy_ms = 0.0;
        {
            std::lock_guard<std::mutex> latency_lock(metric->latency_mutex);
            busy_ms = metric->success_latency_ms_sum;
        }
        op["busy_ms"] = busy_ms;
        const double busy_s = busy_ms / 1000.0;
        const double throughput_mb =
            busy_s > 0.0
                ? static_cast<double>(bytes) / busy_s / (1024.0 * 1024.0)
                : 0.0;
        op["throughput_mb_s"] = throughput_mb;
        op["throughput_gb_s"] = throughput_mb / 1024.0;
        op["miss"] = static_cast<Json::UInt64>(
            metric->miss.load(std::memory_order_relaxed));
        op["mismatch"] = static_cast<Json::UInt64>(
            metric->mismatch.load(std::memory_order_relaxed));
        op["error"] = static_cast<Json::UInt64>(
            metric->error.load(std::memory_order_relaxed));
        op["warmup"] = static_cast<Json::UInt64>(
            metric->warmup.load(std::memory_order_relaxed));
        warmup_total += metric->warmup.load(std::memory_order_relaxed);
        operations[name] = std::move(op);
    }
    root["warmup_total"] = static_cast<Json::UInt64>(warmup_total);
    ApplySummaryExtra(summary_extra_, &root);
    root["operations"] = std::move(operations);
    return JsonString(root);
}

void MetricsCollector::WriteSamples() {
    std::vector<std::pair<std::string, std::shared_ptr<OperationMetrics>>> ops;
    {
        std::lock_guard<std::mutex> lock(operations_mutex_);
        for (const auto &entry : operations_) ops.push_back(entry);
    }
    std::ofstream output(std::filesystem::path(output_dir_) /
                         "latency_samples.csv");
    output << "elapsed_us,instance_id,operation,request_index,latency_ms,"
              "status,bytes\n";
    for (const auto &[name, metric] : ops) {
        std::vector<RequestSample> samples;
        {
            std::lock_guard<std::mutex> lock(metric->latency_mutex);
            samples = metric->samples;
        }
        std::stable_sort(samples.begin(), samples.end(),
                         [](const auto &a, const auto &b) {
                             return a.elapsed_us < b.elapsed_us;
                         });
        for (size_t i = 0; i < samples.size(); ++i) {
            const auto &sample = samples[i];
            std::string status = StatusName(sample.status);
            if (sample.status == RecordStatus::kError && sample.code != 0) {
                status += ":" + std::to_string(sample.code);
            }
            output << sample.elapsed_us << ',' << instance_id_ << ',' << name
                   << ',' << i << ',' << std::fixed << std::setprecision(6)
                   << sample.latency_ms << ',' << status << ',' << sample.bytes
                   << '\n';
        }
    }
}

void MetricsCollector::WriteSummary() {
    Json::Value root(Json::objectValue);
    root["instance_id"] = instance_id_;
    root["elapsed_seconds"] = ElapsedSeconds();
    root["verify_failures"] = static_cast<Json::UInt64>(
        verify_failures_.load(std::memory_order_relaxed));
    Json::Value operations(Json::objectValue);
    uint64_t warmup_total = 0;
    std::lock_guard<std::mutex> lock(operations_mutex_);
    for (const auto &[name, metric] : operations_) {
        std::vector<double> latencies;
        double busy_ms = 0.0;
        {
            std::lock_guard<std::mutex> latency_lock(metric->latency_mutex);
            latencies.reserve(metric->samples.size());
            for (const auto &sample : metric->samples) {
                // Latency percentiles (and throughput) only describe
                // successful operations, matching the Python client.
                if (sample.status == RecordStatus::kOk) {
                    latencies.push_back(sample.latency_ms);
                }
            }
            busy_ms = metric->success_latency_ms_sum;
        }
        auto op = LatencyJson(std::move(latencies));
        op["total"] = static_cast<Json::UInt64>(
            metric->total.load(std::memory_order_relaxed));
        op["success"] = static_cast<Json::UInt64>(
            metric->success.load(std::memory_order_relaxed));
        op["failure"] = static_cast<Json::UInt64>(
            metric->failure.load(std::memory_order_relaxed));
        const uint64_t bytes =
            metric->bytes.load(std::memory_order_relaxed);
        op["bytes"] = static_cast<Json::UInt64>(bytes);
        op["busy_ms"] = busy_ms;
        const double busy_s = busy_ms / 1000.0;
        const double throughput_mb =
            busy_s > 0.0
                ? static_cast<double>(bytes) / busy_s / (1024.0 * 1024.0)
                : 0.0;
        op["throughput_mb_s"] = throughput_mb;
        op["throughput_gb_s"] = throughput_mb / 1024.0;
        op["miss"] = static_cast<Json::UInt64>(
            metric->miss.load(std::memory_order_relaxed));
        op["mismatch"] = static_cast<Json::UInt64>(
            metric->mismatch.load(std::memory_order_relaxed));
        op["error"] = static_cast<Json::UInt64>(
            metric->error.load(std::memory_order_relaxed));
        op["warmup"] = static_cast<Json::UInt64>(
            metric->warmup.load(std::memory_order_relaxed));
        warmup_total += metric->warmup.load(std::memory_order_relaxed);
        operations[name] = std::move(op);
    }
    root["operations"] = std::move(operations);
    root["warmup_total"] = static_cast<Json::UInt64>(warmup_total);
    ApplySummaryExtra(summary_extra_, &root);
    std::ofstream output(std::filesystem::path(output_dir_) / "summary.json");
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "  ";
    output << Json::writeString(builder, root) << '\n';
}

}  // namespace mooncake::kvtest
