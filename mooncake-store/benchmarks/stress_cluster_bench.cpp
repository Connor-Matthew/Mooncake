#include <numa.h>
#include <sched.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <limits>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <optional>
#include <vector>
#include <cstdlib>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "cuda_alike.h"
#include "device/accelerator_registry.h"
#include "master_client.h"
#include "mooncake_logging.h"
#include "nof/nof_runtime.h"
#if defined(USE_NOF_URMA)
#include "nof_gpu_direct_report.h"
#endif
#include "real_client.h"
#include "ub_allocator.h"
#include "ub_gpu_direct_stats.h"
#include "stress_bench_validation.h"
#include "latency_histogram.h"

#include <ylt/struct_json/json_reader.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {
constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;
constexpr size_t GB = 1024 * MB;

const static int NR_SOCKETS =
    numa_available() == 0 ? numa_num_configured_nodes() : 1;

static bool VerifyReadPlacementEnabled() {
    const char* value = std::getenv("MC_BENCH_VERIFY_READ_PLACEMENT");
    return value && std::string_view(value) == "1";
}

static void bindToSocket(int socket_id) {
    if (numa_available() < 0) return;
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    if (socket_id < 0 || socket_id >= numa_num_configured_nodes())
        socket_id = 0;
    struct bitmask* cpu_list = numa_allocate_cpumask();
    numa_node_to_cpus(socket_id, cpu_list);
    int nr_possible_cpus = numa_num_possible_cpus();
    int nr_cpus = 0;
    for (int cpu = 0; cpu < nr_possible_cpus; ++cpu) {
        if (numa_bitmask_isbitset(cpu_list, cpu) &&
            numa_bitmask_isbitset(numa_all_cpus_ptr, cpu)) {
            CPU_SET(cpu, &cpu_set);
            ++nr_cpus;
        }
    }
    numa_free_cpumask(cpu_list);
    if (nr_cpus > 0) {
        if (sched_setaffinity(0, sizeof(cpu_set), &cpu_set) != 0) {
            PLOG(WARNING) << "Failed to set CPU affinity for NUMA socket "
                          << socket_id;
        }
    }
}

static std::string FormatBytes(size_t bytes) {
    if (bytes == 0) return "0 B";
    const char* units[] = {"B", "KB", "MB", "GB", "TB"};
    int i = static_cast<int>(std::floor(std::log2(bytes) / 10));
    if (i > 4) i = 4;
    double val = static_cast<double>(bytes) / std::pow(1024, i);
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << val << " " << units[i];
    return oss.str();
}

static size_t MaxEffectiveBatchSize(size_t total_keys, size_t num_threads,
                                    size_t configured_batch_size) {
    const size_t max_keys_per_thread =
        (total_keys + num_threads - 1) / num_threads;
    return std::min(configured_batch_size, max_keys_per_thread);
}

static void PrewarmAcceleratorDiscovery() {
    const auto start = std::chrono::steady_clock::now();
    const auto runtime =
        mooncake::device::GetAcceleratorRegistry().RuntimeAccelerators();
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start)
            .count();
    LOG(INFO) << "Accelerator discovery prewarm complete: devices="
              << runtime.Devices().size() << " elapsed_ms=" << elapsed_ms
              << " (excluded from benchmark timing)";
}

static std::optional<std::string> FetchMasterAdminBody(
    const std::string& master_host, int master_admin_port,
    const std::string& path) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        LOG(ERROR) << "Failed to create socket for discovering segments";
        return std::nullopt;
    }

    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(master_admin_port);

    std::string host = master_host;
    size_t colon_pos = host.find(':');
    if (colon_pos != std::string::npos) {
        host = host.substr(0, colon_pos);
    }

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        LOG(ERROR) << "Invalid master host: " << host;
        close(sockfd);
        return std::nullopt;
    }

    if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG(ERROR) << "Failed to connect to master admin at " << host << ":"
                   << master_admin_port;
        close(sockfd);
        return std::nullopt;
    }

    std::string request = "GET " + path + " HTTP/1.0\r\nHost: " + host +
                          "\r\nConnection: close\r\n\r\n";
    if (send(sockfd, request.c_str(), request.size(), 0) < 0) {
        LOG(ERROR) << "Failed to send HTTP request to master";
        close(sockfd);
        return std::nullopt;
    }

    std::string response;
    char buf[4096];
    ssize_t n;
    while ((n = recv(sockfd, buf, sizeof(buf), 0)) > 0) {
        response.append(buf, n);
    }
    close(sockfd);
    if (n < 0) {
        LOG(ERROR) << "Incomplete master admin HTTP response";
        return std::nullopt;
    }

    size_t header_end = response.find("\r\n\r\n");
    if (header_end == std::string::npos) {
        LOG(ERROR) << "Invalid HTTP response from master";
        return std::nullopt;
    }

    std::string header = response.substr(0, header_end);
    size_t status_pos = header.find(' ');
    if (status_pos == std::string::npos) {
        LOG(ERROR) << "Invalid HTTP response header from master";
        return std::nullopt;
    }
    size_t status_code_start = status_pos + 1;
    size_t status_code_end = header.find(' ', status_code_start);
    if (status_code_end == std::string::npos) {
        LOG(ERROR) << "Invalid HTTP status line from master";
        return std::nullopt;
    }
    std::string status_code =
        header.substr(status_code_start, status_code_end - status_code_start);
    if (status_code != "200") {
        LOG(ERROR) << "HTTP request failed with status " << status_code
                   << " from master at " << master_host << ":"
                   << master_admin_port;
        return std::nullopt;
    }

    return response.substr(header_end + 4);
}

static std::vector<std::string> DiscoverSegmentsFromMaster(
    const std::string& master_host, int master_admin_port) {
    std::vector<std::string> segments;
    auto body = FetchMasterAdminBody(master_host, master_admin_port,
                                     "/get_all_segments");
    if (!body) return segments;
    std::istringstream iss(*body);
    std::string line;
    // Use an unordered_set to track already-seen segments and avoid duplicates
    std::unordered_set<std::string> seen;
    while (std::getline(iss, line)) {
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n' ||
                                 line.back() == ' ' || line.back() == '\t')) {
            line.pop_back();
        }
        if (!line.empty() && seen.insert(line).second) {
            segments.push_back(line);
        }
    }

    return segments;
}

static std::vector<std::string> DiscoverNoFSegmentsFromMaster(
    const std::string& master_server, bool* success = nullptr) {
    if (success) *success = false;
    mooncake::MasterClient master_client(mooncake::generate_uuid());
    auto connect_result = master_client.Connect(master_server);
    if (connect_result != mooncake::ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to master for NoF segment discovery: "
                   << static_cast<int>(connect_result);
        return {};
    }

    auto result = master_client.GetAllNoFSegments();
    if (!result) {
        LOG(ERROR) << "Failed to discover NoF segments: "
                   << static_cast<int>(result.error());
        return {};
    }

    if (success) *success = true;

    std::vector<std::string> names;
    names.reserve(result->size());
    for (const auto& segment : *result) {
        names.push_back(segment.name);
    }
    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());
    return names;
}

enum class SegmentKind { MEMORY, NOF };

struct BenchmarkSegment {
    std::string name;
    SegmentKind kind;
    std::vector<mooncake::bench::PlacementRegion> regions;
};

struct MemorySegmentDetail {
    std::string segment_name;
    std::string base_address;
    uint64_t size_bytes = 0;
    std::string te_endpoint;
    std::string protocol;
};
YLT_REFL(MemorySegmentDetail, segment_name, base_address, size_bytes,
         te_endpoint, protocol);
struct MemorySegmentsDetail {
    std::vector<MemorySegmentDetail> segments;
};
YLT_REFL(MemorySegmentsDetail, segments);

static const char* SegmentKindName(SegmentKind kind) {
    return kind == SegmentKind::MEMORY ? "memory" : "NoF";
}

static bool NofWholePoolRegistrationDefault() {
    const char* raw = std::getenv("MC_NOF_WHOLE_POOL_REGISTER");
    if (!raw) return true;

    std::string value(raw);
    std::transform(value.begin(), value.end(), value.begin(), [](char c) {
        return c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c;
    });
    return value != "0" && value != "false" && value != "no" && value != "off";
}
}  // namespace

DEFINE_string(local_hostname, "localhost",
              "Local hostname (with optional port, e.g. node1:12345)");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server URL");
DEFINE_string(master_server, "127.0.0.1:50051", "Master server address");
DEFINE_string(protocol, "tcp", "Transport protocol: tcp, rdma, ub");
DEFINE_string(device_name, "", "RDMA/UB device name (comma-separated)");
DEFINE_uint64(global_segment_size, 4 * GB, "Global segment size in bytes");
DEFINE_uint64(local_buffer_size, 512 * MB, "Local buffer size in bytes");
DEFINE_bool(enable_ssd_offload, false, "Enable SSD offload on this client");
DEFINE_string(ssd_offload_path, "", "SSD offload directory path");

DEFINE_string(scenario, "local_memory",
              "Benchmark scenario: local_memory, upsert, remote_memory, "
              "local_disk, remote_disk, segment_write, segment_read, "
              "list_segments, remove, batch_remove");
DEFINE_string(role, "writer",
              "Node role: writer (prefill data) or reader (benchmark reads)");
DEFINE_uint64(value_size, 4 * MB, "Size of each value in bytes");
DEFINE_bool(key_include_value_size, false,
            "Append the value size to segment keys so runs with different "
            "--value_size values do not collide");
DEFINE_uint64(num_keys, 100, "Number of keys to write/read");
DEFINE_uint64(batch_size, 32,
              "Batch size for reads and per-thread buffer sizing");
DEFINE_uint64(num_threads, 1, "Number of concurrent benchmark worker threads");
DEFINE_uint64(warmup_keys, 5, "Number of warmup keys (not counted in stats)");
DEFINE_uint64(wait_seconds, 5,
              "Seconds to wait before reading (for remote scenarios)");
DEFINE_uint64(client_init_wait_seconds, 0,
              "Seconds to wait after RealClient setup succeeds and before "
              "benchmark run starts. This can give asynchronous RPC/transfer "
              "warmup and background initialization time to settle.");
DEFINE_bool(verify, true,
            "Verify each measured read before buffer reuse and stored values "
            "after writes. API latency excludes verification; read wall "
            "throughput includes verification.");
DEFINE_bool(strict_segment_placement, true,
            "Check actual placement after segment PUTs; require one replica "
            "per selected type. False permits preferred-placement fallback.");
DEFINE_bool(require_gpu_direct, false,
            "Fail unless the requested direct GPU path is supported and "
            "NoF GPU I/O completes with successful GPU registrations.");
DEFINE_bool(nof_whole_pool_register, NofWholePoolRegistrationDefault(),
            "Automatically register complete benchmark buffers with selected "
            "URMA NoF endpoints when SPDK supports whole-pool registration. "
            "Set false (or MC_NOF_WHOLE_POOL_REGISTER=0) for compatibility "
            "testing with per-I/O registration.");
DEFINE_uint64(nof_qpairs_per_segment, 0,
              "Optional override for MC_NOF_QPAIRS_PER_SEGMENT. Each qpair "
              "is assigned to one NoF worker. 0 keeps environment/default "
              "(four qpairs).");
DEFINE_uint64(replica_num, 1, "Number of replicas for each object");
DEFINE_uint64(nof_replica_num, 0, "Number of NoF SSD replicas for each object");
DEFINE_bool(hard_pin, false,
            "Pin objects to prevent eviction during benchmark");

DEFINE_string(segments, "",
              "Comma-separated memory segment names for segment scenarios. "
              "Leave empty to auto-discover from master.");
DEFINE_string(nof_segments, "",
              "Comma-separated NoF segment names for segment scenarios. "
              "Leave empty to auto-discover from master when "
              "--nof_replica_num is nonzero.");
DEFINE_uint64(master_admin_port, 9003,
              "Master admin HTTP port for memory segment discovery and "
              "placement checks; NoF uses --master_server RPC");
DEFINE_uint64(read_segment_nums, 0,
              "Number of memory and NoF segments to read/remove from "
              "(0 = all, memory segments first)");
DEFINE_uint64(duration, 0,
              "Duration in seconds for continuous reading in local_memory and "
              "segment_read scenarios (0 = read num_keys once)");
DEFINE_bool(prewarm_accelerator, true,
            "Probe accelerator availability after client setup and before "
            "performance timing");
DEFINE_uint64(statis_interval, 5,
              "Statistics print interval in seconds for segment_read scenario");
DEFINE_uint64(shuffle_seed, 0,
              "Seed for deterministic shuffle of segment_read keys "
              "(0 = disabled)");

DEFINE_string(gpu_mode, "",
              "GPU link mode: gdr-peermem | staging. Empty/host = CPU buffer. "
              "Supported for both roles: reader gets into the GPU buffer; "
              "writer (and segment_write/remove prefill) puts from it "
              "(GPU-source writes).");
DEFINE_int32(gpu_device, 0,
             "GPU device index for gpu_mode (gdr-peermem/staging). Default 0. "
             "Ignored when gpu_mode is empty/host.");

// MC_UB_GPU_MODE selects the GPU memory access path for UB transport.
//   host        - CPU memory only; no GPU involvement (default when unset).
//   staging     - GPU buffer + per-buffer host staging; NIC only sees host.
//   gdr-peermem - NIC directly DMA GPU HBM via nvidia_peermem.
//   gdr-direct  - (future) dma_buf fd path (urma_register_seg_dmabuf).
//   proxy       - (future) HBM mapped to host VA, hardware-coherent migration.
struct GpuRuntimeConfig {
    std::string mode;     // "host" | "staging" | "gdr-peermem"
    bool use_gpu_buffer;  // true for gdr-peermem / staging
};

static GpuRuntimeConfig ResolveGpuConfig() {
    GpuRuntimeConfig cfg;
    cfg.mode = FLAGS_gpu_mode.empty() ? "host" : FLAGS_gpu_mode;
    if (cfg.mode == "gdr-direct" || cfg.mode == "proxy") {
        LOG(FATAL) << "gpu_mode=" << cfg.mode
                   << " is not yet implemented (reserved for future GDR types)";
    }
    if (cfg.mode != "host" && cfg.mode != "staging" &&
        cfg.mode != "gdr-peermem") {
        LOG(FATAL) << "Unknown gpu_mode: " << FLAGS_gpu_mode;
    }
    cfg.use_gpu_buffer = (cfg.mode == "gdr-peermem" || cfg.mode == "staging");
    return cfg;
}

static void ValidateGpuDevice(const GpuRuntimeConfig& cfg) {
    if (!cfg.use_gpu_buffer) return;
#if defined(USE_CUDA)
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || count == 0) {
        LOG(FATAL) << "gpu_mode requires a CUDA device, none available";
    }
    if (FLAGS_gpu_device < 0 || FLAGS_gpu_device >= count) {
        LOG(FATAL) << "gpu_device=" << FLAGS_gpu_device << " out of range [0,"
                   << count << ")";
    }
    if (cudaSetDevice(FLAGS_gpu_device) != cudaSuccess) {
        LOG(FATAL) << "cudaSetDevice(" << FLAGS_gpu_device << ") failed";
    }
#else
    LOG(FATAL) << "gpu_mode requires a USE_CUDA build";
#endif
}

static void ApplyGpuEnv(const GpuRuntimeConfig& cfg) {
    if (!FLAGS_gpu_mode.empty()) {
        ::setenv("MC_UB_GPU_MODE", cfg.mode.c_str(), 1);
        LOG(INFO) << "Set MC_UB_GPU_MODE=" << cfg.mode;
    }
}

#if defined(USE_CUDA)
static void checkCudaError(cudaError_t result, const char* message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << ": " << cudaGetErrorString(result);
        std::exit(EXIT_FAILURE);
    }
}
#endif

using Clock = std::chrono::steady_clock;
using Nanos = std::chrono::nanoseconds;

inline int64_t ElapsedNanos(Clock::time_point t0, Clock::time_point t1) {
    return std::chrono::duration_cast<Nanos>(t1 - t0).count();
}

inline double NanosToUs(int64_t ns) { return static_cast<double>(ns) / 1000.0; }
inline double NanosToMs(int64_t ns) {
    return static_cast<double>(ns) / 1000000.0;
}
inline double NanosToSec(int64_t ns) { return static_cast<double>(ns) / 1e9; }

struct ThreadResult {
    std::vector<int64_t> latencies_ns;  // per-query latency
    size_t total_bytes = 0;
    size_t total_keys = 0;  // number of keys processed
    size_t total_queries =
        0;  // number of API calls (get_into / batch_get_into)
    size_t failed_ops = 0;
};

class BenchmarkStats {
   public:
    void InitThreads(size_t n, size_t expected_per_thread) {
        thread_results_.resize(n);
        expected_per_thread_ = expected_per_thread;
    }

    ThreadResult& GetThreadResult(size_t tid) { return thread_results_[tid]; }

    void StartTimer() { start_ = Clock::now(); }
    void StopTimer() { end_ = Clock::now(); }

    double WallSeconds() const {
        return NanosToSec(ElapsedNanos(start_, end_));
    }

    void Finalize() {
        merged_latencies_ns_.clear();
        total_bytes_ = 0;
        total_keys_ = 0;
        total_queries_ = 0;
        total_failed_ = 0;

        for (auto& tr : thread_results_) {
            merged_latencies_ns_.insert(merged_latencies_ns_.end(),
                                        tr.latencies_ns.begin(),
                                        tr.latencies_ns.end());
            total_bytes_ += tr.total_bytes;
            total_keys_ += tr.total_keys;
            total_queries_ += tr.total_queries;
            total_failed_ += tr.failed_ops;
        }
        std::sort(merged_latencies_ns_.begin(), merged_latencies_ns_.end());
    }

    // Sequential Put loops prepare each value before calling put_from(). Use
    // the sum of API call latencies as the measured wall time so buffer filling
    // does not distort Store/NoF throughput.
    void FinalizeSequential() {
        Finalize();
        const int64_t elapsed_ns =
            std::accumulate(merged_latencies_ns_.begin(),
                            merged_latencies_ns_.end(), int64_t(0));
        start_ = Clock::time_point{};
        end_ = start_ + Nanos(elapsed_ns);
    }

    double PercentileUs(double p) const {
        if (merged_latencies_ns_.empty()) return 0.0;
        double rank = (p / 100.0) * (merged_latencies_ns_.size() - 1);
        size_t lo = static_cast<size_t>(rank);
        size_t hi = std::min(lo + 1, merged_latencies_ns_.size() - 1);
        double frac = rank - lo;
        int64_t ns_val =
            static_cast<int64_t>(merged_latencies_ns_[lo] * (1.0 - frac) +
                                 merged_latencies_ns_[hi] * frac);
        return NanosToUs(ns_val);
    }

    double MeanLatencyUs() const {
        if (merged_latencies_ns_.empty()) return 0.0;
        double sum = static_cast<double>(
            std::accumulate(merged_latencies_ns_.begin(),
                            merged_latencies_ns_.end(), int64_t(0)));
        return NanosToUs(sum /
                         static_cast<double>(merged_latencies_ns_.size()));
    }

    double ThroughputMBps() const {
        double wall = WallSeconds();
        return (wall > 0) ? (static_cast<double>(total_bytes_) / MB) / wall : 0;
    }

    double KeysPerSec() const {
        double wall = WallSeconds();
        return (wall > 0) ? static_cast<double>(total_keys_) / wall : 0;
    }

    double QueriesPerSec() const {
        double wall = WallSeconds();
        return (wall > 0) ? static_cast<double>(total_queries_) / wall : 0;
    }

    void Print(const std::string& title,
               std::optional<size_t> effective_batch_size = std::nullopt,
               const char* transfer_path = nullptr) const {
        std::cout << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  " << title << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << std::fixed << std::setprecision(2);

        std::cout << "  Parameters:\n";
        std::cout << "    Scenario:       " << FLAGS_scenario << "\n";
        std::cout << "    Protocol:       " << FLAGS_protocol << "\n";
        std::cout << "    GPU mode:       " << FLAGS_gpu_mode << "\n";
        std::cout << "    GPU device:     " << FLAGS_gpu_device << "\n";
        std::cout << "    Value size:     " << FLAGS_value_size / MB
                  << " MiB\n";
        std::cout << "    Batch size:     " << FLAGS_batch_size << "\n";
        if (effective_batch_size) {
            std::cout << "    Max batch used: " << *effective_batch_size
                      << "\n";
        }
        std::cout << "    Num threads:    " << FLAGS_num_threads << "\n";
        std::cout << "    Num keys:       " << FLAGS_num_keys << "\n";
        std::cout << "    Verify data:    " << (FLAGS_verify ? "yes" : "no")
                  << "\n";
        if (transfer_path != nullptr) {
            std::cout << "    Transfer path:  " << transfer_path << "\n";
        }

        double wall = WallSeconds();
        std::cout << "\n  Total time:        " << wall << " s\n";
        std::cout << "  Total queries:    " << total_queries_
                  << " (failed: " << total_failed_ << ")\n";
        std::cout << "  Total keys:       " << total_keys_ << "\n";
        std::cout << "  Total data:       " << FormatBytes(total_bytes_)
                  << "\n";
        std::cout << "  Throughput (avg): " << ThroughputMBps() << " MiB/s";
        if (ThroughputMBps() > 1024) {
            std::cout << " (" << ThroughputMBps() / 1024 << " GiB/s)";
        }
        std::cout << "\n";
        std::cout << "  Keys/sec:         " << KeysPerSec() << "\n";
        std::cout << "  Queries/sec:      " << QueriesPerSec() << "\n";

        if (!merged_latencies_ns_.empty()) {
            size_t n = merged_latencies_ns_.size();
            std::cout << "\n  Latency (us)      [n=" << n << ", per-query]\n";
            std::cout << "    Min:   " << std::setw(12)
                      << NanosToUs(merged_latencies_ns_.front()) << "\n";
            std::cout << "    Avg:   " << std::setw(12) << MeanLatencyUs()
                      << "\n";
            std::cout << "    P50:   " << std::setw(12) << PercentileUs(50)
                      << "\n";
            std::cout << "    P90:   " << std::setw(12) << PercentileUs(90)
                      << "\n";
            std::cout << "    P99:   " << std::setw(12) << PercentileUs(99);
            if (n < 100) std::cout << "  (n<100)";
            std::cout << "\n";
            std::cout << "    P999:  " << std::setw(12) << PercentileUs(99.9);
            if (n < 1000) std::cout << "  (n<1000)";
            std::cout << "\n";
            std::cout << "    P9999: " << std::setw(12) << PercentileUs(99.99);
            if (n < 10000) std::cout << "  (n<10000)";
            std::cout << "\n";
            std::cout << "    Max:   " << std::setw(12)
                      << NanosToUs(merged_latencies_ns_.back()) << "\n";
        }
        std::cout << "========================================"
                  << "========================================\n\n";
    }

    size_t total_bytes() const { return total_bytes_; }
    size_t total_keys() const { return total_keys_; }
    size_t total_queries() const { return total_queries_; }
    size_t total_failed() const { return total_failed_; }

   private:
    std::vector<ThreadResult> thread_results_;
    std::vector<int64_t> merged_latencies_ns_;
    size_t total_bytes_ = 0;
    size_t total_keys_ = 0;
    size_t total_queries_ = 0;
    size_t total_failed_ = 0;
    size_t expected_per_thread_ = 0;
    Clock::time_point start_;
    Clock::time_point end_;
};

class StressBenchmark {
   public:
    StressBenchmark()
        : client_(mooncake::RealClient::create()),
          buffer_(nullptr),
          buffer_size_(0) {}

    ~StressBenchmark() {
        // Early return if already cleaned up
        if (!client_) {
            return;
        }

        // Run() has drained all I/O. Release SPDK registrations while their
        // device contexts and buffer allocations are still alive.
        nof_pool_tokens_.clear();

        // Unregister and free thread buffers (these are allocated by this
        // class). In gdr-peermem mode the thread buffers are sub-regions of the
        // single GPU HBM block owned by buffer_, so they are skipped here.
        for (auto& tb : thread_buffers_) {
            if (tb.ptr) {
                if (gpu_config_.mode == "gdr-peermem") {
                    tb.ptr = nullptr;
                    continue;
                }
                try {
                    client_->unregister_buffer(tb.ptr);
                } catch (...) {
                    LOG(WARNING)
                        << "Failed to unregister thread buffer, ignoring";
                }
                if (tb.is_gpu) {
#if defined(USE_UB)
                    mooncake::ub_free_gpu_hbm(tb.ptr);
#else
                    LOG(ERROR) << "Cannot free GPU HBM in non-UB build";
#endif
                } else {
                    numa_free(tb.ptr, tb.size);
                }
                tb.ptr = nullptr;
            }
        }
        thread_buffers_.clear();

        // Unregister and free main buffer (allocated by this class)
        if (buffer_) {
            try {
                client_->unregister_buffer(buffer_);
            } catch (...) {
                LOG(WARNING) << "Failed to unregister main buffer, ignoring";
            }
            if (buffer_is_gpu_) {
#if defined(USE_UB)
                mooncake::ub_free_gpu_hbm(buffer_);
#else
                LOG(ERROR) << "Cannot free GPU HBM in non-UB build";
#endif
            } else {
                numa_free(buffer_, buffer_size_);
            }
            buffer_ = nullptr;
        }
        client_ = nullptr;
    }

    int Setup() {
        gpu_config_ = ResolveGpuConfig();
        if (gpu_config_.mode == "gdr-peermem" &&
            FLAGS_global_segment_size > 0) {
            LOG(INFO) << "gdr-peermem mode: forcing global_segment_size=0 "
                      << "(host segment not needed, GPU buffer registered "
                      << "separately)";
            FLAGS_global_segment_size = 0;
        }
        int ret = client_->setup_real(
            FLAGS_local_hostname, FLAGS_metadata_server,
            FLAGS_global_segment_size, FLAGS_local_buffer_size, FLAGS_protocol,
            FLAGS_device_name, FLAGS_master_server, nullptr, "",
            FLAGS_enable_ssd_offload, FLAGS_ssd_offload_path);
        if (ret != 0) {
            LOG(ERROR) << "RealClient setup_real failed, ret=" << ret;
            return ret;
        }
        LOG(INFO) << "RealClient setup succeeded"
                  << (FLAGS_enable_ssd_offload ? " (SSD offload enabled)" : "");

        if (gpu_config_.use_gpu_buffer && FLAGS_nof_replica_num > 0) {
            if (!EnsurePlacementClient()) return -1;
            auto snapshots = placement_client_->GetAllNoFSegments();
            if (!snapshots || snapshots->empty()) {
                LOG(ERROR) << "Cannot inspect NoF endpoints for GPU I/O";
                return -1;
            }
            const auto requested = ParseSegments(FLAGS_nof_segments);
            const bool segment_scenario = FLAGS_scenario == "segment_write" ||
                                          FLAGS_scenario == "segment_read" ||
                                          FLAGS_scenario == "remove" ||
                                          FLAGS_scenario == "batch_remove";
            for (const auto& segment : *snapshots) {
                if (segment_scenario && FLAGS_strict_segment_placement &&
                    !requested.empty() &&
                    std::find(requested.begin(), requested.end(),
                              segment.name) == requested.end()) {
                    continue;
                }
                if (!IsUrmaEndpoint(segment.te_endpoint)) {
                    LOG(ERROR) << "NoF GPU I/O requires URMA endpoints: "
                               << segment.name << " " << segment.te_endpoint;
                    return -1;
                }
            }
        }

        buffer_size_ = FLAGS_batch_size * FLAGS_value_size;
        if (gpu_config_.use_gpu_buffer) {
#if defined(USE_CUDA) && defined(USE_UB)
            size_t gpu_alloc_size = buffer_size_;
            if (gpu_config_.mode == "gdr-peermem") {
                gpu_alloc_size = (1 + FLAGS_num_threads) * buffer_size_;
            }
            buffer_ = reinterpret_cast<char*>(mooncake::ub_allocate_gpu_hbm(
                gpu_alloc_size, FLAGS_gpu_device));
            if (!buffer_) {
                LOG(ERROR) << "Failed to allocate GPU buffer of "
                           << gpu_alloc_size << " bytes";
                return -1;
            }
            buffer_is_gpu_ = true;
            VLOG(1) << "[bench] GPU buffer allocated mode=" << gpu_config_.mode
                    << " device=" << FLAGS_gpu_device << " addr=" << buffer_
                    << " alloc_size=" << gpu_alloc_size
                    << " buf_size=" << buffer_size_
                    << " is_gpu=" << buffer_is_gpu_;
#else
            LOG(FATAL) << "gpu_mode requires USE_CUDA and USE_UB build";
#endif
        } else {
            buffer_ = reinterpret_cast<char*>(numa_alloc_local(buffer_size_));
            if (!buffer_) {
                LOG(ERROR) << "Failed to allocate buffer of " << buffer_size_
                           << " bytes";
                return -1;
            }
            buffer_is_gpu_ = false;
            std::memset(buffer_, 0, buffer_size_);
        }

        {
            size_t register_size = buffer_size_;
            if (gpu_config_.mode == "gdr-peermem") {
                register_size = (1 + FLAGS_num_threads) * buffer_size_;
            }
            ret = client_->register_buffer(buffer_, register_size);
        }
        if (ret != 0) {
            LOG(ERROR) << "register_buffer failed, ret=" << ret;
            return ret;
        }
        LOG(INFO) << "Registered buffer of " << buffer_size_ / MB << " MiB";
        return 0;
    }

    int RunWriter() {
        LOG(INFO) << "=== WRITER MODE ===";
        LOG(INFO) << "Writing " << FLAGS_num_keys << " keys, each "
                  << FLAGS_value_size / MB << " MiB";

        mooncake::ReplicateConfig config = MakeReplicateConfig();

        BenchmarkStats put_stats;
        put_stats.InitThreads(1, FLAGS_num_keys);
        ThreadResult& put_result = put_stats.GetThreadResult(0);
        put_result.latencies_ns.reserve(FLAGS_num_keys);

        size_t written = 0;
        size_t failed = 0;

        if (!BeginMeasurement()) return 1;
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = MakeKey(i);
            FillBuffer(i);

            auto t0 = Clock::now();
            int ret = client_->put_from(key, buffer_, FLAGS_value_size, config);
            auto t1 = Clock::now();
            if (ret == 0) AccountMeasuredIo(key, FLAGS_value_size, true);
            RecordPutResult(put_result, ElapsedNanos(t0, t1), ret);

            if (ret != 0) {
                LOG(ERROR) << "put_from failed for key=" << key
                           << " ret=" << ret;
                ++failed;
                continue;
            }
            ++written;

            if ((i + 1) % 10 == 0 || i == FLAGS_num_keys - 1) {
                double elapsed_us = NanosToUs(ElapsedNanos(t0, t1));
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys
                          << " last_latency=" << elapsed_us << " us";
            }
        }

        LOG(INFO) << "Write complete: " << written << " succeeded, " << failed
                  << " failed";
        FinishMeasurement("writer");
        put_stats.FinalizeSequential();
        put_stats.Print("PUT BENCHMARK [" + FLAGS_scenario + "]");
        const int verification = FLAGS_verify ? VerifyData() : 0;
        LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                  << " seconds for reader to connect...";
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wait_seconds));

        return mooncake::bench::ExitCode(failed, verification);
    }

    int RunReader() {
        LOG(INFO) << "=== READER MODE ===";
        LOG(INFO) << "Scenario: " << FLAGS_scenario;
        LOG(INFO) << "Reading " << FLAGS_num_keys << " keys with "
                  << FLAGS_num_threads
                  << " threads, batch_size=" << FLAGS_batch_size;

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;
        if (!EnsureNofPools()) return 1;

        if (FLAGS_scenario == "remote_memory" ||
            FLAGS_scenario == "remote_disk") {
            LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                      << " seconds for writer to finish prefill...";
            std::this_thread::sleep_for(
                std::chrono::seconds(FLAGS_wait_seconds));
        }

        int warmup_ret = DoWarmup();
        if (warmup_ret != 0) {
            LOG(WARNING) << "Warmup had errors, continuing anyway";
        }
        system("spdiag clear");

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          FLAGS_num_keys / FLAGS_num_threads);
        if (!BeginMeasurement()) return 1;

        std::latch ready_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads + 1));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads = LaunchReadWorkers(
            FLAGS_num_threads, FLAGS_num_keys, stats, ready_latch, start_latch,
            done_latch, [](size_t idx) { return MakeKey(idx); });

        ready_latch.wait();
        stats.StartTimer();
        start_latch.arrive_and_wait();
        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();

        std::string title = "READ BENCHMARK [" + FLAGS_scenario + "]";
        stats.Print(title,
                    MaxEffectiveBatchSize(FLAGS_num_keys, FLAGS_num_threads,
                                          FLAGS_batch_size),
                    FLAGS_batch_size > 1
                        ? "batch_get_into / strategy selected per batch"
                        : "get_into / per-object strategy");

        FinishMeasurement("reader");
        const int verification = FLAGS_verify ? VerifyData() : 0;
        return mooncake::bench::ExitCode(stats.total_failed(), verification);
    }

    int RunLocalMemory() {
        LOG(INFO) << "=== LOCAL MEMORY BENCHMARK ===";

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;
        if (!EnsureNofPools()) return 1;

        mooncake::ReplicateConfig config = MakeReplicateConfig();

        BenchmarkStats put_stats;
        put_stats.InitThreads(
            FLAGS_num_threads,
            (FLAGS_num_keys + FLAGS_num_threads - 1) / FLAGS_num_threads);

        LOG(INFO) << "Phase 1: Concurrent writes with " << FLAGS_num_threads
                  << " threads for " << FLAGS_num_keys << " keys";
        if (!FLAGS_verify) {
            LOG(INFO) << "Write performance mode: prefill one buffer per "
                         "thread before timing";
        }
        if (!BeginMeasurement()) return 1;

        std::latch put_ready_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch put_start_latch(
            static_cast<ptrdiff_t>(FLAGS_num_threads + 1));
        std::latch put_done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto put_threads = LaunchPutWorkers(
            FLAGS_num_threads, FLAGS_num_keys, config, put_stats,
            put_ready_latch, put_start_latch, put_done_latch,
            [](size_t idx) { return MakeKey(idx); }, WriteApi::PUT);

        put_ready_latch.wait();
        put_stats.StartTimer();
        put_start_latch.arrive_and_wait();
        put_done_latch.wait();
        put_stats.StopTimer();
        for (auto& thread : put_threads) {
            thread.join();
        }

        LOG(INFO) << "Write phase complete";
        FinishMeasurement("local_memory_write");
        put_stats.Finalize();
        put_stats.Print("LOCAL MEMORY PUT BENCHMARK",
                        MaxEffectiveBatchSize(FLAGS_num_keys, FLAGS_num_threads,
                                              FLAGS_batch_size),
                        FLAGS_batch_size > 1
                            ? "batch_put_from / automatic batch strategy"
                            : "put_from / automatic per-object strategy");
        if (put_stats.total_failed() != 0) return 1;

        int warmup_ret = DoWarmup();
        if (warmup_ret != 0) {
            LOG(WARNING) << "Warmup had errors, continuing anyway";
        }

        LOG(INFO) << "Phase 2: Concurrent reads with " << FLAGS_num_threads
                  << " threads, "
                  << (FLAGS_duration > 0
                          ? std::to_string(FLAGS_duration) + "s duration"
                          : "single pass");

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          FLAGS_num_keys / FLAGS_num_threads);
        if (!BeginMeasurement()) return 1;

        std::latch ready_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads + 1));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::atomic<bool> stop_flag{false};
        const std::atomic<bool>* read_stop_flag =
            FLAGS_duration > 0 ? &stop_flag : nullptr;
        auto threads = LaunchReadWorkers(
            FLAGS_num_threads, FLAGS_num_keys, stats, ready_latch, start_latch,
            done_latch, [](size_t idx) { return MakeKey(idx); },
            read_stop_flag);

        ready_latch.wait();
        stats.StartTimer();
        start_latch.arrive_and_wait();
        if (FLAGS_duration > 0) {
            std::this_thread::sleep_for(std::chrono::seconds(FLAGS_duration));
            stop_flag.store(true, std::memory_order_relaxed);
        }
        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();
        stats.Print("LOCAL MEMORY READ BENCHMARK",
                    MaxEffectiveBatchSize(FLAGS_num_keys, FLAGS_num_threads,
                                          FLAGS_batch_size),
                    FLAGS_batch_size > 1
                        ? "batch_get_into / automatic batch strategy"
                        : "get_into / per-object strategy");

        FinishMeasurement("local_memory_read");
        const int verification = FLAGS_verify ? VerifyData() : 0;
        return mooncake::bench::ExitCode(stats.total_failed(), verification);
    }

    int RunUpsert() {
        LOG(INFO) << "=== UPSERT BENCHMARK ===";

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;
        if (!EnsureNofPools()) return 1;

        mooncake::ReplicateConfig config = MakeReplicateConfig();
        LOG(INFO) << "Prefilling " << FLAGS_num_keys
                  << " keys outside the measured interval";
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            FillBuffer(FLAGS_num_keys + i);
            const int ret = client_->put_from(MakeUpsertKey(i), buffer_,
                                              FLAGS_value_size, config);
            if (ret != 0) {
                LOG(ERROR) << "upsert prefill put_from failed for key="
                           << MakeUpsertKey(i) << " ret=" << ret;
                return ret;
            }
        }

        // Keep transfer diagnostics focused on the measured overwrite phase.
        system("spdiag clear");

        BenchmarkStats upsert_stats;
        upsert_stats.InitThreads(
            FLAGS_num_threads,
            (FLAGS_num_keys + FLAGS_num_threads - 1) / FLAGS_num_threads);
        if (!BeginMeasurement()) return 1;

        std::latch ready_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads + 1));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads = LaunchPutWorkers(
            FLAGS_num_threads, FLAGS_num_keys, config, upsert_stats,
            ready_latch, start_latch, done_latch,
            [](size_t idx) { return MakeUpsertKey(idx); }, WriteApi::UPSERT);

        ready_latch.wait();
        upsert_stats.StartTimer();
        start_latch.arrive_and_wait();
        done_latch.wait();
        upsert_stats.StopTimer();
        for (auto& thread : threads) thread.join();

        FinishMeasurement("upsert");
        upsert_stats.Finalize();
        upsert_stats.Print(
            "UPSERT BENCHMARK",
            MaxEffectiveBatchSize(FLAGS_num_keys, FLAGS_num_threads,
                                  FLAGS_batch_size),
            FLAGS_batch_size > 1
                ? "batch_upsert_from / automatic batch strategy"
                : "upsert_from / per-object strategy");
        const int verification =
            FLAGS_verify
                ? VerifyData([](size_t idx) { return MakeUpsertKey(idx); })
                : 0;
        return mooncake::bench::ExitCode(upsert_stats.total_failed(),
                                         verification);
    }

    int RunLocalDisk() {
        LOG(INFO) << "=== LOCAL DISK BENCHMARK ===";
        LOG(INFO) << "NOTE: Disk reads require Master with enable_offload=true "
                  << "and client with enable_ssd_offload=true";

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;
        if (!EnsureNofPools()) return 1;

        mooncake::ReplicateConfig config = MakeReplicateConfig();

        BenchmarkStats put_stats;
        put_stats.InitThreads(1, FLAGS_num_keys);
        ThreadResult& put_result = put_stats.GetThreadResult(0);
        put_result.latencies_ns.reserve(FLAGS_num_keys);

        LOG(INFO) << "Phase 1: Writing " << FLAGS_num_keys
                  << " keys (data may be offloaded to SSD)...";
        int put_ret = 0;
        if (!BeginMeasurement()) return 1;
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = MakeKey(i);
            FillBuffer(i);
            auto t0 = Clock::now();
            int ret = client_->put_from(key, buffer_, FLAGS_value_size, config);
            auto t1 = Clock::now();
            if (ret == 0) AccountMeasuredIo(key, FLAGS_value_size, true);
            RecordPutResult(put_result, ElapsedNanos(t0, t1), ret);
            if (ret != 0) {
                LOG(ERROR) << "put_from failed for key=" << key;
                put_ret = ret;
                break;
            }
            if ((i + 1) % 50 == 0) {
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys;
            }
        }
        LOG(INFO) << "Write phase complete";
        FinishMeasurement("local_disk_write");
        put_stats.FinalizeSequential();
        put_stats.Print("LOCAL DISK PUT BENCHMARK");
        if (put_ret != 0) return put_ret;

        LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                  << " seconds for offload/eviction to complete...";
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wait_seconds));

        int warmup_ret = DoWarmup();
        if (warmup_ret != 0) {
            LOG(WARNING) << "Warmup had errors, continuing anyway";
        }

        LOG(INFO) << "Phase 2: Concurrent disk reads with " << FLAGS_num_threads
                  << " threads";

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          FLAGS_num_keys / FLAGS_num_threads);
        if (!BeginMeasurement()) return 1;

        std::latch ready_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads + 1));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads = LaunchReadWorkers(
            FLAGS_num_threads, FLAGS_num_keys, stats, ready_latch, start_latch,
            done_latch, [](size_t idx) { return MakeKey(idx); });

        ready_latch.wait();
        stats.StartTimer();
        start_latch.arrive_and_wait();
        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();
        stats.Print("LOCAL DISK READ BENCHMARK",
                    MaxEffectiveBatchSize(FLAGS_num_keys, FLAGS_num_threads,
                                          FLAGS_batch_size),
                    FLAGS_batch_size > 1
                        ? "batch_get_into / strategy selected per batch"
                        : "get_into / per-object strategy");

        FinishMeasurement("local_disk_read");
        const int verification = FLAGS_verify ? VerifyData() : 0;
        return mooncake::bench::ExitCode(stats.total_failed(), verification);
    }

    static std::vector<std::string> ParseSegments(const std::string& value) {
        std::vector<std::string> segments;
        std::istringstream iss(value);
        std::string seg;
        while (std::getline(iss, seg, ',')) {
            size_t start = seg.find_first_not_of(" \t");
            size_t end = seg.find_last_not_of(" \t");
            if (start != std::string::npos && end != std::string::npos) {
                segments.push_back(seg.substr(start, end - start + 1));
            }
        }
        return segments;
    }

    static std::string SanitizeSegmentName(const std::string& segment) {
        static const char* kSpecialChars = ".:-/\\[]{}()@#$%^&*+=|<>,;!?`'\"~";
        std::string sanitized = segment;
        for (char& c : sanitized) {
            if (std::strchr(kSpecialChars, c) != nullptr || std::isspace(c)) {
                c = '_';
            }
        }
        return sanitized;
    }

    static std::string MakeSegmentKey(const BenchmarkSegment& segment,
                                      size_t idx) {
        // Keep memory keys compatible with earlier benchmark runs.
        const char* prefix =
            segment.kind == SegmentKind::NOF ? "nof_seg_" : "seg_";
        std::string key = std::string(prefix) +
                          SanitizeSegmentName(segment.name) + "_key_" +
                          std::to_string(idx);
        if (FLAGS_key_include_value_size) {
            key += "_vs" + std::to_string(FLAGS_value_size);
        }
        return key;
    }

    // Key generator for remove/batch_remove scenarios. Uses a distinct
    // "rmv_" prefix so remove keys never collide with segment_write keys
    // ("seg_"). This allows remove and write benchmarks to run independently
    // without cross-contamination.
    static std::string MakeRemoveKey(const BenchmarkSegment& segment,
                                     size_t idx) {
        const char* prefix =
            segment.kind == SegmentKind::NOF ? "nof_rmv_" : "rmv_";
        return std::string(prefix) + SanitizeSegmentName(segment.name) +
               "_key_" + std::to_string(idx);
    }

    int RunSegmentWrite() {
        auto segments = DiscoverBenchmarkSegments();
        if (segments.empty()) {
            return -1;
        }
        LOG(INFO) << "Discovered " << segments.size()
                  << " segments from master";

        LOG(INFO) << "=== SEGMENT WRITE MODE ===";
        LOG(INFO)
            << "PUT measurement uses one serial API stream; --num_threads "
               "controls concurrent reads/removes, not PUT concurrency";
        LOG(INFO) << "Writing to " << segments.size() << " segments, "
                  << FLAGS_num_keys << " keys per segment (interleaved), each "
                  << FLAGS_value_size / MB << " MiB";

        std::vector<size_t> seg_written(segments.size(), 0);
        std::vector<size_t> seg_failed(segments.size(), 0);
        std::vector<mooncake::ReplicateConfig> configs(segments.size());
        for (size_t s = 0; s < segments.size(); ++s) {
            configs[s] = MakeSegmentReplicateConfig(segments[s]);
        }

        {
            size_t memory_cnt = 0;
            size_t nof_cnt = 0;
            for (const auto& segment : segments) {
                if (segment.kind == SegmentKind::NOF) {
                    ++nof_cnt;
                } else {
                    ++memory_cnt;
                }
            }
            LOG(INFO) << "Replica isolation active: memory segment put -> "
                      << "memory replicas only (--replica_num="
                      << FLAGS_replica_num << ", nof=0); NoF segment put -> "
                      << "NoF replicas only (--nof_replica_num="
                      << FLAGS_nof_replica_num
                      << ", mem=0). Segments: " << memory_cnt << " memory, "
                      << nof_cnt << " NoF";
        }

        BenchmarkStats put_stats;
        put_stats.InitThreads(1, FLAGS_num_keys * segments.size());
        ThreadResult& put_result = put_stats.GetThreadResult(0);
        put_result.latencies_ns.reserve(FLAGS_num_keys * segments.size());

        size_t total_written = 0;
        size_t total_failed = 0;

        if (!BeginMeasurement()) return 1;
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            for (size_t s = 0; s < segments.size(); ++s) {
                const auto& segment = segments[s];
                std::string key = MakeSegmentKey(segment, i);
                FillBuffer(i);

                auto t0 = Clock::now();
                int ret = client_->put_from(key, buffer_, FLAGS_value_size,
                                            configs[s]);
                auto t1 = Clock::now();
                if (ret == 0) AccountMeasuredIo(key, FLAGS_value_size, true);
                if (ret == 0 && FLAGS_strict_segment_placement &&
                    !VerifyPlacement(key, segment)) {
                    ret = -1;
                }
                RecordPutResult(put_result, ElapsedNanos(t0, t1), ret);

                if (ret != 0) {
                    LOG(ERROR) << "put_from failed for key=" << key
                               << " segment=" << segment.name
                               << " type=" << SegmentKindName(segment.kind)
                               << " ret=" << ret;
                    ++seg_failed[s];
                    continue;
                }
                ++seg_written[s];
            }

            if ((i + 1) % 10 == 0 || i == FLAGS_num_keys - 1) {
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys
                          << " keys to all " << segments.size() << " segments";
            }
        }

        for (size_t s = 0; s < segments.size(); ++s) {
            total_written += seg_written[s];
            total_failed += seg_failed[s];
            LOG(INFO) << "Requested segment [" << s << "] " << segments[s].name
                      << " type=" << SegmentKindName(segments[s].kind)
                      << " complete: " << seg_written[s] << " succeeded, "
                      << seg_failed[s] << " failed";
        }

        LOG(INFO) << "All segments write complete: " << total_written
                  << " succeeded, " << total_failed << " failed";
        FinishMeasurement("segment_write");
        put_stats.FinalizeSequential();
        put_stats.Print("SEGMENT PUT BENCHMARK");
        const int verification = FLAGS_verify ? VerifySegments(segments) : 0;

        LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                  << " seconds for reader to connect...";
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wait_seconds));

        return mooncake::bench::ExitCode(total_failed, verification);
    }

    int RunSegmentRead() {
        auto segments = DiscoverBenchmarkSegments();
        if (segments.empty()) {
            return -1;
        }
        LOG(INFO) << "Discovered " << segments.size()
                  << " segments from master";

        size_t read_segment_nums = FLAGS_read_segment_nums;
        if (read_segment_nums == 0 || read_segment_nums > segments.size()) {
            read_segment_nums = segments.size();
        }

        std::vector<BenchmarkSegment> read_segments(
            segments.begin(), segments.begin() + read_segment_nums);

        if (VerifyReadPlacementEnabled() &&
            !ConfigureReadPlacementValidation(read_segments)) {
            return 1;
        }

        LOG(INFO) << "=== SEGMENT READ MODE ===";
        LOG(INFO) << "Reading keys for " << read_segment_nums << " segments";
        for (size_t s = 0; s < read_segments.size(); ++s) {
            LOG(INFO) << "  Segment [" << s << "]: " << read_segments[s].name
                      << " (" << SegmentKindName(read_segments[s].kind) << ")";
        }
        LOG(INFO) << "Keys per segment: " << FLAGS_num_keys;
        LOG(INFO) << "Duration: "
                  << (FLAGS_duration > 0 ? std::to_string(FLAGS_duration) + "s"
                                         : "single pass");
        LOG(INFO) << "Stats interval: " << FLAGS_statis_interval << "s";

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;
        if (!EnsureNofPools()) return 1;

        std::vector<std::string> all_keys;
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            for (size_t s = 0; s < read_segments.size(); ++s) {
                all_keys.push_back(MakeSegmentKey(read_segments[s], i));
            }
        }
        if (FLAGS_shuffle_seed != 0) {
            std::mt19937_64 rng(FLAGS_shuffle_seed);
            std::shuffle(all_keys.begin(), all_keys.end(), rng);
            LOG(INFO) << "Shuffled segment_read keys with seed="
                      << FLAGS_shuffle_seed;
        }
        LOG(INFO) << "Total keys to read: " << all_keys.size();

        size_t warmup_end =
            std::min(static_cast<size_t>(FLAGS_warmup_keys), all_keys.size());
        if (warmup_end > 0) {
            LOG(INFO) << "Warmup: reading " << warmup_end << " keys...";
            char* warmup_buffer = GetWarmupBuffer();
            for (size_t i = 0; i < warmup_end; ++i) {
                int64_t ret = client_->get_into(all_keys[i], warmup_buffer,
                                                FLAGS_value_size);
                if (ret < 0) {
                    LOG(WARNING)
                        << "Warmup get_into failed for key=" << all_keys[i]
                        << " ret=" << ret;
                }
            }
            LOG(INFO) << "Warmup complete";
        }

        if (FLAGS_duration == 0) {
            return RunSegmentReadSinglePass(read_segments, all_keys);
        }
        return RunSegmentReadDuration(read_segments, all_keys);
    }

    int RunSegmentReadSinglePass(
        const std::vector<BenchmarkSegment>& read_segments,
        const std::vector<std::string>& all_keys) {
        LOG(INFO) << "Single-pass read with " << FLAGS_num_threads
                  << " threads";

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          all_keys.size() / FLAGS_num_threads);
        if (!BeginMeasurement()) return 1;

        std::latch ready_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads + 1));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads = LaunchReadWorkers(
            FLAGS_num_threads, all_keys.size(), stats, ready_latch, start_latch,
            done_latch, [&all_keys](size_t idx) {
                return all_keys[idx % all_keys.size()];
            });

        ready_latch.wait();
        stats.StartTimer();
        start_latch.arrive_and_wait();
        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();

        std::string title = "SEGMENT READ BENCHMARK [segments=" +
                            std::to_string(read_segments.size()) + "]";
        stats.Print(title,
                    MaxEffectiveBatchSize(all_keys.size(), FLAGS_num_threads,
                                          FLAGS_batch_size),
                    FLAGS_batch_size > 1
                        ? "batch_get_into / strategy selected per batch"
                        : "get_into / per-object strategy");
        FinishMeasurement("segment_read");
        const int verification =
            FLAGS_verify ? VerifySegments(read_segments) : 0;
        return mooncake::bench::ExitCode(stats.total_failed(), verification);
    }

    struct IntervalLatencyStats {
        mooncake::bench::LatencyHistogram histogram;
        int64_t min_latency_ns = std::numeric_limits<int64_t>::max();
        int64_t max_latency_ns = 0;
        double p50_latency_ns = 0;
        double p90_latency_ns = 0;
        double p99_latency_ns = 0;
        double p999_latency_ns = 0;
        double p9999_latency_ns = 0;
        double avg_latency_ns = 0;

        void Finalize() {
            total_samples = histogram.Count();
            min_latency_ns = histogram.MinNs();
            max_latency_ns = histogram.MaxNs();
            avg_latency_ns = histogram.MeanNs();
            p50_latency_ns = histogram.PercentileNs(0.50);
            p90_latency_ns = histogram.PercentileNs(0.90);
            p99_latency_ns = histogram.PercentileNs(0.99);
            p999_latency_ns = histogram.PercentileNs(0.999);
            p9999_latency_ns = histogram.PercentileNs(0.9999);
        }

        size_t total_samples = 0;
    };

    int RunSegmentReadDuration(
        const std::vector<BenchmarkSegment>& read_segments,
        const std::vector<std::string>& all_keys) {
        LOG(INFO) << "Duration-based continuous read with " << FLAGS_num_threads
                  << " threads for " << FLAGS_duration << "s, stats every "
                  << FLAGS_statis_interval << "s";

        if (!BeginMeasurement()) return 1;
        std::atomic<bool> stop_flag{false};
        std::atomic<size_t> global_keys{0};
        std::atomic<size_t> global_queries{0};
        std::atomic<size_t> global_bytes{0};
        std::atomic<size_t> global_failed{0};

        std::vector<mooncake::bench::LatencyHistogram> thread_latencies(
            FLAGS_num_threads);
        std::vector<std::mutex> latency_mutexes(FLAGS_num_threads);

        std::latch ready_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads + 1));
        std::vector<std::thread> threads;

        size_t total_keys = all_keys.size();
        size_t keys_per_thread =
            (total_keys + FLAGS_num_threads - 1) / FLAGS_num_threads;

        for (size_t t = 0; t < FLAGS_num_threads; ++t) {
            threads.emplace_back([&, t, keys_per_thread, total_keys]() {
                bindToSocket(t % NR_SOCKETS);
                char* my_buf = thread_buffers_[t].ptr;

                ready_latch.count_down();
                start_latch.arrive_and_wait();

                size_t key_offset = t * keys_per_thread;
                size_t key_idx = key_offset;

                if (FLAGS_batch_size <= 1) {
                    while (!stop_flag.load(std::memory_order_relaxed)) {
                        const std::string& key = all_keys[key_idx % total_keys];
                        auto t0 = Clock::now();
                        int64_t ret =
                            client_->get_into(key, my_buf, FLAGS_value_size);
                        auto t1 = Clock::now();
                        int64_t latency_ns = ElapsedNanos(t0, t1);

                        {
                            std::lock_guard<std::mutex> lock(
                                latency_mutexes[t]);
                            thread_latencies[t].Record(latency_ns);
                        }

                        AccountMeasuredIo(key, ret, false);
                        if (!ValidateMeasuredRead(key, my_buf, ret)) {
                            global_failed.fetch_add(1,
                                                    std::memory_order_relaxed);
                        } else {
                            global_bytes.fetch_add(static_cast<size_t>(ret),
                                                   std::memory_order_relaxed);
                        }
                        global_keys.fetch_add(1, std::memory_order_relaxed);
                        global_queries.fetch_add(1, std::memory_order_relaxed);
                        ++key_idx;
                    }
                } else {
                    size_t per_key_buf = FLAGS_value_size;
                    while (!stop_flag.load(std::memory_order_relaxed)) {
                        std::vector<std::string> keys;
                        std::vector<void*> bufs;
                        std::vector<size_t> sizes;
                        keys.reserve(FLAGS_batch_size);
                        bufs.reserve(FLAGS_batch_size);
                        sizes.reserve(FLAGS_batch_size);

                        for (size_t b = 0; b < FLAGS_batch_size; ++b) {
                            const std::string& key =
                                all_keys[key_idx % total_keys];
                            keys.push_back(key);
                            bufs.push_back(my_buf + b * per_key_buf);
                            sizes.push_back(FLAGS_value_size);
                            ++key_idx;
                        }

                        auto t0 = Clock::now();
                        auto results =
                            client_->batch_get_into(keys, bufs, sizes);
                        auto t1 = Clock::now();
                        int64_t latency_ns = ElapsedNanos(t0, t1);

                        {
                            std::lock_guard<std::mutex> lock(
                                latency_mutexes[t]);
                            thread_latencies[t].Record(latency_ns);
                        }

                        for (size_t k = 0; k < keys.size(); ++k) {
                            const int64_t ret =
                                k < results.size() ? results[k] : -1;
                            AccountMeasuredIo(keys[k], ret, false);
                            if (!ValidateMeasuredRead(keys[k], bufs[k], ret)) {
                                global_failed.fetch_add(
                                    1, std::memory_order_relaxed);
                            } else {
                                global_bytes.fetch_add(
                                    static_cast<size_t>(results[k]),
                                    std::memory_order_relaxed);
                            }
                            global_keys.fetch_add(1, std::memory_order_relaxed);
                        }
                        global_queries.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }

        ready_latch.wait();
        auto bench_start = Clock::now();
        start_latch.arrive_and_wait();
        auto bench_end = bench_start + std::chrono::seconds(FLAGS_duration);
        auto next_statis =
            bench_start + std::chrono::seconds(FLAGS_statis_interval);

        size_t prev_keys = 0;
        size_t prev_queries = 0;
        size_t prev_bytes = 0;
        size_t prev_failed = 0;
        auto prev_time = bench_start;

        IntervalLatencyStats overall;
        double peak_throughput_mbps = 0;
        double cur_throughput_mbps = 0;

        std::cout << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  SEGMENT READ DURATION BENCHMARK [segments="
                  << read_segments.size() << "]\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << std::fixed << std::setprecision(2);

        while (Clock::now() < bench_end) {
            auto now = Clock::now();
            if (now >= next_statis) {
                size_t cur_keys = global_keys.load(std::memory_order_relaxed);
                size_t cur_queries =
                    global_queries.load(std::memory_order_relaxed);
                size_t cur_bytes = global_bytes.load(std::memory_order_relaxed);
                size_t cur_failed =
                    global_failed.load(std::memory_order_relaxed);

                double interval_sec = NanosToSec(ElapsedNanos(prev_time, now));
                size_t interval_keys = cur_keys - prev_keys;
                size_t interval_queries = cur_queries - prev_queries;
                size_t interval_bytes = cur_bytes - prev_bytes;
                size_t interval_failed = cur_failed - prev_failed;

                double interval_throughput_mbps =
                    (interval_sec > 0)
                        ? (static_cast<double>(interval_bytes) / MB) /
                              interval_sec
                        : 0;
                double interval_keys_per_sec =
                    (interval_sec > 0)
                        ? static_cast<double>(interval_keys) / interval_sec
                        : 0;
                double interval_queries_per_sec =
                    (interval_sec > 0)
                        ? static_cast<double>(interval_queries) / interval_sec
                        : 0;

                IntervalLatencyStats interval_stats;
                for (size_t t = 0; t < FLAGS_num_threads; ++t) {
                    std::lock_guard<std::mutex> lock(latency_mutexes[t]);
                    interval_stats.histogram.Merge(thread_latencies[t]);
                    thread_latencies[t] = {};
                }
                interval_stats.Finalize();
                overall.histogram.Merge(interval_stats.histogram);
                peak_throughput_mbps =
                    std::max(peak_throughput_mbps, interval_throughput_mbps);
                cur_throughput_mbps = interval_throughput_mbps;

                double total_sec = NanosToSec(ElapsedNanos(bench_start, now));
                double total_throughput_mbps =
                    (total_sec > 0)
                        ? (static_cast<double>(cur_bytes) / MB) / total_sec
                        : 0;
                double total_keys_per_sec =
                    (total_sec > 0) ? static_cast<double>(cur_keys) / total_sec
                                    : 0;
                double total_queries_per_sec =
                    (total_sec > 0)
                        ? static_cast<double>(cur_queries) / total_sec
                        : 0;

                std::cout << "  [t=" << std::setw(6) << total_sec << "s]"
                          << "  interval: " << interval_throughput_mbps
                          << " MiB/s, " << interval_keys_per_sec << " keys/s, "
                          << interval_queries_per_sec << " qps"
                          << " (failed=" << interval_failed << ")"
                          << "  lat[us]: avg="
                          << NanosToUs(interval_stats.avg_latency_ns)
                          << ", P50="
                          << NanosToUs(interval_stats.p50_latency_ns)
                          << ", P90="
                          << NanosToUs(interval_stats.p90_latency_ns)
                          << ", P99="
                          << NanosToUs(interval_stats.p99_latency_ns)
                          << "  total: " << cur_queries << " queries, "
                          << cur_keys << " keys, " << total_throughput_mbps
                          << " MiB/s, " << total_keys_per_sec << " keys/s, "
                          << total_queries_per_sec << " qps"
                          << " (failed=" << cur_failed << ")\n";

                prev_keys = cur_keys;
                prev_queries = cur_queries;
                prev_bytes = cur_bytes;
                prev_failed = cur_failed;
                prev_time = now;
                next_statis += std::chrono::seconds(FLAGS_statis_interval);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        stop_flag.store(true, std::memory_order_relaxed);
        for (auto& th : threads) {
            th.join();
        }

        auto final_time = Clock::now();
        double total_sec = NanosToSec(ElapsedNanos(bench_start, final_time));
        size_t final_keys = global_keys.load(std::memory_order_relaxed);
        size_t final_queries = global_queries.load(std::memory_order_relaxed);
        size_t final_bytes = global_bytes.load(std::memory_order_relaxed);
        size_t final_failed = global_failed.load(std::memory_order_relaxed);

        double final_throughput_mbps =
            (total_sec > 0)
                ? (static_cast<double>(final_bytes) / MB) / total_sec
                : 0;
        double final_keys_per_sec =
            (total_sec > 0) ? static_cast<double>(final_keys) / total_sec : 0;
        double final_queries_per_sec =
            (total_sec > 0) ? static_cast<double>(final_queries) / total_sec
                            : 0;

        // Workers have joined: include the final partial interval and requests
        // that completed after the stop flag, using the same wall-time bound.
        for (const auto& histogram : thread_latencies) {
            overall.histogram.Merge(histogram);
        }
        overall.Finalize();
        const double tail_sec = NanosToSec(ElapsedNanos(prev_time, final_time));
        if (tail_sec > 0 && final_queries > prev_queries) {
            cur_throughput_mbps =
                static_cast<double>(final_bytes - prev_bytes) / MB / tail_sec;
            peak_throughput_mbps =
                std::max(peak_throughput_mbps, cur_throughput_mbps);
        }
        size_t total_latency_samples = overall.total_samples;

        std::cout << "\n  FINAL SUMMARY\n";
        std::cout << "  Parameters:\n";
        std::cout << "    Scenario:       " << FLAGS_scenario << "\n";
        std::cout << "    Protocol:       " << FLAGS_protocol << "\n";
        std::cout << "    GPU mode:       " << FLAGS_gpu_mode << "\n";
        std::cout << "    GPU device:     " << FLAGS_gpu_device << "\n";
        std::cout << "    Value size:     " << FLAGS_value_size / MB
                  << " MiB\n";
        std::cout << "    Batch size:     " << FLAGS_batch_size << "\n";
        std::cout << "    Num threads:    " << FLAGS_num_threads << "\n";
        std::cout << "    Num keys:       " << FLAGS_num_keys << "\n";

        std::cout << "\n  Total time:       " << total_sec << " s\n";
        std::cout << "  Total queries:    " << final_queries
                  << " (failed: " << final_failed << ")\n";
        std::cout << "  Total keys:       " << final_keys << "\n";
        std::cout << "  Total data:       " << FormatBytes(final_bytes) << "\n";
        std::cout << "  Throughput (peak): " << peak_throughput_mbps
                  << " MiB/s\n";
        std::cout << "  Throughput (avg):  " << final_throughput_mbps
                  << " MiB/s";
        if (final_throughput_mbps > 1024) {
            std::cout << " (" << final_throughput_mbps / 1024 << " GiB/s)";
        }
        std::cout << "\n";
        std::cout << "  Throughput (cur):  " << cur_throughput_mbps
                  << " MiB/s\n";
        std::cout << "  Keys/sec:         " << final_keys_per_sec << "\n";
        std::cout << "  Queries/sec:      " << final_queries_per_sec << "\n";

        if (total_latency_samples > 0) {
            std::cout << "  Percentiles: merged histogram upper bounds "
                         "(bucket width <= 1.6%)\n";
            std::cout << "\n  Latency (us)      [n=" << total_latency_samples
                      << ", per-query]\n";
            std::cout << "    Min:   " << std::setw(12)
                      << NanosToUs(overall.min_latency_ns) << "\n";
            std::cout << "    Avg:   " << std::setw(12)
                      << NanosToUs(overall.avg_latency_ns) << "\n";
            std::cout << "    P50:   " << std::setw(12)
                      << NanosToUs(overall.p50_latency_ns) << "\n";
            std::cout << "    P90:   " << std::setw(12)
                      << NanosToUs(overall.p90_latency_ns) << "\n";
            std::cout << "    P99:   " << std::setw(12)
                      << NanosToUs(overall.p99_latency_ns);
            if (total_latency_samples < 100) std::cout << "  (n<100)";
            std::cout << "\n";
            std::cout << "    P999:  " << std::setw(12)
                      << NanosToUs(overall.p999_latency_ns);
            if (total_latency_samples < 1000) std::cout << "  (n<1000)";
            std::cout << "\n";
            std::cout << "    P9999: " << std::setw(12)
                      << NanosToUs(overall.p9999_latency_ns);
            if (total_latency_samples < 10000) std::cout << "  (n<10000)";
            std::cout << "\n";
            std::cout << "    Max:   " << std::setw(12)
                      << NanosToUs(overall.max_latency_ns) << "\n";
        }

        std::cout << "========================================"
                  << "========================================\n\n";

        FinishMeasurement("segment_read_duration");
        const int verification =
            FLAGS_verify ? VerifySegments(read_segments) : 0;
        return mooncake::bench::ExitCode(final_failed, verification);
    }

    int RunSegmentRemove(bool use_batch) {
        auto segments = DiscoverBenchmarkSegments();
        if (segments.empty()) {
            return -1;
        }
        LOG(INFO) << "Discovered " << segments.size()
                  << " segments from master";

        size_t remove_segment_nums = FLAGS_read_segment_nums;
        if (remove_segment_nums == 0 || remove_segment_nums > segments.size()) {
            remove_segment_nums = segments.size();
        }
        std::vector<BenchmarkSegment> remove_segments(
            segments.begin(), segments.begin() + remove_segment_nums);

        LOG(INFO) << "=== SEGMENT REMOVE MODE ==="
                  << (use_batch ? " (batch)" : " (single key)");
        LOG(INFO) << "Removing keys for " << remove_segment_nums << " segments";
        for (size_t s = 0; s < remove_segments.size(); ++s) {
            LOG(INFO) << "  Segment [" << s << "]: " << remove_segments[s].name
                      << " (" << SegmentKindName(remove_segments[s].kind)
                      << ")";
        }
        LOG(INFO) << "Keys per segment: " << FLAGS_num_keys;
        LOG(INFO) << "Batch size: " << FLAGS_batch_size;

        if (FLAGS_duration > 0) {
            LOG(WARNING) << "--duration is ignored for remove scenarios: "
                         << "removal is not idempotent, a single pass is used";
        }

        // Phase 1: prefill each segment with the same key layout and placement
        // preferences as RunSegmentWrite (with the same per-segment replica
        // isolation so prefill matches segment_write measurements).
        std::vector<mooncake::ReplicateConfig> configs(remove_segments.size());
        for (size_t s = 0; s < remove_segments.size(); ++s) {
            configs[s] = MakeSegmentReplicateConfig(remove_segments[s]);
        }

        BenchmarkStats put_stats;
        put_stats.InitThreads(1, FLAGS_num_keys * remove_segments.size());
        ThreadResult& put_result = put_stats.GetThreadResult(0);
        put_result.latencies_ns.reserve(FLAGS_num_keys *
                                        remove_segments.size());

        LOG(INFO) << "Phase 1: Prefilling " << FLAGS_num_keys << " keys to "
                  << remove_segment_nums << " segments (interleaved), each "
                  << FLAGS_value_size / MB << " MiB";
        if (!BeginMeasurement()) return 1;
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            for (size_t s = 0; s < remove_segments.size(); ++s) {
                const auto& segment = remove_segments[s];
                std::string key = MakeRemoveKey(segment, i);
                FillBuffer(i);
                auto t0 = Clock::now();
                int ret = client_->put_from(key, buffer_, FLAGS_value_size,
                                            configs[s]);
                auto t1 = Clock::now();
                if (ret == 0) AccountMeasuredIo(key, FLAGS_value_size, true);
                if (ret == 0 && FLAGS_strict_segment_placement &&
                    !VerifyPlacement(key, segment)) {
                    ret = -1;
                }
                RecordPutResult(put_result, ElapsedNanos(t0, t1), ret);
                if (ret != 0) {
                    LOG(ERROR) << "put_from failed for key=" << key
                               << " segment=" << segment.name
                               << " type=" << SegmentKindName(segment.kind)
                               << " ret=" << ret;
                    put_stats.FinalizeSequential();
                    put_stats.Print("REMOVE PREFILL PUT BENCHMARK");
                    return ret;
                }
            }
            if ((i + 1) % 10 == 0 || i == FLAGS_num_keys - 1) {
                LOG(INFO) << "  Prefilled " << (i + 1) << "/" << FLAGS_num_keys
                          << " keys to all " << remove_segment_nums
                          << " segments";
            }
        }
        FinishMeasurement("remove_prefill");
        LOG(INFO) << "Prefill phase complete";
        put_stats.FinalizeSequential();
        put_stats.Print("REMOVE PREFILL PUT BENCHMARK");
        if (FLAGS_verify && VerifySegments(remove_segments, true) != 0) {
            return 1;
        }

        // Phase 2: assemble the full key list in the same order as
        // RunSegmentRead (outer key index, inner segment).
        std::vector<std::string> all_keys;
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            for (size_t s = 0; s < remove_segments.size(); ++s) {
                all_keys.push_back(MakeRemoveKey(remove_segments[s], i));
            }
        }
        LOG(INFO) << "Total keys to remove: " << all_keys.size();

        // Phase 3: concurrent remove (single pass, mirrors segment_read).
        LOG(INFO) << "Phase 3: Concurrent " << (use_batch ? "batch " : "")
                  << "remove with " << FLAGS_num_threads << " threads";

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          all_keys.size() / FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads = LaunchRemoveWorkers(
            FLAGS_num_threads, all_keys.size(), stats, start_latch, done_latch,
            use_batch, [&all_keys](size_t idx) {
                return all_keys[idx % all_keys.size()];
            });

        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();
        stats.Print(use_batch ? "SEGMENT BATCH REMOVE BENCHMARK"
                              : "SEGMENT REMOVE BENCHMARK");

        return mooncake::bench::ExitCode(stats.total_failed());
    }

    int RunListSegments() {
        LOG(INFO) << "Discovering segments from master at "
                  << FLAGS_master_server << ":" << FLAGS_master_admin_port;

        auto memory_segments = DiscoverSegmentsFromMaster(
            FLAGS_master_server, static_cast<int>(FLAGS_master_admin_port));
        bool nof_discovery_succeeded = false;
        auto nof_segments = DiscoverNoFSegmentsFromMaster(
            FLAGS_master_server, &nof_discovery_succeeded);
        if (!nof_discovery_succeeded) return -1;
        std::sort(memory_segments.begin(), memory_segments.end());

        if (memory_segments.empty() && nof_segments.empty()) {
            LOG(ERROR) << "No memory or NoF segments discovered from master";
            return -1;
        }

        std::cout << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  DISCOVERED SEGMENTS [memory=" << memory_segments.size()
                  << ", NoF=" << nof_segments.size() << "]\n";
        std::cout << "========================================"
                  << "========================================\n";

        for (size_t i = 0; i < memory_segments.size(); ++i) {
            std::cout << "  [memory " << std::setw(4) << i << "] "
                      << memory_segments[i] << "\n";
        }
        for (size_t i = 0; i < nof_segments.size(); ++i) {
            std::cout << "  [NoF    " << std::setw(4) << i << "] "
                      << nof_segments[i] << "\n";
        }

        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  Total segments: "
                  << memory_segments.size() + nof_segments.size() << "\n";
        std::cout << "========================================"
                  << "========================================\n\n";

        return 0;
    }

    int Run() {
        if (FLAGS_scenario == "local_memory") {
            return RunLocalMemory();
        } else if (FLAGS_scenario == "upsert") {
            return RunUpsert();
        } else if (FLAGS_scenario == "local_disk") {
            return RunLocalDisk();
        } else if (FLAGS_scenario == "segment_write") {
            return RunSegmentWrite();
        } else if (FLAGS_scenario == "segment_read") {
            return RunSegmentRead();
        } else if (FLAGS_scenario == "list_segments") {
            return RunListSegments();
        } else if (FLAGS_scenario == "remove") {
            return RunSegmentRemove(false);
        } else if (FLAGS_scenario == "batch_remove") {
            return RunSegmentRemove(true);
        } else if (FLAGS_scenario == "remote_memory" ||
                   FLAGS_scenario == "remote_disk") {
            if (FLAGS_role == "writer") {
                return RunWriter();
            } else {
                return RunReader();
            }
        } else {
            LOG(ERROR) << "Unknown scenario: " << FLAGS_scenario;
            return -1;
        }
    }

    bool DirectAcceptancePassed() const { return direct_acceptance_passed_; }

   private:
    enum class WriteApi { PUT, UPSERT };

    bool EnsureNofPools() {
        if (!FLAGS_nof_whole_pool_register || FLAGS_nof_replica_num == 0) {
            return true;
        }
#if defined(USE_NOF_URMA)
        if (!mooncake::NofWholePoolRegistrationAvailable()) {
            if (!nof_pool_fallback_logged_) {
                LOG(ERROR)
                    << "NoF whole-pool registration is unavailable in this "
                       "SPDK build; refusing per-I/O registration because "
                       "large batches can exceed the URMA registration cache";
                nof_pool_fallback_logged_ = true;
            }
            return false;
        }
        if (!EnsurePlacementClient()) {
            return false;
        }
        auto segments = placement_client_->GetAllNoFSegments();
        if (!segments || segments->empty()) return false;
        const auto requested = ParseSegments(FLAGS_nof_segments);
        std::vector<std::pair<void*, size_t>> buffers;
        buffers.emplace_back(buffer_,
                             gpu_config_.mode == "gdr-peermem"
                                 ? (1 + FLAGS_num_threads) * buffer_size_
                                 : buffer_size_);
        if (gpu_config_.mode != "gdr-peermem") {
            for (const auto& buffer : thread_buffers_) {
                if (buffer.ptr) buffers.emplace_back(buffer.ptr, buffer.size);
            }
        }
        for (const auto& segment : *segments) {
            if (!requested.empty() && FLAGS_strict_segment_placement &&
                std::find(requested.begin(), requested.end(), segment.name) ==
                    requested.end()) {
                continue;
            }
            if (!IsUrmaEndpoint(segment.te_endpoint)) {
                VLOG(1) << "Skipping whole-pool registration for non-URMA NoF "
                           "segment "
                        << segment.name;
                continue;
            }
            for (const auto& [buffer, requested_size] : buffers) {
                const auto id =
                    segment.te_endpoint + ":buffer=" +
                    std::to_string(reinterpret_cast<uintptr_t>(buffer));
                if (registered_nof_pools_.count(id)) continue;
                // ub_allocate_gpu_hbm owns a padded 64 KiB allocation.
                const auto size =
                    gpu_config_.use_gpu_buffer
                        ? (requested_size + 65535) & ~size_t{65535}
                        : requested_size;
                if (!nof_runtime_.initiator) {
                    nof_runtime_ = mooncake::CreateNofRuntime();
                }
                if (!nof_runtime_.initiator) {
                    LOG(ERROR) << "NoF whole-pool registration requires an "
                                  "available NoF initiator";
                    return false;
                }
                auto token = nof_runtime_.initiator->RegisterMemoryForEndpoint(
                    segment.te_endpoint, buffer, size);
                if (!token) {
                    LOG(ERROR)
                        << "NoF whole-pool registration was available but "
                           "failed; refusing unsafe per-I/O fallback for "
                        << segment.name;
                    return false;
                }
                nof_pool_tokens_.push_back(std::move(token));
                registered_nof_pools_.insert(id);
                LOG(INFO) << "NoF whole-pool registered segment="
                          << segment.name << " size=" << size;
            }
        }
        return true;
#else
        if (!nof_pool_fallback_logged_) {
            LOG(ERROR) << "NoF whole-pool registration requested without "
                          "USE_NOF_URMA";
            nof_pool_fallback_logged_ = true;
        }
        return false;
#endif
    }

    char* GetWarmupBuffer() const {
        // Reuse an address that the measured reads will use. A separate
        // warmup buffer creates another set of per-chunk URMA registrations
        // and can consume the fixed registration cache before measurement.
        if (!thread_buffers_.empty() && thread_buffers_.front().ptr &&
            thread_buffers_.front().size >= FLAGS_value_size) {
            return thread_buffers_.front().ptr;
        }
        return buffer_;
    }

    bool BeginMeasurement() {
        if (!EnsureNofPools()) return false;
        for (auto& bytes : measured_memory_bytes_) bytes.store(0);
        for (auto& bytes : measured_nof_bytes_) bytes.store(0);
        ub_before_ = mooncake::GetUbGpuDirectCounters().Snapshot();
#if defined(USE_NOF_URMA)
        nof_before_ = mooncake::GetNofGpuDirectStats();
#endif
        return true;
    }

    void AccountMeasuredIo(const std::string& key, int64_t bytes, bool write) {
        if (bytes != static_cast<int64_t>(FLAGS_value_size)) return;
        const bool segment_key =
            key.starts_with("nof_seg_") || key.starts_with("nof_rmv_") ||
            key.starts_with("seg_") || key.starts_with("rmv_");
        if (write && !segment_key) {
            measured_memory_bytes_[1].fetch_add(bytes * FLAGS_replica_num,
                                                std::memory_order_relaxed);
            measured_nof_bytes_[1].fetch_add(bytes * FLAGS_nof_replica_num,
                                             std::memory_order_relaxed);
            return;
        }
        const bool nof = key.starts_with("nof_seg_") ||
                         key.starts_with("nof_rmv_") ||
                         (!segment_key && FLAGS_replica_num == 0);
        const uint64_t replicas =
            write ? (nof ? FLAGS_nof_replica_num : FLAGS_replica_num) : 1;
        auto& counter = nof ? measured_nof_bytes_[write ? 1 : 0]
                            : measured_memory_bytes_[write ? 1 : 0];
        counter.fetch_add(static_cast<uint64_t>(bytes) * replicas,
                          std::memory_order_relaxed);
    }

    bool ValidateMeasuredRead(const std::string& key, const void* data,
                              int64_t bytes) const {
        if (bytes != static_cast<int64_t>(FLAGS_value_size)) return false;
        if (!FLAGS_verify) return true;
        const auto seed = mooncake::bench::KeySeed(key);
        if (!seed || !CheckBuffer(*seed, data, FLAGS_value_size)) {
            LOG(ERROR) << "Measured read payload mismatch key=" << key;
            return false;
        }
        return true;
    }

    void FinishMeasurement(const char* phase) {
        const auto memory_read = measured_memory_bytes_[0].load();
        const auto memory_write = measured_memory_bytes_[1].load();
        const auto nof_read = measured_nof_bytes_[0].load();
        const auto nof_write = measured_nof_bytes_[1].load();
        bool passed = memory_read > 0 || memory_write > 0 || nof_read > 0 ||
                      nof_write > 0;
        if (gpu_config_.use_gpu_buffer &&
            (memory_read > 0 || memory_write > 0)) {
            const auto stats = mooncake::UbGpuDirectDelta(
                ub_before_, mooncake::GetUbGpuDirectCounters().Snapshot());
            LOG(INFO)
                << "Measurement phase=" << phase
                << " path=ub expected_read_bytes=" << memory_read
                << " expected_write_bytes=" << memory_write
                << " direct_read_submitted_bytes="
                << stats.reads.submitted_bytes
                << " direct_read_success_bytes=" << stats.reads.success_bytes
                << " direct_read_failed_bytes=" << stats.reads.failed_bytes
                << " direct_write_submitted_bytes="
                << stats.writes.submitted_bytes
                << " direct_write_success_bytes=" << stats.writes.success_bytes
                << " direct_write_failed_bytes=" << stats.writes.failed_bytes
                << " staged_bytes=" << stats.staged_bytes;
            passed = passed && mooncake::UbGpuDirectPassed(stats, memory_read,
                                                           memory_write);
        }
#if defined(USE_NOF_URMA)
        if (nof_read > 0 || nof_write > 0) {
            const auto stats = mooncake::NofGpuDirectDelta(
                nof_before_, mooncake::GetNofGpuDirectStats());
            LOG(INFO)
                << "Measurement phase=" << phase
                << " path=nof expected_read_bytes=" << nof_read
                << " expected_write_bytes=" << nof_write
                << " (I/O scoped to measurement; registrations cumulative)\n"
                << mooncake::NofGpuDirectAcceptanceReport(stats);
            passed = passed &&
                     mooncake::NofGpuDirectPassed(stats, nof_read > 0,
                                                  nof_write > 0) &&
                     stats.reads.submitted_bytes == nof_read &&
                     stats.reads.success_bytes == nof_read &&
                     stats.writes.submitted_bytes == nof_write &&
                     stats.writes.success_bytes == nof_write;
        }
#endif
        if (FLAGS_require_gpu_direct && !passed) {
            direct_acceptance_passed_ = false;
            LOG(ERROR) << "Measured GPU Direct acceptance FAILED phase="
                       << phase;
        }
    }

    static mooncake::ReplicateConfig MakeReplicateConfig() {
        mooncake::ReplicateConfig config;
        config.replica_num = FLAGS_replica_num;
        config.nof_replica_num = FLAGS_nof_replica_num;
        config.with_hard_pin = FLAGS_hard_pin;
        return config;
    }

    static void SetPreferredSegment(mooncake::ReplicateConfig& config,
                                    const BenchmarkSegment& segment) {
        if (segment.kind == SegmentKind::NOF) {
            config.preferred_nof_segments = {segment.name};
        } else {
            config.preferred_segments = {segment.name};
        }
    }

    // External review #1: segment 场景的副本隔离。全局 --replica_num /
    // --nof_replica_num 若同时生效于每个 put，memory segment 的 put 也会申请
    // NoF 副本、NoF segment 的 put 也会申请 memory 副本，按 segment 统计的
    // PUT latency/成功数就无法按单一存储路径解释。这里按 segment 类型强制
    // 只保留对应类型的副本数：memory segment → 仅 memory 副本，NoF segment
    // → 仅 NoF SSD 副本（master 侧 replica_num/nof_replica_num 为 0 时跳过
    // 对应类型的分配，语义安全）。因此既可一次运行同时覆盖两类 segment，
    // 也可按审阅建议分开运行（--replica_num=1 --nof_replica_num=0 测
    // memory，--replica_num=0 --nof_replica_num=1 测 NoF），结果等价。
    static mooncake::ReplicateConfig MakeSegmentReplicateConfig(
        const BenchmarkSegment& segment) {
        mooncake::ReplicateConfig config = MakeReplicateConfig();
        SetPreferredSegment(config, segment);
        if (segment.kind == SegmentKind::NOF) {
            config.replica_num = 0;
        } else {
            config.nof_replica_num = 0;
        }
        return config;
    }

    static void RecordPutResult(ThreadResult& result, int64_t latency_ns,
                                int ret) {
        result.latencies_ns.push_back(latency_ns);
        ++result.total_keys;
        ++result.total_queries;
        if (ret == 0) {
            result.total_bytes += FLAGS_value_size;
        } else {
            ++result.failed_ops;
        }
    }

    static std::string MakeKey(size_t idx) {
        return "bench_key_" + std::to_string(idx);
    }

    static std::string MakeUpsertKey(size_t idx) {
        return "upsert_bench_key_vs" + std::to_string(FLAGS_value_size) + "_" +
               std::to_string(idx);
    }

    void FillBuffer(void* destination, size_t seed) {
        void* target = destination;
        static thread_local std::vector<uint8_t> shadow;
        if (buffer_is_gpu_) {
            if (shadow.size() != FLAGS_value_size) {
                shadow.assign(FLAGS_value_size, 0);
            }
            target = shadow.data();
        }
        mooncake::bench::FillPattern(target, FLAGS_value_size, seed);
#if defined(USE_CUDA)
        if (buffer_is_gpu_) {
            checkCudaError(cudaMemcpy(destination, shadow.data(),
                                      FLAGS_value_size, cudaMemcpyHostToDevice),
                           "FillBuffer H2D copy failed");
            static std::atomic<uint64_t> fill_h2d_log_counter{0};
            if (VLOG_IS_ON(2) &&
                fill_h2d_log_counter.fetch_add(1, std::memory_order_relaxed) %
                        10000 ==
                    0) {
                VLOG(2) << "[bench] FillBuffer H2D seed=" << seed
                        << " size=" << FLAGS_value_size
                        << " dst=" << destination;
            }
        }
#endif
    }

    void FillBuffer(size_t seed) { FillBuffer(buffer_, seed); }

    bool CheckBuffer(size_t seed, const void* data, size_t size) const {
        if (size != FLAGS_value_size) return false;
        const void* compare_data = data;
        static thread_local std::vector<uint8_t> shadow;
#if defined(USE_CUDA)
        if (buffer_is_gpu_) {
            shadow.resize(size);
            checkCudaError(
                cudaMemcpy(shadow.data(), data, size, cudaMemcpyDeviceToHost),
                "CheckBuffer D2H copy failed");
            static std::atomic<uint64_t> check_d2h_log_counter{0};
            if (VLOG_IS_ON(2) &&
                check_d2h_log_counter.fetch_add(1, std::memory_order_relaxed) %
                        10000 ==
                    0) {
                VLOG(2) << "[bench] CheckBuffer D2H seed=" << seed
                        << " size=" << size << " src=" << data;
            }
            compare_data = shadow.data();
        }
#endif
        return mooncake::bench::CheckPattern(compare_data, size,
                                             FLAGS_value_size, seed);
    }

    int DoWarmup() {
        if (FLAGS_warmup_keys == 0) return 0;
        LOG(INFO) << "Warmup: reading " << FLAGS_warmup_keys << " keys...";

        size_t warmup_end = std::min(static_cast<size_t>(FLAGS_warmup_keys),
                                     static_cast<size_t>(FLAGS_num_keys));
        char* warmup_buffer = GetWarmupBuffer();
        for (size_t i = 0; i < warmup_end; ++i) {
            std::string key = MakeKey(i);
            int64_t ret =
                client_->get_into(key, warmup_buffer, FLAGS_value_size);
            if (ret < 0) {
                LOG(WARNING) << "Warmup get_into failed for key=" << key
                             << " ret=" << ret;
            }
        }
        LOG(INFO) << "Warmup complete";
        return 0;
    }

    void BatchReadWorker(size_t tid, size_t my_keys, size_t key_offset,
                         BenchmarkStats& stats, std::latch& ready_latch,
                         std::latch& start_latch, std::latch& done_latch,
                         const std::function<std::string(size_t)>& key_func,
                         const std::atomic<bool>* stop_flag) {
        bindToSocket(tid % NR_SOCKETS);

        ThreadResult& result = stats.GetThreadResult(tid);
        result.latencies_ns.reserve(my_keys);

        char* my_buf = thread_buffers_[tid].ptr;

        ready_latch.count_down();
        start_latch.arrive_and_wait();

        size_t keys = 0;
        size_t queries = 0;
        size_t failed = 0;
        size_t bytes = 0;

        if (my_keys > 0) {
            do {
                if (FLAGS_batch_size <= 1) {
                    for (size_t i = 0; i < my_keys; ++i) {
                        if (stop_flag != nullptr &&
                            stop_flag->load(std::memory_order_relaxed)) {
                            break;
                        }
                        size_t key_idx = key_offset + i;
                        std::string key = key_func(key_idx);

                        auto t0 = Clock::now();
                        int64_t ret =
                            client_->get_into(key, my_buf, FLAGS_value_size);
                        auto t1 = Clock::now();

                        int64_t lat_ns = ElapsedNanos(t0, t1);

                        AccountMeasuredIo(key, ret, false);
                        if (!ValidateMeasuredRead(key, my_buf, ret)) {
                            ++failed;
                            LOG_EVERY_N(ERROR, 100)
                                << "get_into failed key=" << key
                                << " ret=" << ret;
                        } else {
                            bytes += static_cast<size_t>(ret);
                        }
                        result.latencies_ns.push_back(lat_ns);
                        ++keys;
                        ++queries;
                    }
                } else {
                    size_t per_key_buf = FLAGS_value_size;
                    size_t i = 0;
                    while (i < my_keys) {
                        if (stop_flag != nullptr &&
                            stop_flag->load(std::memory_order_relaxed)) {
                            break;
                        }
                        std::vector<std::string> key_list;
                        std::vector<void*> bufs;
                        std::vector<size_t> sizes;
                        size_t batch_end =
                            std::min(i + FLAGS_batch_size, my_keys);
                        key_list.reserve(batch_end - i);
                        bufs.reserve(batch_end - i);
                        sizes.reserve(batch_end - i);

                        for (size_t j = i; j < batch_end; ++j) {
                            size_t key_idx = key_offset + j;
                            key_list.push_back(key_func(key_idx));
                            bufs.push_back(my_buf + (j - i) * per_key_buf);
                            sizes.push_back(FLAGS_value_size);
                        }

                        auto t0 = Clock::now();
                        auto results =
                            client_->batch_get_into(key_list, bufs, sizes);
                        auto t1 = Clock::now();

                        int64_t lat_ns = ElapsedNanos(t0, t1);
                        result.latencies_ns.push_back(lat_ns);

                        for (size_t k = 0; k < key_list.size(); ++k) {
                            const int64_t ret =
                                k < results.size() ? results[k] : -1;
                            AccountMeasuredIo(key_list[k], ret, false);
                            if (!ValidateMeasuredRead(key_list[k], bufs[k],
                                                      ret)) {
                                ++failed;
                            } else {
                                bytes += static_cast<size_t>(results[k]);
                            }
                            ++keys;
                        }
                        ++queries;

                        i = batch_end;
                    }
                }
            } while (stop_flag != nullptr &&
                     !stop_flag->load(std::memory_order_relaxed));
        }

        result.total_bytes = bytes;
        result.total_keys = keys;
        result.total_queries = queries;
        result.failed_ops = failed;

        done_latch.arrive_and_wait();
    }

    std::vector<std::thread> LaunchReadWorkers(
        size_t num_threads, size_t total_keys, BenchmarkStats& stats,
        std::latch& ready_latch, std::latch& start_latch,
        std::latch& done_latch,
        const std::function<std::string(size_t)>& key_func,
        const std::atomic<bool>* stop_flag = nullptr) {
        std::vector<std::thread> threads;
        size_t keys_per_thread = total_keys / num_threads;
        size_t remainder = total_keys % num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            size_t my_keys = keys_per_thread + (t < remainder ? 1 : 0);
            size_t key_offset = t * keys_per_thread + std::min(t, remainder);

            threads.emplace_back([&, t, my_keys, key_offset, key_func,
                                  stop_flag]() {
                BatchReadWorker(t, my_keys, key_offset, stats, ready_latch,
                                start_latch, done_latch, key_func, stop_flag);
            });
        }
        return threads;
    }

    std::vector<std::thread> LaunchPutWorkers(
        size_t num_threads, size_t total_keys,
        const mooncake::ReplicateConfig& config, BenchmarkStats& stats,
        std::latch& ready_latch, std::latch& start_latch,
        std::latch& done_latch,
        const std::function<std::string(size_t)>& key_func,
        WriteApi write_api) {
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        const size_t keys_per_thread = total_keys / num_threads;
        const size_t remainder = total_keys % num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            const size_t my_keys = keys_per_thread + (t < remainder ? 1 : 0);
            const size_t key_offset =
                t * keys_per_thread + std::min(t, remainder);

            threads.emplace_back([&, t, my_keys, key_offset, key_func,
                                  write_api]() {
                bindToSocket(t % NR_SOCKETS);
                ThreadResult& result = stats.GetThreadResult(t);
                result.latencies_ns.reserve((my_keys + FLAGS_batch_size - 1) /
                                            FLAGS_batch_size);
                char* write_buffer = thread_buffers_[t].ptr;

                if (!FLAGS_verify) FillBuffer(write_buffer, t);
                ready_latch.count_down();
                start_latch.arrive_and_wait();
                if (FLAGS_batch_size <= 1) {
                    for (size_t i = 0; i < my_keys; ++i) {
                        const size_t key_idx = key_offset + i;
                        const std::string key = key_func(key_idx);
                        if (FLAGS_verify) FillBuffer(write_buffer, key_idx);

                        const auto t0 = Clock::now();
                        const int ret =
                            write_api == WriteApi::UPSERT
                                ? client_->upsert_from(key, write_buffer,
                                                       FLAGS_value_size, config)
                                : client_->put_from(key, write_buffer,
                                                    FLAGS_value_size, config);
                        const auto t1 = Clock::now();
                        if (ret == 0) {
                            AccountMeasuredIo(key, FLAGS_value_size, true);
                        } else {
                            LOG(ERROR)
                                << (write_api == WriteApi::UPSERT
                                        ? "upsert_from"
                                        : "put_from")
                                << " failed for key=" << key << " ret=" << ret;
                        }
                        RecordPutResult(result, ElapsedNanos(t0, t1), ret);
                    }
                } else {
                    size_t i = 0;
                    while (i < my_keys) {
                        const size_t batch_end =
                            std::min(i + FLAGS_batch_size, my_keys);
                        std::vector<std::string> keys;
                        std::vector<void*> buffers;
                        std::vector<size_t> sizes;
                        keys.reserve(batch_end - i);
                        buffers.reserve(batch_end - i);
                        sizes.reserve(batch_end - i);

                        for (size_t j = i; j < batch_end; ++j) {
                            const size_t key_idx = key_offset + j;
                            char* key_buffer =
                                write_buffer + (j - i) * FLAGS_value_size;
                            if (FLAGS_verify) {
                                FillBuffer(key_buffer, key_idx);
                            }
                            keys.emplace_back(key_func(key_idx));
                            buffers.emplace_back(key_buffer);
                            sizes.emplace_back(FLAGS_value_size);
                        }

                        const auto t0 = Clock::now();
                        const auto results =
                            write_api == WriteApi::UPSERT
                                ? client_->batch_upsert_from(keys, buffers,
                                                             sizes, config)
                                : client_->batch_put_from(keys, buffers, sizes,
                                                          config);
                        const auto t1 = Clock::now();
                        result.latencies_ns.emplace_back(ElapsedNanos(t0, t1));
                        ++result.total_queries;

                        for (size_t k = 0; k < keys.size(); ++k) {
                            const int ret =
                                k < results.size() ? results[k] : -1;
                            ++result.total_keys;
                            if (ret == 0) {
                                result.total_bytes += FLAGS_value_size;
                                AccountMeasuredIo(keys[k], FLAGS_value_size,
                                                  true);
                            } else {
                                ++result.failed_ops;
                                LOG(ERROR) << (write_api == WriteApi::UPSERT
                                                   ? "batch_upsert_from"
                                                   : "batch_put_from")
                                           << " failed for key=" << keys[k]
                                           << " ret=" << ret;
                            }
                        }
                        i = batch_end;
                    }
                }
                done_latch.arrive_and_wait();
            });
        }
        return threads;
    }

    void RemoveWorker(size_t tid, size_t my_keys, size_t key_offset,
                      BenchmarkStats& stats, std::latch& start_latch,
                      std::latch& done_latch, bool use_batch,
                      const std::function<std::string(size_t)>& key_func) {
        bindToSocket(tid % NR_SOCKETS);

        ThreadResult& result = stats.GetThreadResult(tid);
        result.latencies_ns.reserve(my_keys);

        start_latch.arrive_and_wait();

        size_t keys = 0;
        size_t queries = 0;
        size_t failed = 0;
        size_t bytes = 0;

        if (!use_batch) {
            for (size_t i = 0; i < my_keys; ++i) {
                size_t key_idx = key_offset + i;
                std::string key = key_func(key_idx);

                auto t0 = Clock::now();
                int ret = client_->remove(key, /*force=*/true);
                auto t1 = Clock::now();

                int64_t lat_ns = ElapsedNanos(t0, t1);
                result.latencies_ns.push_back(lat_ns);

                if (ret != 0) {
                    ++failed;
                    LOG_EVERY_N(ERROR, 100)
                        << "remove failed key=" << key << " ret=" << ret;
                } else {
                    // Account removed payload as transferred bytes so
                    // throughput stats stay comparable with the read benchmark.
                    bytes += FLAGS_value_size;
                }
                ++keys;
                ++queries;
            }
        } else {
            size_t per_key_buf = FLAGS_value_size;
            size_t i = 0;
            while (i < my_keys) {
                std::vector<std::string> key_list;
                size_t batch_end = std::min(i + FLAGS_batch_size, my_keys);
                key_list.reserve(batch_end - i);

                for (size_t j = i; j < batch_end; ++j) {
                    size_t key_idx = key_offset + j;
                    key_list.push_back(key_func(key_idx));
                }

                auto t0 = Clock::now();
                auto results = client_->batchRemove(key_list, /*force=*/true);
                auto t1 = Clock::now();

                int64_t lat_ns = ElapsedNanos(t0, t1);
                result.latencies_ns.push_back(lat_ns);

                for (size_t k = 0; k < key_list.size(); ++k) {
                    if (k >= results.size() || results[k] != 0) {
                        ++failed;
                    } else {
                        bytes += per_key_buf;
                    }
                    ++keys;
                }
                ++queries;

                i = batch_end;
            }
        }

        result.total_bytes = bytes;
        result.total_keys = keys;
        result.total_queries = queries;
        result.failed_ops = failed;

        done_latch.arrive_and_wait();
    }

    std::vector<std::thread> LaunchRemoveWorkers(
        size_t num_threads, size_t total_keys, BenchmarkStats& stats,
        std::latch& start_latch, std::latch& done_latch, bool use_batch,
        const std::function<std::string(size_t)>& key_func) {
        std::vector<std::thread> threads;
        size_t keys_per_thread = total_keys / num_threads;
        size_t remainder = total_keys % num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            size_t my_keys = keys_per_thread + (t < remainder ? 1 : 0);
            size_t key_offset = t * keys_per_thread + std::min(t, remainder);

            threads.emplace_back(
                [&, t, my_keys, key_offset, use_batch, key_func]() {
                    RemoveWorker(t, my_keys, key_offset, stats, start_latch,
                                 done_latch, use_batch, key_func);
                });
        }
        return threads;
    }

    std::vector<BenchmarkSegment> DiscoverBenchmarkSegments() {
        std::vector<BenchmarkSegment> segments;
        auto append = [&segments](std::vector<std::string> names,
                                  SegmentKind kind) {
            std::sort(names.begin(), names.end());
            names.erase(std::unique(names.begin(), names.end()), names.end());
            for (auto& name : names) {
                segments.push_back({std::move(name), kind, {}});
            }
        };

        if (FLAGS_replica_num > 0) {
            auto memory_segments = ParseSegments(FLAGS_segments);
            if (memory_segments.empty()) {
                memory_segments = DiscoverSegmentsFromMaster(
                    FLAGS_master_server,
                    static_cast<int>(FLAGS_master_admin_port));
            }
            if (memory_segments.empty()) {
                LOG(ERROR) << "Memory replicas requested but no memory "
                              "segments were found";
                return {};
            }
            append(std::move(memory_segments), SegmentKind::MEMORY);
        }
        if (FLAGS_nof_replica_num > 0) {
            auto nof_segments = ParseSegments(FLAGS_nof_segments);
            if (nof_segments.empty()) {
                nof_segments =
                    DiscoverNoFSegmentsFromMaster(FLAGS_master_server);
            }
            if (nof_segments.empty()) {
                LOG(ERROR) << "NoF replicas requested but no NoF segments "
                              "were found";
                return {};
            }
            append(std::move(nof_segments), SegmentKind::NOF);
        }
        if (FLAGS_strict_segment_placement || FLAGS_require_gpu_direct ||
            VerifyReadPlacementEnabled()) {
            if (!LoadSegmentRegions(segments)) return {};
        }
        return segments;
    }

    static bool IsUrmaEndpoint(const std::string& endpoint) {
        std::istringstream tokens(endpoint);
        std::string token;
        while (tokens >> token) {
            if (token == "trtype:URMA") return true;
        }
        return false;
    }

    bool EnsurePlacementClient() {
        if (placement_client_) return true;
        auto master =
            std::make_unique<mooncake::MasterClient>(mooncake::generate_uuid());
        if (master->Connect(FLAGS_master_server) != mooncake::ErrorCode::OK) {
            LOG(ERROR) << "Cannot connect to master for placement checks";
            return false;
        }
        placement_client_ = std::move(master);
        return true;
    }

    bool EnsureMemorySegmentDetails() {
        if (memory_segment_details_) return true;
        auto body = FetchMasterAdminBody(
            FLAGS_master_server, static_cast<int>(FLAGS_master_admin_port),
            "/get_segments_detail");
        if (!body) return false;
        try {
            MemorySegmentsDetail details;
            struct_json::from_json(details, *body);
            memory_segment_details_ = std::move(details);
            return true;
        } catch (const std::exception& error) {
            LOG(ERROR) << "Cannot inspect memory segment details: "
                       << error.what();
            return false;
        }
    }

    static std::optional<mooncake::bench::PlacementRegion> MemoryRegion(
        const MemorySegmentDetail& item) {
        try {
            size_t consumed = 0;
            const auto base = std::stoull(item.base_address, &consumed, 0);
            if (consumed == item.base_address.size() && item.size_bytes > 0 &&
                !item.te_endpoint.empty()) {
                return mooncake::bench::PlacementRegion{
                    base, item.size_bytes, item.te_endpoint, item.protocol};
            }
        } catch (const std::exception&) {
        }
        LOG(ERROR) << "Invalid memory segment region: " << item.segment_name;
        return std::nullopt;
    }

    bool LoadSegmentRegions(std::vector<BenchmarkSegment>& segments) {
        if (FLAGS_replica_num > 0 && !EnsureMemorySegmentDetails())
            return false;
        std::vector<mooncake::NoFSegment> nof;
        if (FLAGS_nof_replica_num > 0) {
            if (!EnsurePlacementClient()) return false;
            auto result = placement_client_->GetAllNoFSegments();
            if (!result) return false;
            nof = std::move(*result);
        }
        for (auto& segment : segments) {
            if (segment.kind == SegmentKind::NOF) {
                for (const auto& region : nof) {
                    if (region.name == segment.name) {
                        segment.regions.push_back({region.base, region.size,
                                                   region.te_endpoint, "nof"});
                    }
                }
            } else {
                for (const auto& item : memory_segment_details_->segments) {
                    if (item.segment_name != segment.name) continue;
                    auto region = MemoryRegion(item);
                    if (!region) return false;
                    segment.regions.push_back(std::move(*region));
                }
            }
            if (segment.regions.empty()) {
                LOG(ERROR) << "Requested segment is not mounted: "
                           << segment.name;
                return false;
            }
            for (const auto& region : segment.regions) {
                if (FLAGS_require_gpu_direct &&
                    segment.kind == SegmentKind::MEMORY &&
                    region.protocol != "ub") {
                    LOG(ERROR) << "Direct GPU memory segment must use UB: "
                               << segment.name;
                    return false;
                }
                LOG(INFO) << "Requested segment=" << segment.name
                          << " endpoint=" << region.endpoint
                          << " effective_path="
                          << (segment.kind == SegmentKind::NOF
                                  ? (gpu_config_.use_gpu_buffer
                                         ? "nof/urma/cuda-domain"
                                         : "nof/host")
                                  : region.protocol + "/" + gpu_config_.mode);
            }
        }
        return true;
    }

    bool VerifyDirectMemoryPlacement(const std::string& key) {
        if (!FLAGS_require_gpu_direct || FLAGS_replica_num == 0) return true;
        if (!EnsurePlacementClient()) return false;
        auto result = placement_client_->GetReplicaList(key);
        if (!result || result->replicas.empty()) return false;
        if (!EnsureMemorySegmentDetails()) return false;
        for (const auto& replica : result->replicas) {
            if (replica.is_nof_replica()) continue;
            if (!replica.is_memory_replica()) return false;
            const auto& descriptor =
                replica.get_memory_descriptor().buffer_descriptor;
            bool direct = false;
            for (const auto& item : memory_segment_details_->segments) {
                if (item.protocol != "ub") continue;
                auto region = MemoryRegion(item);
                if (region && region->Contains(
                                  descriptor.buffer_address_, descriptor.size_,
                                  descriptor.transport_endpoint_)) {
                    direct = true;
                    break;
                }
            }
            if (!direct) {
                LOG(ERROR) << "Memory GPU Direct placement rejected key=" << key
                           << " actual_endpoint="
                           << descriptor.transport_endpoint_;
                return false;
            }
        }
        return true;
    }

    static bool ReplicaMatchesSegment(
        const mooncake::Replica::Descriptor& replica,
        const BenchmarkSegment& segment) {
        const mooncake::AllocatedBuffer::Descriptor* descriptor = nullptr;
        if (segment.kind == SegmentKind::NOF && replica.is_nof_replica()) {
            descriptor = &replica.get_nof_descriptor().buffer_descriptor;
        } else if (segment.kind == SegmentKind::MEMORY &&
                   replica.is_memory_replica()) {
            descriptor = &replica.get_memory_descriptor().buffer_descriptor;
        }
        if (!descriptor) return false;
        for (const auto& region : segment.regions) {
            if (region.Contains(descriptor->buffer_address_, descriptor->size_,
                                descriptor->transport_endpoint_)) {
                return true;
            }
        }
        return false;
    }

    bool ConfigureReadPlacementValidation(
        const std::vector<BenchmarkSegment>& segments) {
        // Only expectations are retained; object metadata is queried afresh by
        // each read. The guard runs after the Store's final replica selection.
        std::unordered_map<std::string, size_t> expected_segments;
        for (size_t s = 0; s < segments.size(); ++s) {
            for (size_t i = 0; i < FLAGS_num_keys; ++i) {
                if (!expected_segments
                         .emplace(MakeSegmentKey(segments[s], i), s)
                         .second) {
                    LOG(ERROR) << "Ambiguous segment benchmark key namespace";
                    return false;
                }
            }
        }
        const bool installed = client_->set_read_replica_validator(
            [segments, expected = std::move(expected_segments)](
                const std::string& key,
                const mooncake::Replica::Descriptor& replica) {
                const auto found = expected.find(key);
                if (found != expected.end() &&
                    ReplicaMatchesSegment(replica, segments[found->second])) {
                    return true;
                }
                const auto* descriptor =
                    replica.is_memory_replica()
                        ? &replica.get_memory_descriptor().buffer_descriptor
                    : replica.is_nof_replica()
                        ? &replica.get_nof_descriptor().buffer_descriptor
                        : nullptr;
                LOG_EVERY_N(ERROR, 100)
                    << "Read placement rejected key=" << key
                    << " expected_segment="
                    << (found == expected.end() ? "unknown"
                                                : segments[found->second].name)
                    << " actual_type="
                    << (replica.is_memory_replica() ? "memory"
                        : replica.is_nof_replica()  ? "nof"
                                                    : "other")
                    << " actual_endpoint="
                    << (descriptor ? descriptor->transport_endpoint_ : "-")
                    << " actual_address="
                    << (descriptor ? descriptor->buffer_address_ : 0)
                    << " actual_size=" << (descriptor ? descriptor->size_ : 0);
                return false;
            });
        if (installed) {
            LOG(INFO) << "Actual read placement validation enabled; mismatch "
                         "fails the read, including hot-cache redirection. "
                         "No additional metadata queries or metadata cache.";
        }
        return installed;
    }

    bool VerifyPlacement(const std::string& key,
                         const BenchmarkSegment& segment) {
        if (!EnsurePlacementClient()) return false;
        auto result = placement_client_->GetReplicaList(key);
        if (!result || result->replicas.size() != 1) {
            LOG(ERROR) << "Expected one replica for key=" << key
                       << " requested_segment=" << segment.name;
            return false;
        }
        const auto& replica = result->replicas.front();
        const mooncake::AllocatedBuffer::Descriptor* descriptor = nullptr;
        if (segment.kind == SegmentKind::NOF && replica.is_nof_replica()) {
            descriptor = &replica.get_nof_descriptor().buffer_descriptor;
        } else if (segment.kind == SegmentKind::MEMORY &&
                   replica.is_memory_replica()) {
            descriptor = &replica.get_memory_descriptor().buffer_descriptor;
        }
        if (descriptor) {
            for (const auto& region : segment.regions) {
                if (region.Contains(descriptor->buffer_address_,
                                    descriptor->size_,
                                    descriptor->transport_endpoint_)) {
                    VLOG(1) << "Placement verified key=" << key
                            << " requested_segment=" << segment.name
                            << " actual_endpoint=" << region.endpoint;
                    return true;
                }
            }
        }
        LOG(ERROR) << "Placement mismatch key=" << key
                   << " requested_segment=" << segment.name
                   << " actual_endpoint="
                   << (descriptor ? descriptor->transport_endpoint_
                                  : "wrong-type");
        return false;
    }

    int VerifySegments(const std::vector<BenchmarkSegment>& segments,
                       bool remove_keys = false) {
        size_t errors = 0;
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            for (const auto& segment : segments) {
                const auto key = remove_keys ? MakeRemoveKey(segment, i)
                                             : MakeSegmentKey(segment, i);
                if (!VerifyDirectMemoryPlacement(key)) {
                    ++errors;
                    continue;
                }
                if (FLAGS_strict_segment_placement &&
                    !VerifyPlacement(key, segment)) {
                    ++errors;
                    continue;
                }
                const auto ret =
                    client_->get_into(key, buffer_, FLAGS_value_size);
                if (ret != static_cast<int64_t>(FLAGS_value_size) ||
                    !CheckBuffer(i, buffer_, FLAGS_value_size)) {
                    LOG(ERROR) << "Segment verification failed key=" << key;
                    ++errors;
                }
            }
        }
        LOG(INFO) << "Segment verification: errors=" << errors;
        return mooncake::bench::ExitCode(errors);
    }

    int VerifyData(const std::function<std::string(size_t)>& key_func = {}) {
        LOG(INFO) << "Verifying data integrity for " << FLAGS_num_keys
                  << " keys...";
        int errors = 0;

        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = key_func ? key_func(i) : MakeKey(i);
            if (!VerifyDirectMemoryPlacement(key)) {
                ++errors;
                continue;
            }
            int64_t ret = client_->get_into(key, buffer_, FLAGS_value_size);
            if (ret != static_cast<int64_t>(FLAGS_value_size)) {
                LOG(ERROR) << "Verify: get_into failed for key=" << key;
                ++errors;
                continue;
            }
            if (!CheckBuffer(i, buffer_, static_cast<size_t>(ret))) {
                LOG(ERROR) << "Verify: data mismatch for key=" << key;
                ++errors;
            }
        }

        LOG(INFO) << "Verification complete: " << errors << " errors out of "
                  << FLAGS_num_keys << " keys";
        return errors > 0 ? -1 : 0;
    }

    std::shared_ptr<mooncake::RealClient> client_;
    mooncake::NofRuntime nof_runtime_{};
    std::unique_ptr<mooncake::MasterClient> placement_client_;
    std::optional<MemorySegmentsDetail> memory_segment_details_;
    char* buffer_;
    size_t buffer_size_;
    bool buffer_is_gpu_ = false;
    GpuRuntimeConfig gpu_config_;
    std::vector<std::shared_ptr<void>> nof_pool_tokens_;
    std::unordered_set<std::string> registered_nof_pools_;
    bool nof_pool_fallback_logged_ = false;
    bool direct_acceptance_passed_ = true;
    std::array<std::atomic<uint64_t>, 2> measured_memory_bytes_{};
    std::array<std::atomic<uint64_t>, 2> measured_nof_bytes_{};
    mooncake::UbGpuDirectStats ub_before_;
#if defined(USE_NOF_URMA)
    mooncake::NofGpuDirectStats nof_before_;
#endif

    struct ThreadBuffer {
        char* ptr = nullptr;
        size_t size = 0;
        int numa_node = -1;
        bool is_gpu = false;
    };
    std::vector<ThreadBuffer> thread_buffers_;

    int AllocateThreadBuffers(size_t num_threads) {
        thread_buffers_.resize(num_threads);
        size_t per_buf_size = FLAGS_batch_size * FLAGS_value_size;
        for (size_t t = 0; t < num_threads; ++t) {
            int node = t % NR_SOCKETS;
            thread_buffers_[t].size = per_buf_size;
            thread_buffers_[t].numa_node = node;
            thread_buffers_[t].is_gpu = gpu_config_.use_gpu_buffer;
            if (gpu_config_.use_gpu_buffer) {
#if defined(USE_CUDA) && defined(USE_UB)
                if (gpu_config_.mode == "gdr-peermem") {
                    thread_buffers_[t].ptr = buffer_ + (1 + t) * per_buf_size;
                } else {
                    thread_buffers_[t].ptr =
                        reinterpret_cast<char*>(mooncake::ub_allocate_gpu_hbm(
                            per_buf_size, FLAGS_gpu_device));
                }
                if (!thread_buffers_[t].ptr) {
                    LOG(ERROR) << "Failed to allocate GPU buffer for thread "
                               << t << " size=" << per_buf_size;
                    return -1;
                }
#else
                LOG(FATAL) << "gpu_mode requires USE_CUDA and USE_UB build";
#endif
            } else {
                thread_buffers_[t].ptr = reinterpret_cast<char*>(
                    numa_alloc_onnode(per_buf_size, node));
                if (!thread_buffers_[t].ptr) {
                    LOG(ERROR) << "Failed to allocate buffer for thread " << t
                               << " on NUMA node " << node;
                    return -1;
                }
                std::memset(thread_buffers_[t].ptr, 0, per_buf_size);
            }
            if (gpu_config_.mode == "gdr-peermem") {
                VLOG(1) << "[bench] gdr-peermem thread buffer " << t
                        << " -> main GPU block offset "
                        << (1 + t) * per_buf_size;
            } else {
                int ret = client_->register_buffer(thread_buffers_[t].ptr,
                                                   per_buf_size);
                if (ret != 0) {
                    LOG(ERROR) << "register_buffer failed for thread " << t
                               << " on NUMA node " << node;
                    return ret;
                }
            }
        }
        LOG(INFO) << "Allocated " << num_threads << " thread buffers, each "
                  << per_buf_size / MB << " MiB (NUMA-aware, " << NR_SOCKETS
                  << " sockets)";
        return 0;
    }
};

int main(int argc, char* argv[]) {
    if (!google::IsGoogleLoggingInitialized()) {
        google::InitGoogleLogging(argv[0]);
    }
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_nof_qpairs_per_segment > 0) {
        ::setenv("MC_NOF_QPAIRS_PER_SEGMENT",
                 std::to_string(FLAGS_nof_qpairs_per_segment).c_str(), 1);
    }

    GpuRuntimeConfig gpu_config = ResolveGpuConfig();
#if defined(USE_NOF_URMA)
    const bool nof_whole_pool_available =
        mooncake::NofWholePoolRegistrationAvailable();
#else
    const bool nof_whole_pool_available = false;
#endif

    // External review #2: GPU 写入是显式路径，不靠 role 绕行。writer（含
    // segment_write/remove 预填等直接写场景）带 --gpu_mode 时即从 GPU buffer
    // 发起 put（GPU-source write），Setup/FillBuffer/put 路径均已支持。
    const bool remote_scenario =
        FLAGS_scenario == "remote_memory" || FLAGS_scenario == "remote_disk";
    const bool writes_gpu_source =
        FLAGS_scenario == "segment_write" || FLAGS_scenario == "remove" ||
        FLAGS_scenario == "batch_remove" || FLAGS_scenario == "upsert" ||
        FLAGS_scenario == "local_memory" || FLAGS_scenario == "local_disk" ||
        (remote_scenario && FLAGS_role == "writer");
    if (remote_scenario && FLAGS_role != "writer" && FLAGS_role != "reader") {
        LOG(ERROR) << "Remote scenarios require --role=writer or --role=reader";
        return 1;
    }
    if (gpu_config.use_gpu_buffer && writes_gpu_source) {
        LOG(INFO) << "GPU-source write path enabled: gpu_mode="
                  << gpu_config.mode
                  << " will put from a GPU buffer (scenario=" << FLAGS_scenario
                  << ")";
    }

    const char* verbose_env = std::getenv("MC_VERBOSE");
    if (verbose_env) {
        FLAGS_v = std::atoi(verbose_env);
        LOG(INFO) << "Verbose logging enabled: -v=" << FLAGS_v;
    }

    if (std::getenv("MC_LOG_DIR") == nullptr) {
        FLAGS_logtostderr = true;
    }
    mooncake::logging::ApplyMooncakeLogEnableToGlog();

    LOG(INFO) << "Mooncake Stress Cluster Benchmark";
    LOG(INFO) << "  Scenario:       " << FLAGS_scenario;
    LOG(INFO) << "  Protocol:       " << FLAGS_protocol;
    LOG(INFO) << "  GPU mode:       " << gpu_config.mode;
    LOG(INFO) << "  GPU device:     " << FLAGS_gpu_device;
    LOG(INFO) << "  Value size:     " << FLAGS_value_size / MB << " MiB";
    LOG(INFO) << "  Key value size suffix: "
              << (FLAGS_key_include_value_size ? "enabled" : "disabled");
    LOG(INFO) << "  Num keys:       " << FLAGS_num_keys;
    LOG(INFO) << "  Batch size:     " << FLAGS_batch_size;
    LOG(INFO) << "  Num threads:    " << FLAGS_num_threads;
    LOG(INFO) << "  Memory replicas: " << FLAGS_replica_num;
    LOG(INFO) << "  NoF replicas:   " << FLAGS_nof_replica_num;
    LOG(INFO) << "  NoF whole-pool: "
              << (!FLAGS_nof_whole_pool_register
                      ? "disabled"
                      : (nof_whole_pool_available ? "auto/available"
                                                  : "auto/unavailable"));
    LOG(INFO) << "  NoF qpairs/segment: "
              << (FLAGS_nof_qpairs_per_segment == 0
                      ? "environment/default"
                      : std::to_string(FLAGS_nof_qpairs_per_segment));
    LOG(INFO) << "  Hard pin:       " << (FLAGS_hard_pin ? "yes" : "no");
    LOG(INFO) << "  SSD offload:    "
              << (FLAGS_enable_ssd_offload ? "yes" : "no");
    if (!FLAGS_segments.empty()) {
        LOG(INFO) << "  Memory segments: " << FLAGS_segments;
    } else {
        LOG(INFO) << "  Memory segments: auto-discover from master";
    }
    if (!FLAGS_nof_segments.empty()) {
        LOG(INFO) << "  NoF segments:   " << FLAGS_nof_segments;
    } else {
        LOG(INFO) << "  NoF segments:   auto-discover from master";
    }
    LOG(INFO) << "  Master admin:   " << FLAGS_master_admin_port;
    LOG(INFO) << "  Read seg nums:  " << FLAGS_read_segment_nums;
    LOG(INFO) << "  Duration:       " << FLAGS_duration << "s";
    LOG(INFO) << "  Stats interval: " << FLAGS_statis_interval << "s";
    LOG(INFO) << "  Client init wait: " << FLAGS_client_init_wait_seconds
              << "s";
    LOG(INFO) << "  Request trace sample rate: "
              << mooncake::logging::HiFreqLogSampleRate();
    {
        // External review #3: 编译开关进入验收输出。SPDK 侧 GPU Direct 依赖
        // USE_NOF_URMA/USE_NOF_GPU_DIRECT 独立构建，与 UB transport 的
        // --gpu_mode（USE_CUDA/USE_UB）是两套路径。
        std::string build_flags;
#if defined(USE_NOF)
        build_flags += " USE_NOF=ON";
#else
        build_flags += " USE_NOF=OFF";
#endif
#if defined(USE_NOF_URMA)
        build_flags += " USE_NOF_URMA=ON";
#else
        build_flags += " USE_NOF_URMA=OFF";
#endif
#if defined(USE_NOF_GPU_DIRECT)
        build_flags += " USE_NOF_GPU_DIRECT=ON";
#else
        build_flags += " USE_NOF_GPU_DIRECT=OFF";
#endif
#if defined(USE_CUDA)
        build_flags += " USE_CUDA=ON";
#else
        build_flags += " USE_CUDA=OFF";
#endif
#if defined(USE_UB)
        build_flags += " USE_UB=ON";
#else
        build_flags += " USE_UB=OFF";
#endif
        LOG(INFO) << "  Build switches:" << build_flags;
    }
#if defined(USE_NOF_URMA)
    LOG(INFO) << "  NoF GPU Direct available: "
              << (mooncake::NofGpuDirectAvailable() ? "yes" : "no");
#endif

    // Listing is read-only. Do not mount the benchmark's own memory segment
    // before taking a snapshot of the segments already registered with Master.
    if (FLAGS_scenario == "list_segments") {
        if (FLAGS_require_gpu_direct) {
            LOG(ERROR) << "list_segments does not perform GPU I/O";
            return 1;
        }
        StressBenchmark bench;
        return bench.Run();
    }

    if (FLAGS_num_keys == 0 || FLAGS_num_threads == 0 ||
        FLAGS_batch_size == 0 || FLAGS_value_size == 0 ||
        (FLAGS_duration > 0 && FLAGS_statis_interval == 0)) {
        LOG(ERROR) << "num_keys, num_threads, batch_size, value_size and "
                      "duration statistics interval must be positive";
        return 1;
    }
    const size_t max_size = std::numeric_limits<size_t>::max();
    const auto max_read_size =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    if (FLAGS_value_size > max_read_size ||
        FLAGS_batch_size > max_size / FLAGS_value_size ||
        FLAGS_num_keys > max_size / FLAGS_value_size ||
        FLAGS_num_threads == max_size ||
        (gpu_config.mode == "gdr-peermem" &&
         FLAGS_num_threads + 1 >
             max_size / (FLAGS_batch_size * FLAGS_value_size))) {
        LOG(ERROR)
            << "Requested benchmark sizes overflow buffer or byte counts";
        return 1;
    }
    const bool segment_scenario =
        FLAGS_scenario == "segment_write" || FLAGS_scenario == "segment_read" ||
        FLAGS_scenario == "remove" || FLAGS_scenario == "batch_remove";
    if (segment_scenario && FLAGS_strict_segment_placement &&
        (FLAGS_replica_num > 1 || FLAGS_nof_replica_num > 1)) {
        LOG(ERROR) << "Strict segment placement requires one replica per type; "
                      "use --strict_segment_placement=false for replica tests";
        return 1;
    }
    if (FLAGS_require_gpu_direct && !segment_scenario &&
        FLAGS_replica_num > 0 && FLAGS_nof_replica_num > 0) {
        LOG(ERROR)
            << "Direct acceptance of mixed replicas requires segment "
               "scenarios; "
               "select one replica type for remote/local memory scenarios";
        return 1;
    }
    mooncake::bench::GpuPathOptions path;
    path.mode = gpu_config.mode;
    path.protocol = FLAGS_protocol;
    path.memory_replicas = FLAGS_replica_num > 0;
    path.nof_replicas = FLAGS_nof_replica_num > 0;
    path.require_direct = FLAGS_require_gpu_direct;
#if defined(USE_CUDA)
    path.cuda_build = true;
#endif
#if defined(USE_UB)
    path.ub_build = true;
#endif
#if defined(USE_NOF_URMA) && defined(USE_NOF_GPU_DIRECT)
    path.nof_gpu_build = true;
    path.nof_gpu_available = mooncake::NofGpuDirectAvailable();
#endif
    const auto path_error = mooncake::bench::ValidateGpuPath(path);
    if (!path_error.empty()) {
        LOG(ERROR) << path_error;
        return 1;
    }
    if (gpu_config.use_gpu_buffer && FLAGS_nof_replica_num > 0) {
        const auto byte_limit = [](const char* name, uint64_t default_value) {
            const char* raw = std::getenv(name);
            if (!raw) return default_value;
            char* end = nullptr;
            const auto value = std::strtoull(raw, &end, 10);
            if (*raw == '-' || raw == end || *end != '\0' || value == 0 ||
                value >
                    static_cast<uint64_t>(std::numeric_limits<int>::max())) {
                return uint64_t{0};
            }
            return static_cast<uint64_t>(value);
        };
        const auto chunk = byte_limit("MC_NOF_SUBMIT_CHUNK_BYTES", 128 * 1024);
        const auto window =
            byte_limit("MC_NOF_INFLIGHT_BYTES_LIMIT", 32 * 1024 * 1024);
        const bool whole_pool_active =
            FLAGS_nof_whole_pool_register && nof_whole_pool_available;
        if (chunk == 0 || window == 0 ||
            !mooncake::bench::NofGpuLayoutSupported(
                FLAGS_value_size, chunk, window, whole_pool_active)) {
            LOG(ERROR) << "NoF peer-memory requires 64 KiB-aligned "
                          "value/chunk/window sizes, or "
                          "--nof_whole_pool_register=true with compatible SPDK";
            return 1;
        }
    }
    if (FLAGS_require_gpu_direct &&
        (!FLAGS_verify || FLAGS_enable_ssd_offload ||
         FLAGS_scenario == "local_disk" || FLAGS_scenario == "remote_disk")) {
        LOG(ERROR) << "--require_gpu_direct requires --verify=true and "
                      "memory/NoF replicas without local SSD offload";
        return 1;
    }
    ValidateGpuDevice(gpu_config);
    ApplyGpuEnv(gpu_config);
    if (FLAGS_require_gpu_direct) {
        // A host hot-cache hit would measure a local copy instead of UB/NoF
        // DMA.
        ::setenv("MC_STORE_LOCAL_HOT_CACHE_SIZE", "0", 1);
        LOG(INFO) << "Local hot cache disabled for direct GPU acceptance";
    }
    LOG(INFO) << "Requested memory path: " << FLAGS_protocol << "/"
              << gpu_config.mode;
    if (FLAGS_nof_replica_num > 0) {
        LOG(INFO) << "Requested NoF path: "
                  << (gpu_config.use_gpu_buffer ? "urma/cuda-domain" : "host");
    }

    if (FLAGS_replica_num == 0 && FLAGS_nof_replica_num == 0) {
        LOG(ERROR) << "--replica_num and --nof_replica_num cannot both be 0";
        return -1;
    }
#ifndef USE_NOF
    if (FLAGS_nof_replica_num > 0) {
        LOG(ERROR) << "--nof_replica_num requires a build with -DUSE_NOF=ON";
        return -1;
    }
#endif

    size_t total_data = FLAGS_num_keys * FLAGS_value_size;
    if (FLAGS_replica_num > 0 &&
        total_data > FLAGS_global_segment_size * 9.5 / 10) {
        LOG(WARNING) << "Total data (" << total_data / MB << " MiB) may exceed "
                     << "95% of segment (" << FLAGS_global_segment_size / MB
                     << " MiB). Master eviction may delete objects. "
                     << "Consider increasing --global_segment_size or "
                     << "decreasing --num_keys, or use --hard_pin=true.";
    }

    StressBenchmark bench;
    int ret = bench.Setup();
    if (ret != 0) {
        LOG(ERROR) << "Benchmark setup failed";
        return ret;
    }

    if (FLAGS_client_init_wait_seconds > 0) {
        LOG(INFO) << "Waiting " << FLAGS_client_init_wait_seconds
                  << " seconds after client setup before benchmark run...";
        std::this_thread::sleep_for(
            std::chrono::seconds(FLAGS_client_init_wait_seconds));
        LOG(INFO) << "Client init wait complete";
    }

    if (FLAGS_prewarm_accelerator) {
        PrewarmAcceleratorDiscovery();
    }

    ret = bench.Run();
#if defined(USE_NOF_URMA)
    // External review #3: 验收输出——buffer-domain 识别结果与 SPDK URMA
    // 统计。cuda_io_submitted>0 说明 SPDK 接受过 GPU memory-domain I/O
    // 提交（提交口径）；最终成功以 cuda_io_success 为准（完成口径，按
    // SPDK 子任务 chunk 计，结合本 bench 的操作成功/失败统计解读）；
    // accelerator_attempts 是注册尝试数，注册成功看 dmabuf+peer_memory。
    const auto gpu_stats = mooncake::GetNofGpuDirectStats();
    LOG(INFO) << "NoF cumulative diagnostics (includes warmup/verification):\n"
              << mooncake::NofGpuDirectAcceptanceReport(gpu_stats);

#endif
    if (FLAGS_require_gpu_direct && !bench.DirectAcceptancePassed()) ret = 1;
    return ret;
}
