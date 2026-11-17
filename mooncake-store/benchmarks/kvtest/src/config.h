#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace mooncake::kvtest {

struct NodeConfig {
    std::string host;
    int port{9000};
    int notify_rpc_port{19000};
    int instance_id{0};
    std::string role;
};

struct StoreConfig {
    std::string local_hostname;
    std::string metadata_server;
    uint64_t global_segment_size{0};
    uint64_t local_buffer_size{8ULL * 1024 * 1024 * 1024};
    std::string protocol{"tcp"};
    std::string rdma_devices;
    std::string master_server_address{"127.0.0.1:50051"};
    std::string tenant_id{"default"};
    size_t replica_num{1};
};

// Runtime mode:
//   pipeline   - writer/reader/dual HTTP-notify workload (existing behaviour)
//   basic      - single-interface benchmark (one process = one client)
//   prefill    - one-shot data preparation for read-family basic scenarios
//   cleanup    - one-shot key-range or full-store cleanup
struct Config {
    std::string mode{"pipeline"};
    int instance_id{0};
    int listen_port{9000};
    int notify_rpc_port{19000};
    int notify_rpc_threads{4};
    std::string role{"writer"};

    // Pipeline operation lists (alias names accepted, canonicalised later).
    std::vector<std::string> pipeline{"upsertFrom"};
    std::vector<std::string> notify_pipeline{"getBuffer"};

    // Basic mode: a single interface name or "<put_iface>/<get_iface>".
    std::string operation;
    double put_ratio{0.5};

    // Traffic generation.
    double target_qps{100.0};
    std::string traffic_mode{"balanced"};  // balanced | pulse
    int pulse_window_ms{1000};
    bool enable_jitter{true};
    bool pacing_catch_up{true};

    // Phases. duration == 0 means "single pass" in basic mode and "run until
    // /stop" in pipeline mode.
    double duration{0.0};
    double warmup_duration{0.0};
    std::string warmup_key_prefix{"mc_warmup"};

    int num_threads{4};
    int num_total_threads{16};
    int notify_threads{100};
    size_t notify_rpc_connections_per_peer{4};
    int notify_rpc_reuse_limit{4};
    int notify_rpc_timeout_ms{3000};
    int notify_count{1};
    int batch_keys_count{1};

    // Default true keeps the pre-existing C++ behaviour (writers did not wait
    // for notifications). Set false for Python-client parity (wait per batch).
    bool notify_async{true};
    std::string notify_overflow{"drop"};  // drop | block
    std::string notify_transport{"http"};  // http | rpc
    std::string notify_format{"json"};    // json | query
    std::string notify_read_format{"both"};

    // Key space. key_pool_size applies to pipeline; key_count/key_start/key_end
    // apply to basic/prefill/cleanup.
    uint64_t key_pool_size{0};
    std::string key_prefix{"mc_bench"};
    std::string key_scenario;
    uint64_t key_count{1000};
    uint64_t key_start{0};
    uint64_t key_end{0};  // 0 = key_count

    // Value source.
    uint64_t value_seed{0};
    std::string value_file;

    // One-shot maintenance modes.
    std::string scope{"range"};  // range | all
    int prefill_batch{64};

    // Metrics.
    uint64_t samples_max{0};
    size_t notify_queue_max{65536};
    bool start_paused{false};
    std::vector<uint64_t> data_sizes{1024 * 1024};
    // 0 = window CSV disabled (default): no flush thread, no window samples.
    int metrics_interval_ms{0};
    std::string output_dir{"mooncake_kvtest_output"};
    std::string cpu_affinity;
    std::string verify_level{"size"};
    uint64_t verify_sample_bytes{4096};
    uint64_t verify_sample_step_bytes{1024 * 1024};
    bool verify_fail_op{false};

    std::vector<NodeConfig> nodes;
    std::vector<std::string> peers;
    StoreConfig store;

    int NumReadThreads() const { return num_total_threads - num_threads; }
    int BatchSize() const {
        return batch_keys_count > 0 ? batch_keys_count : 1;
    }
    uint64_t EffectiveKeyEnd() const {
        return key_end == 0 ? key_count : key_end;
    }
    bool IsBasic() const { return mode == "basic"; }
    bool IsPipeline() const { return mode == "pipeline"; }
    bool IsOneShot() const { return mode == "prefill" || mode == "cleanup"; }
};

uint64_t ParseSize(const std::string &value);
bool LoadConfig(const std::string &path, Config *config, std::string *error);
bool ApplyCpuAffinity(const std::string &spec, std::string *error);

}  // namespace mooncake::kvtest
