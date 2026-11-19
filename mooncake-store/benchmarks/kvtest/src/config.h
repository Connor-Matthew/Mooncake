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

struct Config {
    std::string mode{"pipeline"};
    int instance_id{0};
    int listen_port{9000};
    int notify_rpc_port{19000};
    int notify_rpc_threads{4};
    std::string role{"writer"};
    std::vector<std::string> pipeline{"upsertFrom"};
    std::vector<std::string> notify_pipeline{"getBuffer"};
    int target_qps{100};
    int num_threads{4};
    int num_total_threads{16};
    int notify_threads{100};
    size_t notify_rpc_connections_per_peer{4};
    int notify_rpc_reuse_limit{4};
    int notify_rpc_timeout_ms{3000};
    int notify_count{1};
    int batch_keys_count{1};
    uint64_t key_pool_size{0};
    size_t notify_queue_max{65536};
    bool enable_jitter{true};
    bool start_paused{false};
    std::vector<uint64_t> data_sizes{1024 * 1024};
    int metrics_interval_ms{3000};
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
};

uint64_t ParseSize(const std::string &value);
bool LoadConfig(const std::string &path, Config *config, std::string *error);
bool ApplyCpuAffinity(const std::string &spec, std::string *error);

}  // namespace mooncake::kvtest
