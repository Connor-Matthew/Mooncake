#include "config.h"

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif
#include <sched.h>

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <cmath>
#include <cstring>
#include <fstream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace mooncake::kvtest {
namespace {

std::string Upper(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return value;
}

std::vector<std::string> StringArray(const Json::Value &root,
                                     const char *name,
                                     std::vector<std::string> fallback) {
    if (!root.isMember(name)) return fallback;
    std::vector<std::string> values;
    for (const auto &value : root[name]) values.push_back(value.asString());
    return values;
}

uint64_t SizeField(const Json::Value &root, const char *name,
                   uint64_t fallback) {
    if (!root.isMember(name)) return fallback;
    const auto &value = root[name];
    if (!value.isString()) return value.asUInt64();
    return value.asString() == "0" ? 0 : ParseSize(value.asString());
}

std::string PeerUrl(const NodeConfig &node) {
    return "http://" + node.host + ":" + std::to_string(node.port);
}

std::string PeerRpcAddress(const NodeConfig &node) {
    return node.host + ":" + std::to_string(node.notify_rpc_port);
}

std::string NormalizeRpcAddress(std::string address) {
    constexpr const char *http = "http://";
    if (address.rfind(http, 0) == 0)
        address.erase(0, std::char_traits<char>::length(http));
    const auto slash = address.find('/');
    if (slash != std::string::npos) address.resize(slash);
    return address;
}

bool ParseCpuList(const std::string &spec, cpu_set_t *set,
                  std::string *error) {
    CPU_ZERO(set);
    std::stringstream input(spec);
    std::string token;
    bool any = false;
    while (std::getline(input, token, ',')) {
        if (token.empty()) continue;
        const auto dash = token.find('-');
        try {
            int first = std::stoi(token.substr(0, dash));
            int last = dash == std::string::npos
                           ? first
                           : std::stoi(token.substr(dash + 1));
            if (first < 0 || last < first || last >= CPU_SETSIZE) {
                throw std::out_of_range("CPU id");
            }
            for (int cpu = first; cpu <= last; ++cpu) CPU_SET(cpu, set);
            any = true;
        } catch (const std::exception &) {
            if (error) *error = "invalid cpu_affinity token: " + token;
            return false;
        }
    }
    if (!any && error) *error = "cpu_affinity is empty";
    return any;
}

bool OneOf(const std::string &value, std::initializer_list<const char *> set) {
    for (const char *candidate : set) {
        if (value == candidate) return true;
    }
    return false;
}

}  // namespace

uint64_t ParseSize(const std::string &input) {
    std::string value = Upper(input);
    value.erase(std::remove_if(value.begin(), value.end(), ::isspace),
                value.end());
    static const std::vector<std::pair<std::string, uint64_t>> units = {
        {"TIB", 1ULL << 40}, {"TB", 1ULL << 40},
        {"GIB", 1ULL << 30}, {"GB", 1ULL << 30},
        {"MIB", 1ULL << 20}, {"MB", 1ULL << 20},
        {"KIB", 1ULL << 10}, {"KB", 1ULL << 10}, {"B", 1}};
    for (const auto &[suffix, multiplier] : units) {
        if (value.size() >= suffix.size() &&
            value.compare(value.size() - suffix.size(), suffix.size(), suffix) ==
                0) {
            const std::string number =
                value.substr(0, value.size() - suffix.size());
            const double parsed = std::stod(number);
            if (parsed <= 0) throw std::invalid_argument("size must be > 0");
            return static_cast<uint64_t>(parsed * multiplier);
        }
    }
    const auto parsed = std::stoull(value);
    if (parsed == 0) throw std::invalid_argument("size must be > 0");
    return parsed;
}

bool LoadConfig(const std::string &path, Config *config, std::string *error) {
    try {
        std::ifstream file(path);
        if (!file) throw std::runtime_error("cannot open config: " + path);
        Json::CharReaderBuilder reader;
        Json::Value root;
        std::string parse_error;
        if (!Json::parseFromStream(reader, file, &root, &parse_error)) {
            throw std::runtime_error("invalid JSON: " + parse_error);
        }

        Config cfg;
        cfg.mode = root.get("mode", cfg.mode).asString();
        const bool role_present = root.isMember("role");
        cfg.role = root.get("role", cfg.role).asString();
        cfg.instance_id = root.get("instance_id", cfg.instance_id).asInt();
        cfg.listen_port = root.get("listen_port", cfg.listen_port).asInt();
        cfg.notify_rpc_port =
            root.get("notify_rpc_port", cfg.listen_port + 10000).asInt();
        cfg.notify_rpc_threads =
            root.get("notify_rpc_threads", cfg.notify_rpc_threads).asInt();
        cfg.pipeline = StringArray(root, "pipeline", cfg.pipeline);
        cfg.notify_pipeline =
            StringArray(root, "notify_pipeline", cfg.notify_pipeline);
        cfg.operation = root.get("operation", cfg.operation).asString();
        cfg.put_ratio = root.get("put_ratio", cfg.put_ratio).asDouble();

        cfg.target_qps = root.get("target_qps", cfg.target_qps).asDouble();
        cfg.traffic_mode =
            root.get("traffic_mode", cfg.traffic_mode).asString();
        cfg.pulse_window_ms =
            root.get("pulse_window_ms", cfg.pulse_window_ms).asInt();
        cfg.enable_jitter =
            root.get("enable_jitter", cfg.enable_jitter).asBool();
        cfg.pacing_catch_up =
            root.get("pacing_catch_up", cfg.pacing_catch_up).asBool();

        cfg.duration = root.get("duration", cfg.duration).asDouble();
        if (root.isMember("warmup_duration")) {
            cfg.warmup_duration = root["warmup_duration"].asDouble();
        } else if (root.isMember("warmup") && root["warmup"].isObject()) {
            cfg.warmup_duration =
                root["warmup"].get("duration", cfg.warmup_duration).asDouble();
        }
        cfg.warmup_key_prefix =
            root.get("warmup_key_prefix", cfg.warmup_key_prefix).asString();

        cfg.num_threads = root.get("num_threads", cfg.num_threads).asInt();
        cfg.num_total_threads =
            root.get("num_total_threads", cfg.num_total_threads).asInt();
        cfg.notify_threads =
            root.get("notify_threads", cfg.notify_threads).asInt();
        cfg.notify_rpc_connections_per_peer = static_cast<size_t>(
            root.get("notify_rpc_connections_per_peer",
                     static_cast<Json::UInt64>(
                         cfg.notify_rpc_connections_per_peer))
                .asUInt64());
        cfg.notify_rpc_reuse_limit = root.get(
            "notify_rpc_reuse_limit", cfg.notify_rpc_reuse_limit).asInt();
        cfg.notify_rpc_timeout_ms =
            root.get("notify_rpc_timeout_ms", cfg.notify_rpc_timeout_ms).asInt();
        cfg.notify_count = root.get("notify_count", cfg.notify_count).asInt();
        cfg.batch_keys_count =
            root.get("batch_keys_count", cfg.batch_keys_count).asInt();
        if (root.isMember("batch_size")) {
            const int batch_size = root["batch_size"].asInt();
            if (root.isMember("batch_keys_count") &&
                batch_size != cfg.batch_keys_count) {
                throw std::runtime_error(
                    "batch_size and batch_keys_count disagree");
            }
            cfg.batch_keys_count = batch_size;
        }
        cfg.notify_async =
            root.get("notify_async", cfg.notify_async).asBool();
        cfg.notify_overflow =
            root.get("notify_overflow", cfg.notify_overflow).asString();
        cfg.notify_format =
            root.get("notify_format", cfg.notify_format).asString();
        cfg.notify_transport =
            root.get("notify_transport", cfg.notify_transport).asString();
        cfg.notify_read_format =
            root.get("notify_read_format", cfg.notify_read_format).asString();

        cfg.key_pool_size = static_cast<uint64_t>(
            root.get("key_pool_size",
                     static_cast<Json::UInt64>(cfg.key_pool_size))
                .asUInt64());
        cfg.key_prefix = root.get("key_prefix", cfg.key_prefix).asString();
        cfg.key_scenario =
            root.get("key_scenario", cfg.key_scenario).asString();
        cfg.key_count = static_cast<uint64_t>(
            root.get("key_count", static_cast<Json::UInt64>(cfg.key_count))
                .asUInt64());
        cfg.key_start = static_cast<uint64_t>(
            root.get("key_start", static_cast<Json::UInt64>(cfg.key_start))
                .asUInt64());
        cfg.key_end = static_cast<uint64_t>(
            root.get("key_end", static_cast<Json::UInt64>(cfg.key_end))
                .asUInt64());

        cfg.value_seed = static_cast<uint64_t>(
            root.get("value_seed", static_cast<Json::UInt64>(cfg.value_seed))
                .asUInt64());
        cfg.value_file = root.get("value_file", cfg.value_file).asString();

        cfg.scope = root.get("scope", cfg.scope).asString();
        cfg.prefill_batch =
            root.get("prefill_batch", cfg.prefill_batch).asInt();
        cfg.samples_max = static_cast<uint64_t>(
            root.get("samples_max", static_cast<Json::UInt64>(cfg.samples_max))
                .asUInt64());

        if (root.isMember("notify_queue_max")) {
            cfg.notify_queue_max = static_cast<size_t>(
                root["notify_queue_max"].asUInt64());
        } else if (root.isMember("notify_queue_size")) {
            cfg.notify_queue_max = static_cast<size_t>(
                root["notify_queue_size"].asUInt64());
        }
        cfg.start_paused =
            root.get("start_paused", cfg.start_paused).asBool();
        cfg.metrics_interval_ms =
            root.get("metrics_interval_ms", cfg.metrics_interval_ms).asInt();
        cfg.output_dir = root.get("output_dir", cfg.output_dir).asString();
        cfg.cpu_affinity =
            root.get("cpu_affinity", cfg.cpu_affinity).asString();
        cfg.verify_level =
            root.get("verify_level", cfg.verify_level).asString();
        cfg.verify_sample_bytes = SizeField(
            root, "verify_sample_bytes", cfg.verify_sample_bytes);
        cfg.verify_sample_step_bytes = SizeField(
            root, "verify_sample_step_bytes", cfg.verify_sample_step_bytes);
        cfg.verify_fail_op =
            root.get("verify_fail_op", cfg.verify_fail_op).asBool();

        if (root.isMember("data_sizes")) {
            cfg.data_sizes.clear();
            for (const auto &value : root["data_sizes"]) {
                cfg.data_sizes.push_back(value.isString()
                                             ? ParseSize(value.asString())
                                             : value.asUInt64());
            }
        }
        cfg.peers = StringArray(root, "peers", {});
        for (auto &peer : cfg.peers) {
            if (cfg.notify_transport == "rpc") {
                peer = NormalizeRpcAddress(peer);
            } else if (peer.rfind("http://", 0) != 0 &&
                       peer.rfind("https://", 0) != 0) {
                peer = "http://" + peer;
            }
        }
        if (root.isMember("nodes")) {
            for (const auto &entry : root["nodes"]) {
                NodeConfig node;
                node.host = entry.get("host", "").asString();
                node.port = entry.get("port", 9000).asInt();
                node.notify_rpc_port =
                    entry.get("notify_rpc_port", node.port + 10000).asInt();
                node.instance_id = entry.get("instance_id", 0).asInt();
                node.role = entry.get("role", "").asString();
                if (!node.host.empty()) cfg.nodes.push_back(std::move(node));
            }
        }

        const Json::Value store = root.get("store", Json::Value{});
        cfg.store.local_hostname =
            store.get("local_hostname", "").asString();
        cfg.store.metadata_server =
            store.get("metadata_server", "").asString();
        cfg.store.global_segment_size = SizeField(
            store, "global_segment_size", cfg.store.global_segment_size);
        cfg.store.local_buffer_size =
            SizeField(store, "local_buffer_size", cfg.store.local_buffer_size);
        cfg.store.protocol = store.get("protocol", cfg.store.protocol).asString();
        cfg.store.rdma_devices = store.get("rdma_devices", "").asString();
        cfg.store.master_server_address =
            store.get("master_server_address", cfg.store.master_server_address)
                .asString();
        cfg.store.tenant_id =
            store.get("tenant_id", cfg.store.tenant_id).asString();
        cfg.store.replica_num = static_cast<size_t>(
            store.get("replica_num",
                      static_cast<Json::UInt64>(cfg.store.replica_num))
                .asUInt64());

        // Mode/role normalisation before peer derivation.
        if (!OneOf(cfg.mode, {"pipeline", "basic", "prefill", "cleanup"})) {
            throw std::runtime_error(
                "mode must be pipeline, basic, prefill, or cleanup");
        }
        // Basic mode defaults to free-run (like the Python client); pipeline
        // keeps the historical 100 QPS default.
        if (cfg.mode == "basic" && !root.isMember("target_qps")) {
            cfg.target_qps = 0.0;
        }
        const bool one_shot = cfg.mode == "prefill" || cfg.mode == "cleanup";
        if (one_shot || cfg.mode == "basic") {
            if (!role_present) cfg.role = "client";
            if (cfg.role != "client" && cfg.role != "writer" &&
                cfg.role != "dual") {
                throw std::runtime_error(
                    "role must be client for basic/prefill/cleanup");
            }
            cfg.role = "client";
        } else if (cfg.role != "writer" && cfg.role != "reader" &&
                   cfg.role != "dual") {
            throw std::runtime_error("role must be writer, reader, or dual");
        }

        if (cfg.mode == "pipeline" && cfg.peers.empty()) {
            for (const auto &node : cfg.nodes) {
                if (node.instance_id == cfg.instance_id) continue;
                if ((cfg.role == "writer" || cfg.role == "dual") &&
                    !node.role.empty() && node.role != "reader" &&
                    node.role != "dual") {
                    continue;
                }
                cfg.peers.push_back(cfg.notify_transport == "rpc"
                                        ? PeerRpcAddress(node)
                                        : PeerUrl(node));
            }
        }
        if (cfg.mode != "pipeline") {
            // Basic/prefill/cleanup never notify peers.
            cfg.peers.clear();
        }

        // ---- validation ----
        if (!OneOf(cfg.traffic_mode, {"balanced", "pulse"})) {
            throw std::runtime_error(
                "traffic_mode must be balanced or pulse");
        }
        if (cfg.pulse_window_ms <= 0) {
            throw std::runtime_error("pulse_window_ms must be > 0");
        }
        if (cfg.target_qps < 0.0) {
            throw std::runtime_error("target_qps must be >= 0");
        }
        if (cfg.duration < 0.0 || cfg.warmup_duration < 0.0) {
            throw std::runtime_error("duration/warmup_duration must be >= 0");
        }
        if (!OneOf(cfg.notify_overflow, {"block", "drop"})) {
            throw std::runtime_error(
                "notify_overflow must be block or drop");
        }
        if (!OneOf(cfg.notify_format, {"json", "query"})) {
            throw std::runtime_error("notify_format must be json or query");
        }
        if (!OneOf(cfg.notify_transport, {"http", "rpc"})) {
            throw std::runtime_error("notify_transport must be http or rpc");
        }
        if (!OneOf(cfg.notify_read_format, {"both", "json", "query"})) {
            throw std::runtime_error(
                "notify_read_format must be both, json, or query");
        }
        if (cfg.num_threads <= 0) {
            throw std::runtime_error("num_threads must be > 0");
        }
        if (cfg.mode == "pipeline" &&
            cfg.num_total_threads <= cfg.num_threads) {
            throw std::runtime_error(
                "num_total_threads must be greater than num_threads");
        }
        if (cfg.mode != "pipeline" &&
            cfg.num_total_threads < cfg.num_threads) {
            cfg.num_total_threads = cfg.num_threads;
        }
        if (cfg.notify_threads <= 0 || cfg.notify_rpc_threads <= 0 ||
            cfg.notify_rpc_connections_per_peer == 0 ||
            cfg.notify_rpc_reuse_limit < 0 || cfg.notify_rpc_timeout_ms <= 0 ||
            cfg.notify_count < -1 || cfg.batch_keys_count <= 0) {
            throw std::runtime_error("invalid notify/batch thread counts");
        }
        const uint64_t minimum_pool =
            static_cast<uint64_t>(cfg.num_threads) *
            static_cast<uint64_t>(cfg.batch_keys_count);
        if (cfg.mode == "pipeline" && cfg.key_pool_size != 0 &&
            cfg.key_pool_size < minimum_pool) {
            throw std::runtime_error(
                "key_pool_size must be 0 or at least " +
                std::to_string(minimum_pool));
        }
        if (cfg.mode == "pipeline" && cfg.key_pool_size != 0 &&
            cfg.key_pool_size % minimum_pool != 0) {
            throw std::runtime_error(
                "key_pool_size must be divisible by num_threads * "
                "batch_keys_count");
        }
        if (cfg.data_sizes.empty()) {
            throw std::runtime_error("data_sizes must not be empty");
        }
        if (std::any_of(cfg.data_sizes.begin(), cfg.data_sizes.end(),
                        [](uint64_t size) { return size == 0; })) {
            throw std::runtime_error("every data_sizes entry must be > 0");
        }
        if (cfg.data_sizes.size() > 1 && !cfg.value_file.empty()) {
            throw std::runtime_error(
                "value_file supports exactly one data_sizes entry");
        }
        if (cfg.listen_port <= 0 || cfg.listen_port > 65535) {
            throw std::runtime_error("listen_port is invalid");
        }
        if (cfg.notify_rpc_port <= 0 || cfg.notify_rpc_port > 65535 ||
            cfg.notify_rpc_port == cfg.listen_port) {
            throw std::runtime_error("notify_rpc_port is invalid");
        }
        for (const auto &node : cfg.nodes) {
            if (node.notify_rpc_port <= 0 || node.notify_rpc_port > 65535) {
                throw std::runtime_error("node notify_rpc_port is invalid");
            }
        }
        if (cfg.store.local_hostname.empty() ||
            cfg.store.metadata_server.empty() ||
            cfg.store.master_server_address.empty()) {
            throw std::runtime_error(
                "store.local_hostname, metadata_server, and "
                "master_server_address are required");
        }
        if (cfg.store.replica_num == 0) {
            throw std::runtime_error("store.replica_num must be > 0");
        }
        if (cfg.metrics_interval_ms < 0) {
            throw std::runtime_error("metrics_interval_ms must be >= 0");
        }
        if (!OneOf(cfg.verify_level, {"off", "size", "sample", "full"})) {
            throw std::runtime_error("verify_level is invalid");
        }
        if (cfg.verify_level == "sample" &&
            (cfg.verify_sample_bytes == 0 ||
             cfg.verify_sample_step_bytes == 0)) {
            throw std::runtime_error(
                "sample verification requires non-zero sample bytes and "
                "step bytes");
        }
        if (cfg.warmup_duration > 0.0 && cfg.warmup_key_prefix.empty()) {
            throw std::runtime_error(
                "warmup_key_prefix must not be empty when warmup is enabled");
        }
        if (cfg.warmup_duration > 0.0 && cfg.mode == "pipeline") {
            // Warmup notifications must be distinguishable from measured
            // ones, which needs at least one reader peer.
            if (cfg.notify_count == 0) {
                throw std::runtime_error(
                    "pipeline warmup requires notify_count != 0");
            }
        }

        if (cfg.mode == "pipeline") {
            if ((cfg.role == "writer" || cfg.role == "dual") &&
                cfg.notify_count > 0 && cfg.peers.empty()) {
                throw std::runtime_error("writer has no reader peers");
            }
            if ((cfg.role == "writer" || cfg.role == "dual") &&
                cfg.notify_count > static_cast<int>(cfg.peers.size())) {
                throw std::runtime_error(
                    "notify_count exceeds reader peer count");
            }
            if ((cfg.role == "writer" || cfg.role == "dual") &&
                cfg.pipeline.empty()) {
                throw std::runtime_error("writer pipeline must not be empty");
            }
            if (cfg.notify_pipeline.empty()) {
                throw std::runtime_error("notify_pipeline must not be empty");
            }
        } else if (cfg.mode == "basic") {
            if (cfg.operation.empty()) {
                throw std::runtime_error(
                    "basic mode requires a non-empty operation");
            }
            if (cfg.key_count == 0) {
                throw std::runtime_error("basic mode requires key_count > 0");
            }
            if (cfg.key_end != 0 &&
                (cfg.key_start >= cfg.key_end ||
                 cfg.key_end > cfg.key_count)) {
                throw std::runtime_error(
                    "basic mode requires key_start < key_end <= key_count");
            }
            if (cfg.put_ratio < 0.0 || cfg.put_ratio > 1.0) {
                throw std::runtime_error("put_ratio must be within [0, 1]");
            }
            if (cfg.data_sizes.size() != 1) {
                throw std::runtime_error(
                    "basic mode requires exactly one data_sizes entry");
            }
            if (!cfg.value_file.empty() && cfg.value_seed != 0) {
                throw std::runtime_error(
                    "value_seed is ignored when value_file is set");
            }
        } else {
            if (cfg.key_count == 0) {
                throw std::runtime_error(
                    "prefill/cleanup requires key_count > 0");
            }
            if (cfg.key_end != 0 &&
                (cfg.key_start >= cfg.key_end ||
                 cfg.key_end > cfg.key_count)) {
                throw std::runtime_error(
                    "prefill/cleanup requires key_start < key_end <= "
                    "key_count");
            }
            if (cfg.prefill_batch <= 0) {
                throw std::runtime_error("prefill_batch must be > 0");
            }
            if (cfg.mode == "prefill" && cfg.data_sizes.size() != 1) {
                throw std::runtime_error(
                    "prefill requires exactly one data_sizes entry");
            }
            if (cfg.mode == "cleanup" &&
                !OneOf(cfg.scope, {"range", "all"})) {
                throw std::runtime_error("cleanup scope must be range or all");
            }
        }

        *config = std::move(cfg);
        return true;
    } catch (const std::exception &e) {
        if (error) *error = e.what();
        return false;
    }
}

bool ApplyCpuAffinity(const std::string &spec, std::string *error) {
    if (spec.empty()) return true;
    cpu_set_t set;
    if (!ParseCpuList(spec, &set, error)) return false;
    if (sched_setaffinity(0, sizeof(set), &set) != 0) {
        if (error)
            *error = "sched_setaffinity failed: " +
                     std::string(std::strerror(errno));
        return false;
    }
    return true;
}

}  // namespace mooncake::kvtest
