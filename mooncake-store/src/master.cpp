#include <gflags/gflags.h>
#include <glog/logging.h>

#include <dirent.h>  // For opendir (MC_LOG_DIR validation)
#include <unistd.h>  // For access/W_OK (MC_LOG_DIR validation)

#include <atomic>     // For std::atomic
#include <chrono>     // For std::chrono
#include <algorithm>  // For std::max
#include <csignal>
#include <cstdlib>  // For std::getenv
#include <fstream>  // For std::ifstream
#include <memory>   // For std::unique_ptr
#include <optional>
#include <string>
#include <thread>  // For std::thread
#include <json/json.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog/record.hpp>

#include "allocator_status.h"
#include "config/metrics_bootstrap_config_loader.h"
#include "config/rpc_protocol_config.h"
#include "default_config.h"
#include "duration_utils.h"
#include "etcd_helper.h"
#include "ha/ha_types.h"
#include "ha/leadership/master_service_supervisor.h"
#include "ha/snapshot/batch_oplog/config.h"

#include "http_metadata_server.h"
#include "master_admin_service.h"
#include "rpc_worker_pool.h"
#include "mooncake_logging.h"
#include "rpc_service.h"
#include "shutdown_request.h"
#include "types.h"
#include "common/network.h"
#include "glog_compat.h"

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#include "vsegment/partition_quota_planner.h"
#endif

#include "master_config.h"
#include "version.h"

using namespace coro_rpc;
using namespace async_simple;
using namespace async_simple::coro;

static_assert(mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL == 10000,
              "Update kDefaultKvLeaseTtlFlagValue when "
              "DEFAULT_DEFAULT_KV_LEASE_TTL changes");
static_assert(mooncake::DEFAULT_KV_SOFT_PIN_TTL_MS == 30 * 60 * 1000,
              "Update kDefaultKvSoftPinTtlFlagValue when "
              "DEFAULT_KV_SOFT_PIN_TTL_MS changes");
static_assert(mooncake::DEFAULT_MAX_KV_SOFT_PIN_TTL_MS ==
                  7 * 24 * 60 * 60 * 1000,
              "Update kDefaultMaxKvSoftPinTtlFlagValue when "
              "DEFAULT_MAX_KV_SOFT_PIN_TTL_MS changes");

constexpr char kDefaultKvLeaseTtlFlagValue[] = "10000";
constexpr char kDefaultKvSoftPinTtlFlagValue[] = "1800000";
constexpr char kDefaultMaxKvSoftPinTtlFlagValue[] = "604800000";
// Allowed range of --ha_leader_lease_ttl_sec.
constexpr int64_t kMinHaLeaderLeaseTtlSec = 2;
constexpr int64_t kMaxHaLeaderLeaseTtlSec = 60;

namespace {

bool ValidateDurationFlag(const char* flagname, const std::string& value) {
    uint64_t parsed_value = 0;
    std::string error;
    if (!mooncake::ParseDurationMs(value, &parsed_value, &error)) {
        LOG(ERROR) << "Invalid value for --" << flagname << ": " << value
                   << ". " << error;
        return false;
    }
    return true;
}

uint64_t ParseDurationFlagOrDie(const char* flag_name,
                                const std::string& value) {
    uint64_t parsed_value = 0;
    std::string error;
    if (!mooncake::ParseDurationMs(value, &parsed_value, &error)) {
        LOG(FATAL) << "Invalid value for --" << flag_name << ": " << value
                   << ". " << error;
    }
    return parsed_value;
}

// Derive the metadata server address for cleanup when it is deployed
// separately. Priority: MOONCAKE_TE_META_DATA_SERVER, then the
// "metadata_server" field of MOONCAKE_CONFIG_PATH. Returns "" if none found;
// the caller validates the scheme (only http(s) is supported).
std::string ResolveMetadataServerForCleanup() {
    if (const char* env = std::getenv("MOONCAKE_TE_META_DATA_SERVER")) {
        std::string value(env);
        // P2PHANDSHAKE has no central metadata server, nothing to clean up.
        if (!value.empty() && value != "P2PHANDSHAKE") {
            return value;
        }
    }

    if (const char* cfg = std::getenv("MOONCAKE_CONFIG_PATH")) {
        if (cfg[0] != '\0') {
            try {
                std::ifstream fin(cfg);
                if (fin) {
                    Json::CharReaderBuilder builder;
                    Json::Value root;
                    std::string errs;
                    if (Json::parseFromStream(builder, fin, &root, &errs) &&
                        root.isMember("metadata_server") &&
                        root["metadata_server"].isString()) {
                        return root["metadata_server"].asString();
                    }
                    if (!errs.empty()) {
                        LOG(WARNING) << "Failed to parse MOONCAKE_CONFIG_PATH ("
                                     << cfg << "): " << errs;
                    }
                } else {
                    LOG(WARNING) << "Cannot open MOONCAKE_CONFIG_PATH (" << cfg
                                 << "): file does not exist or is not readable";
                }
            } catch (const std::exception& e) {
                LOG(WARNING) << "Error reading MOONCAKE_CONFIG_PATH (" << cfg
                             << "): " << e.what();
            }
        }
    }

    return {};
}

}  // namespace

DEFINE_string(config_path, "", "master service config file path");
DEFINE_int32(port, 50051,
             "Port for master service to listen on (deprecated, use rpc_port)");
DEFINE_int32(
    max_threads, 16,
    "Maximum number of threads to use (deprecated, use rpc_thread_num)");
DEFINE_bool(enable_metric_reporting,
            mooncake::MetricsBootstrapConfig::kDefaultEnabled,
            "Enable periodic metric reporting");
DEFINE_int32(metrics_port, mooncake::MetricsBootstrapConfig::kDefaultPort,
             "Port for HTTP metrics server to listen on");
DEFINE_string(metrics_host,
              std::string(mooncake::MetricsBootstrapConfig::kDefaultHost),
              "Address for the HTTP metrics/admin server to listen on. "
              "Use \"::\" to listen on IPv6 (and IPv4 on dual-stack hosts)");
DEFINE_string(metrics_key_groups, "",
              "Break key count and value bytes down by key, e.g. "
              "\"kv:suffix:_k,state:prefix:state_\"; empty disables it");

DEFINE_int32(admin_metrics_log_interval_sec, 10,
             "Interval in seconds for the admin metrics log and the periodic "
             "VSegment status log (clamped to >= 1)");
DEFINE_string(default_kv_lease_ttl, kDefaultKvLeaseTtlFlagValue,
              "Default lease time for kv objects. Supports raw milliseconds "
              "or duration strings with ms, s, m, or h suffixes");
DEFINE_string(default_kv_soft_pin_ttl, kDefaultKvSoftPinTtlFlagValue,
              "Default soft pin TTL for kv objects. Supports raw milliseconds "
              "or duration strings with ms, s, m, or h suffixes");
DEFINE_string(max_kv_soft_pin_ttl, kDefaultMaxKvSoftPinTtlFlagValue,
              "Maximum request-level soft pin TTL for kv objects. Supports "
              "raw milliseconds or duration strings with ms, s, m, or h "
              "suffixes");
DEFINE_bool(allow_evict_soft_pinned_objects,
            mooncake::DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS,
            "Whether to allow eviction of soft pinned objects during eviction");
DEFINE_validator(default_kv_lease_ttl, ValidateDurationFlag);
DEFINE_validator(default_kv_soft_pin_ttl, ValidateDurationFlag);
DEFINE_validator(max_kv_soft_pin_ttl, ValidateDurationFlag);
DEFINE_double(eviction_ratio, mooncake::DEFAULT_EVICTION_RATIO,
              "Ratio of objects to evict when Memory space is full");
DEFINE_double(eviction_high_watermark_ratio,
              mooncake::DEFAULT_EVICTION_HIGH_WATERMARK_RATIO,
              "Ratio of high watermark trigger eviction in Memory");
DEFINE_double(tenant_eviction_high_watermark_ratio,
              mooncake::DEFAULT_TENANT_EVICTION_HIGH_WATERMARK_RATIO,
              "Per-tenant high watermark, as a fraction of that tenant's own "
              "effective quota, at which background eviction starts for it. "
              "Defaults to the same value as -eviction_high_watermark_ratio; "
              "0 disables it. Only meaningful with -enable_multi_tenants. "
              "This is the only mechanism that frees space for a tenant: "
              "admission itself never evicts, it rejects with "
              "TENANT_QUOTA_EXCEEDED. Without it, a tenant whose effective "
              "quota sits at or below eviction_high_watermark_ratio reaches "
              "its own ceiling before the pool crosses the pool-wide "
              "watermark, so nothing ever reclaims on its behalf");
DEFINE_double(nof_eviction_ratio, mooncake::DEFAULT_NOF_EVICTION_RATIO,
              "Ratio of objects to evict when NoF SSD space is full");
DEFINE_double(nof_eviction_high_watermark_ratio,
              mooncake::DEFAULT_NOF_EVICTION_HIGH_WATERMARK_RATIO,
              "Ratio of high watermark trigger eviction in NoF SSD");
DEFINE_bool(io_pattern_cold_eviction, false,
            "Enable report-driven cold-data eviction (embedded CFM component): "
            "merged client reports may drive bounded evictions of the coldest "
            "keys even when the memory high-watermark is not exceeded");
DEFINE_uint64(io_pattern_cold_eviction_bytes_per_cycle, 0,
              "Max bytes one report-driven cold-eviction pass may request "
              "(0 disables the cold driver)");
DEFINE_uint64(io_pattern_cold_idle_threshold_us, 0,
              "Minimum idle_time_us for a key to be eligible for a "
              "report-driven cold-eviction pass (0 disables the idle gate)");
DEFINE_uint64(
    io_pattern_tier_down_bytes_per_cycle, 0,
    "Max bytes one report-driven tier-down pass may copy per cycle. "
    "The pass runs below the memory watermark and copies the coldest "
    "objects down while keeping their MEMORY replica, so a later "
    "reclaim can discard them safely; a cycle that reaches the "
    "watermark evicts instead. 0 keeps the driver off, so this budget "
    "is the only switch the driver has");
DEFINE_uint32(io_pattern_admission_frequency_threshold, 2,
              "Min access count for a reported key before the io_pattern "
              "policy admits it (set 1 to admit on first sight)");
DEFINE_double(io_pattern_admission_watermark_ratio, -1.0,
              "Capacity watermark for io_pattern admission, as a ratio of "
              "capacity; a negative value derives it from "
              "eviction_high_watermark_ratio so admission stops before "
              "eviction starts");
// RPC server configuration parameters (new, preferred)
// TODO: deprecate port and max_threads in the future
DEFINE_int32(rpc_thread_num, 0,
             "Number of io threads for RPC server (0 = use max_threads, "
             "preferred over max_threads)");
DEFINE_int32(handler_worker_thread_num, 0,
             "Number of worker threads that run RPC handlers apart from the "
             "io threads, so a slow handler cannot delay a Ping (0 = as "
             "many as rpc_thread_num)");
DEFINE_validator(handler_worker_thread_num,
                 [](const char* flagname, int32_t value) {
                     // The config stores it unsigned, so a negative value
                     // would become a thread count asio refuses to start.
                     if (value < 0) {
                         LOG(ERROR) << flagname << " must not be negative";
                         return false;
                     }
                     return true;
                 });
DEFINE_int32(
    rpc_port, 0,
    "Port for RPC server to listen on (0 = use port, preferred over port)");
DEFINE_string(rpc_address, "0.0.0.0",
              "Address for RPC server to bind to, required in HA mode");
DEFINE_string(rpc_interface, "",
              "Network interface name for RPC server address resolution. "
              "When set, its IPv4 address overrides rpc_address");
DEFINE_int32(rpc_conn_timeout_seconds, 0,
             "Connection timeout in seconds (0 = no timeout)");
DEFINE_bool(rpc_enable_tcp_no_delay, true,
            "Enable TCP_NODELAY for RPC connections");
DEFINE_validator(eviction_ratio, [](const char* flagname, double value) {
    if (value < 0.0 || value > 1.0) {
        LOG(FATAL) << "Mem eviction ratio must be between 0.0 and 1.0";
        return false;
    }
    return true;
});
DEFINE_validator(tenant_eviction_high_watermark_ratio,
                 [](const char* flagname, double value) {
                     if (value < 0.0 || value > 1.0) {
                         LOG(FATAL) << "Tenant eviction high watermark ratio "
                                       "must be between 0.0 and 1.0";
                         return false;
                     }
                     return true;
                 });
DEFINE_validator(nof_eviction_ratio, [](const char* flagname, double value) {
    if (value < 0.0 || value > 1.0) {
        LOG(FATAL) << "NoF eviction ratio must be between 0.0 and 1.0";
        return false;
    }
    return true;
});
DEFINE_bool(enable_ha, false,
            "Enable high availability using the configured HA backend");

// vsegment 配额自动规划（启动 bootstrap，etcd HA 模式专属）：quota 快照
// 不存在时按以下策略自动「发现→规划→发布」（等价 vsegment_quota_planner
// --publish；高级参数 required_medium/io_alignment 等走 master 的
// POST /vsegment_quota 端点）。多实例并发安全：etcd 原子首写，
// first-writer-wins。变更策略需先删除已发布快照再重启（或改用端点重发）。
DEFINE_bool(vsegment_auto_plan, false,
            "Auto-plan and publish the vsegment quota snapshot at startup "
            "when none exists (etcd HA mode only)");
DEFINE_uint32(vsegment_member_count, 0,
              "vsegment auto-plan: number of distinct members per vsegment");
DEFINE_uint64(vsegment_stripe_size, 0,
              "vsegment auto-plan: stripe size in bytes");
DEFINE_uint64(vsegment_member_extent_size, 0,
              "vsegment auto-plan: extent size per member in bytes");
DEFINE_double(vsegment_reserved_ratio, 0.0,
              "vsegment auto-plan: fraction reserved outside vsegment "
              "quotas, in [0, 1). Default 0 means no reservation.");

DEFINE_bool(enable_offload, false, "Enable offload availability");
DEFINE_bool(offload_on_evict, false,
            "Defer LOCAL_DISK offload to eviction time instead of PutEnd");
DEFINE_bool(offload_force_evict, false,
            "Force-evict objects exceeding offload cap without disk offload");
DEFINE_bool(strict_replica_allocation, false,
            "Require memory-only multi-replica PutStart/UpsertStart to "
            "allocate exactly replica_num replicas instead of best-effort "
            "degradation to fewer replicas");
DEFINE_uint64(offloading_queue_limit, 50000,
              "Maximum number of objects allowed in the offloading queue per "
              "local disk segment. Increase to allow more objects to be "
              "offloaded to SSD before force-eviction kicks in");
DEFINE_validator(offloading_queue_limit, [](const char* flagname,
                                            uint64_t value) {
    // Zero would cause PushOffloadingQueue to always return
    // KEYS_ULTRA_LIMIT, disabling offload entirely. The upper
    // bound (1e8) keeps `offloading_queue_limit_ *
    // offload_cap_ratio_` well within signed long range to
    // avoid overflow when computing offload_cap in
    // BatchEvict / EvictNamespaceMemory.
    if (value == 0) {
        LOG(FATAL) << "offloading_queue_limit must be greater than 0";
        return false;
    }
    if (value > 100'000'000ULL) {
        LOG(FATAL) << "offloading_queue_limit must be <= "
                      "100000000 to avoid overflow";
        return false;
    }
    return true;
});
DEFINE_double(offload_cap_ratio, 0.5,
              "Per-cycle offload cap as a fraction of offloading_queue_limit. "
              "Controls how many objects can be queued for offload in a single "
              "eviction cycle before falling back to force-evict");
DEFINE_validator(offload_cap_ratio, [](const char* flagname, double value) {
    if (value < 0.0 || value > 1.0) {
        LOG(FATAL) << "offload_cap_ratio must be between 0.0 and 1.0";
        return false;
    }
    return true;
});
DEFINE_bool(promotion_on_hit, false,
            "Promote LOCAL_DISK-only keys to MEMORY on read access (mirror of "
            "offload_on_evict)");
DEFINE_uint32(promotion_admission_threshold, 2,
              "Min CountMinSketch count for a key before promotion fires "
              "(set 1 to disable second-touch gating)");
DEFINE_uint32(promotion_queue_limit, 50000,
              "Max in-flight promotion tasks across all shards");
DEFINE_uint32(promotion_max_per_heartbeat, 1,
              "Max promotion tasks returned to a single client per "
              "PromotionObjectHeartbeat call. Each task is a synchronous "
              "SSD-read + RDMA-write on the client; serializing them avoids "
              "blocking past the client-liveness window. Default 1 is "
              "conservative.");
DEFINE_string(dynamic_replication_mode, "off",
              "Dynamic MEMORY replica fanout mode: off, observe, or enforce");
DEFINE_uint32(dynamic_replication_heat_window_seconds, 10,
              "Heat observation window for dynamic MEMORY replication");
DEFINE_double(
    dynamic_replication_admission_qps_threshold, 0.8,
    "Per-key access frequency threshold for dynamic MEMORY replication");
DEFINE_uint64(
    dynamic_replication_max_memory_replicas, 2,
    "Maximum MEMORY replicas allowed for one dynamically replicated key");
DEFINE_bool(enable_kv_events, false,
            "Enable RFC #1527 KV cache event publisher over ZMQ");
DEFINE_string(kv_events_bind_endpoint, "",
              "ZMQ PUB bind endpoint for KV events, e.g. tcp://0.0.0.0:5557");
DEFINE_string(kv_events_model_name, "",
              "Fixed model name fallback for published KV events");
DEFINE_string(kv_events_backend_id, "",
              "backend_id for published KV events (cache owner identity)");
DEFINE_string(kv_events_tenant_id, "default",
              "Config compatibility default; events use per-object tenant_id");
DEFINE_string(kv_events_additional_salt, "",
              "Fixed hash namespace salt for published KV events");
DEFINE_string(
    kv_events_lora_name, "",
    "Fixed LoRA name for published KV events; empty means base model");
DEFINE_uint32(kv_events_block_size, 0,
              "Fixed KV block size for published events; 0 means unknown");
DEFINE_uint32(kv_events_dp_rank, 0,
              "Fixed data-parallel rank for published KV events");
DEFINE_bool(kv_events_emit_legacy_compat, true,
            "Include vLLM/SGLang-compatible type/block_hashes fields");
DEFINE_bool(kv_events_emit_object_key, true,
            "Include Mooncake object_key in published KV events");
DEFINE_uint32(kv_events_queue_capacity, 65536,
              "Maximum pending KV events; oldest events are dropped when "
              "the queue is full (0 = unbounded)");
DEFINE_string(ha_backend_type, "etcd",
              "HA backend type, e.g. etcd | redis | k8s");
DEFINE_int64(ha_leader_lease_ttl_sec,
             mooncake::DEFAULT_MASTER_VIEW_LEASE_TTL_SEC,
             "Leadership lease TTL in seconds for the etcd HA backend. A "
             "longer TTL tolerates longer etcd apply stalls but delays "
             "failover after a leader crash (range 2..60)");
DEFINE_validator(ha_leader_lease_ttl_sec, [](const char* flagname,
                                             int64_t value) {
    if (value < kMinHaLeaderLeaseTtlSec || value > kMaxHaLeaderLeaseTtlSec) {
        LOG(ERROR) << flagname << " must be in [" << kMinHaLeaderLeaseTtlSec
                   << ", " << kMaxHaLeaderLeaseTtlSec << "], got " << value;
        return false;
    }
    return true;
});
DEFINE_string(ha_backend_connstring, "",
              "HA backend connection string. If unset, only backend_type=etcd "
              "falls back to etcd_endpoints for backward compatibility");
DEFINE_string(
    etcd_endpoints, "",
    "Endpoints of ETCD server, separated by semicolon, required in HA mode");
DEFINE_int32(etcd_max_send_message_bytes, 2 * 1024 * 1024,
             "Maximum outgoing gRPC message size in bytes for the HA store "
             "etcd client; must be positive. Does not change server limits.");
DEFINE_int64(client_ttl, mooncake::DEFAULT_CLIENT_LIVE_TTL_SEC,
             "Deprecated alias for --client_active_ttl_sec");
DEFINE_int64(client_active_ttl_sec, mooncake::DEFAULT_CLIENT_LIVE_TTL_SEC,
             "Seconds a client remains active after its last liveness "
             "observation");
DEFINE_int64(client_suspicion_ttl_sec,
             mooncake::DEFAULT_CLIENT_SUSPICION_TTL_SEC,
             "Seconds a suspected client may recover before going offline; "
             "defaults to 20 seconds, or to an explicitly configured active "
             "TTL when omitted");

DEFINE_int64(master_view_lease_ttl_sec,
             mooncake::DEFAULT_MASTER_VIEW_LEASE_TTL_SEC,
             "Alias for --ha_leader_lease_ttl_sec; explicit values override "
             "the shared leader lease TTL.");
DEFINE_int64(nof_heartbeat_interval_sec,
             mooncake::DEFAULT_NOF_HEARTBEAT_INTERVAL_SEC,
             "How often master probes each mounted NoF segment");
DEFINE_uint32(nof_heartbeat_probe_timeout_ms,
              mooncake::DEFAULT_NOF_HEARTBEAT_PROBE_TIMEOUT_MS,
              "Timeout in milliseconds for a single NoF heartbeat probe");
DEFINE_uint32(
    nof_heartbeat_failures_threshold,
    mooncake::DEFAULT_NOF_HEARTBEAT_FAILURES_THRESHOLD,
    "Consecutive NoF heartbeat failures required before unmounting a NoF "
    "segment");

DEFINE_string(root_fs_dir, mooncake::DEFAULT_ROOT_FS_DIR,
              "Root directory for storage backend, used in HA mode");
DEFINE_int64(global_file_segment_size,
             mooncake::DEFAULT_GLOBAL_FILE_SEGMENT_SIZE,
             "Size of global NFS/3FS segment in bytes");
DEFINE_string(cluster_id, mooncake::DEFAULT_CLUSTER_ID,
              "Cluster ID for the master service, used for kvcache persistence "
              "in HA mode");
DEFINE_uint32(submaster_count, 1,
              "Maximum number of simultaneously serving submaster in the "
              "cluster (CVM quota coordination, first-come-first-served). "
              "Masters ranked beyond this limit are demoted to standby.");
DEFINE_uint32(cvm_http_port, 0,
              "Port for the CVM external HTTP API (CvmHttpServer). 0 keeps it "
              "disabled.");
DEFINE_string(cvm_http_host, "0.0.0.0",
              "Bind host for the CVM external HTTP API (CvmHttpServer).");
// 资源事实（Store 注册时上报到 SegmentDescriptor，供 vsegment 自动发现）。
DEFINE_string(cvm_segment_default_medium, "REGISTERED_MEMORY",
              "Default physical medium reported for segments at mount time. "
              "Must match VSegmentProfile.required_medium used by the vsegment "
              "quota planner. Default \"REGISTERED_MEMORY\".");
DEFINE_bool(cvm_segments_vsegment_exclusive, false,
            "Optional hint: declare that this cluster's segments are "
            "dedicated to vsegment and currently empty. true makes the "
            "planner verify used_bytes==0 (fast path, full capacity used); "
            "false (default) makes the planner use [used_bytes, capacity) "
            "as the allocatable range. No longer required — users need not "
            "set this for normal deployment.");

// OpLog store configuration
DEFINE_bool(enable_oplog, false,
            "Enable HA metadata replication through batch-record OpLog");
DEFINE_bool(weight_management_oplog_capability_confirmed, false,
            "Confirm every configured OpLog standby supports weight metadata "
            "and lease entry types before enabling weight mutations");
DEFINE_bool(enable_oplog_snapshot, false,
            "Enable standby batch OpLog snapshot production");
DEFINE_uint64(weight_import_idle_timeout_ms, 5 * 60 * 1000,
              "Abort managed weight imports after this many milliseconds "
              "without accepted member writes");
DEFINE_uint64(snapshot_chunk_object_count, 1000000,
              "Maximum objects per standby batch OpLog snapshot chunk");
DEFINE_uint64(snapshot_interval_seconds, 600,
              "Interval between standby batch OpLog snapshots in seconds");
DEFINE_string(snapshot_object_store_type, "",
              "Batch OpLog snapshot object store type: local or s3");
DEFINE_int32(oplog_poll_interval_ms, 1000,
             "Batch-record standby poll interval.");
DEFINE_uint32(oplog_batch_max_entries, 1024,
              "Maximum number of committed/reserved entries in the open "
              "batch-record OpLog waiting batch.");
DEFINE_uint32(batch_oplog_retry_timeout_sec, 180,
              "Maximum time to retry transient batch OpLog standby errors.");

// Metrics reporting to HA backend (etcd/redis)
DEFINE_bool(enable_metrics_report_to_backend,
            mooncake::DEFAULT_ENABLE_METRICS_REPORT_TO_BACKEND,
            "Periodically push master storage metrics to the HA backend "
            "(etcd/redis) so external monitoring services can discover "
            "cluster-wide usage without scraping each master's /metrics "
            "endpoint.");
DEFINE_int32(metrics_report_interval_sec,
             mooncake::DEFAULT_METRICS_REPORT_INTERVAL_SEC,
             "Interval in seconds between master metrics pushes.");
DEFINE_int32(metrics_report_lease_ttl_sec,
             mooncake::DEFAULT_METRICS_REPORT_LEASE_TTL_SEC,
             "TTL in seconds for the dedicated metrics-report lease. "
             "Must be > metrics_report_interval_sec.");

DEFINE_string(memory_allocator, "offset",
              "Memory allocator for global segments, cachelib | offset");
DEFINE_string(
    allocation_strategy, "random",
    "Allocation strategy for segments, random | free_ratio_first | cxl | "
    "ssd_free_ratio_first | local_first");
DEFINE_bool(enable_http_metadata_server, false,
            "Enable HTTP metadata server instead of etcd");
DEFINE_int32(http_metadata_server_port, 8080,
             "Port for HTTP metadata server to listen on");
DEFINE_string(http_metadata_server_host, "0.0.0.0",
              "Host for HTTP metadata server to bind to");
DEFINE_bool(
    enable_metadata_cleanup_on_timeout, false,
    "Enable cleanup of HTTP metadata (mooncake/ram/*, mooncake/rpc_meta/*) "
    "when client heartbeat times out. Works in two modes: (1) co-located "
    "(enable_http_metadata_server=true) via in-process removal, or "
    "(2) separately-deployed metadata server via async HTTP DELETE.");

DEFINE_string(pod_name, "",
              "Pod name for K8s label-based routing (default: $POD_NAME)");
DEFINE_string(pod_namespace, "",
              "Pod namespace for K8s label-based routing "
              "(default: $POD_NAMESPACE)");

DEFINE_uint64(put_start_discard_timeout_sec,
              mooncake::DEFAULT_PUT_START_DISCARD_TIMEOUT,
              "Timeout for discarding uncompleted PutStart operations");
DEFINE_uint64(put_start_release_timeout_sec,
              mooncake::DEFAULT_PUT_START_RELEASE_TIMEOUT,
              "Timeout for releasing space allocated in uncompleted PutStart "
              "operations");
DEFINE_bool(enable_disk_eviction, true,
            "Enable disk eviction feature for storage backend (default: true)");
DEFINE_uint64(
    quota_bytes, 0,
    "Quota for storage backend in bytes (0 = use default 90% of capacity)");
DEFINE_bool(enable_multi_tenants, false,
            "Enable strict multi-tenant namespace and quota admission");
DEFINE_string(tenant_quota_connector_type, "file",
              "Tenant quota policy connector type");
DEFINE_string(tenant_quota_connector_uri, "",
              "Tenant quota policy connector URI");

// Task manager configuration
DEFINE_uint32(max_total_finished_tasks, 10000,
              "Maximum number of finished tasks to keep in memory");
DEFINE_uint32(max_total_pending_tasks, 10000,
              "Maximum number of pending tasks to keep in memory");
DEFINE_uint32(max_total_processing_tasks, 10000,
              "Maximum number of processing tasks to keep in memory");
DEFINE_uint64(pending_task_timeout_sec, 300,
              "Timeout in seconds for pending tasks (0 = no timeout)");
DEFINE_uint64(processing_task_timeout_sec, 300,
              "Timeout in seconds for processing tasks (0 = no timeout)");
DEFINE_uint32(max_retry_attempts, 10,
              "Maximum number of retry attempts for failed tasks");

DEFINE_string(cxl_path, mooncake::DEFAULT_CXL_PATH,
              "DAX device path for CXL memory");
DEFINE_uint64(cxl_size, mooncake::DEFAULT_CXL_SIZE, "CXL memory size in bytes");
DEFINE_bool(enable_cxl, false, "Whether to enable CXL memory support");

namespace {

std::string ResolveHABackendConnstring(
    const mooncake::MasterConfig& master_config) {
    return mooncake::ResolveConfiguredHABackendConnstring(
        master_config.ha_backend_type, master_config.ha_backend_connstring,
        master_config.etcd_endpoints);
}

mooncake::MetricsBootstrapCommandLineOverrides
GetMetricsBootstrapCommandLineOverrides() {
    mooncake::MetricsBootstrapCommandLineOverrides command_line;
    google::CommandLineFlagInfo info;
    if (google::GetCommandLineFlagInfo("enable_metric_reporting", &info) &&
        !info.is_default) {
        command_line.enabled = FLAGS_enable_metric_reporting;
    }
    if (google::GetCommandLineFlagInfo("metrics_port", &info) &&
        !info.is_default) {
        command_line.port = static_cast<uint32_t>(FLAGS_metrics_port);
    }
    if (google::GetCommandLineFlagInfo("metrics_host", &info) &&
        !info.is_default) {
        command_line.host = FLAGS_metrics_host;
    }
    return command_line;
}

void ResolveRpcAddressFromInterfaceOrDie(
    mooncake::MasterConfig& master_config) {
    if (master_config.rpc_interface.empty()) {
        return;
    }

    if (!master_config.rpc_address.empty() &&
        master_config.rpc_address != "0.0.0.0") {
        LOG(WARNING) << "rpc_interface is set. Overriding rpc_address="
                     << master_config.rpc_address;
    }

    auto resolved_address =
        mooncake::GetInterfaceIPv4Address(master_config.rpc_interface);
    if (!resolved_address) {
        LOG(FATAL) << "Failed to resolve rpc_interface="
                   << master_config.rpc_interface << ": "
                   << resolved_address.error();
    }

    master_config.rpc_address = resolved_address.value();
    LOG(INFO) << "Resolved rpc_interface=" << master_config.rpc_interface
              << " to rpc_address=" << master_config.rpc_address;
}

}  // namespace

void InitMasterConf(const mooncake::DefaultConfig& default_config,
                    mooncake::MasterConfig& master_config) {
    // Initialize the master service configuration from the default config
    default_config.GetBool("enable_cxl", &master_config.enable_cxl,
                           FLAGS_enable_cxl);
    default_config.GetString("cxl_path", &master_config.cxl_path,
                             FLAGS_cxl_path);
    // cxl_size is size_t, which is not uint64_t on every platform (macOS).
    uint64_t cxl_size = master_config.cxl_size;
    default_config.GetUInt64("cxl_size", &cxl_size, FLAGS_cxl_size);
    master_config.cxl_size = cxl_size;
    default_config.GetUInt32("rpc_port", &master_config.rpc_port,
                             FLAGS_rpc_port);
    default_config.GetUInt32("rpc_thread_num", &master_config.rpc_thread_num,
                             FLAGS_rpc_thread_num);
    default_config.GetUInt32("handler_worker_thread_num",
                             &master_config.handler_worker_thread_num,
                             FLAGS_handler_worker_thread_num);
    default_config.GetString("rpc_address", &master_config.rpc_address,
                             FLAGS_rpc_address);
    default_config.GetString("rpc_interface", &master_config.rpc_interface,
                             FLAGS_rpc_interface);
    default_config.GetInt32("rpc_conn_timeout_seconds",
                            &master_config.rpc_conn_timeout_seconds,
                            FLAGS_rpc_conn_timeout_seconds);
    default_config.GetBool("rpc_enable_tcp_no_delay",
                           &master_config.rpc_enable_tcp_no_delay,
                           FLAGS_rpc_enable_tcp_no_delay);
    default_config.GetDurationMs("default_kv_lease_ttl",
                                 &master_config.default_kv_lease_ttl,
                                 mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL);
    default_config.GetDurationMs("default_kv_soft_pin_ttl",
                                 &master_config.default_kv_soft_pin_ttl,
                                 mooncake::DEFAULT_KV_SOFT_PIN_TTL_MS);
    default_config.GetDurationMs("max_kv_soft_pin_ttl",
                                 &master_config.max_kv_soft_pin_ttl,
                                 mooncake::DEFAULT_MAX_KV_SOFT_PIN_TTL_MS);
    default_config.GetBool("allow_evict_soft_pinned_objects",
                           &master_config.allow_evict_soft_pinned_objects,
                           FLAGS_allow_evict_soft_pinned_objects);
    default_config.GetDouble("eviction_ratio", &master_config.eviction_ratio,
                             FLAGS_eviction_ratio);
    default_config.GetDouble("eviction_high_watermark_ratio",
                             &master_config.eviction_high_watermark_ratio,
                             FLAGS_eviction_high_watermark_ratio);
    default_config.GetDouble(
        "tenant_eviction_high_watermark_ratio",
        &master_config.tenant_eviction_high_watermark_ratio,
        FLAGS_tenant_eviction_high_watermark_ratio);
    default_config.GetDouble("nof_eviction_ratio",
                             &master_config.nof_eviction_ratio,
                             FLAGS_nof_eviction_ratio);
    default_config.GetDouble("nof_eviction_high_watermark_ratio",
                             &master_config.nof_eviction_high_watermark_ratio,
                             FLAGS_nof_eviction_high_watermark_ratio);
    default_config.GetBool("io_pattern_cold_eviction",
                           &master_config.io_pattern_cold_eviction,
                           FLAGS_io_pattern_cold_eviction);
    default_config.GetUInt64(
        "io_pattern_cold_eviction_bytes_per_cycle",
        &master_config.io_pattern_cold_eviction_bytes_per_cycle,
        FLAGS_io_pattern_cold_eviction_bytes_per_cycle);
    default_config.GetUInt64("io_pattern_cold_idle_threshold_us",
                             &master_config.io_pattern_cold_idle_threshold_us,
                             FLAGS_io_pattern_cold_idle_threshold_us);
    default_config.GetUInt64(
        "io_pattern_tier_down_bytes_per_cycle",
        &master_config.io_pattern_tier_down_bytes_per_cycle,
        FLAGS_io_pattern_tier_down_bytes_per_cycle);
    default_config.GetUInt32(
        "io_pattern_admission_frequency_threshold",
        &master_config.io_pattern_admission_frequency_threshold,
        FLAGS_io_pattern_admission_frequency_threshold);
    default_config.GetDouble(
        "io_pattern_admission_watermark_ratio",
        &master_config.io_pattern_admission_watermark_ratio,
        FLAGS_io_pattern_admission_watermark_ratio);
    default_config.GetInt64("nof_heartbeat_interval_sec",
                            &master_config.nof_heartbeat_interval_sec,
                            FLAGS_nof_heartbeat_interval_sec);
    default_config.GetUInt32("nof_heartbeat_probe_timeout_ms",
                             &master_config.nof_heartbeat_probe_timeout_ms,
                             FLAGS_nof_heartbeat_probe_timeout_ms);
    default_config.GetUInt32("nof_heartbeat_failures_threshold",
                             &master_config.nof_heartbeat_failures_threshold,
                             FLAGS_nof_heartbeat_failures_threshold);

    default_config.GetBool("enable_ha", &master_config.enable_ha,
                           FLAGS_enable_ha);
    default_config.GetBool("enable_offload", &master_config.enable_offload,
                           FLAGS_enable_offload);
    default_config.GetBool("offload_on_evict", &master_config.offload_on_evict,
                           FLAGS_offload_on_evict);
    default_config.GetBool("offload_force_evict",
                           &master_config.offload_force_evict,
                           FLAGS_offload_force_evict);
    default_config.GetBool("strict_replica_allocation",
                           &master_config.strict_replica_allocation,
                           FLAGS_strict_replica_allocation);
    {
        uint64_t tmp_offloading_queue_limit = FLAGS_offloading_queue_limit;
        default_config.GetUInt64("offloading_queue_limit",
                                 &tmp_offloading_queue_limit,
                                 FLAGS_offloading_queue_limit);
        master_config.offloading_queue_limit =
            static_cast<size_t>(tmp_offloading_queue_limit);
    }
    default_config.GetDouble("offload_cap_ratio",
                             &master_config.offload_cap_ratio,
                             FLAGS_offload_cap_ratio);
    default_config.GetBool("promotion_on_hit", &master_config.promotion_on_hit,
                           FLAGS_promotion_on_hit);
    default_config.GetUInt32("promotion_admission_threshold",
                             &master_config.promotion_admission_threshold,
                             FLAGS_promotion_admission_threshold);
    default_config.GetUInt32("promotion_queue_limit",
                             &master_config.promotion_queue_limit,
                             FLAGS_promotion_queue_limit);
    default_config.GetUInt32("promotion_max_per_heartbeat",
                             &master_config.promotion_max_per_heartbeat,
                             FLAGS_promotion_max_per_heartbeat);
    default_config.GetString("dynamic_replication_mode",
                             &master_config.dynamic_replication_mode,
                             FLAGS_dynamic_replication_mode);
    default_config.GetUInt32(
        "dynamic_replication_heat_window_seconds",
        &master_config.dynamic_replication_heat_window_seconds,
        FLAGS_dynamic_replication_heat_window_seconds);
    default_config.GetDouble(
        "dynamic_replication_admission_qps_threshold",
        &master_config.dynamic_replication_admission_qps_threshold,
        FLAGS_dynamic_replication_admission_qps_threshold);
    {
        uint64_t tmp_dynamic_replication_max_memory_replicas =
            FLAGS_dynamic_replication_max_memory_replicas;
        default_config.GetUInt64("dynamic_replication_max_memory_replicas",
                                 &tmp_dynamic_replication_max_memory_replicas,
                                 FLAGS_dynamic_replication_max_memory_replicas);
        master_config.dynamic_replication_max_memory_replicas =
            static_cast<size_t>(tmp_dynamic_replication_max_memory_replicas);
    }
    default_config.GetBool("enable_kv_events", &master_config.enable_kv_events,
                           FLAGS_enable_kv_events);
    default_config.GetString("kv_events_bind_endpoint",
                             &master_config.kv_events_bind_endpoint,
                             FLAGS_kv_events_bind_endpoint);
    default_config.GetString("kv_events_model_name",
                             &master_config.kv_events_model_name,
                             FLAGS_kv_events_model_name);
    default_config.GetString("kv_events_backend_id",
                             &master_config.kv_events_backend_id,
                             FLAGS_kv_events_backend_id);
    default_config.GetString("kv_events_tenant_id",
                             &master_config.kv_events_tenant_id,
                             FLAGS_kv_events_tenant_id);
    default_config.GetString("kv_events_additional_salt",
                             &master_config.kv_events_additional_salt,
                             FLAGS_kv_events_additional_salt);
    default_config.GetString("kv_events_lora_name",
                             &master_config.kv_events_lora_name,
                             FLAGS_kv_events_lora_name);
    default_config.GetUInt32("kv_events_block_size",
                             &master_config.kv_events_block_size,
                             FLAGS_kv_events_block_size);
    default_config.GetUInt32("kv_events_dp_rank",
                             &master_config.kv_events_dp_rank,
                             FLAGS_kv_events_dp_rank);
    default_config.GetBool("kv_events_emit_legacy_compat",
                           &master_config.kv_events_emit_legacy_compat,
                           FLAGS_kv_events_emit_legacy_compat);
    default_config.GetBool("kv_events_emit_object_key",
                           &master_config.kv_events_emit_object_key,
                           FLAGS_kv_events_emit_object_key);
    default_config.GetUInt32("kv_events_queue_capacity",
                             &master_config.kv_events_queue_capacity,
                             FLAGS_kv_events_queue_capacity);
    default_config.GetString("ha_backend_type", &master_config.ha_backend_type,
                             FLAGS_ha_backend_type);
    default_config.GetString("ha_backend_connstring",
                             &master_config.ha_backend_connstring,
                             FLAGS_ha_backend_connstring);
    default_config.GetInt64("ha_leader_lease_ttl_sec",
                            &master_config.ha_leader_lease_ttl_sec,
                            FLAGS_ha_leader_lease_ttl_sec);
    default_config.GetString("etcd_endpoints", &master_config.etcd_endpoints,
                             FLAGS_etcd_endpoints);
    default_config.GetInt32("etcd_max_send_message_bytes",
                            &master_config.etcd_max_send_message_bytes,
                            FLAGS_etcd_max_send_message_bytes);
    default_config.GetInt64("master_view_lease_ttl_sec",
                            &master_config.master_view_lease_ttl_sec,
                            FLAGS_master_view_lease_ttl_sec);
    default_config.GetString("cluster_id", &master_config.cluster_id,
                             FLAGS_cluster_id);
    default_config.GetBool("enable_oplog", &master_config.enable_oplog,
                           FLAGS_enable_oplog);
    default_config.GetBool(
        "weight_management_oplog_capability_confirmed",
        &master_config.weight_management_oplog_capability_confirmed,
        FLAGS_weight_management_oplog_capability_confirmed);
    default_config.GetBool("enable_oplog_snapshot",
                           &master_config.enable_oplog_snapshot,
                           FLAGS_enable_oplog_snapshot);
    default_config.GetUInt64("weight_import_idle_timeout_ms",
                             &master_config.weight_import_idle_timeout_ms,
                             FLAGS_weight_import_idle_timeout_ms);
    default_config.GetUInt64("snapshot_chunk_object_count",
                             &master_config.snapshot_chunk_object_count,
                             FLAGS_snapshot_chunk_object_count);
    default_config.GetUInt64("snapshot_interval_seconds",
                             &master_config.snapshot_interval_seconds, 600);
    default_config.GetString("snapshot_object_store_type",
                             &master_config.snapshot_object_store_type, "");
    default_config.GetInt32("oplog_poll_interval_ms",
                            &master_config.oplog_poll_interval_ms,
                            FLAGS_oplog_poll_interval_ms);
    default_config.GetUInt32("oplog_batch_max_entries",
                             &master_config.oplog_batch_max_entries,
                             FLAGS_oplog_batch_max_entries);
    default_config.GetUInt32("batch_oplog_retry_timeout_sec",
                             &master_config.batch_oplog_retry_timeout_sec,
                             FLAGS_batch_oplog_retry_timeout_sec);
    default_config.GetBool("enable_metrics_report_to_backend",
                           &master_config.enable_metrics_report_to_backend,
                           FLAGS_enable_metrics_report_to_backend);
    default_config.GetInt32("metrics_report_interval_sec",
                            &master_config.metrics_report_interval_sec,
                            FLAGS_metrics_report_interval_sec);
    default_config.GetInt32("metrics_report_lease_ttl_sec",
                            &master_config.metrics_report_lease_ttl_sec,
                            FLAGS_metrics_report_lease_ttl_sec);
    default_config.GetString("root_fs_dir", &master_config.root_fs_dir,
                             FLAGS_root_fs_dir);
    default_config.GetInt64("global_file_segment_size",
                            &master_config.global_file_segment_size,
                            FLAGS_global_file_segment_size);
    default_config.GetString("memory_allocator",
                             &master_config.memory_allocator,
                             FLAGS_memory_allocator);
    default_config.GetString("allocation_strategy",
                             &master_config.allocation_strategy,
                             FLAGS_allocation_strategy);
    default_config.GetBool("enable_http_metadata_server",
                           &master_config.enable_http_metadata_server,
                           FLAGS_enable_http_metadata_server);
    default_config.GetUInt32("http_metadata_server_port",
                             &master_config.http_metadata_server_port,
                             FLAGS_http_metadata_server_port);
    default_config.GetString("http_metadata_server_host",
                             &master_config.http_metadata_server_host,
                             FLAGS_http_metadata_server_host);
    default_config.GetString("pod_name", &master_config.pod_name,
                             FLAGS_pod_name);
    default_config.GetString("pod_namespace", &master_config.pod_namespace,
                             FLAGS_pod_namespace);
    default_config.GetBool("enable_metadata_cleanup_on_timeout",
                           &master_config.enable_metadata_cleanup_on_timeout,
                           FLAGS_enable_metadata_cleanup_on_timeout);
    default_config.GetUInt64("put_start_discard_timeout_sec",
                             &master_config.put_start_discard_timeout_sec,
                             FLAGS_put_start_discard_timeout_sec);
    default_config.GetUInt64("put_start_release_timeout_sec",
                             &master_config.put_start_release_timeout_sec,
                             FLAGS_put_start_release_timeout_sec);
    default_config.GetBool("enable_disk_eviction",
                           &master_config.enable_disk_eviction,
                           FLAGS_enable_disk_eviction);
    default_config.GetUInt64("quota_bytes", &master_config.quota_bytes,
                             FLAGS_quota_bytes);
    default_config.GetBool("enable_multi_tenants",
                           &master_config.enable_multi_tenants,
                           FLAGS_enable_multi_tenants);
    default_config.GetString("tenant_quota_connector_type",
                             &master_config.tenant_quota_connector_type,
                             FLAGS_tenant_quota_connector_type);
    default_config.GetString("tenant_quota_connector_uri",
                             &master_config.tenant_quota_connector_uri,
                             FLAGS_tenant_quota_connector_uri);

    default_config.GetUInt32("max_total_finished_tasks",
                             &master_config.max_total_finished_tasks,
                             FLAGS_max_total_finished_tasks);
    default_config.GetUInt32("max_total_pending_tasks",
                             &master_config.max_total_pending_tasks,
                             FLAGS_max_total_pending_tasks);
    default_config.GetUInt32("max_total_processing_tasks",
                             &master_config.max_total_processing_tasks,
                             FLAGS_max_total_processing_tasks);
    default_config.GetUInt64("pending_task_timeout_sec",
                             &master_config.pending_task_timeout_sec,
                             FLAGS_pending_task_timeout_sec);
    default_config.GetUInt64("processing_task_timeout_sec",
                             &master_config.processing_task_timeout_sec,
                             FLAGS_processing_task_timeout_sec);
    default_config.GetUInt32("max_retry_attempts",
                             &master_config.max_retry_attempts,
                             FLAGS_max_retry_attempts);
}

void LoadConfigFromCmdline(mooncake::MasterConfig& master_config,
                           bool conf_set) {
    if (FLAGS_max_threads != 16) {  // 16 is the default value
        LOG(WARNING) << "max_threads is deprecated, use rpc_thread_num instead";
    }
    if (FLAGS_port != 50051) {  // 50051 is the default value
        LOG(WARNING) << "port is deprecated, use rpc_port instead";
    }
    int server_thread_num =
        std::min(FLAGS_max_threads,
                 static_cast<int>(std::thread::hardware_concurrency()));

    // Handle backward compatibility for RPC configuration
    // Determine RPC server thread number with compatibility warnings
    // TODO: remove this in the future
    size_t rpc_thread_num;
    if (FLAGS_rpc_thread_num > 0) {
        rpc_thread_num = static_cast<size_t>(FLAGS_rpc_thread_num);
        if (FLAGS_max_threads != 16) {  // 16 is the default value
            LOG(WARNING) << "Both rpc_thread_num and max_threads are set. "
                         << "Using rpc_thread_num=" << FLAGS_rpc_thread_num
                         << ". Please migrate to use rpc_thread_num only.";
        }
        master_config.rpc_thread_num = rpc_thread_num;
    } else if (!conf_set || master_config.rpc_thread_num == 0) {
        // A config file without rpc_thread_num leaves it at the flag's 0,
        // which must fall back like the command line does: the RPC handler
        // worker pool below is sized from it.
        master_config.rpc_thread_num = server_thread_num;
    }

    // Determine RPC server port with compatibility warnings
    int rpc_port;
    if (FLAGS_rpc_port > 0) {
        rpc_port = FLAGS_rpc_port;
        if (FLAGS_port != 50051) {  // 50051 is the default value
            LOG(WARNING) << "Both rpc_port and port are set. "
                         << "Using rpc_port=" << FLAGS_rpc_port
                         << ". Please migrate to use rpc_port only.";
        }
        master_config.rpc_port = rpc_port;
    } else {
        if (!conf_set) {
            master_config.rpc_port = FLAGS_port;
        }
    }

    google::CommandLineFlagInfo info;
    if ((google::GetCommandLineFlagInfo("enable_cxl", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_cxl = FLAGS_enable_cxl;
    }
    if ((google::GetCommandLineFlagInfo("cxl_path", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.cxl_path = FLAGS_cxl_path;
    }
    if ((google::GetCommandLineFlagInfo("cxl_size", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.cxl_size = FLAGS_cxl_size;
    }
    if ((google::GetCommandLineFlagInfo("rpc_address", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.rpc_address = FLAGS_rpc_address;
    }
    if ((google::GetCommandLineFlagInfo("rpc_interface", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.rpc_interface = FLAGS_rpc_interface;
    }
    if ((google::GetCommandLineFlagInfo("rpc_conn_timeout_seconds", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.rpc_conn_timeout_seconds = FLAGS_rpc_conn_timeout_seconds;
    }
    if ((google::GetCommandLineFlagInfo("rpc_enable_tcp_no_delay", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.rpc_enable_tcp_no_delay = FLAGS_rpc_enable_tcp_no_delay;
    }
    if ((google::GetCommandLineFlagInfo("handler_worker_thread_num", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.handler_worker_thread_num =
            FLAGS_handler_worker_thread_num;
    }
    if (master_config.handler_worker_thread_num == 0) {
        master_config.handler_worker_thread_num = master_config.rpc_thread_num;
    }
    if ((google::GetCommandLineFlagInfo("default_kv_lease_ttl", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.default_kv_lease_ttl = ParseDurationFlagOrDie(
            "default_kv_lease_ttl", FLAGS_default_kv_lease_ttl);
    }
    if ((google::GetCommandLineFlagInfo("default_kv_soft_pin_ttl", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.default_kv_soft_pin_ttl = ParseDurationFlagOrDie(
            "default_kv_soft_pin_ttl", FLAGS_default_kv_soft_pin_ttl);
    }
    if ((google::GetCommandLineFlagInfo("max_kv_soft_pin_ttl", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.max_kv_soft_pin_ttl = ParseDurationFlagOrDie(
            "max_kv_soft_pin_ttl", FLAGS_max_kv_soft_pin_ttl);
    }
    if ((google::GetCommandLineFlagInfo("allow_evict_soft_pinned_objects",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.allow_evict_soft_pinned_objects =
            FLAGS_allow_evict_soft_pinned_objects;
    }
    if ((google::GetCommandLineFlagInfo("eviction_ratio", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.eviction_ratio = FLAGS_eviction_ratio;
    }
    if ((google::GetCommandLineFlagInfo("eviction_high_watermark_ratio",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.eviction_high_watermark_ratio =
            FLAGS_eviction_high_watermark_ratio;
    }
    if ((google::GetCommandLineFlagInfo("tenant_eviction_high_watermark_ratio",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.tenant_eviction_high_watermark_ratio =
            FLAGS_tenant_eviction_high_watermark_ratio;
    }
    if ((google::GetCommandLineFlagInfo("nof_eviction_ratio", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.nof_eviction_ratio = FLAGS_nof_eviction_ratio;
    }
    if ((google::GetCommandLineFlagInfo("nof_eviction_high_watermark_ratio",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.nof_eviction_high_watermark_ratio =
            FLAGS_nof_eviction_high_watermark_ratio;
    }
    if ((google::GetCommandLineFlagInfo("io_pattern_cold_eviction", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.io_pattern_cold_eviction = FLAGS_io_pattern_cold_eviction;
    }
    if ((google::GetCommandLineFlagInfo(
             "io_pattern_cold_eviction_bytes_per_cycle", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.io_pattern_cold_eviction_bytes_per_cycle =
            FLAGS_io_pattern_cold_eviction_bytes_per_cycle;
    }
    if ((google::GetCommandLineFlagInfo("io_pattern_cold_idle_threshold_us",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.io_pattern_cold_idle_threshold_us =
            FLAGS_io_pattern_cold_idle_threshold_us;
    }
    if ((google::GetCommandLineFlagInfo("io_pattern_tier_down_bytes_per_cycle",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.io_pattern_tier_down_bytes_per_cycle =
            FLAGS_io_pattern_tier_down_bytes_per_cycle;
    }
    if ((google::GetCommandLineFlagInfo(
             "io_pattern_admission_frequency_threshold", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.io_pattern_admission_frequency_threshold =
            FLAGS_io_pattern_admission_frequency_threshold;
    }
    if ((google::GetCommandLineFlagInfo("io_pattern_admission_watermark_ratio",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.io_pattern_admission_watermark_ratio =
            FLAGS_io_pattern_admission_watermark_ratio;
    }
    if ((google::GetCommandLineFlagInfo("enable_ha", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_ha = FLAGS_enable_ha;
    }
    if ((google::GetCommandLineFlagInfo("enable_offload", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_offload = FLAGS_enable_offload;
    }
    if ((google::GetCommandLineFlagInfo("offload_on_evict", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.offload_on_evict = FLAGS_offload_on_evict;
    }
    if ((google::GetCommandLineFlagInfo("offload_force_evict", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.offload_force_evict = FLAGS_offload_force_evict;
    }
    if ((google::GetCommandLineFlagInfo("strict_replica_allocation", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.strict_replica_allocation =
            FLAGS_strict_replica_allocation;
    }
    if ((google::GetCommandLineFlagInfo("offloading_queue_limit", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.offloading_queue_limit =
            static_cast<size_t>(FLAGS_offloading_queue_limit);
    }
    if ((google::GetCommandLineFlagInfo("offload_cap_ratio", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.offload_cap_ratio = FLAGS_offload_cap_ratio;
    }
    if ((google::GetCommandLineFlagInfo("promotion_on_hit", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.promotion_on_hit = FLAGS_promotion_on_hit;
    }
    if ((google::GetCommandLineFlagInfo("promotion_admission_threshold",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.promotion_admission_threshold =
            FLAGS_promotion_admission_threshold;
    }
    if ((google::GetCommandLineFlagInfo("promotion_queue_limit", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.promotion_queue_limit = FLAGS_promotion_queue_limit;
    }
    if ((google::GetCommandLineFlagInfo("promotion_max_per_heartbeat", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.promotion_max_per_heartbeat =
            FLAGS_promotion_max_per_heartbeat;
    }
    if ((google::GetCommandLineFlagInfo("dynamic_replication_mode", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.dynamic_replication_mode = FLAGS_dynamic_replication_mode;
    }
    if ((google::GetCommandLineFlagInfo(
             "dynamic_replication_heat_window_seconds", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.dynamic_replication_heat_window_seconds =
            FLAGS_dynamic_replication_heat_window_seconds;
    }
    if ((google::GetCommandLineFlagInfo(
             "dynamic_replication_admission_qps_threshold", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.dynamic_replication_admission_qps_threshold =
            FLAGS_dynamic_replication_admission_qps_threshold;
    }
    if ((google::GetCommandLineFlagInfo(
             "dynamic_replication_max_memory_replicas", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.dynamic_replication_max_memory_replicas =
            static_cast<size_t>(FLAGS_dynamic_replication_max_memory_replicas);
    }
    if ((google::GetCommandLineFlagInfo("enable_kv_events", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_kv_events = FLAGS_enable_kv_events;
    }
    if ((google::GetCommandLineFlagInfo("kv_events_bind_endpoint", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.kv_events_bind_endpoint = FLAGS_kv_events_bind_endpoint;
    }
    if ((google::GetCommandLineFlagInfo("kv_events_model_name", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.kv_events_model_name = FLAGS_kv_events_model_name;
    }
    if ((google::GetCommandLineFlagInfo("kv_events_backend_id", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.kv_events_backend_id = FLAGS_kv_events_backend_id;
    }
    if ((google::GetCommandLineFlagInfo("kv_events_tenant_id", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.kv_events_tenant_id = FLAGS_kv_events_tenant_id;
    }
    if ((google::GetCommandLineFlagInfo("kv_events_additional_salt", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.kv_events_additional_salt =
            FLAGS_kv_events_additional_salt;
    }
    if ((google::GetCommandLineFlagInfo("kv_events_lora_name", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.kv_events_lora_name = FLAGS_kv_events_lora_name;
    }
    if ((google::GetCommandLineFlagInfo("kv_events_block_size", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.kv_events_block_size = FLAGS_kv_events_block_size;
    }
    if ((google::GetCommandLineFlagInfo("kv_events_dp_rank", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.kv_events_dp_rank = FLAGS_kv_events_dp_rank;
    }
    if ((google::GetCommandLineFlagInfo("kv_events_emit_legacy_compat",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.kv_events_emit_legacy_compat =
            FLAGS_kv_events_emit_legacy_compat;
    }
    if ((google::GetCommandLineFlagInfo("kv_events_emit_object_key", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.kv_events_emit_object_key =
            FLAGS_kv_events_emit_object_key;
    }
    if ((google::GetCommandLineFlagInfo("kv_events_queue_capacity", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.kv_events_queue_capacity = FLAGS_kv_events_queue_capacity;
    }
    // Clamp promotion_admission_threshold into the sketch counter's
    // representable range. The CountMinSketch uses 8-bit saturating
    // counters (max 255) so any threshold beyond that would silently
    // make the gate unreachable; clamping at parse time fails loudly and
    // gives the gate a stable contract to compare uint8_t against.
    if (master_config.promotion_admission_threshold > 255) {
        LOG(WARNING) << "promotion_admission_threshold="
                     << master_config.promotion_admission_threshold
                     << " exceeds the CountMinSketch counter max (255). "
                     << "Clamping to 255. Lower the configured value to "
                     << "silence this warning.";
        master_config.promotion_admission_threshold = 255;
    }
    if (master_config.dynamic_replication_mode != "off" &&
        master_config.dynamic_replication_mode != "observe" &&
        master_config.dynamic_replication_mode != "enforce") {
        LOG(FATAL)
            << "dynamic_replication_mode must be one of off, observe, enforce";
    }
    if (master_config.dynamic_replication_heat_window_seconds == 0) {
        LOG(WARNING)
            << "dynamic_replication_heat_window_seconds=0; clamping to 1";
        master_config.dynamic_replication_heat_window_seconds = 1;
    }
    if (master_config.dynamic_replication_admission_qps_threshold <= 0.0) {
        LOG(FATAL) << "dynamic_replication_admission_qps_threshold must be > 0";
    }
    if (master_config.dynamic_replication_max_memory_replicas == 0) {
        LOG(WARNING)
            << "dynamic_replication_max_memory_replicas=0; clamping to 1";
        master_config.dynamic_replication_max_memory_replicas = 1;
    }
    if ((google::GetCommandLineFlagInfo("ha_backend_type", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.ha_backend_type = FLAGS_ha_backend_type;
    }
    if ((google::GetCommandLineFlagInfo("ha_backend_connstring", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.ha_backend_connstring = FLAGS_ha_backend_connstring;
    }
    if ((google::GetCommandLineFlagInfo("ha_leader_lease_ttl_sec", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.ha_leader_lease_ttl_sec = FLAGS_ha_leader_lease_ttl_sec;
    }
    if (master_config.ha_leader_lease_ttl_sec < kMinHaLeaderLeaseTtlSec ||
        master_config.ha_leader_lease_ttl_sec > kMaxHaLeaderLeaseTtlSec) {
        LOG(FATAL) << "ha_leader_lease_ttl_sec must be in ["
                   << kMinHaLeaderLeaseTtlSec << ", " << kMaxHaLeaderLeaseTtlSec
                   << "], got " << master_config.ha_leader_lease_ttl_sec;
    }
    if ((google::GetCommandLineFlagInfo("etcd_endpoints", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.etcd_endpoints = FLAGS_etcd_endpoints;
    }
    if ((google::GetCommandLineFlagInfo("etcd_max_send_message_bytes", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.etcd_max_send_message_bytes =
            FLAGS_etcd_max_send_message_bytes;
    }
    if ((google::GetCommandLineFlagInfo("master_view_lease_ttl_sec", &info) &&
         !info.is_default)) {
        master_config.master_view_lease_ttl_sec =
            FLAGS_master_view_lease_ttl_sec;
    } else if (master_config.master_view_lease_ttl_sec ==
               mooncake::DEFAULT_MASTER_VIEW_LEASE_TTL_SEC) {
        // Alias: no explicit flag and no config-file value -> share the
        // leader lease TTL.
        master_config.master_view_lease_ttl_sec =
            master_config.ha_leader_lease_ttl_sec;
    }
    if (master_config.etcd_max_send_message_bytes <= 0) {
        LOG(FATAL) << "etcd_max_send_message_bytes must be positive";
    }
    if ((google::GetCommandLineFlagInfo("nof_heartbeat_interval_sec", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.nof_heartbeat_interval_sec =
            FLAGS_nof_heartbeat_interval_sec;
    }
    if ((google::GetCommandLineFlagInfo("nof_heartbeat_probe_timeout_ms",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.nof_heartbeat_probe_timeout_ms =
            FLAGS_nof_heartbeat_probe_timeout_ms;
    }
    if ((google::GetCommandLineFlagInfo("nof_heartbeat_failures_threshold",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.nof_heartbeat_failures_threshold =
            FLAGS_nof_heartbeat_failures_threshold;
    }
    if ((google::GetCommandLineFlagInfo("cluster_id", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.cluster_id = FLAGS_cluster_id;
    }
    if ((google::GetCommandLineFlagInfo("submaster_count", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.submaster_count = FLAGS_submaster_count;
    }
    if ((google::GetCommandLineFlagInfo("cvm_http_port", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.cvm_http_port =
            static_cast<uint16_t>(FLAGS_cvm_http_port);
    }
    if ((google::GetCommandLineFlagInfo("admin_metrics_log_interval_sec",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.admin_metrics_log_interval_sec =
            std::max(1, FLAGS_admin_metrics_log_interval_sec);
    }
    if ((google::GetCommandLineFlagInfo("cvm_http_host", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.cvm_http_host = FLAGS_cvm_http_host;
    }
    if ((google::GetCommandLineFlagInfo("cvm_segment_default_medium", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.cvm_segment_default_medium =
            FLAGS_cvm_segment_default_medium;
    }
    if ((google::GetCommandLineFlagInfo("cvm_segments_vsegment_exclusive",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.cvm_segments_vsegment_exclusive =
            FLAGS_cvm_segments_vsegment_exclusive;
    }
    if ((google::GetCommandLineFlagInfo("enable_oplog", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_oplog = FLAGS_enable_oplog;
    }
    if ((google::GetCommandLineFlagInfo(
             "weight_management_oplog_capability_confirmed", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.weight_management_oplog_capability_confirmed =
            FLAGS_weight_management_oplog_capability_confirmed;
    }
    if ((google::GetCommandLineFlagInfo("enable_oplog_snapshot", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_oplog_snapshot = FLAGS_enable_oplog_snapshot;
    }
    if ((google::GetCommandLineFlagInfo("weight_import_idle_timeout_ms",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.weight_import_idle_timeout_ms =
            FLAGS_weight_import_idle_timeout_ms;
    }
    if ((google::GetCommandLineFlagInfo("snapshot_chunk_object_count", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.snapshot_chunk_object_count =
            FLAGS_snapshot_chunk_object_count;
    }
    if ((google::GetCommandLineFlagInfo("snapshot_interval_seconds", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.snapshot_interval_seconds =
            FLAGS_snapshot_interval_seconds;
    }
    if ((google::GetCommandLineFlagInfo("snapshot_object_store_type", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.snapshot_object_store_type =
            FLAGS_snapshot_object_store_type;
    }
    if ((google::GetCommandLineFlagInfo("oplog_poll_interval_ms", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.oplog_poll_interval_ms = FLAGS_oplog_poll_interval_ms;
    }
    if ((google::GetCommandLineFlagInfo("oplog_batch_max_entries", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.oplog_batch_max_entries = FLAGS_oplog_batch_max_entries;
    }
    if ((google::GetCommandLineFlagInfo("batch_oplog_retry_timeout_sec",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.batch_oplog_retry_timeout_sec =
            FLAGS_batch_oplog_retry_timeout_sec;
    }
    if ((google::GetCommandLineFlagInfo("enable_metrics_report_to_backend",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_metrics_report_to_backend =
            FLAGS_enable_metrics_report_to_backend;
    }
    if ((google::GetCommandLineFlagInfo("metrics_report_interval_sec", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.metrics_report_interval_sec =
            FLAGS_metrics_report_interval_sec;
    }
    if ((google::GetCommandLineFlagInfo("metrics_report_lease_ttl_sec",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.metrics_report_lease_ttl_sec =
            FLAGS_metrics_report_lease_ttl_sec;
    }
    if ((google::GetCommandLineFlagInfo("root_fs_dir", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.root_fs_dir = FLAGS_root_fs_dir;
    }
    if ((google::GetCommandLineFlagInfo("global_file_segment_size", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.global_file_segment_size = FLAGS_global_file_segment_size;
    }
    if ((google::GetCommandLineFlagInfo("memory_allocator", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.memory_allocator = FLAGS_memory_allocator;
    }
    if ((google::GetCommandLineFlagInfo("allocation_strategy", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.allocation_strategy = FLAGS_allocation_strategy;
    }
    if ((google::GetCommandLineFlagInfo("enable_http_metadata_server", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_http_metadata_server =
            FLAGS_enable_http_metadata_server;
    }
    if ((google::GetCommandLineFlagInfo("http_metadata_server_port", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.http_metadata_server_port =
            FLAGS_http_metadata_server_port;
    }
    if ((google::GetCommandLineFlagInfo("http_metadata_server_host", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.http_metadata_server_host =
            FLAGS_http_metadata_server_host;
    }
    if ((google::GetCommandLineFlagInfo("pod_name", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.pod_name = FLAGS_pod_name;
    }
    if ((google::GetCommandLineFlagInfo("pod_namespace", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.pod_namespace = FLAGS_pod_namespace;
    }
    if ((google::GetCommandLineFlagInfo("enable_metadata_cleanup_on_timeout",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_metadata_cleanup_on_timeout =
            FLAGS_enable_metadata_cleanup_on_timeout;
    }
    if ((google::GetCommandLineFlagInfo("put_start_discard_timeout_sec",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.put_start_discard_timeout_sec =
            FLAGS_put_start_discard_timeout_sec;
    }
    if ((google::GetCommandLineFlagInfo("put_start_release_timeout_sec",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.put_start_release_timeout_sec =
            FLAGS_put_start_release_timeout_sec;
    }
    if ((google::GetCommandLineFlagInfo("enable_disk_eviction", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_disk_eviction = FLAGS_enable_disk_eviction;
    }
    if ((google::GetCommandLineFlagInfo("quota_bytes", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.quota_bytes = FLAGS_quota_bytes;
    }
    if ((google::GetCommandLineFlagInfo("enable_multi_tenants", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_multi_tenants = FLAGS_enable_multi_tenants;
    }
    if ((google::GetCommandLineFlagInfo("tenant_quota_connector_type", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.tenant_quota_connector_type =
            FLAGS_tenant_quota_connector_type;
    }
    if ((google::GetCommandLineFlagInfo("tenant_quota_connector_uri", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.tenant_quota_connector_uri =
            FLAGS_tenant_quota_connector_uri;
    }
    if ((google::GetCommandLineFlagInfo("max_total_finished_tasks", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.max_total_finished_tasks = FLAGS_max_total_finished_tasks;
    }
    if ((google::GetCommandLineFlagInfo("max_total_pending_tasks", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.max_total_pending_tasks = FLAGS_max_total_pending_tasks;
    }
    if ((google::GetCommandLineFlagInfo("max_total_processing_tasks", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.max_total_processing_tasks =
            FLAGS_max_total_processing_tasks;
    }
    if ((google::GetCommandLineFlagInfo("pending_task_timeout_sec", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.pending_task_timeout_sec = FLAGS_pending_task_timeout_sec;
    }
    if ((google::GetCommandLineFlagInfo("processing_task_timeout_sec", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.processing_task_timeout_sec =
            FLAGS_processing_task_timeout_sec;
    }
    if ((google::GetCommandLineFlagInfo("max_retry_attempts", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.max_retry_attempts = FLAGS_max_retry_attempts;
    }
}

std::optional<int64_t> GetConfiguredInt64(
    const mooncake::DefaultConfig* default_config, const std::string& key) {
    if (default_config == nullptr || !default_config->Contains(key)) {
        return std::nullopt;
    }
    int64_t value = 0;
    default_config->GetInt64(key, &value);
    return value;
}

std::optional<int64_t> GetExplicitInt64Flag(const char* name, int64_t value) {
    google::CommandLineFlagInfo info;
    if (google::GetCommandLineFlagInfo(name, &info) && !info.is_default) {
        return value;
    }
    return std::nullopt;
}

void InitClientLivenessConf(const mooncake::DefaultConfig* default_config,
                            mooncake::MasterConfig& master_config) {
    const mooncake::ClientLivenessConfigSource config_file{
        .active_ttl_sec =
            GetConfiguredInt64(default_config, "client_active_ttl_sec"),
        .legacy_ttl_sec =
            GetConfiguredInt64(default_config, "client_live_ttl_sec"),
        .suspicion_ttl_sec =
            GetConfiguredInt64(default_config, "client_suspicion_ttl_sec"),
    };
    const mooncake::ClientLivenessConfigSource command_line{
        .active_ttl_sec = GetExplicitInt64Flag("client_active_ttl_sec",
                                               FLAGS_client_active_ttl_sec),
        .legacy_ttl_sec = GetExplicitInt64Flag("client_ttl", FLAGS_client_ttl),
        .suspicion_ttl_sec = GetExplicitInt64Flag(
            "client_suspicion_ttl_sec", FLAGS_client_suspicion_ttl_sec),
    };

    const auto resolved = mooncake::ResolveClientLivenessConfig(
        config_file, command_line, mooncake::DEFAULT_CLIENT_LIVE_TTL_SEC,
        mooncake::DEFAULT_CLIENT_SUSPICION_TTL_SEC);
    if (resolved.config_active_conflict) {
        LOG(WARNING) << "Both client_active_ttl_sec and deprecated "
                        "client_live_ttl_sec are configured with different "
                        "values; using client_active_ttl_sec="
                     << *config_file.active_ttl_sec;
    }
    if (resolved.command_line_active_conflict) {
        LOG(WARNING) << "Both --client_active_ttl_sec and deprecated "
                        "--client_ttl are set with different values; using "
                        "--client_active_ttl_sec="
                     << *command_line.active_ttl_sec;
    }
    master_config.client_active_ttl_sec = resolved.active_ttl_sec;
    master_config.client_suspicion_ttl_sec = resolved.suspicion_ttl_sec;
}

// Function to start HTTP metadata server
std::unique_ptr<mooncake::HttpMetadataServer> StartHttpMetadataServer(
    int port, const std::string& host) {
    LOG(INFO) << "Starting C++ HTTP metadata server on " << host << ":" << port;

    try {
        auto server =
            std::make_unique<mooncake::HttpMetadataServer>(port, host);
        server->start();

        // Check if server started successfully
        if (server->is_running()) {
            LOG(INFO) << "C++ HTTP metadata server started successfully";
            return server;
        } else {
            LOG(ERROR) << "Failed to start C++ HTTP metadata server";
            return nullptr;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start C++ HTTP metadata server: " << e.what();
        return nullptr;
    }
}

#ifdef STORE_USE_ETCD
// --vsegment_auto_plan 策略解析（flags → VSegmentUserPolicy，一次解析两处
// 使用：启动 bootstrap + supervisor 注入 MasterService 重试组件）。返回空
// 表示未启用 / 参数不完整 / 后端不符（各告警一次，不阻断启动——quota 可
// 随后经 planner CLI 或 master POST /vsegment_quota 端点手工发布，参数
// 补齐后随重启生效）。
std::optional<mooncake::vsegment::VSegmentUserPolicy>
BuildVSegmentAutoPlanPolicy(const mooncake::MasterConfig& config) {
    if (!FLAGS_vsegment_auto_plan) return std::nullopt;
    if (config.ha_backend_type != "etcd") {
        LOG(WARNING) << "--vsegment_auto_plan requires the etcd HA backend; "
                        "skipping";
        return std::nullopt;
    }
    if (FLAGS_vsegment_member_count == 0 || FLAGS_vsegment_stripe_size == 0 ||
        FLAGS_vsegment_member_extent_size == 0) {
        LOG(WARNING) << "--vsegment_auto_plan is enabled but requires "
                        "--vsegment_member_count, --vsegment_stripe_size and "
                        "--vsegment_member_extent_size; skipping";
        return std::nullopt;
    }
    mooncake::vsegment::VSegmentUserPolicy policy;
    policy.member_count = FLAGS_vsegment_member_count;
    policy.stripe_size = FLAGS_vsegment_stripe_size;
    policy.member_extent_size = FLAGS_vsegment_member_extent_size;
    policy.reserved_ratio = FLAGS_vsegment_reserved_ratio;
    LOG(INFO) << "VSegment auto-plan policy: member_count="
              << policy.member_count << ", stripe_size=" << policy.stripe_size
              << ", member_extent_size=" << policy.member_extent_size
              << ", reserved_ratio=" << policy.reserved_ratio
              << ", required_medium="
              << (policy.required_medium.empty() ? "auto"
                                                 : policy.required_medium)
              << ", io_alignment=" << policy.io_alignment;
    return policy;
}

// --vsegment_auto_plan 启动 bootstrap（一次性，etcd HA 模式专属）：quota
// 快照不存在时按用户策略「发现→规划→发布」。执行于 supervisor 启动前。
// 多实例并发安全：EtcdHelper::Create 原子首写，first-writer-wins（输家得
// TRANSACTION_FAIL，视为已完成）。失败仅告警不阻断启动——quota 可随后经
// planner CLI 或 master POST /vsegment_quota 端点手工发布，或由
// MasterService::MaybeRetryVSegmentAutoPlan 在运行期节流重试自愈。
void TryAutoPlanVSegmentQuota(
    const mooncake::MasterConfig& config,
    const std::optional<mooncake::vsegment::VSegmentUserPolicy>& policy_opt) {
    if (!policy_opt) return;
    const std::string connstring = ResolveHABackendConnstring(config);
    if (connstring.empty()) {
        LOG(WARNING) << "vsegment auto-plan: no etcd endpoints configured; "
                        "skipping";
        return;
    }
    auto err = mooncake::EtcdHelper::ConnectToEtcdStoreClient(connstring);
    if (err != mooncake::ErrorCode::OK) {
        LOG(ERROR) << "vsegment auto-plan: cannot connect to etcd: "
                   << mooncake::toString(err);
        return;
    }
    mooncake::vsegment::PartitionPhysicalQuotaSnapshot existing;
    mooncake::vsegment::EtcdPartitionQuotaSnapshotStore store(
        config.cluster_id);
    if (store.Load(&existing) == mooncake::ErrorCode::OK) {
        LOG(INFO) << "vsegment auto-plan: quota generation "
                  << existing.config_generation
                  << " already published; skipping";
        return;
    }
    const auto& policy = *policy_opt;
    std::string summary;
    err = mooncake::vsegment::PlanAndPublishDiscoveredQuota(
        config.cluster_id, policy,
        mooncake::vsegment::kDefaultQuotaMaxEtcdValueBytes,
        /*detail=*/nullptr, &summary);
    if (err == mooncake::ErrorCode::OK) {
        LOG(INFO) << "vsegment auto-plan published: " << summary;
        mooncake::vsegment::LogVSegmentPoolFormed(config.cluster_id);
    } else if (err == mooncake::ErrorCode::ETCD_TRANSACTION_FAIL) {
        LOG(INFO) << "vsegment auto-plan: another instance published "
                     "concurrently; skipping";
    } else {
        LOG(WARNING) << "VSegment auto-plan deferred: no eligible psegment "
                        "pool yet ("
                     << mooncake::toString(err)
                     << "), will retry on psegment mount / heartbeat";
    }
}
#endif  // STORE_USE_ETCD

int main(int argc, char* argv[]) {
    mooncake::init_ylt_log_level();
    // Initialize gflags
    gflags::SetVersionString(mooncake::MOONCAKE_DISPLAY_VERSION);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // The master does not go through the transfer engine's globalConfig(),
    // which is where MC_LOG_DIR is normally applied. Without this, master logs
    // ignore MC_LOG_DIR and only go to stderr. Honor it here, mirroring
    // config.cpp: validate the directory and fall back to stderr if it is
    // missing or unwritable. --log_dir (if passed) takes precedence.
    const char* mc_log_dir =
        FLAGS_log_dir.empty() ? std::getenv("MC_LOG_DIR") : nullptr;
    // Resolve the log directory BEFORE initializing glog: SetLogDestination
    // below builds the journal path from FLAGS_log_dir, so FLAGS_log_dir must
    // already hold the MC_LOG_DIR value at that point. Validate the directory
    // and fall back to stderr if it is missing or unwritable. These early
    // LOG(WARNING)s run before InitGoogleLogging and therefore go to stderr,
    // which is the intended fallback sink anyway.
    if (mc_log_dir) {
        if (opendir(mc_log_dir) == nullptr) {
            LOG(WARNING) << "MC_LOG_DIR [" << mc_log_dir
                         << "] is not a valid directory. Logging to stderr.";
        } else if (access(mc_log_dir, W_OK) != 0) {
            LOG(WARNING) << "MC_LOG_DIR [" << mc_log_dir
                         << "] is not writable. Logging to stderr.";
        } else {
            FLAGS_log_dir = mc_log_dir;
            FLAGS_logtostderr = 0;
            FLAGS_stop_logging_if_full_disk = true;
        }
    }
    // Init glog if either --log_dir was passed or MC_LOG_DIR resolved to a
    // valid directory above. main() runs at most once per process, so a
    // local latch is enough to avoid a double init without depending on
    // glog's version-specific IsGoogleLoggingInitialized().
    static bool glog_initialized = false;
    if (!FLAGS_log_dir.empty() && !glog_initialized) {
        glog_initialized = true;
        google::InitGoogleLogging(argv[0]);
        // Merge all master logs into a single journal file in --log_dir,
        // reusing glog: every record is already written to its own severity
        // file and all lower ones, so the INFO sink is a complete journal.
        // Disable the higher-severity files so everything lands in one file.
        const std::string log_base = FLAGS_log_dir + "/mooncake_master.";
        google::SetLogDestination(google::GLOG_INFO, log_base.c_str());
        google::SetLogDestination(google::GLOG_WARNING, "");
        google::SetLogDestination(google::GLOG_ERROR, "");
        google::SetLogDestination(google::GLOG_FATAL, "");
        google::SetLogSymlink(google::GLOG_INFO, "mooncake_master");
    }
    mooncake::LogAllocatorStatus();
    mooncake::InstallAllocatorStatsCollector();

    LOG(INFO) << "Mooncake master version: "
              << mooncake::MOONCAKE_DISPLAY_VERSION;
    mooncake::logging::ApplyMooncakeLogEnableToGlog();

    // Initialize the master configuration
    mooncake::MasterConfig master_config;
    std::string conf_path = FLAGS_config_path;
    mooncake::DefaultConfig default_config;
    const mooncake::DefaultConfig* loaded_default_config = nullptr;
    if (!conf_path.empty()) {
        default_config.SetPath(conf_path);
        try {
            default_config.Load();
        } catch (const std::exception& e) {
            LOG(FATAL) << "Failed to initialize default config: " << e.what();
            return 1;
        }
        InitMasterConf(default_config, master_config);
        loaded_default_config = &default_config;
    }
    LoadConfigFromCmdline(master_config, !conf_path.empty());
    try {
        master_config.metrics = mooncake::ResolveMetricsBootstrapConfig(
            loaded_default_config, GetMetricsBootstrapCommandLineOverrides());
        mooncake::MasterMetricManager::instance().configure_key_groups(
            FLAGS_metrics_key_groups);
    } catch (const std::exception& error) {
        LOG(ERROR) << "Invalid metrics bootstrap configuration: "
                   << error.what();
        return 1;
    }
    try {
        InitClientLivenessConf(loaded_default_config, master_config);
    } catch (const std::invalid_argument& error) {
        LOG(ERROR) << "Invalid Client liveness configuration: " << error.what();
        return 1;
    }
    ResolveRpcAddressFromInterfaceOrDie(master_config);

    // Fall back to environment variables for pod identity (K8s Downward API)
    if (master_config.pod_name.empty()) {
        const char* env = std::getenv("POD_NAME");
        if (env) master_config.pod_name = env;
    }
    if (master_config.pod_namespace.empty()) {
        const char* env = std::getenv("POD_NAMESPACE");
        if (env) master_config.pod_namespace = env;
    }

    const std::string ha_backend_connstring =
        ResolveHABackendConnstring(master_config);
    if (master_config.enable_ha && ha_backend_connstring.empty()) {
        LOG(FATAL) << "HA backend connection string must be set when "
                   << "enable_ha is true, backend_type="
                   << master_config.ha_backend_type
                   << ". Only backend_type=etcd may fall back to "
                   << "etcd_endpoints";
        return 1;
    }
    if (master_config.enable_oplog && !master_config.enable_ha) {
        LOG(FATAL) << "enable_oplog requires enable_ha=true";
        return 1;
    }
    if (master_config.enable_oplog &&
        !mooncake::ha::HaBackendSupportsOpLog(master_config.ha_backend_type)) {
        LOG(FATAL) << "enable_oplog requires ha_backend_type=etcd or redis";
        return 1;
    }
    if (auto error = ValidateBatchOpLogSnapshotConfig(master_config)) {
        LOG(FATAL) << *error;
        return 1;
    }
    if (!master_config.enable_ha && (!ha_backend_connstring.empty() ||
                                     !master_config.etcd_endpoints.empty())) {
        LOG(WARNING)
            << "HA backend connection string is set but will not be used in "
            << "non-HA mode";
    }
    if (!master_config.ha_backend_connstring.empty() &&
        !master_config.etcd_endpoints.empty() &&
        master_config.ha_backend_connstring != master_config.etcd_endpoints) {
        LOG(WARNING) << "Both ha_backend_connstring and etcd_endpoints are "
                     << "set. Using ha_backend_connstring="
                     << master_config.ha_backend_connstring
                     << " for HA coordinator setup";
    }
    if (master_config.memory_allocator != "cachelib" &&
        master_config.memory_allocator != "offset") {
        LOG(FATAL) << "Invalid memory allocator: "
                   << master_config.memory_allocator
                   << ", must be 'cachelib' or 'offset'";
        return 1;
    }

    if (master_config.enable_ha && master_config.ha_backend_type == "etcd" &&
        mooncake::EtcdHelper::SetMaxSendMessageBytes(
            master_config.etcd_max_send_message_bytes) !=
            mooncake::ErrorCode::OK) {
        LOG(ERROR) << "Failed to configure etcd_max_send_message_bytes";
        return 1;
    }

    const auto rpc_protocol_config =
        mooncake::RpcProtocolConfig::FromEnvironment();
#ifdef YLT_ENABLE_IBV
    const std::string protocol = rpc_protocol_config.use_rdma ? "rdma" : "tcp";
#else
    const std::string protocol = "tcp";
    if (rpc_protocol_config.use_rdma) {
        LOG(WARNING) << "RDMA RPC is disabled at compile time; using TCP RPC";
    }
#endif

    // enable_metadata_cleanup_on_timeout requires a reachable HTTP metadata
    // server. Two topologies are supported:
    //   1) Co-located: enable_http_metadata_server=true -> the master cleans
    //      up via the in-process server (no network overhead).
    //   2) Separately deployed: the master derives the metadata server address
    //      from the cluster's existing configuration
    //      (MOONCAKE_TE_META_DATA_SERVER, or MOONCAKE_CONFIG_PATH json's
    //      "metadata_server") and cleans up via HTTP DELETE. Only http(s)
    //      endpoints are supported for now (etcd/redis left for future work).
    // If neither is available, cleanup is disabled with a warning so the main
    // process is never affected.
    std::string http_metadata_remote_url;
    if (master_config.enable_metadata_cleanup_on_timeout &&
        !master_config.enable_http_metadata_server) {
        std::string derived = ResolveMetadataServerForCleanup();
        if (derived.rfind("http://", 0) == 0 ||
            derived.rfind("https://", 0) == 0) {
            http_metadata_remote_url = std::move(derived);
            LOG(INFO) << "enable_metadata_cleanup_on_timeout: HTTP metadata "
                         "server is deployed separately; cleanup will target "
                      << http_metadata_remote_url;
        } else {
            LOG(WARNING)
                << "enable_metadata_cleanup_on_timeout is set to true but "
                   "enable_http_metadata_server is false and no HTTP metadata "
                   "server address could be derived from the cluster config "
                   "(set "
                   "MOONCAKE_TE_META_DATA_SERVER=http://host:port/metadata "
                   "or MOONCAKE_CONFIG_PATH). Disabling metadata cleanup on "
                   "timeout.";
            master_config.enable_metadata_cleanup_on_timeout = false;
        }
    }

    LOG(INFO)
        << "Master service started on port " << master_config.rpc_port
        << ", max_threads=" << master_config.rpc_thread_num
        << ", enable_metric_reporting=" << master_config.metrics.enabled
        << ", metrics_port=" << master_config.metrics.port
        << ", metrics_host=" << master_config.metrics.host
        << ", default_kv_lease_ttl=" << master_config.default_kv_lease_ttl
        << ", default_kv_soft_pin_ttl=" << master_config.default_kv_soft_pin_ttl
        << ", max_kv_soft_pin_ttl=" << master_config.max_kv_soft_pin_ttl
        << ", allow_evict_soft_pinned_objects="
        << master_config.allow_evict_soft_pinned_objects
        << ", eviction_ratio=" << master_config.eviction_ratio
        << ", eviction_high_watermark_ratio="
        << master_config.eviction_high_watermark_ratio
        << ", tenant_eviction_high_watermark_ratio="
        << master_config.tenant_eviction_high_watermark_ratio
        << ", enable_ha=" << master_config.enable_ha
        << ", enable_oplog=" << master_config.enable_oplog
        << ", weight_management_oplog_capability_confirmed="
        << master_config.weight_management_oplog_capability_confirmed
        << ", enable_oplog_snapshot=" << master_config.enable_oplog_snapshot
        << ", snapshot_chunk_object_count="
        << master_config.snapshot_chunk_object_count
        << ", enable_offload=" << master_config.enable_offload
        << ", enable_kv_events=" << master_config.enable_kv_events
        << ", kv_events_bind_endpoint=" << master_config.kv_events_bind_endpoint
        << ", kv_events_backend_id=" << master_config.kv_events_backend_id
        << ", offload_on_evict=" << master_config.offload_on_evict
        << ", offload_force_evict=" << master_config.offload_force_evict
        << ", offloading_queue_limit=" << master_config.offloading_queue_limit
        << ", offload_cap_ratio=" << master_config.offload_cap_ratio
        << ", ha_backend_type=" << master_config.ha_backend_type
        << ", ha_backend_connstring=" << ha_backend_connstring
        << ", ha_leader_lease_ttl_sec=" << master_config.ha_leader_lease_ttl_sec
        << ", etcd_endpoints=" << master_config.etcd_endpoints
        << ", etcd_max_send_message_bytes="
        << master_config.etcd_max_send_message_bytes
        << ", client_active_ttl_sec=" << master_config.client_active_ttl_sec
        << ", client_suspicion_ttl_sec="
        << master_config.client_suspicion_ttl_sec
        << ", rpc_thread_num=" << master_config.rpc_thread_num
        << ", handler_worker_thread_num="
        << master_config.handler_worker_thread_num
        << ", rpc_port=" << master_config.rpc_port
        << ", rpc_address=" << master_config.rpc_address
        << ", rpc_interface=" << master_config.rpc_interface
        << ", rpc_conn_timeout_seconds="
        << master_config.rpc_conn_timeout_seconds
        << ", rpc_enable_tcp_no_delay=" << master_config.rpc_enable_tcp_no_delay
        << ", rpc protocol=" << protocol
        << ", cluster_id=" << master_config.cluster_id
        << ", root_fs_dir=" << master_config.root_fs_dir
        << ", global_file_segment_size="
        << master_config.global_file_segment_size
        << ", memory_allocator=" << master_config.memory_allocator
        << ", enable_http_metadata_server="
        << master_config.enable_http_metadata_server
        << ", http_metadata_server_port="
        << master_config.http_metadata_server_port
        << ", http_metadata_server_host="
        << master_config.http_metadata_server_host
        << ", enable_metadata_cleanup_on_timeout="
        << master_config.enable_metadata_cleanup_on_timeout
        << ", put_start_discard_timeout_sec="
        << master_config.put_start_discard_timeout_sec
        << ", put_start_release_timeout_sec="
        << master_config.put_start_release_timeout_sec
        << ", max_total_finished_tasks="
        << master_config.max_total_finished_tasks
        << ", max_total_pending_tasks=" << master_config.max_total_pending_tasks
        << ", max_total_processing_tasks="
        << master_config.max_total_processing_tasks
        << ", pending_task_timeout_sec="
        << master_config.pending_task_timeout_sec
        << ", processing_task_timeout_sec="
        << master_config.processing_task_timeout_sec
        << ", max_retry_attempts=" << master_config.max_retry_attempts
        << ", enable_cxl=" << master_config.enable_cxl
        << ", cxl_path=" << master_config.cxl_path
        << ", cxl_size=" << master_config.cxl_size;

    // Start HTTP metadata server if enabled
    std::unique_ptr<mooncake::HttpMetadataServer> http_metadata_server;
    if (master_config.enable_http_metadata_server) {
        http_metadata_server =
            StartHttpMetadataServer(master_config.http_metadata_server_port,
                                    master_config.http_metadata_server_host);

        if (!http_metadata_server) {
            LOG(FATAL) << "Failed to start HTTP metadata server";
            return 1;
        }

        // Give the server some time to start
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Metadata cleanup on client timeout (used by both the HA and non-HA
    // paths): prefer the co-located in-process server, else the separate URL.
    mooncake::HttpMetadataServer* metadata_server_ptr = nullptr;
    if (master_config.enable_metadata_cleanup_on_timeout &&
        master_config.enable_http_metadata_server) {
        metadata_server_ptr = http_metadata_server.get();
    }

    if (master_config.enable_ha) {
        // Arm the shutdown bound and the handlers before any lease can be
        // acquired; without them a SIGTERM kills a leader mid-term. This
        // call is serial and one-shot from main(): the check-then-store in
        // StartShutdownWatchdog assumes no concurrent caller.
        if (!mooncake::StartShutdownWatchdog(
                mooncake::ha::kHaShutdownDeadline)) {
            LOG(ERROR) << "Failed to start the shutdown watchdog, "
                          "refusing to start";
            return 1;
        }
        if (!mooncake::InstallShutdownSignalHandlers()) {
            PLOG(ERROR) << "Failed to install SIGINT/SIGTERM handlers, "
                           "refusing to start";
            return 1;
        }
#ifdef STORE_USE_ETCD
        // vsegment 配额自动规划（--vsegment_auto_plan）：supervisor 启动前
        // 一次性 bootstrap（仅当快照不存在时执行，详见函数注释）；同时把
        // 策略注入 supervisor → MasterService，供心跳/mount 触发点在快照
        // 缺失期间节流重试发布（发布侧自愈，
        // 见 MasterService::MaybeRetryVSegmentAutoPlan）。
        const auto auto_plan_policy =
            BuildVSegmentAutoPlanPolicy(master_config);
        TryAutoPlanVSegmentQuota(master_config, auto_plan_policy);
#endif
        mooncake::MasterServiceSupervisorConfig supervisor_config{
            master_config};
        supervisor_config.http_metadata_server = metadata_server_ptr;
        supervisor_config.http_metadata_remote_url = http_metadata_remote_url;
#ifdef STORE_USE_ETCD
        if (auto_plan_policy) {
            supervisor_config.vsegment_auto_plan = true;
            supervisor_config.vsegment_auto_plan_member_count =
                auto_plan_policy->member_count;
            supervisor_config.vsegment_auto_plan_stripe_size =
                auto_plan_policy->stripe_size;
            supervisor_config.vsegment_auto_plan_member_extent_size =
                auto_plan_policy->member_extent_size;
            supervisor_config.vsegment_auto_plan_reserved_ratio =
                auto_plan_policy->reserved_ratio;
        }
#endif
        mooncake::ha::MasterServiceSupervisor supervisor(supervisor_config);
        const int result = supervisor.Start();
        // Finish a requested process exit without further main/static
        // teardown; supervisor-owned cleanup may already have run on this
        // error path.
        if (mooncake::ShutdownRequested() || mooncake::ShutdownLatched()) {
            mooncake::ExitAfterShutdown(result);
        }
        return result;
    } else {
#ifdef STORE_USE_ETCD
        // enable_ha=false 时 auto_plan flag 被静默忽略：显式提示，防止
        // 用户以为已自动发布（quota 可在 HA 启用后随重启生效，或届时
        // 手工 curl POST /vsegment_quota）。
        if (FLAGS_vsegment_auto_plan) {
            LOG(WARNING) << "--vsegment_auto_plan is set but enable_ha is "
                            "false; the flag is ignored (vsegment quota "
                            "auto-planning requires the etcd HA mode)";
        }
#endif
        // version is not used in non-HA mode, just pass a dummy value
        mooncake::ViewVersionId version = 0;
        coro_rpc::coro_rpc_server server(
            master_config.rpc_thread_num, master_config.rpc_port,
            master_config.rpc_address,
            std::chrono::seconds(master_config.rpc_conn_timeout_seconds),
            master_config.rpc_enable_tcp_no_delay);
        if (rpc_protocol_config.use_rdma) {
#ifdef YLT_ENABLE_IBV
            server.init_ibv();
#else
            LOG(WARNING)
                << "RDMA RPC is disabled at compile time; using TCP RPC";
#endif
        }
        auto wrapped_master_service =
            std::make_shared<mooncake::WrappedMasterService>(
                mooncake::WrappedMasterServiceConfig(master_config, version),
                metadata_server_ptr, http_metadata_remote_url);
        mooncake::MasterAdminServer admin_server(
            static_cast<uint16_t>(master_config.metrics.port),
            master_config.metrics.enabled, master_config.metrics.host,
            master_config.admin_metrics_log_interval_sec);
        if (!admin_server.Start()) {
            LOG(ERROR) << "Failed to start master admin server";
            return 1;
        }
        admin_server.SetRuntimeState(
            mooncake::ha::MasterRuntimeState::kServing);
        admin_server.SetServiceDelegate(wrapped_master_service);
        admin_server.SetServiceAvailable(true);

        // Open the restore lifecycle (RFC #4700, slice 1): the non-HA path
        // has no promotion restore, so it moves Constructed -> Ready ->
        // Serving explicitly, before the RPC service is registered.
        wrapped_master_service->MarkReadyForServing();
        wrapped_master_service->ActivateServing();

        // Declared after the service so that it drains before the service
        // is destroyed.
        mooncake::RpcWorkerPool worker_pool(
            master_config.handler_worker_thread_num);
        mooncake::RegisterRpcService(server, *wrapped_master_service,
                                     &worker_pool);

        static std::atomic<bool> shutdown_requested{false};
        auto signal_handler = [](int /* signum */) {
            shutdown_requested.store(true);
        };
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);

        int server_result = 0;
        std::atomic<bool> server_done{false};
        std::thread server_thread([&server, &server_result, &server_done]() {
            server_result = server.start();
            server_done.store(true);
        });

        while (!shutdown_requested.load() && !server_done.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        server.stop();
        server_thread.join();

        if (shutdown_requested.load()) {
            LOG(INFO) << "Shutdown signal received, exiting gracefully";
            return 0;
        }
        return server_result;
    }
}
