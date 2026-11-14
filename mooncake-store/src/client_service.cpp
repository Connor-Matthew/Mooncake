#include "client_service.h"

#include <boost/algorithm/string.hpp>
#include <glog/logging.h>

#include "ascii_string.h"
#include "allocator.h"
#include "segment.h"
#include "nof/nof_runtime.h"
#include "store_diagnostics.h"

#include <csignal>
#include <algorithm>
#include <cassert>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>
#ifdef USE_NOF
#include <numa.h>
#endif
#include <optional>
#include <ranges>
#include <map>
#include <span>
#include <sched.h>
#include <thread>
#include <set>
#include <utility>
#include <vector>
#include <ylt/struct_json/json_reader.h>

#include "transfer_engine.h"
#include "topology.h"
#include "transfer_task.h"
#include "transport/transport.h"
#include "config.h"
#include "ha/leadership/leader_coordinator_factory.h"
#include "cvm/cvm_keys.h"
#include "cvm/cvm_types.h"
#include "cvm/etcd_view_store.h"
#include "etcd_helper.h"
#include "types.h"
#include "client_buffer.h"
#include "common/network.h"
#include "config/client_host_identity_config.h"
#include "config/client_object_checksum_config.h"
#include "replica_selection.h"
#include "rpc_types.h"
#include "local_hot_cache.h"
#include "config/client_auto_discovery_config.h"
#include "device/accelerator_registry.h"
#ifdef USE_INTRA_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif
#include "crc_checksum.h"
#include "config/client_numa_config.h"
#include "storage/distributed/distributed_storage_backend.h"

#define SPDIAG_PERF_DEF_FILE "mooncake_perf_points.def"
#define SPDIAG_PROGRAM_NAME "mooncake_store"
#include "spdiag/auto_perf.h"

namespace mooncake {

std::optional<size_t> GetTransportRegistrationLimit(
    const std::string& protocol) {
    if (protocol == "efa" || protocol == "cxi") {
        return globalConfig().max_mr_size;
    }
    if (protocol == "ub") {
        return globalConfig().max_seg_size;
    }
    return std::nullopt;
}

bool Client::IsUbTransportSegment(const std::string& endpoint) {
    if (!transfer_engine_) return false;
    const auto metadata = transfer_engine_->getMetadata();
    if (!metadata) return false;
    const auto segment = metadata->getSegmentDescByName(endpoint);
    return segment && segment->protocol == "ub";
}

namespace {

constexpr size_t kObjectChecksumD2HChunkSize = 8 * 1024 * 1024;
constexpr auto kInitialLeaderReadyTimeout = std::chrono::seconds(30);

class ScopedObjectChecksumBuffer {
   public:
    ScopedObjectChecksumBuffer(PinnedBufferPool& pool, size_t size)
        : pool_(pool), buffer_(pool_.Acquire(size)) {}

    ~ScopedObjectChecksumBuffer() { pool_.Release(std::move(buffer_)); }

    char* data() const { return buffer_.data; }

   private:
    PinnedBufferPool& pool_;
    PinnedBufferPool::Buffer buffer_;
};

int GetCurrentNumaSocketId() {
#if defined(__linux__)
    if (numa_available() < 0) {
        return 0;
    }
    int cpu = sched_getcpu();
    if (cpu < 0) {
        return 0;
    }
    int node = numa_node_of_cpu(cpu);
    return node < 0 ? 0 : node;
#else
    // libnuma is Linux-only; default to socket 0 elsewhere.
    return 0;
#endif
}

// Backing slot for Client::SetDefaultNofInitiatorFactoryForTesting.
std::function<std::shared_ptr<NVMeoFInitiator>()>&
DefaultNofInitiatorFactoryForTestingSlot() {
    static std::function<std::shared_ptr<NVMeoFInitiator>()> factory;
    return factory;
}

struct ContiguousSliceRange {
    void* ptr = nullptr;
    size_t size = 0;
};

std::optional<ContiguousSliceRange> GetContiguousSliceRange(
    std::span<const Slice> slices) {
    if (slices.empty()) {
        return std::nullopt;
    }

    uintptr_t expected_ptr = 0;
    uintptr_t start_ptr = 0;
    size_t total_size = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        if (slice.ptr == nullptr) {
            return std::nullopt;
        }

        const auto current_ptr = reinterpret_cast<uintptr_t>(slice.ptr);
        if (i == 0) {
            start_ptr = current_ptr;
            expected_ptr = current_ptr;
        } else if (current_ptr != expected_ptr) {
            return std::nullopt;
        }

        if (slice.size > std::numeric_limits<size_t>::max() - total_size) {
            return std::nullopt;
        }
        if (slice.size > std::numeric_limits<uintptr_t>::max() - current_ptr) {
            return std::nullopt;
        }

        total_size += slice.size;
        expected_ptr = current_ptr + slice.size;
    }

    if (total_size == 0) {
        return std::nullopt;
    }

    return ContiguousSliceRange{.ptr = reinterpret_cast<void*>(start_ptr),
                                .size = total_size};
}

ErrorCode ScatterFragmentError(const Status& status) {
    return status.IsInvalidArgument() ? ErrorCode::INVALID_PARAMS
                                      : ErrorCode::TRANSFER_FAIL;
}

void MarkScatterOperationFailure(
    std::vector<tl::expected<int64_t, ErrorCode>>& results,
    const std::vector<std::optional<ErrorCode>>& entry_errors,
    const Status& status) {
    const auto error = ScatterFragmentError(status);
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i].has_value() && !entry_errors[i].has_value()) {
            results[i] = tl::unexpected(error);
        }
    }
}

// Collects the fragments of many ranged entries into a single scatter submit.
//
// One submit per entry would hand the transport one tiny transfer per key per
// layer; batching them lets the transport coalesce every fragment bound for the
// same segment into one transfer, which is what keeps layer-wise sessions at
// full bandwidth.
//
// ScatterTransferRange holds non-owning spans, so the offset/length storage
// lives here and must outlive the submit. Both vectors are reserved to the
// exact fragment count up front so pushes never reallocate under a live span.
class ScatterRangeBuilder {
   public:
    explicit ScatterRangeBuilder(size_t fragment_count)
        : zero_offsets_(fragment_count, 0) {
        remote_offsets_.reserve(fragment_count);
        lengths_.reserve(fragment_count);
        ranges_.reserve(fragment_count);
    }

    // `error_slot` is shared by every fragment of one entry and keeps the first
    // failure. Callbacks run on the waiting thread, so no locking is needed.
    void Add(TransferRequest::OpCode opcode, const std::string& remote_segment,
             uint64_t remote_base, size_t remote_size, const Slice& slice,
             uint64_t remote_offset, std::optional<ErrorCode>* error_slot) {
        const size_t index = remote_offsets_.size();
        remote_offsets_.push_back(static_cast<size_t>(remote_offset));
        lengths_.push_back(slice.size);
        ranges_.push_back(TransferEngine::ScatterTransferRange{
            .opcode = opcode,
            .remote_segment = remote_segment,
            .remote_base_offset = remote_base,
            .remote_size = remote_size,
            .local_buffer = slice.ptr,
            .local_capacity = slice.size,
            .local_offsets = std::span<const size_t>(&zero_offsets_[index], 1),
            .remote_offsets =
                std::span<const size_t>(&remote_offsets_[index], 1),
            .lengths = std::span<const size_t>(&lengths_[index], 1),
            .on_fragment_complete =
                [error_slot](size_t, const Status& status) {
                    if (!status.ok() && !error_slot->has_value()) {
                        *error_slot = ScatterFragmentError(status);
                    }
                },
        });
    }

    void Add(TransferRequest::OpCode opcode,
             const AllocatedBuffer::Descriptor& handle, const Slice& slice,
             uint64_t remote_offset, std::optional<ErrorCode>* error_slot) {
        Add(opcode, handle.transport_endpoint_, handle.buffer_address_,
            static_cast<size_t>(handle.size_), slice, remote_offset,
            error_slot);
    }

    bool empty() const { return ranges_.empty(); }

    const std::vector<TransferEngine::ScatterTransferRange>& ranges() const {
        return ranges_;
    }

   private:
    std::vector<size_t> zero_offsets_;
    std::vector<size_t> remote_offsets_;
    std::vector<size_t> lengths_;
    std::vector<TransferEngine::ScatterTransferRange> ranges_;
};

void CompleteScatterRanges(
    Client& client, ScatterRangeBuilder& builder,
    std::vector<tl::expected<int64_t, ErrorCode>>& results,
    const std::vector<std::optional<ErrorCode>>& entry_errors,
    const char* submit_fail_msg, const char* entry_fail_prefix) {
    if (builder.empty()) {
        return;
    }
    auto operation = client.SubmitScatter(builder.ranges());
    if (!operation) {
        LOG(ERROR) << submit_fail_msg;
        for (auto& result : results) {
            if (result.has_value()) {
                result = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
        }
        return;
    }
    (void)operation->wait();
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value() || !entry_errors[i].has_value()) {
            continue;
        }
        LOG(ERROR) << entry_fail_prefix << ", entry=" << i
                   << ", error=" << static_cast<int>(entry_errors[i].value());
        results[i] = tl::unexpected(entry_errors[i].value());
    }
}

struct ReplicaTransferSummary {
    size_t allocated_memory_replicas = 0;
    size_t allocated_nof_replicas = 0;
    size_t allocated_dfs_replicas = 0;
    size_t successful_memory_transfers = 0;
    size_t successful_nof_transfers = 0;
    size_t successful_dfs_transfers = 0;
    size_t failed_memory_transfers = 0;
    size_t failed_nof_transfers = 0;
    size_t failed_dfs_transfers = 0;
    ErrorCode first_error = ErrorCode::OK;

    void RecordAllocatedReplica(const Replica::Descriptor& replica) {
        if (replica.is_memory_replica() || replica.is_vsegment_replica()) {
            ++allocated_memory_replicas;
        } else if (replica.is_nof_replica()) {
            ++allocated_nof_replicas;
        } else if (replica.is_dfs_replica()) {
            ++allocated_dfs_replicas;
        }
    }

    void RecordSuccess(ReplicaType replica_type) {
        if (replica_type == ReplicaType::MEMORY) {
            ++successful_memory_transfers;
        } else if (replica_type == ReplicaType::NOF_SSD) {
            ++successful_nof_transfers;
        } else if (replica_type == ReplicaType::DFS) {
            ++successful_dfs_transfers;
        }
    }

    void RecordFailure(ReplicaType replica_type, ErrorCode error) {
        if (replica_type == ReplicaType::MEMORY) {
            ++failed_memory_transfers;
        } else if (replica_type == ReplicaType::NOF_SSD) {
            ++failed_nof_transfers;
        } else if (replica_type == ReplicaType::DFS) {
            ++failed_dfs_transfers;
        }
        if (first_error == ErrorCode::OK) {
            first_error = error;
        }
    }
};

bool NonDfsTransfersSucceeded(const ReplicaTransferSummary& summary) {
    return summary.successful_memory_transfers ==
               summary.allocated_memory_replicas &&
           summary.successful_nof_transfers == summary.allocated_nof_replicas &&
           summary.failed_memory_transfers == 0 &&
           summary.failed_nof_transfers == 0;
}

bool AllAllocatedTransfersSucceeded(const ReplicaTransferSummary& summary) {
    return NonDfsTransfersSucceeded(summary) &&
           summary.successful_dfs_transfers == summary.allocated_dfs_replicas &&
           summary.failed_dfs_transfers == 0;
}

bool HasExpectedReplicaAllocation(const ReplicateConfig& config,
                                  const ReplicaTransferSummary& summary) {
    if (config.nof_replica_num == 0 && config.dfs_replica_num == 0) {
        return summary.allocated_memory_replicas > 0;
    }
    if (DetermineReplicaWriteMode(config) ==
        ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        return summary.allocated_memory_replicas +
                   summary.allocated_nof_replicas >
               0;
    }
    return summary.allocated_memory_replicas == config.replica_num &&
           summary.allocated_nof_replicas == config.nof_replica_num &&
           summary.allocated_dfs_replicas == config.dfs_replica_num;
}

std::optional<ErrorCode> ValidatePreferSameNodeWriteConfig(
    const ReplicateConfig& config) {
    if (config.nof_replica_num > 0) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                      "NoF replicas";
        return ErrorCode::INVALID_PARAMS;
    }
    if (config.replica_num != 1) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                      "replica_num != 1";
        return ErrorCode::INVALID_PARAMS;
    }

    return std::nullopt;
}

// success describes whether the overall put should succeed. Reliable modes
// require all allocated replicas to complete. Flexible dual-replica mode only
// requires one replica type to succeed, so success may be true while
// revoke_type is also set for the failed side.
struct FinalizeDecision {
    std::optional<ReplicaType> end_type;
    std::optional<ReplicaType> revoke_type;
    bool success = false;
    ErrorCode error = ErrorCode::OK;
};

FinalizeDecision DetermineFinalizeDecision(
    const ReplicateConfig& config, const ReplicaTransferSummary& summary) {
    const auto write_mode = DetermineReplicaWriteMode(config);
    const bool allocation_satisfied =
        HasExpectedReplicaAllocation(config, summary);

    if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        const bool all_transfers_succeeded =
            AllAllocatedTransfersSucceeded(summary);
        if (allocation_satisfied && all_transfers_succeeded) {
            return {.end_type = ReplicaType::ALL,
                    .revoke_type = std::nullopt,
                    .success = true,
                    .error = ErrorCode::OK};
        }
        return {.end_type = std::nullopt,
                .revoke_type = ReplicaType::ALL,
                .success = false,
                .error = allocation_satisfied
                             ? (summary.first_error == ErrorCode::OK
                                    ? ErrorCode::TRANSFER_FAIL
                                    : summary.first_error)
                             : ErrorCode::NO_AVAILABLE_HANDLE};
    }

    const bool memory_succeeded = summary.successful_memory_transfers > 0;
    const bool nof_succeeded = summary.successful_nof_transfers > 0;

    if (memory_succeeded && nof_succeeded) {
        return {.end_type = ReplicaType::ALL,
                .revoke_type = std::nullopt,
                .success = true,
                .error = ErrorCode::OK};
    }
    if (memory_succeeded) {
        return {.end_type = ReplicaType::MEMORY,
                .revoke_type = ReplicaType::NOF_SSD,
                .success = true,
                .error = ErrorCode::OK};
    }
    if (nof_succeeded) {
        return {.end_type = ReplicaType::NOF_SSD,
                .revoke_type = ReplicaType::MEMORY,
                .success = true,
                .error = ErrorCode::OK};
    }

    return {.end_type = std::nullopt,
            .revoke_type = ReplicaType::ALL,
            .success = false,
            .error = summary.first_error == ErrorCode::OK
                         ? ErrorCode::NO_AVAILABLE_HANDLE
                         : summary.first_error};
}

}  // namespace

[[nodiscard]] size_t CalculateSliceSize(const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

[[nodiscard]] size_t CalculateSliceSize(std::span<const Slice> slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

Client::Client(const std::string& local_hostname,
               const std::string& metadata_connstring,
               const std::string& protocol,
               const std::map<std::string, std::string>& labels,
               const std::string& tenant_id)
    : client_id_(generate_uuid()),
      metrics_(ClientMetric::Create(merge_labels(labels))),
      master_client_(client_id_,
                     metrics_ ? &metrics_->master_client_metric : nullptr,
                     tenant_id),
      local_hostname_(local_hostname),
      host_id_(
          ClientHostIdentityConfig::FromEnvironment(local_hostname).host_id),
      metadata_connstring_(metadata_connstring),
      protocol_(protocol),
      object_checksum_enabled_(
          ClientObjectChecksumConfig::IsEnabledAtFirstUse()),
      pinned_buffer_pool_(std::make_unique<PinnedBufferPool>()),
      write_thread_pool_(2),
      task_thread_pool_(4) {
    LOG(INFO) << "client_id=" << client_id_;
    if (!host_id_.empty()) {
        LOG(INFO) << "client_id=" << client_id_ << ", host_id=" << host_id_;
    }
    if (object_checksum_enabled_) {
        LOG(INFO) << "Object checksum validation is enabled";
    }

    if (metrics_) {
        if (metrics_->GetReportingInterval() > 0) {
            LOG(INFO) << "Client metrics enabled with reporting thread started "
                         "(interval: "
                      << metrics_->GetReportingInterval() << "s)";
        } else {
            LOG(INFO)
                << "Client metrics enabled but reporting disabled (interval=0)";
        }
    } else {
        LOG(INFO) << "Client metrics disabled (set MC_STORE_CLIENT_METRIC=1 to "
                     "enable)";
    }
}

Client::~Client() {
    task_poll_running_ = false;
    if (task_poll_thread_.joinable()) {
        task_poll_thread_.join();
    }

    storage_heartbeat_running_ = false;
    if (storage_heartbeat_thread_.joinable()) {
        storage_heartbeat_thread_.join();
    }
    if (!gather_endpoint_.empty())
        (void)master_client_.PublishGatherEndpoint(GetSegmentEndpoint(),
                                                   gather_endpoint_, true);

    leader_monitor_running_ = false;
    if (leader_monitor_thread_.joinable()) {
        leader_monitor_thread_.join();
    }

    routing_refresh_running_ = false;
    if (routing_refresh_thread_.joinable()) {
        routing_refresh_thread_.join();
    }

    {
        std::lock_guard<std::mutex> lock(graceful_unmount_timer_mutex_);
        graceful_unmount_timer_stopping_ = true;
    }
    graceful_unmount_timer_cv_.notify_all();

    // Stop queued timer/task callbacks before tearing down segment state.
    task_running_ = false;
    task_thread_pool_.stop();

    // Unmount callbacks must finish before the service and sources are drained.
    gather_service_.reset();
    gather_clients_.clear();

    // Make copies to avoid modifying while iterating
    std::vector<Segment> mounted_segments_copy;
    std::vector<Segment> gracefully_unmounting_segments_copy;
    std::vector<std::pair<UUID, std::function<void(const UUID&)>>>
        graceful_cleanup_callbacks;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        mounted_segments_copy.reserve(mounted_segments_.size());
        for (auto& entry : mounted_segments_) {
            mounted_segments_copy.emplace_back(entry.second);
        }
        for (auto& entry : gracefully_unmounting_segments_) {
            gracefully_unmounting_segments_copy.emplace_back(entry.second);
        }
        graceful_cleanup_callbacks.reserve(
            graceful_unmount_cleanup_callbacks_.size());
        for (auto& entry : graceful_unmount_cleanup_callbacks_) {
            graceful_cleanup_callbacks.emplace_back(entry.first,
                                                    std::move(entry.second));
        }
        graceful_unmount_cleanup_callbacks_.clear();
    }

    // Unmount mounted segments: notify master + local cleanup
    for (auto& segment : mounted_segments_copy) {
        auto result =
            UnmountSegment(reinterpret_cast<void*>(segment.base), segment.size);
        if (!result) {
            LOG(ERROR) << "Failed to unmount segment in destructor: "
                       << toString(result.error());
        }
    }

    // Unregister gracefully unmounting segments: master already has timer
    for (auto& segment : gracefully_unmounting_segments_copy) {
        int rc = transfer_engine_->unregisterLocalMemory(
            reinterpret_cast<void*>(segment.base));
        if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
            LOG(ERROR) << "Failed to unregister transfer buffer in destructor: "
                       << rc;
        }
    }

    // Clear any remaining segments
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        mounted_segments_.clear();
        gracefully_unmounting_segments_.clear();
    }

    for (auto& entry : graceful_cleanup_callbacks) {
        if (entry.second) {
            entry.second(entry.first);
        }
    }

    // Stop hot cache handler before unregistering/freeing its backing memory.
    hot_cache_handler_.reset();
    UnregisterLocalHotCacheMemory();
    hot_cache_.reset();
}

static std::optional<size_t> parse_replica_count_env(const char* name) {
    const char* raw = std::getenv(name);
    if (!raw || !*raw) {
        return std::nullopt;
    }
    // Strict non-negative decimal integer: reject sign, whitespace, and
    // trailing characters before parsing. std::stoull alone would silently
    // accept "-1" (wrapping to ULLONG_MAX) and "1abc" (prefix parse), turning
    // a typo into an astronomic replica count with an unrelated allocation
    // failure far from the misconfiguration.
    for (const char* p = raw; *p != '\0'; ++p) {
        if (*p < '0' || *p > '9') {
            LOG(ERROR) << "Invalid " << name << "='" << raw
                       << "', expected a non-negative decimal integer "
                          "without sign, whitespace, or trailing "
                          "characters; the environment variable is ignored "
                          "and the caller's ReplicateConfig wins";
            return std::nullopt;
        }
    }
    try {
        return static_cast<size_t>(std::stoull(raw));
    } catch (const std::exception&) {
        // Only reachable on overflow now that the character set is checked.
        LOG(ERROR) << "Invalid " << name << "='" << raw
                   << "', value out of range for size_t; the environment "
                      "variable is ignored and the caller's ReplicateConfig "
                      "wins";
        return std::nullopt;
    }
}

// JD-Cases: 透明数据放置开关（上层应用不感知 NoF segment）。
//
// 部署侧通过环境变量选择写路径，应用侧 ReplicateConfig 无需任何改动：
//   MC_NOF_REPLICA_NUM -> 覆盖 nof_replica_num（NoF SSD segment，
//                         SPDK NoF 注册通道）
//   MC_MEM_REPLICA_NUM -> 覆盖 replica_num（transfer engine 默认内存
//                         segment）
//
// 未设置的环境变量不干预（保留调用方传入值；默认 replica_num=1/nof=0
// 即纯内存语义）。常用组合：
//   纯内存压测：MC_MEM_REPLICA_NUM=1（或两个都不设）
//   纯 NoF 压测：MC_NOF_REPLICA_NUM=1 MC_MEM_REPLICA_NUM=0
//     （mem=0 且 nof=1 为 strict 模式：NoF 段未注册时 put 显式失败
//      NO_AVAILABLE_HANDLE，避免 1+1 FLEXIBLE 模式下的静默内存退化）
//   1+1 双副本：MC_NOF_REPLICA_NUM=1 MC_MEM_REPLICA_NUM=1
//
// 两者解析结果均为 0 属配置错误：master 分配阶段会以 INVALID_PARAMS
// 拒绝 PutStart（put 失败），客户端在此打一次 ERROR 便于定位。
struct EnvReplicaPlacement {
    std::optional<size_t> nof_replica_num;
    std::optional<size_t> mem_replica_num;

    EnvReplicaPlacement() {
        nof_replica_num = parse_replica_count_env("MC_NOF_REPLICA_NUM");
        mem_replica_num = parse_replica_count_env("MC_MEM_REPLICA_NUM");
    }

    bool active() const { return nof_replica_num || mem_replica_num; }
};

static void ApplyEnvReplicaPlacement(ReplicateConfig& config) {
    static const EnvReplicaPlacement env_placement;
    if (!env_placement.active()) {
        return;
    }
    if (env_placement.nof_replica_num) {
        config.nof_replica_num = *env_placement.nof_replica_num;
    }
    if (env_placement.mem_replica_num) {
        config.replica_num = *env_placement.mem_replica_num;
    }
    static std::once_flag info_once;
    // env_placement 具有静态存储期，lambda 无需捕获即可直接使用
    std::call_once(info_once, [] {
        LOG(INFO) << "Env replica placement override active:"
                  << (env_placement.mem_replica_num
                          ? " MC_MEM_REPLICA_NUM=" +
                                std::to_string(*env_placement.mem_replica_num)
                          : "")
                  << (env_placement.nof_replica_num
                          ? " MC_NOF_REPLICA_NUM=" +
                                std::to_string(*env_placement.nof_replica_num)
                          : "")
                  << " (caller ReplicateConfig values are overridden on "
                     "every write path)";
    });
    if (config.replica_num == 0 && config.nof_replica_num == 0) {
        static std::once_flag zero_once;
        std::call_once(zero_once, [] {
            LOG(ERROR) << "Env replica placement resolved to 0 memory and 0 "
                          "NoF replicas; master will reject every put with "
                          "INVALID_PARAMS. Fix MC_MEM_REPLICA_NUM / "
                          "MC_NOF_REPLICA_NUM";
        });
    }
}

ReplicateConfig Client::AttachHostId(const ReplicateConfig& config) const {
    ReplicateConfig client_cfg = config;
    if (dfs_storage_backend_ && dfs_storage_backend_->UsesKvcs()) {
        // KVCS is represented by one Mooncake DFS descriptor. The adapter
        // independently handles target placement and provider replicas, so a
        // Mooncake replica count greater than one must not create a policy the
        // single target descriptor cannot satisfy.
        client_cfg.dfs_replica_num = 1;
    }
    if (!host_id_.empty()) {
        client_cfg.host_id = host_id_;
    }
    ApplyEnvReplicaPlacement(client_cfg);
    return client_cfg;
}

std::optional<SegmentLocality> Client::DiscoverSegmentLocality(
    const Topology& topology, const std::string& location) {
    if (!location.starts_with("cpu:")) return std::nullopt;
    int32_t node = -1;
    const auto value = std::string_view(location).substr(4);
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), node);
    if (error != std::errc{} || end != value.data() + value.size() || node < 0)
        return std::nullopt;
    const auto matrix = topology.getMatrix();
    const auto entry = matrix.find(location);
    return SegmentLocality{node, entry == matrix.end()
                                     ? std::vector<std::string>{}
                                     : entry->second.preferred_hca};
}

Client::HcaAffinity Client::MakeHcaAffinity(
    const Topology& topology, std::vector<SegmentBufferInfo> buffers) {
    auto matrix = topology.getMatrix();
    for (auto& [location, entry] : matrix) {
        auto& hcas = entry.preferred_hca;
        std::sort(hcas.begin(), hcas.end());
        hcas.erase(std::unique(hcas.begin(), hcas.end()), hcas.end());
    }
    return
        [matrix = std::move(matrix), buffers = std::move(buffers)](
            std::span<const Slice> slices) -> const std::vector<std::string>& {
            static const std::vector<std::string> empty;
            const std::vector<std::string>* common = nullptr;
            for (const auto& slice : slices) {
                if (slice.size == 0) continue;
                if (!slice.ptr) return empty;
                const auto address = reinterpret_cast<uintptr_t>(slice.ptr);
                std::string location;
                for (const auto& buffer : buffers) {
                    if (address < buffer.addr) continue;
                    const auto offset = address - buffer.addr;
                    if (offset >= buffer.length ||
                        slice.size > buffer.length - offset)
                        continue;
                    location = buffer.location;
                    SegmentsLocationInfo segments;
                    if (parseSegmentsLocation(location, segments)) {
                        location = resolveSegmentsLocation(
                            segments, buffer.length, offset);
                        if (location !=
                            resolveSegmentsLocation(segments, buffer.length,
                                                    offset + slice.size - 1))
                            return empty;
                    }
                    break;
                }
                const auto entry = matrix.find(location);
                if (entry == matrix.end() ||
                    entry->second.preferred_hca.empty())
                    return empty;
                if (!common)
                    common = &entry->second.preferred_hca;
                else if (*common != entry->second.preferred_hca)
                    return empty;
            }
            return common ? *common : empty;
        };
}

Client::HcaAffinity Client::MakeHcaAffinity() const {
    if (protocol_ != "rdma" || !StoreHcaAffinityEnabled() || !transfer_engine_)
        return {};
    const auto topology = transfer_engine_->getLocalTopology();
    std::vector<SegmentBufferInfo> buffers;
    if (!topology ||
        transfer_engine_->getSegmentBuffers(LOCAL_SEGMENT_ID, buffers) != 0)
        return {};
    return MakeHcaAffinity(*topology, std::move(buffers));
}

void Client::AttachHcaAffinity(ReplicateConfig& config,
                               const std::vector<Slice>& slices) const {
    if (config.preferred_hca_names) return;
    if (auto infer = MakeHcaAffinity()) {
        auto hcas = infer(slices);
        if (!hcas.empty()) config.preferred_hca_names = std::move(hcas);
    }
}

ErrorCode Client::ReadReplicaSelector::operator()(
    const std::vector<Replica::Descriptor>& replicas,
    std::span<const Slice> destinations, Replica::Descriptor& result) {
    // No topology copies, registration lookups or callback allocations for
    // disabled affinity, non-RDMA clients or single-copy reads.
    if (!StoreHcaAffinityEnabled() || client_->protocol_ != "rdma" ||
        replicas.size() <= 1) {
        return client_->FindFirstCompleteReplica(replicas, result);
    }
    if (!initialized_) {
        initialized_ = true;
        if (auto infer = client_->MakeHcaAffinity()) {
            context_ = std::make_unique<Context>(std::move(infer),
                                                 client_->GetLocalEndpoints());
        }
    }
    if (!context_) return client_->FindFirstCompleteReplica(replicas, result);
    const auto& hcas = context_->infer(destinations);
    if (hcas.empty())
        return client_->FindFirstCompleteReplica(replicas, result);
    const auto* selected =
        SelectBestReplica(replicas, context_->endpoints, hcas);
    if (!selected) return ErrorCode::INVALID_REPLICA;
    result = *selected;
    return ErrorCode::OK;
}

static std::vector<std::string> ParseDeviceNames(std::string_view value) {
    std::vector<std::string> devices;
    boost::split(devices, std::string(value), boost::is_any_of(","),
                 boost::token_compress_on);
    for (auto& device : devices) {
        device = std::string(TrimAsciiWhitespace(device));
    }
    return devices;
}

tl::expected<std::optional<ha::HABackendSpec>, ErrorCode> ParseHABackendSpec(
    const std::string& master_server_entry) {
    const auto delimiter_pos = master_server_entry.find("://");
    if (delimiter_pos == std::string::npos) {
        return std::optional<ha::HABackendSpec>{std::nullopt};
    }

    auto backend_type =
        ha::ParseHABackendType(master_server_entry.substr(0, delimiter_pos));
    if (!backend_type.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto availability = ha::ValidateHABackendAvailability(backend_type.value());
    if (availability != ErrorCode::OK) {
        return tl::make_unexpected(availability);
    }

    return std::optional<ha::HABackendSpec>{ha::HABackendSpec{
        .type = backend_type.value(),
        .connstring = master_server_entry.substr(delimiter_pos + 3),
        .cluster_namespace = "",
    }};
}

tl::expected<void, ErrorCode> CheckRegisterMemoryParams(
    const void* addr, size_t length, const std::string& protocol) {
    if (addr == nullptr) {
        LOG(ERROR) << "addr is nullptr";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (length == 0) {
        LOG(ERROR) << "length is 0";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (auto limit = GetTransportRegistrationLimit(protocol);
        limit.has_value() && length > *limit) {
        LOG(ERROR) << "length " << length << " exceeds registration limit "
                   << *limit << " for protocol " << protocol;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

ErrorCode Client::ConnectToMaster(const std::string& master_server_entry) {
    auto ha_backend_spec = ParseHABackendSpec(master_server_entry);
    if (!ha_backend_spec) {
        LOG(ERROR) << "Invalid HA backend entry: " << master_server_entry;
        return ha_backend_spec.error();
    }

    if (ha_backend_spec.value().has_value()) {
        auto coordinator =
            ha::CreateLeaderCoordinator(ha_backend_spec.value().value());
        if (!coordinator) {
            LOG(ERROR) << "Failed to create HA backend coordinator: "
                       << toString(coordinator.error());
            return coordinator.error();
        }

        auto master_view = ha::ReadCurrentViewOrWaitForReady(
            *coordinator.value(), kInitialLeaderReadyTimeout);
        if (!master_view) {
            LOG(ERROR) << "Failed to discover a ready master view: "
                       << toString(master_view.error());
            return master_view.error();
        }
        if (!master_view->has_value()) {
            // Multi-submaster deployments do not publish a single leader
            // view. Try their CVM registry only on etcd-backed deployments.
            if (ha_backend_spec.value()->type != ha::HABackendType::ETCD) {
                LOG(ERROR) << "No master became ready within "
                           << kInitialLeaderReadyTimeout.count() << " seconds";
                return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
            }
            auto err =
                ConnectToCvmSubmasters(ha_backend_spec.value()->connstring);
            if (err != ErrorCode::OK) {
                return err;
            }
            direct_master_address_.clear();
            etcd_connstring_ = ha_backend_spec.value()->connstring;
            return ErrorCode::OK;
        }

        auto err = SwitchLeader(master_view->value());
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master";
            return err;
        }

        leader_coordinator_ = std::move(coordinator.value());
        direct_master_address_.clear();
        if (ha_backend_spec.value()->type == ha::HABackendType::ETCD) {
            etcd_connstring_ = ha_backend_spec.value()->connstring;
        }
        if (!leader_monitor_running_.exchange(true)) {
            leader_monitor_thread_ =
                std::thread([this]() { this->LeaderMonitorThreadMain(); });
        }
        return ErrorCode::OK;
    } else {
        auto err = ConnectMasterEndpoint(master_server_entry);
        if (err != ErrorCode::OK) {
            return err;
        }
        direct_master_address_ = master_server_entry;
        {
            std::lock_guard<std::mutex> lock(leader_switch_mutex_);
            current_master_view_.reset();
        }
        last_ping_success_.store(true);
        return ErrorCode::OK;
    }
}

void Client::EnterHaRuntimeMode() {
    if (!leader_coordinator_) {
        return;
    }

    // Foreground RPCs keep their normal retry policy. Only the HA heartbeat
    // and leader-readiness probes use the separately configured fast-fail
    // control pool once initialization has completed.
    master_client_.EnableHaConnectionPolicy();
}

ErrorCode Client::ConnectMasterEndpoint(const std::string& address) {
    if (!metrics_) return master_client_.Connect(address);

    return metrics_->master_heartbeat_metric.ObserveConnect(
        [this, &address] { return master_client_.Connect(address); });
}

    ErrorCode Client::ConnectToCvmSubmasters(
        const std::string& etcd_endpoints) {
        ErrorCode err = EtcdHelper::ConnectToEtcdStoreClient(etcd_endpoints);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "ConnectToCvmSubmasters: failed to connect etcd: "
                       << err;
            return err;
        }

        // Discover cluster namespaces that have registered masters by scanning
        // the /cvm/ key space for /cvm/<ns>/masters/<master_id> keys.
        const std::string cvm_root(std::string(mooncake::cvm::kCvmRootPrefix));
        const std::string cvm_root_end = mooncake::cvm::PrefixEnd(cvm_root);
        const std::string masters_suffix("/masters/");
        std::string json;
        EtcdRevisionId revision = 0;
        err = EtcdHelper::GetRangeAsJson(
            cvm_root.data(), cvm_root.size(), cvm_root_end.data(),
            cvm_root_end.size(), 0 /* no limit */, json, revision);
        if (err != ErrorCode::OK) {
            LOG(ERROR)
                << "ConnectToCvmSubmasters: failed to scan /cvm/ prefix: "
                << err;
            return err;
        }

        std::set<std::string> namespaces;
        {
            Json::Value root;
            Json::CharReaderBuilder reader;
            std::string errors;
            std::istringstream stream(json);
            if (!Json::parseFromStream(reader, stream, &root, &errors) ||
                !root.isArray()) {
                LOG(ERROR) << "ConnectToCvmSubmasters: failed to parse /cvm/ "
                              "range JSON: "
                           << errors;
                return ErrorCode::INTERNAL_ERROR;
            }
            for (const auto& item : root) {
                if (!item.isObject() || !item["key"].isString()) {
                    continue;
                }
                const std::string key = item["key"].asString();
                if (!key.starts_with(cvm_root)) {
                    continue;
                }
                const size_t ns_begin = cvm_root.size();
                const size_t masters_pos = key.find(masters_suffix, ns_begin);
                // Key layout: /cvm/<ns>/masters/<master_id>. Reject namespaces
                // containing '/' (not a valid masters key).
                if (masters_pos == std::string::npos ||
                    key.find('/', ns_begin) != masters_pos) {
                    continue;
                }
                namespaces.insert(key.substr(ns_begin, masters_pos - ns_begin));
            }
        }
        if (namespaces.empty()) {
            LOG(ERROR) << "ConnectToCvmSubmasters: no /cvm/<ns>/masters/ "
                          "registry found in etcd";
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
        if (namespaces.size() > 1) {
            std::ostringstream oss;
            for (const auto& ns : namespaces) {
                oss << " " << ns;
            }
            LOG(WARNING) << "ConnectToCvmSubmasters: multiple CVM namespaces "
                            "found ("
                         << oss.str() << "); using the first one";
        }

        // Try each namespace; within a namespace, prefer primaries ordered by
        // registration time (first-registered first), matching the
        // CvmController first-come-first-served ranking. Fall back to standbys
        // only if no primary is reachable, so the client can still bootstrap.
        for (const auto& ns : namespaces) {
            std::vector<cvm::MasterRegistration> masters;
            ViewVersionId version = 0;
            err = cvm::EtcdViewStore::LoadAllMasters(ns, masters, version);
            if (err != ErrorCode::OK) {
                LOG(WARNING) << "ConnectToCvmSubmasters: LoadAllMasters failed "
                                "for namespace "
                             << ns << ": " << err;
                continue;
            }
            std::sort(masters.begin(), masters.end(),
                      [](const cvm::MasterRegistration& a,
                         const cvm::MasterRegistration& b) {
                          if (a.registered_at_ms != b.registered_at_ms) {
                              return a.registered_at_ms < b.registered_at_ms;
                          }
                          return a.master_id < b.master_id;
                      });

            auto try_connect_group = [&](bool primaries_only) {
                for (const auto& reg : masters) {
                    if (primaries_only &&
                        reg.role !=
                            static_cast<int32_t>(cvm::MasterRole::kPrimary)) {
                        continue;
                    }
                    auto connect_err = master_client_.Connect(reg.address);
                    if (connect_err != ErrorCode::OK) {
                        LOG(WARNING) << "ConnectToCvmSubmasters: connect to "
                                     << reg.master_id << " (" << reg.address
                                     << ") failed: " << toString(connect_err);
                        continue;
                    }
                    cvm_cluster_namespace_ = ns;
                    // 收集该 namespace 下所有 primary 地址，用于 client 侧全量
                    // mount 与多 submaster 心跳（同一 segment 挂到所有
                    // submaster）。
                    {
                        std::lock_guard<std::mutex> lock(
                            cvm_submaster_addresses_mutex_);
                        cvm_submaster_addresses_.clear();
                        // 去重：etcd 中可能存在指向同一 address 的重复 primary
                        // 注册（如重启残留 lease），不去重会导致全量 mount
                        // 对同一 submaster 重复调用、master 日志刷屏。
                        std::set<std::string> seen;
                        for (const auto& reg2 : masters) {
                            if (reg2.role == static_cast<int32_t>(
                                                 cvm::MasterRole::kPrimary) &&
                                !reg2.address.empty() &&
                                seen.insert(reg2.address).second) {
                                cvm_submaster_addresses_.push_back(
                                    reg2.address);
                            }
                        }
                    }
                    {
                        std::lock_guard<std::mutex> lock(leader_switch_mutex_);
                        current_master_view_.reset();
                    }
                    // Seed cluster_id_ with the CVM namespace so
                    // TryLoadRoutingOnce skips fsdir-based discovery: GetFsdir
                    // returns an empty string when the submaster has no storage
                    // root configured, which would otherwise leave the
                    // partition routing permanently unloaded.
                    {
                        std::lock_guard<std::mutex> lock(routing_load_mutex_);
                        cluster_id_ = ns;
                    }
                    last_ping_success_.store(true);
                    LOG(INFO)
                        << "ConnectToCvmSubmasters: connected to submaster "
                        << reg.master_id << " (" << reg.address
                        << ", role=" << reg.role << ", namespace=" << ns << ")";
                    return ErrorCode::OK;
                }
                return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
            };

            if (try_connect_group(true) == ErrorCode::OK) {
                return ErrorCode::OK;
            }
            if (try_connect_group(false) == ErrorCode::OK) {
                return ErrorCode::OK;
            }
            LOG(WARNING) << "ConnectToCvmSubmasters: no reachable master in "
                            "namespace "
                         << ns;
        }

        LOG(ERROR) << "ConnectToCvmSubmasters: no reachable submaster in any "
                      "CVM namespace";
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    void Client::LoadPartitionRouting() {
        // Routing is only meaningful in HA mode where etcd is available.
        if (etcd_connstring_.empty()) {
            LOG(INFO) << "LoadPartitionRouting skipped: non-HA mode (no etcd "
                         "endpoints)";
            return;
        }
        LOG(INFO) << "LoadPartitionRouting: etcd_endpoints=["
                  << etcd_connstring_ << "]";

        // First attempt (best effort). A failure here must not fail client
        // startup; the refresh loop started below keeps retrying until it
        // succeeds.
        TryLoadRoutingOnce();

        // Start the periodic refresh/retry loop unconditionally in HA mode, so
        // a first-attempt failure (etcd temporarily unavailable, GetFsdir
        // hiccup) is retried on the next cycle instead of leaving the client
        // permanently in single-master fallback.
        if (!routing_refresh_running_.exchange(true)) {
            routing_refresh_thread_ =
                std::thread([this]() { this->RoutingRefreshThreadMain(); });
        }
    }

    void Client::TryLoadRoutingOnce() {
        // 串行化 routing-refresh 线程与 SLOT_NOT_OWNED 触发的按需刷新，
        // 避免并发读写 cluster_id_ 与路由表。
        std::lock_guard<std::mutex> lock(routing_load_mutex_);
        // Resolve the cluster namespace (cluster_id) from the fsdir returned by
        // master, which is formatted as "<root_fs_dir>/<cluster_id>". The
        // result is stable, so it is cached in cluster_id_ and reused across
        // retries.
        std::string cluster_id = cluster_id_;
        if (cluster_id.empty()) {
            auto fsdir_response = master_client_.GetFsdir();
            if (!fsdir_response) {
                LOG(WARNING)
                    << "TryLoadRoutingOnce: GetFsdir failed, err="
                    << toString(fsdir_response.error()) << " (will retry)";
                return;
            }
            const std::string& fsdir = fsdir_response.value();
            if (fsdir.empty()) {
                LOG(WARNING)
                    << "TryLoadRoutingOnce: GetFsdir returned empty fsdir";
                return;
            }
            LOG(INFO) << "TryLoadRoutingOnce: fsdir=" << fsdir;

            const size_t pos = fsdir.find_last_of('/');
            if (pos == std::string::npos) {
                LOG(WARNING)
                    << "TryLoadRoutingOnce: invalid fsdir format (no '/'), "
                       "fsdir="
                    << fsdir;
                return;
            }
            cluster_id = fsdir.substr(pos + 1);
            if (cluster_id.empty()) {
                LOG(WARNING)
                    << "TryLoadRoutingOnce: empty cluster_id parsed from "
                       "fsdir="
                    << fsdir;
                return;
            }
            cluster_id_ = cluster_id;
            LOG(INFO) << "TryLoadRoutingOnce: resolved cluster_id="
                      << cluster_id;
        }

        ErrorCode err =
            master_client_.LoadRoutingFromEtcd(etcd_connstring_, cluster_id);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "TryLoadRoutingOnce: failed to load routing, "
                            "cluster_id="
                         << cluster_id << " err=" << err;
            return;
        }
        LOG(INFO) << "TryLoadRoutingOnce: routing loaded, cluster_id="
                  << cluster_id;
    }

    void Client::RoutingRefreshThreadMain() {
        // Refresh cadence aligned with SlotOwnerHeartbeat (5s) and
        // CvmController role reconciliation (5s). Slot migration is a
        // low-frequency event, so a fixed sleep (rather than a fast poll) keeps
        // steady-state etcd load low while still picking up changes promptly.
        constexpr auto kRefreshInterval = std::chrono::milliseconds(5000);

        while (routing_refresh_running_.load()) {
            std::this_thread::sleep_for(kRefreshInterval);
            TryLoadRoutingOnce();
            // CVM 多 submaster：同步刷新已发现的 primary 地址，并对新增的
            // submaster 全量 mount，处理运行期 submaster 增删。
            RefreshSubmasterAddresses();
        }
    }

    ErrorCode Client::SwitchLeader(const ha::MasterView& target_view) {
        std::lock_guard<std::mutex> lock(leader_switch_mutex_);

        if (current_master_view_.has_value()) {
            const auto& current_view = current_master_view_.value();
            if (target_view.view_version < current_view.view_version) {
                return ErrorCode::OK;
            }

            if (target_view.view_version == current_view.view_version &&
                target_view.leader_address == current_view.leader_address &&
                last_ping_success_.load()) {
                return ErrorCode::OK;
            }
        }

        auto err = ConnectMasterEndpoint(target_view.leader_address);
        if (err != ErrorCode::OK) {
            last_ping_success_.store(false);
            return err;
        }

        current_master_view_ = target_view;
        last_ping_success_.store(true);
        return ErrorCode::OK;
    }

    void Client::LeaderMonitorThreadMain() {
        // WaitForViewChange now blocks on a backend watch and returns as soon
        // as leadership actually changes, so this timeout no longer drives
        // polling cadence -- it is only a periodic re-sync safety net that
        // re-reads the current view in case a watch event was missed (e.g. a
        // connection blip). Keeping it long (30s instead of 1s) collapses
        // steady-state backend load from ~1 read/sec/client to ~1
        // read/30s/client without affecting failover responsiveness, which is
        // driven by the watch.
        constexpr auto kViewChangeTimeout = std::chrono::milliseconds(30000);
        constexpr auto kErrorRetryInterval = std::chrono::milliseconds(1000);

        while (leader_monitor_running_.load()) {
            std::optional<ViewVersionId> known_version;
            {
                std::lock_guard<std::mutex> lock(leader_switch_mutex_);
                if (current_master_view_.has_value()) {
                    known_version = current_master_view_->view_version;
                }
            }

            auto view_change = leader_coordinator_->WaitForViewChange(
                known_version, kViewChangeTimeout);
            if (!view_change) {
                LOG(WARNING) << "Failed to wait for leader view change: "
                             << toString(view_change.error());
                std::this_thread::sleep_for(kErrorRetryInterval);
                continue;
            }

            if (!view_change->changed ||
                !view_change->current_view.has_value()) {
                // A warming leader owns master_view without exposing a routable
                // endpoint. Acknowledge the empty observation so the next wait
                // blocks on the ready-value update instead of spinning on the
                // old view version.
                if (view_change->changed &&
                    !view_change->current_view.has_value()) {
                    std::lock_guard<std::mutex> lock(leader_switch_mutex_);
                    // Do not let a stale empty watch result erase a newer view
                    // installed concurrently by the heartbeat recovery path.
                    const bool still_on_observed_view =
                        known_version.has_value()
                            ? current_master_view_.has_value() &&
                                  current_master_view_->view_version ==
                                      known_version.value()
                            : !current_master_view_.has_value();
                    if (still_on_observed_view) {
                        current_master_view_.reset();
                    }
                }
                continue;
            }

            auto err = SwitchLeader(view_change->current_view.value());
            if (err != ErrorCode::OK) {
                LOG(WARNING) << "Failed to switch to leader "
                             << view_change->current_view->leader_address
                             << ": " << toString(err);
                std::this_thread::sleep_for(kErrorRetryInterval);
            }
        }
    }

    void Client::EnsureStorageControlPlaneStarted() {
        if (!storage_heartbeat_running_.exchange(true)) {
            storage_heartbeat_thread_ =
                std::thread([this]() { this->StorageHeartbeatThreadMain(); });
        }

        if (!task_poll_running_.exchange(true)) {
            task_poll_thread_ =
                std::thread([this]() { this->TaskPollThreadMain(); });
        }
    }

    ErrorCode Client::InitTransferEngine(
        const std::string& local_hostname,
        const std::string& metadata_connstring, const std::string& protocol,
        const std::optional<std::string>& device_names) {
        // TEs created through the Store entry (Client::Create ->
        // InitTransferEngine) are tagged so ascend_direct can resolve a
        // per-role link config (e.g. Store=RoCE/D2H, P2P=HCCS/D2D). The flag
        // only needs to be live while the ascend transport is installed below;
        // reset it on every exit path. Assumes TE inits are serialized within
        // the process.
        struct StoreTeInitGuard {
            ~StoreTeInitGuard() { globalConfig().ascend_store_te_init = false; }
        } store_te_init_guard;
        if (protocol == "ascend") {
            globalConfig().ascend_store_te_init = true;
        }

        // Check if using TENT mode - TENT handles transport configuration
        // internally. Use the engine's own check rather than the raw env var:
        // if Mooncake was built without USE_TENT, the env var has no effect on
        // the TransferEngine, so the store must still install transports.
        bool use_tent = transfer_engine_->isUsingTent();

        bool auto_discover = false;
        if (!use_tent) {
            auto config = ClientAutoDiscoveryConfig::FromEnvironment(
                protocol, device_names.has_value());
            auto_discover = config.enabled;
            transfer_engine_->setAutoDiscover(
                {.enabled = auto_discover, .protocol = protocol});

            // Honor filters when auto-discovery is enabled; otherwise warn once
            if (auto_discover) {
                LOG(INFO) << "Transfer engine auto discovery is enabled for "
                             "protocol: "
                          << protocol;
            }

            config.LoadFiltersFromEnvironment();
            if (auto_discover) {
                transfer_engine_->setWhitelistFilters(
                    std::move(config.filters));
            }
        }

        if (protocol == "ascend" || protocol == "ubshmem") {
            const char* ascend_use_fabric_mem =
                std::getenv("ASCEND_ENABLE_USE_FABRIC_MEM");
            if (ascend_use_fabric_mem) {
                globalConfig().ascend_use_fabric_mem = true;
            }
        }
        if (protocol == "sunrise_link") {
            const char* sunrise_use_device_mem_env =
                std::getenv("SUNRISE_USE_DEVICE_MEM");
            if (sunrise_use_device_mem_env) {
                globalConfig().sunrise_use_device_mem = true;
                LOG(INFO) << "SUNRISE_USE_DEVICE_MEM enabled: segments will be "
                          << "allocated in device memory via tangMalloc";
            }
        }
        auto [hostname, port] = parseHostNameWithPort(local_hostname);
        int rc = transfer_engine_->init(metadata_connstring, local_hostname,
                                        hostname, port);
        if (rc != 0) {
            LOG(ERROR) << "Failed to initialize transfer engine, rc=" << rc;
            return ErrorCode::INTERNAL_ERROR;
        }

        // TENT mode: Skip manual transport installation - TENT handles this
        // internally
        if (use_tent) {
            LOG(INFO) << "Using TENT mode - transport configuration handled "
                         "internally";
            if (device_names.has_value()) {
                LOG(INFO)
                    << "Note: device_names parameter is ignored in TENT mode. "
                    << "Configure devices via TENT config file or environment "
                       "variables.";
            }
            return ErrorCode::OK;
        }

        if (!auto_discover) {
            LOG(INFO)
                << "Transfer engine auto discovery is disabled for protocol: "
                << protocol;

            Transport* transport = nullptr;

            if (protocol == "rdma" || protocol == "efa") {
                if (!device_names.has_value() || device_names->empty()) {
                    LOG(ERROR)
                        << "RDMA protocol requires device names when auto "
                           "discovery is disabled";
                    return ErrorCode::INVALID_PARAMS;
                }

                LOG(INFO) << "Using specified RDMA devices: "
                          << device_names.value();

                std::vector<std::string> devices =
                    ParseDeviceNames(device_names.value());

                // Manually discover topology with specified devices only
                auto topology = transfer_engine_->getLocalTopology();
                if (topology) {
                    topology->discover(devices);
                    LOG(INFO) << "Topology discovery complete with specified "
                                 "devices. Found "
                              << topology->getHcaList().size() << " HCAs";
                }

                transport =
                    transfer_engine_->installTransport(protocol, nullptr);
                if (!transport) {
                    LOG(ERROR)
                        << "Failed to install RDMA transport with specified "
                           "devices";
                    return ErrorCode::INTERNAL_ERROR;
                }
            } else if (protocol == "tcp") {
                if (device_names.has_value()) {
                    LOG(WARNING)
                        << "TCP protocol does not use device names, ignoring";
                }

                try {
                    transport =
                        transfer_engine_->installTransport("tcp", nullptr);
                } catch (std::exception& e) {
                    LOG(ERROR)
                        << "tcp_transport_install_failed error_message=\""
                        << e.what() << "\"";
                    return ErrorCode::INTERNAL_ERROR;
                }

                if (!transport) {
                    LOG(ERROR) << "Failed to install TCP transport";
                    return ErrorCode::INTERNAL_ERROR;
                }
            } else if (protocol == "nvlink" || protocol == "ascend" ||
                       protocol == "ubshmem" || protocol == "sunrise_link") {
                if (device_names.has_value()) {
                    LOG(WARNING)
                        << protocol
                        << " protocol does not use device names, ignoring";
                }
                try {
                    transport =
                        transfer_engine_->installTransport(protocol, nullptr);
                } catch (std::exception& e) {
                    LOG(ERROR) << protocol
                               << "_transport_install_failed error_message=\""
                               << e.what() << "\"";
                    return ErrorCode::INTERNAL_ERROR;
                }

                if (!transport) {
                    LOG(ERROR)
                        << "Failed to install " << protocol << " transport";
                    return ErrorCode::INTERNAL_ERROR;
                }
            } else if (protocol == "cxl") {
                if (device_names.has_value()) {
                    LOG(WARNING) << "CXL protocol does not use device "
                                    "names, ignoring";
                }
                try {
                    transport =
                        transfer_engine_->installTransport("cxl", nullptr);
                } catch (std::exception& e) {
                    LOG(ERROR)
                        << "cxl_transport_install_failed error_message=\""
                        << e.what() << "\"";
                    return ErrorCode::INTERNAL_ERROR;
                }

                if (!transport) {
                    LOG(ERROR) << "Failed to install CXL transport";
                    return ErrorCode::INTERNAL_ERROR;
                }
            } else if (protocol == "cxi") {
                try {
                    transport =
                        transfer_engine_->installTransport("cxi", nullptr);
                } catch (std::exception& e) {
                    LOG(ERROR)
                        << "cxi_transport_install_failed error_message=\""
                        << e.what() << "\"";
                }
            } else if (protocol == "ub") {
                auto deviceName = device_names.value_or("bonding_dev_0");
                LOG(ERROR) << "ub protocol entable device names is "
                           << deviceName;
                auto devices = ParseDeviceNames(deviceName);
                auto topology = transfer_engine_->getLocalTopology();
                if (topology) {
                    topology->discover(devices);
                    LOG(INFO) << "Topology discovery complete with specified "
                                 "devices. Found "
                              << topology->getHcaList().size() << " HCAs";
                }
                transport = transfer_engine_->installTransport("ub", nullptr);
                if (!transport) {
                    LOG(ERROR)
                        << "Failed to install ub transport with specified "
                           "devices";
                    return ErrorCode::INTERNAL_ERROR;
                }
            } else if (protocol == "nvlink_intra") {
#ifdef USE_INTRA_NVLINK
                LOG(INFO) << "Using intra-NVLink protocol.";
                transport =
                    transfer_engine_->installTransport("nvlink_intra", nullptr);
                if (!transport) {
                    LOG(ERROR) << "Failed to install nvlink_intra transport.";
                    return ErrorCode::INTERNAL_ERROR;
                }
#else
                LOG(ERROR)
                    << "--protocol=nvlink_intra requires USE_INTRA_NVLINK=ON, "
                       "please rebuild mooncake from source.";
                return ErrorCode::INVALID_PARAMS;
#endif
            } else {
                LOG(ERROR) << "unsupported_protocol protocol=" << protocol;
                return ErrorCode::INVALID_PARAMS;
            }
        }

        return ErrorCode::OK;
    }

    Status Client::configureScheduling(
        const scheduling::SchedulerConfig& scheduler_config) {
        if (!transfer_engine_)
            return Status::InvalidArgument(
                "Transfer engine is not initialized");
        return transfer_engine_->configureScheduling(scheduler_config);
    }

    void Client::InitTransferSubmitter() {
        // Initialize TransferSubmitter after transfer engine is ready
        // Keep using logical local_hostname for name-based behaviors; endpoint
        // is used separately where needed. Unconditional; no NofWorkerPool is
        // created when nof_initiator_ is null.
        const int numa_socket_id =
            ClientNumaConfig::FromEnvironment().socket_id.value_or(
                GetCurrentNumaSocketId());
        // ClientMetric::nof_metric exists only in USE_NOF builds.
        NofMetric* nof_metric = nullptr;
#ifdef USE_NOF
        nof_metric = metrics_ ? &metrics_->nof_metric : nullptr;
#endif
        transfer_submitter_ = std::make_unique<TransferSubmitter>(
            *transfer_engine_, storage_backend_, local_hostname_,
            metrics_ ? &metrics_->transfer_metric : nullptr, numa_socket_id,
            nof_initiator_, nof_metric);
        vsegment_transfer_planner_ =
            std::make_shared<vsegment::VSegmentTransferPlanner>();
    }

    void Client::SetDefaultNofInitiatorFactoryForTesting(
        std::function<std::shared_ptr<NVMeoFInitiator>()> factory) {
        DefaultNofInitiatorFactoryForTestingSlot() = std::move(factory);
    }

    std::shared_ptr<NVMeoFInitiator> Client::GetNofInitiatorForTesting() const {
        return nof_initiator_;
    }

    std::optional<std::shared_ptr<Client>> Client::Create(
        const std::string& local_hostname,
        const std::string& metadata_connstring, const std::string& protocol,
        const std::optional<std::string>& device_names,
        const std::string& master_server_entry,
        const std::shared_ptr<TransferEngine>& transfer_engine,
        std::map<std::string, std::string> labels, const std::string& tenant_id,
        std::shared_ptr<NVMeoFInitiator> nof_initiator) {
        // Reused engines retain their metadata mode. In particular,
        // P2PHANDSHAKE must publish the engine's actual endpoint rather than
        // the Store hostname.
        const auto& resolved_metadata =
            transfer_engine && metadata_connstring.empty()
                ? transfer_engine->getMetadataConnectionString()
                : metadata_connstring;
        auto client = std::shared_ptr<Client>(new Client(
            local_hostname, resolved_metadata, protocol, labels, tenant_id));
        if (!nof_initiator) {
            // nullptr means "use the process default", not "disable NoF" —
            // existing callers keep the pre-parameter behavior. Explicit
            // disable is MC_NOF_BACKEND=none; explicit injection is a non-null
            // initiator.
            auto& factory = DefaultNofInitiatorFactoryForTestingSlot();
            nof_initiator = factory ? factory() : CreateNofRuntime().initiator;
        }
        client->nof_initiator_ = std::move(nof_initiator);

        ErrorCode err = client->ConnectToMaster(master_server_entry);
        if (err != ErrorCode::OK) {
            return std::nullopt;
        }

        // Load the KV partition routing table from etcd (HA mode only). Best
        // effort: a routing load failure must not fail client startup.
        client->LoadPartitionRouting();

        // Initialize storage backend if storage_root_dir is valid
        auto config_response = client->master_client_.GetStorageConfig();
        if (!config_response) {
            LOG(ERROR) << "Failed to get storage config from master";
            // Fallback to GetFsdir for backward compatibility
            auto response = client->master_client_.GetFsdir();
            if (!response) {
                LOG(ERROR) << "Failed to get fsdir from master";
            } else if (response.value().empty()) {
                LOG(INFO)
                    << "Storage root directory is not set. persisting data is "
                       "disabled.";
            } else {
                auto dir_string = response.value();
                size_t pos = dir_string.find_last_of('/');
                if (pos != std::string::npos) {
                    std::string storage_root_dir = dir_string.substr(0, pos);
                    std::string fs_subdir = dir_string.substr(pos + 1);
                    LOG(INFO)
                        << "Storage root directory is: " << storage_root_dir;
                    LOG(INFO) << "Fs subdir is: " << fs_subdir;
                    // Initialize storage backend with default eviction settings
                    auto prep_err = client->PrepareStorageBackend(
                        storage_root_dir, fs_subdir, true, 0);
                    if (prep_err != ErrorCode::OK) {
                        LOG(ERROR)
                            << "Failed to initialize storage backend: "
                            << prep_err
                            << ". Persistence was requested via fsdir but is "
                               "unavailable.";
                        return std::nullopt;
                    }
                } else {
                    LOG(ERROR) << "Invalid fsdir format: " << dir_string;
                    return std::nullopt;
                }
            }
        } else {
            auto config = config_response.value();
            if (config.fsdir.empty()) {
                LOG(INFO)
                    << "Storage root directory is not set. persisting data is "
                       "disabled.";
            } else {
                size_t pos = config.fsdir.find_last_of('/');
                if (pos != std::string::npos) {
                    std::string storage_root_dir = config.fsdir.substr(0, pos);
                    std::string fs_subdir = config.fsdir.substr(pos + 1);
                    LOG(INFO)
                        << "Storage root directory is: " << storage_root_dir;
                    LOG(INFO) << "Fs subdir is: " << fs_subdir;
                    LOG(INFO) << "Disk eviction enabled: "
                              << config.enable_disk_eviction;
                    LOG(INFO) << "Quota bytes: " << config.quota_bytes;
                    // Initialize storage backend with config from master
                    auto prep_err = client->PrepareStorageBackend(
                        storage_root_dir, fs_subdir,
                        config.enable_disk_eviction, config.quota_bytes);
                    if (prep_err != ErrorCode::OK) {
                        LOG(ERROR)
                            << "Failed to initialize storage backend: "
                            << prep_err
                            << ". Persistence was requested via storage config "
                               "but is unavailable.";
                        return std::nullopt;
                    }
                } else {
                    LOG(ERROR) << "Invalid fsdir format: " << config.fsdir;
                    return std::nullopt;
                }
            }
        }

        client->EnterHaRuntimeMode();

        // this only performs RPC calls
        if (protocol == "rpc_only") {
            LOG(INFO) << "Use rpc only. Skip initializing transfer engine.";
            return client;
        }

        // Initialize transfer engine
        if (transfer_engine == nullptr) {
            client->transfer_engine_ = std::make_shared<TransferEngine>();
            err = client->InitTransferEngine(
                local_hostname, metadata_connstring, protocol, device_names);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to initialize transfer engine";
                return std::nullopt;
            }
        } else {
            client->transfer_engine_ = transfer_engine;
            LOG(INFO) << "Use existing transfer engine instance. Skip its "
                         "initialization.";
        }

        client->InitTransferSubmitter();
        // Initialize local hot cache
        err = client->InitLocalHotCache();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize local hot cache";
        }

        return client;
    }

    tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                              std::vector<Slice>& slices) {
        auto query_result = Query(object_key);
        if (!query_result) {
            return tl::unexpected(query_result.error());
        }
        return Get(object_key, query_result.value(), slices);
    }

    std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
        const std::vector<std::string>& object_keys,
        std::unordered_map<std::string, std::vector<Slice>>& slices) {
        auto batched_query_results = BatchQuery(object_keys);

        // If any queries failed, return error results immediately for failed
        // queries
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());

        std::vector<QueryResult> valid_query_results;
        std::vector<size_t> valid_indices;
        std::vector<std::string> valid_keys;

        for (size_t i = 0; i < batched_query_results.size(); ++i) {
            if (batched_query_results[i]) {
                valid_query_results.emplace_back(
                    batched_query_results[i].value());
                valid_indices.emplace_back(i);
                valid_keys.emplace_back(object_keys[i]);
                results.emplace_back();  // placeholder for successful results
            } else {
                results.emplace_back(
                    tl::unexpected(batched_query_results[i].error()));
            }
        }

        // If we have any valid queries, process them
        if (!valid_keys.empty()) {
            std::unordered_map<std::string, std::vector<Slice>> valid_slices;
            for (const auto& key : valid_keys) {
                auto it = slices.find(key);
                if (it != slices.end()) {
                    valid_slices[key] = it->second;
                }
            }

            auto valid_results =
                BatchGet(valid_keys, valid_query_results, valid_slices);

            // Merge results back
            for (size_t i = 0; i < valid_indices.size(); ++i) {
                results[valid_indices[i]] = valid_results[i];
            }
        }

        return results;
    }

    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    Client::BatchQueryIp(const std::vector<UUID>& client_ids) {
        auto result = master_client_.BatchQueryIp(client_ids);
        return result;
    }

    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    Client::QueryByRegex(const std::string& str) {
        return master_client_.GetReplicaListByRegex(str);
    }

    namespace {
    std::shared_ptr<const std::vector<VSegmentLayoutEntry>> MakeVSegmentLayouts(
        const GetReplicaListResponse& response) {
        if (!response.vsegment_layouts.has_value()) return nullptr;
        SpDiag::PerfPoint pt(PerfKey::VSEGMENT_READ_LAYOUT_COPY,
                             SpDiag::PerfLevel::MODULE);
        pt.Start();
        auto layouts = std::make_shared<const std::vector<VSegmentLayoutEntry>>(
            *response.vsegment_layouts);
        pt.End(0);
        return layouts;
    }
    }  // namespace

    tl::expected<QueryResult, ErrorCode> Client::Query(
        const std::string& object_key) {
        std::chrono::steady_clock::time_point start_time =
            std::chrono::steady_clock::now();
        auto result = master_client_.GetReplicaList(object_key);
        // 松耦合重路由：SLOT_NOT_OWNED 表示本地 slot→submaster 视图过期
        // （分区已重平衡）。刷新一次路由快照后重试，submaster 经 etcd
        // 收敛视图。
        if (!result && result.error() == ErrorCode::SLOT_NOT_OWNED &&
            !etcd_connstring_.empty()) {
            LOG(WARNING) << "Query: SLOT_NOT_OWNED for key=" << object_key
                         << ", refreshing slot routing and retrying once";
            TryLoadRoutingOnce();
            result = master_client_.GetReplicaList(object_key);
            if (result) {
                LOG(INFO) << "Query: re-route retry succeeded for key="
                          << object_key;
            }
        }
        if (!result) {
            return tl::unexpected(result.error());
        }
        QueryResult query(
            std::move(result.value().replicas),
            start_time + std::chrono::milliseconds(result.value().lease_ttl_ms),
            result.value().object_checksum, result.value().resolved_tenant_id);
        query.vsegment_layouts = MakeVSegmentLayouts(result.value());
        return query;
    }

    tl::expected<QueryResult, ErrorCode> Client::QueryReadOnly(
        const std::string& object_key) {
        const auto start_time = std::chrono::steady_clock::now();
        auto result = master_client_.GetReplicaListReadOnly(object_key);
        if (!result) return tl::unexpected(result.error());
        QueryResult query(std::move(result->replicas), start_time,
                          result->object_checksum, result->resolved_tenant_id);
        query.vsegment_layouts = MakeVSegmentLayouts(*result);
        return query;
    }

    std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQuery(
        const std::vector<std::string>& object_keys) {
        return BatchQuery(object_keys, master_client_.tenant_id());
    }

    std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQuery(
        const std::vector<std::string>& object_keys,
        const std::string& tenant_id) {
        SpDiag::PerfPoint pt(PerfKey::CLIENT_BATCH_QUERY,
                             SpDiag::PerfLevel::MODULE);
        pt.Start();
        std::chrono::steady_clock::time_point start_time =
            std::chrono::steady_clock::now();
        auto response =
            master_client_.BatchGetReplicaList(object_keys, tenant_id);

        // 松耦合重路由：任一项返回 SLOT_NOT_OWNED 说明视图过期，刷新一次后
        // 整体重试；读操作幂等，重复查询已成功项无害。
        if (!etcd_connstring_.empty()) {
            bool has_slot_not_owned = false;
            for (const auto& r : response) {
                if (!r && r.error() == ErrorCode::SLOT_NOT_OWNED) {
                    has_slot_not_owned = true;
                    break;
                }
            }
            if (has_slot_not_owned) {
                LOG(WARNING)
                    << "BatchQuery: SLOT_NOT_OWNED in batch of "
                    << object_keys.size()
                    << " keys, refreshing slot routing and retrying once";
                // 巨尾归因（一）：路由刷新整窗。正常 0 tick；若 ~4s
                // 巨尾样本在此段 出现且 avg 达秒级，则尾延迟源于 etcd
                // 路由全表重载。
                SpDiag::PerfPoint pt_refresh(
                    PerfKey::CVM_BATCH_QUERY_ROUTE_REFRESH,
                    SpDiag::PerfLevel::MODULE);
                pt_refresh.Start();
                TryLoadRoutingOnce();
                pt_refresh.End(0);
                response =
                    master_client_.BatchGetReplicaList(object_keys, tenant_id);
            }
        }

        // Check if we got the expected number of responses
        if (response.size() != object_keys.size()) {
            LOG(ERROR) << "BatchQuery response size mismatch. Expected: "
                       << object_keys.size() << ", Got: " << response.size();
            // Return vector of RPC_FAIL errors
            std::vector<tl::expected<QueryResult, ErrorCode>> results;
            results.reserve(object_keys.size());
            for (size_t i = 0; i < object_keys.size(); ++i) {
                results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
            }
            pt.End(-1);
            return results;
        }
        // 批量级别的去重表挂在第一个成功响应上（wire 类型不变，天然兼容）；
        // 整个批次共享同一个 shared_ptr。Hard C 不提供旧 RPC 回退，缺表会在
        // planner 中按 SEGMENT_NOT_FOUND 失败。
        const bool has_vsegment_layout = std::any_of(
            response.begin(), response.end(), [](const auto& result) {
                return result && result->vsegment_layouts.has_value();
            });
        SpDiag::PerfPoint pt_vsegment_layout_copy(
            PerfKey::VSEGMENT_READ_LAYOUT_COPY, SpDiag::PerfLevel::MODULE);
        if (has_vsegment_layout) pt_vsegment_layout_copy.Start();
        std::vector<VSegmentLayoutEntry> batch_layout_entries;
        MergeVSegmentLayoutTablesFromResults(response, batch_layout_entries);
        std::shared_ptr<const std::vector<VSegmentLayoutEntry>> batch_layouts;
        if (!batch_layout_entries.empty()) {
            batch_layouts =
                std::make_shared<const std::vector<VSegmentLayoutEntry>>(
                    std::move(batch_layout_entries));
        }
        if (has_vsegment_layout) pt_vsegment_layout_copy.End(0);
        std::vector<tl::expected<QueryResult, ErrorCode>> results;
        results.reserve(response.size());
        for (size_t i = 0; i < response.size(); ++i) {
            if (response[i]) {
                QueryResult query(
                    std::move(response[i].value().replicas),
                    start_time + std::chrono::milliseconds(
                                     response[i].value().lease_ttl_ms),
                    response[i].value().object_checksum,
                    response[i].value().resolved_tenant_id);
                query.vsegment_layouts = batch_layouts;
                results.emplace_back(std::move(query));
            } else {
                results.emplace_back(tl::unexpected(response[i].error()));
            }
        }
        pt.End(
            std::all_of(results.begin(), results.end(),
                        [](const auto& result) { return result.has_value(); })
                ? 0
                : -1);
        return results;
    }

    std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQueryReadOnly(
        const std::vector<std::string>& object_keys) {
        return BatchQueryReadOnly(object_keys, master_client_.tenant_id());
    }

    std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQueryReadOnly(
        const std::vector<std::string>& object_keys, const std::string& tenant_id) {
        std::chrono::steady_clock::time_point start_time =
            std::chrono::steady_clock::now();
        auto response =
            master_client_.BatchGetReplicaListReadOnly(object_keys, tenant_id);

        // Check if we got the expected number of responses
        if (response.size() != object_keys.size()) {
            LOG(ERROR) << "BatchQueryReadOnly response size mismatch. Expected: "
                       << object_keys.size() << ", Got: " << response.size();
            // Return vector of RPC_FAIL errors
            std::vector<tl::expected<QueryResult, ErrorCode>> results;
            results.reserve(object_keys.size());
            for (size_t i = 0; i < object_keys.size(); ++i) {
                results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
            }
            return results;
        }
        std::vector<tl::expected<QueryResult, ErrorCode>> results;
        results.reserve(response.size());
        for (size_t i = 0; i < response.size(); ++i) {
            if (response[i]) {
                // No lease granted on the read-only path: expiry pinned to now.
                results.emplace_back(
                    QueryResult(std::move(response[i].value().replicas), start_time,
                                response[i].value().object_checksum));
            } else {
                results.emplace_back(tl::unexpected(response[i].error()));
            }
        }
        return results;
    }


    tl::expected<uint64_t, ErrorCode> Client::ComputeObjectChecksumForSlices(
        const std::string& object_key, const std::vector<Slice>& slices,
        size_t object_size) {
        CrcChecksum checksum;
        size_t remaining = object_size;
        auto runtime_accelerator =
            device::GetAcceleratorRegistry().RuntimeAccelerators();
        std::unique_ptr<ScopedObjectChecksumBuffer> staging;

        for (const auto& slice : slices) {
            const size_t bytes = std::min(slice.size, remaining);
            if (bytes == 0) {
                continue;
            }
            if (slice.ptr == nullptr) {
                LOG(ERROR) << "object_checksum_null_slice key=" << object_key;
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }

            if (!runtime_accelerator.FindDeviceForPointer(slice.ptr)) {
                checksum.Update(slice.ptr, bytes);
                remaining -= bytes;
                continue;
            }

            if (!staging) {
                staging = std::make_unique<ScopedObjectChecksumBuffer>(
                    *pinned_buffer_pool_,
                    std::min(kObjectChecksumD2HChunkSize, object_size));
            }
            size_t offset = 0;
            while (offset < bytes) {
                const size_t chunk =
                    std::min(kObjectChecksumD2HChunkSize, bytes - offset);
                const auto* source =
                    static_cast<const char*>(slice.ptr) + offset;
                if (!runtime_accelerator.CopyToHost(staging->data(), source,
                                                    chunk)) {
                    LOG(ERROR)
                        << "object_checksum_d2h_failed key=" << object_key
                        << " size=" << chunk;
                    return tl::unexpected(ErrorCode::TRANSFER_FAIL);
                }
                checksum.Update(staging->data(), chunk);
                offset += chunk;
            }
            remaining -= bytes;
        }

        if (remaining != 0) {
            LOG(ERROR) << "object_checksum_slices_too_small key=" << object_key
                       << " missing_bytes=" << remaining;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        return checksum.Finalize();
    }

    tl::expected<void, ErrorCode> Client::VerifyObjectChecksum(
        const std::string& object_key, const std::vector<Slice>& slices,
        size_t object_size, std::optional<uint64_t> expected_checksum) {
        if (!object_checksum_enabled_) {
            return {};
        }
        if (!expected_checksum.has_value()) {
            VLOG(1) << "object_checksum_absent key=" << object_key;
            return {};
        }

        auto actual_checksum =
            ComputeObjectChecksumForSlices(object_key, slices, object_size);
        if (!actual_checksum) {
            return tl::unexpected(actual_checksum.error());
        }
        if (*actual_checksum != *expected_checksum) {
            LOG(ERROR) << "object_checksum_mismatch key=" << object_key
                       << " expected=" << *expected_checksum
                       << " actual=" << *actual_checksum;
            return tl::unexpected(ErrorCode::CHECKSUM_MISMATCH);
        }
        return {};
    }

    tl::expected<std::vector<std::string>, ErrorCode> Client::BatchReplicaClear(
        const std::vector<std::string>& object_keys, const UUID& client_id,
        const std::string& segment_name) {
        auto result = master_client_.BatchReplicaClear(object_keys, client_id,
                                                       segment_name);
        return result;
    }

    tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                              const QueryResult& query_result,
                                              std::vector<Slice>& slices) {
        // 编排层整窗：GET_INTERNAL_* 六段之和与本窗的差值即编排层占比
        SpDiag::PerfPoint pt_full(PerfKey::GET_SINGLE_FULL,
                                  SpDiag::PerfLevel::KEY_MODULE);
        pt_full.Start();
        // Find the first complete replica
        Replica::Descriptor replica;
        ErrorCode err = ErrorCode::OK;
        {
            SpDiag::PerfPoint pt_find(PerfKey::GET_SINGLE_FIND_REPLICA,
                                      SpDiag::PerfLevel::MODULE);
            pt_find.Start();
            err = MakeReadReplicaSelector()(query_result.replicas, slices,
                                            replica);
            pt_find.End(err == ErrorCode::OK ? 0 : -1);
            if (err != ErrorCode::OK) {
                if (err == ErrorCode::INVALID_REPLICA) {
                    LOG(ERROR)
                        << "no_complete_replicas_found key=" << object_key;
                }
                pt_full.End(-1);
                return tl::unexpected(err);
            }
        }

        // Check local hot cache and update replica descriptor if cache hit
        bool cache_used = false;
        if (hot_cache_ && replica.is_memory_replica()) {
            // status: 0=hit（改写 replica 指向本地 cache，走本地 memcpy）/
            // -1=miss（走 TE 远读）——miss 率 = badCount/tickCount
            SpDiag::PerfPoint pt_cache(PerfKey::GET_SINGLE_HOT_CACHE,
                                       SpDiag::PerfLevel::KEY_MODULE);
            pt_cache.Start();
            cache_used = RedirectToHotCache(object_key, replica);
            pt_cache.End(cache_used ? 0 : -1);
        }

        if (read_replica_validator_ &&
            !read_replica_validator_(object_key, replica)) {
            if (cache_used) hot_cache_->ReleaseHotKey(object_key);
            return tl::unexpected(ErrorCode::INVALID_REPLICA);
        }

        auto t0_get = std::chrono::steady_clock::now();
        if (replica.is_dfs_replica()) {
            err = ReadDfsReplica(object_key, replica, slices);
        } else {
            err = TransferRead(replica, slices, TransferIntent::kForegroundGet,
                               query_result.vsegment_layouts.get());
        }

        // Release the cache block after transfer completes (memcpy is done)
        if (hot_cache_ && cache_used) {
            SpDiag::PerfPoint pt_release(PerfKey::GET_SINGLE_RELEASE_CACHE,
                                         SpDiag::PerfLevel::MODULE);
            pt_release.Start();
            hot_cache_->ReleaseHotKey(object_key);
            pt_release.End(0);
        }

        auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - t0_get)
                          .count();
        if (metrics_) {
            metrics_->transfer_metric.get_latency_us.observe(us_get);
        }

        if (err != ErrorCode::OK) {
            if (err == ErrorCode::OBJECT_NOT_FOUND) {
                ReconcileMissingKvcsReplica(object_key, query_result, replica);
            }
            LOG(ERROR) << "transfer_read_failed key=" << object_key;
            pt_full.End(-1);
            return tl::unexpected(err);
        }

        auto checksum_result = VerifyObjectChecksum(
            object_key, slices, calculate_total_size(replica),
            query_result.object_checksum);
        if (!checksum_result) {
            // add checksum err for single-key
            auto* dfs_metric = GetDfsMetricPtr();
            if (replica.is_dfs_replica() && dfs_metric) {
                dfs_metric->RecordReadErrors(toString(checksum_result.error()));
            }
            pt_full.End(-1);
            return tl::unexpected(checksum_result.error());
        }

        // Frequency admission: only promote frequently accessed keys to hot
        // cache. Skip when cache_used — data was already served from local
        // cache, no need to re-promote or increment the CMS counter.
        if (ShouldAdmitToHotCache(object_key, cache_used)) {
            // 仅观测 enqueue 开销与 promote 触发频率；真正的异步拷贝在
            // hot_cache_handler_ 线程池内执行，不在本窗内。
            SpDiag::PerfPoint pt_async(PerfKey::GET_SINGLE_ASYNC_CACHE,
                                       SpDiag::PerfLevel::MODULE);
            pt_async.Start();
            ProcessSlicesAsync(object_key, slices, replica);
            pt_async.End(0);
        }

        if (query_result.IsLeaseExpired()) {
            LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                         << object_key;
            pt_full.End(-1);
            return tl::unexpected(ErrorCode::LEASE_EXPIRED);
        }
        // Log cache hit statistics
        if (hot_cache_ && replica.is_memory_replica()) {
            VLOG(1) << "Get completed: key=" << object_key
                    << " cache_hit=" << (cache_used ? 1 : 0);
        }

        pt_full.End(0);
        return {};
    }

    tl::expected<void, ErrorCode> Client::Get(
        const std::string& object_key, const QueryResult& query_result,
        std::vector<Slice>& slices, uint64_t src_offset) {
        Replica::Descriptor replica;
        ErrorCode err =
            MakeReadReplicaSelector()(query_result.replicas, slices, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
            }
            return tl::unexpected(err);
        }
        if (!replica.is_memory_replica() && !replica.is_vsegment_replica()) {
            LOG(ERROR) << "Range read only supported for memory or vsegment "
                          "replicas, key="
                       << object_key;
            return tl::unexpected(ErrorCode::INVALID_REPLICA);
        }

        if (read_replica_validator_ &&
            !read_replica_validator_(object_key, replica)) {
            return tl::unexpected(ErrorCode::INVALID_REPLICA);
        }

        auto t0_get = std::chrono::steady_clock::now();
        err = TransferReadRange(replica, slices, src_offset,
                                query_result.vsegment_layouts.get());
        auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - t0_get)
                          .count();
        if (metrics_) {
            metrics_->transfer_metric.get_latency_us.observe(us_get);
        }

        if (err != ErrorCode::OK) {
            LOG(ERROR) << "transfer_read_range_failed key=" << object_key;
            return tl::unexpected(err);
        }
        if (query_result.IsLeaseExpired()) {
            LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                         << object_key;
            return tl::unexpected(ErrorCode::LEASE_EXPIRED);
        }
        return {};
    }

    std::optional<TransferEngine::ScatterTransferOperation>
    Client::SubmitScatter(
        const std::vector<TransferEngine::ScatterTransferRange>& transfers,
        TransferIntent intent) {
        if (!transfer_engine_) {
            LOG(ERROR) << "TransferSubmitter not initialized";
            return std::nullopt;
        }
        auto mutable_transfers = transfers;
        for (auto& transfer : mutable_transfers)
            transfer.intent_type = static_cast<int>(intent);
        return transfer_engine_->submitScatter(mutable_transfers);
    }

    std::optional<StoreScatterTransferOperation> Client::SubmitScatterNative(
        const std::vector<TransferEngine::ScatterTransferRange>& transfers,
        TransferIntent intent) {
        if (!transfer_submitter_) {
            LOG(ERROR) << "TransferSubmitter not initialized";
            return std::nullopt;
        }
        return transfer_submitter_->submitNativeScatter(transfers, intent);
    }

struct BatchGetOperation {
    std::vector<Replica::Descriptor> replicas;
    std::vector<std::vector<Slice>> batched_slices;
    std::vector<size_t> key_indexes;
    std::vector<bool> cache_used;  // Track which keys used cache
    std::vector<TransferFuture> futures;
};

std::vector<tl::expected<void, ErrorCode>> Client::BatchGetWhenPreferSameNode(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.resize(object_keys.size());
    if (!transfer_submitter_ || query_results.size() != object_keys.size()) {
        return std::vector<tl::expected<void, ErrorCode>>(
            object_keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::unordered_map<std::string, BatchGetOperation> seg_to_op_map{};
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& replica_list = query_results[i].replicas;
        auto slices_it = slices.find(key);
        if (slices_it == slices.end()) {
            LOG(ERROR) << "Slices not found for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        Replica::Descriptor replica;
        ErrorCode err = FindFirstCompleteReplica(replica_list, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }
        if (!replica.is_memory_replica()) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }

        // Check local hot cache and update replica descriptor if cache hit
        bool cache_used = false;
        if (hot_cache_ && replica.is_memory_replica()) {
            cache_used = RedirectToHotCache(key, replica);
        }

        if (read_replica_validator_ && !read_replica_validator_(key, replica)) {
            if (cache_used) hot_cache_->ReleaseHotKey(key);
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }

        auto& memory_descriptor = replica.get_memory_descriptor();
        if (memory_descriptor.buffer_descriptor.size_ == 0) {
            if (hot_cache_ && cache_used) hot_cache_->ReleaseHotKey(key);
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        auto& op = seg_to_op_map[seg];
        op.replicas.emplace_back(replica);
        op.batched_slices.emplace_back(slices_it->second);
        op.key_indexes.emplace_back(i);
        op.cache_used.emplace_back(cache_used);
    }
    for (auto& seg_to_op : seg_to_op_map) {
        auto& op = seg_to_op.second;
        auto future = transfer_submitter_->submit_batch(
            op.replicas, op.batched_slices, TransferRequest::READ);
        if (!future) {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }
                results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << object_keys[index];
            }
            continue;
        }
        op.futures.emplace_back(std::move(*future));
    }
    for (auto& seg_to_op : seg_to_op_map) {
        auto& op = seg_to_op.second;
        if (op.futures.empty()) {
            continue;
        }
        ErrorCode result = op.futures[0].get();
        if (result != ErrorCode::OK) {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                // Release cache block even on failure (memcpy may have started)
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }
                results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << object_keys[index];
            }
        } else {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                VLOG(1) << "Transfer completed successfully for key: "
                        << object_keys[index];

                // Release the cache block after transfer completes (memcpy is
                // done)
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }

                auto checksum_result = VerifyObjectChecksum(
                    object_keys[index], op.batched_slices[idx],
                    calculate_total_size(op.replicas[idx]),
                    query_results[index].object_checksum);
                if (!checksum_result) {
                    results[index] = tl::unexpected(checksum_result.error());
                    continue;
                }
                results[index] = {};

                // Frequency admission: only promote frequently accessed keys.
                // Skip when cache was used (data served from local cache).
                if (idx < op.replicas.size() &&
                    idx < op.batched_slices.size() &&
                    ShouldAdmitToHotCache(
                        object_keys[index],
                        idx < op.cache_used.size() && op.cache_used[idx])) {
                    ProcessSlicesAsync(object_keys[index],
                                       op.batched_slices[idx],
                                       op.replicas[idx]);
                }
            }
        }
    }
    auto now = std::chrono::steady_clock::now();
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i] && query_results[i].IsLeaseExpired(now)) {
            results[i] = tl::unexpected(ErrorCode::LEASE_EXPIRED);
        }
    }
    return results;
}

    std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
        const std::vector<std::string>& object_keys,
        const std::vector<QueryResult>& query_results,
        std::unordered_map<std::string, std::vector<Slice>>& slices,
        bool group_by_target, bool prefer_alloc_in_same_node) {
        SpDiag::PerfPoint pt_full(PerfKey::GET_BATCH_FULL,
                                 SpDiag::PerfLevel::KEY_MODULE);
        pt_full.Start();
        if (!transfer_submitter_) {
            LOG(ERROR) << "TransferSubmitter not initialized";
            std::vector<tl::expected<void, ErrorCode>> results;
            results.reserve(object_keys.size());
            for (size_t i = 0; i < object_keys.size(); ++i) {
                results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
            }
            pt_full.End(-1);
            return results;
        }

        // Validate input size consistency
        if (query_results.size() != object_keys.size()) {
            LOG(ERROR) << "Query results size (" << query_results.size()
                       << ") doesn't match object keys size ("
                       << object_keys.size() << ")";
            std::vector<tl::expected<void, ErrorCode>> results;
            results.reserve(object_keys.size());
            for (size_t i = 0; i < object_keys.size(); ++i) {
                results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
            }
            pt_full.End(-1);
            return results;
        }
        if (prefer_alloc_in_same_node) {
            // The same-node fast path batches direct memory handles by
            // endpoint. VSegment replicas need view expansion first, so use the
            // general parallel path when a batch contains one.
            for (const auto& query_result : query_results) {
                Replica::Descriptor replica;
                if (MakeReadReplicaSelector()(query_result.replicas,
                                              std::span<const Slice>{},
                                              replica) == ErrorCode::OK &&
                    replica.is_vsegment_replica()) {
                    prefer_alloc_in_same_node = false;
                    break;
                }
            }
        }
        if (prefer_alloc_in_same_node) {
            // prefer_same_node 委托路径：FULL 窗覆盖委托调用本身；子路径内部
            // （submit_batch / futures[0].get）暂无独立打点。
            auto pref_results =
                BatchGetWhenPreferSameNode(object_keys, query_results, slices);
            pt_full.End(std::any_of(pref_results.begin(), pref_results.end(),
                                    [](const auto& r) { return !r; })
                            ? -1
                            : 0);
            return pref_results;
        }

        const uint64_t trace_id = mooncake::logging::CurrentTraceId();
        const bool breakdown_log =
            ShouldSampleStoreTransferDiagnostics(trace_id);
#ifdef USE_NOF
        const int64_t nof_slow_trace_threshold_us =
            GetSpdkNofSlowTraceThresholdUs();
#else
        const int64_t nof_slow_trace_threshold_us = 0;
#endif
        const auto batch_start = breakdown_log
                                     ? std::chrono::steady_clock::now()
                                     : std::chrono::steady_clock::time_point{};
        auto elapsed_us = [](auto begin, auto end) {
            return std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                         begin)
                .count();
        };

        // Collect all transfer operations for parallel execution
        // Tuple: (index, key, future, replica, cache_used)
        std::vector<std::tuple<size_t, std::string, TransferFuture,
                               Replica::Descriptor, bool>>
            pending_transfers;
        struct TargetReadGroup {
            std::vector<size_t> indexes;
            std::vector<Replica::Descriptor> replicas;
            std::vector<std::vector<Slice>> slices;
            std::vector<bool> cache_used;
        };
        std::unordered_map<std::string, TargetReadGroup> target_groups;
        std::vector<DfsReadRequest> dfs_read_requests;
        std::vector<size_t> dfs_read_indices;
        std::vector<Replica::Descriptor> dfs_read_replicas;
        std::vector<tl::expected<void, ErrorCode>> results(object_keys.size());
        // Record batch get transfer latency (Submit + Wait)
        auto t0_batch_get = std::chrono::steady_clock::now();

        // Collect cache hit statistics for the entire batch
        size_t total_cache_hits = 0;
        size_t submitted_bytes = 0;
        int64_t wait_blocked_us = 0;
        int64_t checksum_us = 0;
        size_t ready_before_wait = 0;
        size_t blocked_futures = 0;
        int64_t max_future_wait_us = 0;
        size_t nof_timing_count = 0;
        uint64_t nof_sub_io_count = 0;
        uint32_t slowest_lane = 0;
        size_t slowest_task_index = 0;
        int64_t slowest_task_us = 0;
        int64_t nof_dispatch_queue_sum_us = 0;
        int64_t nof_dispatch_queue_max_us = 0;
        int64_t nof_worker_setup_sum_us = 0;
        int64_t nof_worker_setup_max_us = 0;
        uint64_t nof_qos_tasks_ahead_sum = 0;
        uint32_t nof_qos_tasks_ahead_max = 0;
        int64_t nof_qos_fifo_wait_sum_us = 0;
        int64_t nof_qos_fifo_wait_max_us = 0;
        int64_t nof_qos_fifo_full_wait_sum_us = 0;
        int64_t nof_qos_fifo_full_wait_max_us = 0;
        int64_t nof_qos_fifo_partial_wait_sum_us = 0;
        int64_t nof_qos_fifo_partial_wait_max_us = 0;
        int64_t nof_qos_fifo_zero_wait_sum_us = 0;
        int64_t nof_qos_fifo_zero_wait_max_us = 0;
        int64_t nof_qos_fifo_unattributed_sum_us = 0;
        int64_t nof_qos_fifo_unattributed_max_us = 0;
        int64_t nof_head_to_submit_sum_us = 0;
        int64_t nof_head_to_submit_max_us = 0;
        int64_t nof_credit_wait_sum_us = 0;
        int64_t nof_credit_wait_max_us = 0;
        int64_t nof_first_credit_wait_sum_us = 0;
        int64_t nof_first_credit_wait_max_us = 0;
        int64_t nof_submit_credit_wait_sum_us = 0;
        int64_t nof_submit_credit_wait_max_us = 0;
        int64_t nof_qos_queue_sum_us = 0;
        int64_t nof_qos_queue_max_us = 0;
        int64_t nof_queue_to_submit_sum_us = 0;
        int64_t nof_queue_to_submit_max_us = 0;
        int64_t nof_submit_span_sum_us = 0;
        int64_t nof_submit_span_max_us = 0;
        int64_t nof_drain_sum_us = 0;
        int64_t nof_drain_max_us = 0;
        int64_t nof_first_completion_sum_us = 0;
        int64_t nof_first_completion_max_us = 0;
        int64_t nof_completion_submit_gap_sum_us = 0;
        int64_t nof_completion_submit_gap_min_us =
            std::numeric_limits<int64_t>::max();
        int64_t nof_completion_submit_gap_max_us =
            std::numeric_limits<int64_t>::min();
        size_t nof_completion_before_last_submit = 0;
        int64_t nof_completion_span_sum_us = 0;
        int64_t nof_completion_span_max_us = 0;
        int64_t nof_task_total_sum_us = 0;
        int64_t nof_task_total_max_us = 0;
        int64_t nof_observe_delay_sum_us = 0;
        int64_t nof_observe_delay_max_us = 0;

        auto select_replica = MakeReadReplicaSelector();
        // One submit sample per batch; wait samples below are per future.
        SpDiag::PerfPoint submit_point(PerfKey::GET_BATCH_SUBMIT,
                                       SpDiag::PerfLevel::MODULE);
        submit_point.Start();
        // Submit all transfers in parallel
        const auto submit_start = breakdown_log
                                      ? std::chrono::steady_clock::now()
                                      : std::chrono::steady_clock::time_point{};
        for (size_t i = 0; i < object_keys.size(); ++i) {
            const auto& key = object_keys[i];
            const auto& query_result = query_results[i];

            auto slices_it = slices.find(key);
            if (slices_it == slices.end()) {
                LOG(ERROR) << "Slices not found for key: " << key;
                results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }

            // Select a complete replica for this key's destination.
            Replica::Descriptor replica;
            {
                SpDiag::PerfPoint pt_find(PerfKey::GET_BATCH_FIND_REPLICA,
                                          SpDiag::PerfLevel::MODULE);
                pt_find.Start();
                ErrorCode err = select_replica(
                    query_result.replicas,
                    std::span<const Slice>(slices_it->second), replica);
                pt_find.End(err == ErrorCode::OK ? 0 : -1);
                if (err != ErrorCode::OK) {
                    if (err == ErrorCode::INVALID_REPLICA) {
                        LOG(ERROR) << "no_complete_replicas_found key=" << key;
                    }
                    results[i] = tl::unexpected(err);
                    continue;
                }
            }

            bool cache_used = false;
            if (hot_cache_ && replica.is_memory_replica()) {
                // status: 0=hit / -1=miss（同 GET_SINGLE_HOT_CACHE 编码）
                SpDiag::PerfPoint pt_cache(PerfKey::GET_BATCH_HOT_CACHE,
                                           SpDiag::PerfLevel::KEY_MODULE);
                pt_cache.Start();
                cache_used = RedirectToHotCache(key, replica);
                pt_cache.End(cache_used ? 0 : -1);
                if (cache_used) {
                    total_cache_hits++;
                }
            }

            if (read_replica_validator_ &&
                !read_replica_validator_(key, replica)) {
                if (cache_used) hot_cache_->ReleaseHotKey(key);
                results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
                continue;
            }

            // Submit transfer operation asynchronously
            std::optional<TransferFuture> future;
            if (replica.is_dfs_replica()) {
                if (!dfs_storage_backend_) {
                    LOG(ERROR) << "DFS backend is not initialized";
                    if (auto* dfs_metric = GetDfsMetricPtr()) {
                        dfs_metric->RecordReadErrors(
                            toString(ErrorCode::DFS_SERVICE_UNAVAILABLE));
                    }
                    results[i] =
                        tl::unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
                    continue;
                }
                const auto& desc = replica.get_dfs_descriptor();
                dfs_read_requests.push_back(
                    DfsReadRequest{key, desc, slices_it->second});
                dfs_read_indices.push_back(i);
                dfs_read_replicas.push_back(replica);
                continue;
            } else if (replica.is_vsegment_replica()) {
                if (!vsegment_transfer_planner_) {
                    results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                    continue;
                }
                std::vector<vsegment::ClientSlice> client_slices;
                client_slices.reserve(slices_it->second.size());
                for (const auto& slice : slices_it->second) {
                    client_slices.push_back(
                        {reinterpret_cast<uint64_t>(slice.ptr), slice.size});
                }
                SpDiag::PerfPoint pt_vsegment_plan(PerfKey::VSEGMENT_READ_PLAN,
                                                   SpDiag::PerfLevel::MODULE);
                pt_vsegment_plan.Start();
                auto plan = vsegment_transfer_planner_->Plan(
                    replica.get_vsegment_descriptor(), client_slices,
                    query_result.vsegment_layouts.get());
                pt_vsegment_plan.End(plan ? 0 : -1);
                if (!plan) {
                    results[i] = tl::unexpected(plan.error);
                    continue;
                }
                SpDiag::PerfPoint pt_vsegment_submit(
                    PerfKey::VSEGMENT_READ_SUBMIT, SpDiag::PerfLevel::MODULE);
                pt_vsegment_submit.Start();
                future = transfer_submitter_->submitVSegment(
                    plan, TransferRequest::READ);
                pt_vsegment_submit.End(future ? 0 : -1);
            } else if (replica.is_nof_replica()) {
                auto contiguous_range =
                    GetContiguousSliceRange(slices_it->second);
                if (!contiguous_range.has_value()) {
                    LOG(ERROR) << "NoF transfer requires contiguous slices";
                    results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                    continue;
                }
                future = transfer_submitter_->submit(
                    replica, slices_it->second, TransferRequest::READ,
                    contiguous_range->ptr, contiguous_range->size,
                    TransferIntent::kForegroundGet);
            } else if (group_by_target && replica.is_memory_replica() &&
                       CalculateSliceSize(slices_it->second) ==
                           replica.get_memory_descriptor()
                               .buffer_descriptor.size_) {
                // A size mismatch would fail the whole group in submit_batch,
                // so such a key stays on the per-key path and fails alone.
                auto& group =
                    target_groups[replica.get_memory_descriptor()
                                      .buffer_descriptor.transport_endpoint_];
                group.indexes.push_back(i);
                group.replicas.push_back(replica);
                group.slices.push_back(slices_it->second);
                group.cache_used.push_back(cache_used);
                continue;
            } else {
                future = transfer_submitter_->submit(
                    replica, slices_it->second, TransferRequest::READ, nullptr,
                    0, TransferIntent::kForegroundGet);
            }
            if (!future) {
                // Release cache block if submit failed
                if (hot_cache_ && cache_used) {
                    SpDiag::PerfPoint pt_release(
                        PerfKey::GET_BATCH_RELEASE_CACHE,
                        SpDiag::PerfLevel::MODULE);
                    pt_release.Start();
                    hot_cache_->ReleaseHotKey(key);
                    pt_release.End(0);
                }
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << key;
                results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                continue;
            }

            VLOG(1) << "Submitted transfer for key " << key
                    << " using strategy: "
                    << static_cast<int>(future->strategy());

            pending_transfers.emplace_back(i, key, std::move(*future), replica,
                                           cache_used);
            if (breakdown_log) {
                for (const auto& slice : slices_it->second) {
                    submitted_bytes += slice.size;
                }
            }
        }
        submit_point.End(pending_transfers.size() == object_keys.size() ? 0
                                                                        : -1);
        const auto submit_end = breakdown_log
                                    ? std::chrono::steady_clock::now()
                                    : std::chrono::steady_clock::time_point{};

        for (auto& [endpoint, group] : target_groups) {
            auto future = transfer_submitter_->submit_batch(
                group.replicas, group.slices, TransferRequest::READ,
                TransferIntent::kForegroundGet);
            for (size_t j = 0; j < group.indexes.size(); ++j) {
                const size_t index = group.indexes[j];
                const auto& key = object_keys[index];
                if (!future) {
                    if (hot_cache_ && group.cache_used[j]) {
                        hot_cache_->ReleaseHotKey(key);
                    }
                    LOG(ERROR) << "Failed to submit batch transfer to "
                               << endpoint << " for key: " << key;
                    results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                    continue;
                }
                pending_transfers.emplace_back(index, key, future->share(),
                                               group.replicas[j],
                                               group.cache_used[j]);
            }
            VLOG(1) << "Submitted batch read of " << group.indexes.size()
                    << " keys from " << endpoint;
        }

        if (!dfs_read_requests.empty()) {
            auto* dfs_metric = GetDfsMetricPtr();
            const auto dfs_read_start = std::chrono::steady_clock::now();
            auto dfs_results =
                dfs_storage_backend_->BatchRead(dfs_read_requests);
            const auto dfs_read_latency_us = elapsed_us_since(dfs_read_start);
            int64_t dfs_success_keys = 0;
            uint64_t dfs_success_bytes = 0;
            if (dfs_results.size() != dfs_read_requests.size()) {
                LOG(ERROR) << "DFS BatchRead response size mismatch: expected "
                           << dfs_read_requests.size() << ", got "
                           << dfs_results.size();
                for (size_t index : dfs_read_indices) {
                    results[index] = tl::unexpected(ErrorCode::INTERNAL_ERROR);
                }
                if (dfs_metric) {
                    dfs_metric->RecordReadErrors(
                        toString(ErrorCode::INTERNAL_ERROR),
                        static_cast<int64_t>(dfs_read_indices.size()));
                }
            } else {
                for (size_t i = 0; i < dfs_results.size(); ++i) {
                    const size_t index = dfs_read_indices[i];
                    const auto& request = dfs_read_requests[i];
                    if (!dfs_results[i]) {
                        LOG(WARNING)
                            << "DFS read failed, action=batch_read, key="
                            << request.key
                            << ", offset=" << request.descriptor.offset
                            << ", size=" << request.descriptor.object_size
                            << ", error=" << dfs_results[i].error();
                        if (dfs_metric) {
                            dfs_metric->RecordReadErrors(
                                toString(dfs_results[i].error()));
                        }
                        results[index] = tl::unexpected(dfs_results[i].error());
                        if (dfs_results[i].error() ==
                            ErrorCode::OBJECT_NOT_FOUND) {
                            ReconcileMissingKvcsReplica(request.key,
                                                        query_results[index],
                                                        dfs_read_replicas[i]);
                        }
                        continue;
                    }
                    // The I/O itself delivered these bytes, so they count even
                    // if the checksum below rejects the payload; a checksum
                    // mismatch is additionally recorded as an error so the two
                    // are distinguishable.
                    ++dfs_success_keys;
                    dfs_success_bytes += request.descriptor.object_size;
                    auto checksum_result = VerifyObjectChecksum(
                        request.key, request.slices,
                        request.descriptor.object_size,
                        query_results[index].object_checksum);
                    if (!checksum_result) {
                        if (dfs_metric) {
                            dfs_metric->RecordReadErrors(
                                toString(checksum_result.error()));
                        }
                        results[index] =
                            tl::unexpected(checksum_result.error());
                        continue;
                    }
                    results[index] = {};
                }
            }
            if (dfs_metric && dfs_success_keys > 0) {
                dfs_metric->ObserveRead(dfs_success_keys,
                                        static_cast<int64_t>(dfs_success_bytes),
                                        dfs_read_latency_us);
            }
            VLOG(1) << "DFS batch read finished, action=batch_read, requested="
                    << dfs_read_requests.size()
                    << ", succeeded=" << dfs_success_keys
                    << ", bytes=" << dfs_success_bytes
                    << ", latency_us=" << dfs_read_latency_us;
        }

        // Wait for all transfers to complete
        for (auto& [index, key, future, stored_replica, cache_used] :
             pending_transfers) {
            const bool is_vsegment = stored_replica.is_vsegment_replica();
            SpDiag::PerfPoint pt_vsegment_wait(PerfKey::VSEGMENT_READ_WAIT,
                                               SpDiag::PerfLevel::MODULE);
            if (is_vsegment) pt_vsegment_wait.Start();
            const bool was_ready = breakdown_log && future.isReady();
            if (was_ready) {
                ++ready_before_wait;
            } else if (breakdown_log) {
                ++blocked_futures;
            }
            const auto wait_start =
                breakdown_log ? std::chrono::steady_clock::now()
                              : std::chrono::steady_clock::time_point{};
            SpDiag::PerfPoint wait_point(PerfKey::GET_BATCH_WAIT,
                                         SpDiag::PerfLevel::MODULE);
            wait_point.Start();
            ErrorCode result = future.get();
            wait_point.End(result == ErrorCode::OK ? 0 : -1);
            if (is_vsegment) {
                pt_vsegment_wait.End(result == ErrorCode::OK ? 0 : -1);
            }
            if (breakdown_log) {
                const int64_t future_wait_us =
                    elapsed_us(wait_start, std::chrono::steady_clock::now());
                wait_blocked_us += future_wait_us;
                max_future_wait_us =
                    std::max(max_future_wait_us, future_wait_us);

            }


            // Release the cache block after transfer completes (memcpy is done)
            if (hot_cache_ && cache_used) {
                SpDiag::PerfPoint pt_release(PerfKey::GET_BATCH_RELEASE_CACHE,
                                             SpDiag::PerfLevel::MODULE);
                pt_release.Start();
                hot_cache_->ReleaseHotKey(key);
                pt_release.End(0);
            }
            if (result != ErrorCode::OK) {
                LOG(ERROR) << "Transfer failed for key: " << key
                           << " with error: " << static_cast<int>(result);
                results[index] = tl::unexpected(result);
            } else {
                VLOG(1) << "Transfer completed successfully for key: " << key;

                auto slices_it = slices.find(key);
                if (slices_it == slices.end()) {
                    results[index] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                    continue;
                }
                const auto checksum_start =
                    breakdown_log ? std::chrono::steady_clock::now()
                                  : std::chrono::steady_clock::time_point{};
                auto checksum_result =
                    VerifyObjectChecksum(key, slices_it->second,
                                         calculate_total_size(stored_replica),
                                         query_results[index].object_checksum);
                if (breakdown_log) {
                    checksum_us += elapsed_us(checksum_start,
                                              std::chrono::steady_clock::now());
                }
                if (!checksum_result) {
                    results[index] = tl::unexpected(checksum_result.error());
                    continue;
                }
                results[index] = {};

                // Frequency admission: only promote frequently accessed keys.
                // Skip when cache was used (data served from local cache).
                if (hot_cache_) {
                    auto slices_it = slices.find(key);
                    if (slices_it != slices.end() &&
                        ShouldAdmitToHotCache(key, cache_used)) {
                        // 仅观测 enqueue 开销与 promote 触发频率（同单笔语义）
                        SpDiag::PerfPoint pt_async(
                            PerfKey::GET_BATCH_ASYNC_CACHE,
                            SpDiag::PerfLevel::MODULE);
                        pt_async.Start();
                        ProcessSlicesAsync(key, slices_it->second,
                                           stored_replica);
                        pt_async.End(0);
                    }
                }
            }
        }
        const auto wait_loop_end =
            breakdown_log ? std::chrono::steady_clock::now()
                          : std::chrono::steady_clock::time_point{};

        // As lease expired is a rare case, we check all the results with the
        // same time_point to avoid too many syscalls
        const auto lease_start = breakdown_log
                                     ? std::chrono::steady_clock::now()
                                     : std::chrono::steady_clock::time_point{};
        std::chrono::steady_clock::time_point now =
            std::chrono::steady_clock::now();
        for (size_t i = 0; i < object_keys.size(); ++i) {
            if (results[i].has_value() &&
                query_results[i].IsLeaseExpired(now)) {
                LOG(WARNING)
                    << "lease_expired_before_data_transfer_completed key="
                    << object_keys[i];
                results[i] = tl::unexpected(ErrorCode::LEASE_EXPIRED);
            }
        }
        const auto batch_end = breakdown_log
                                   ? std::chrono::steady_clock::now()
                                   : std::chrono::steady_clock::time_point{};

        const auto t1_batch_get = std::chrono::steady_clock::now();
        auto us_batch_get =
            std::chrono::duration_cast<std::chrono::microseconds>(t1_batch_get -
                                                                  t0_batch_get)
                .count();
        if (metrics_) {
            metrics_->transfer_metric.batch_get_latency_us.observe(
                us_batch_get);
        }

        if (nof_slow_trace_threshold_us > 0 &&
            us_batch_get >= nof_slow_trace_threshold_us) {
            size_t success_count = 0;
            for (const auto& result : results) {
                if (result.has_value()) {
                    ++success_count;
                }
            }
            const auto mono_ns = [](auto value) {
                return std::chrono::duration_cast<std::chrono::nanoseconds>(
                           value.time_since_epoch())
                    .count();
            };
            MC_LOG(INFO) << "batch_get_slow_trace trace_id=" << trace_id
                         << " keys=" << object_keys.size()
                         << " pending=" << pending_transfers.size()
                         << " start_mono_ns=" << mono_ns(t0_batch_get)
                         << " end_mono_ns=" << mono_ns(t1_batch_get)
                         << " total_us=" << us_batch_get
                         << " success=" << success_count
                         << " failed=" << (results.size() - success_count);
        }

        if (breakdown_log) {
            const int64_t setup_us = elapsed_us(batch_start, submit_start);
            const int64_t submit_us = elapsed_us(submit_start, submit_end);
            const int64_t wait_loop_us = elapsed_us(submit_end, wait_loop_end);
            const int64_t result_us = std::max<int64_t>(
                0, wait_loop_us - wait_blocked_us - checksum_us);
            const int64_t lease_us = elapsed_us(lease_start, batch_end);
            const int64_t total_us = elapsed_us(batch_start, batch_end);
            size_t success_count = 0;
            for (const auto& result : results) {
                if (result.has_value()) {
                    ++success_count;
                }
            }
            MC_LOG(INFO) << "batch_get_data_breakdown trace_id=" << trace_id
                         << " keys=" << object_keys.size()
                         << " pending=" << pending_transfers.size()
                         << " bytes=" << submitted_bytes
                         << " setup_us=" << setup_us
                         << " submit_us=" << submit_us
                         << " wait_blocked_us=" << wait_blocked_us
                         << " checksum_us=" << checksum_us
                         << " result_us=" << result_us
                         << " lease_us=" << lease_us << " total_us=" << total_us
                         << " success=" << success_count
                         << " failed=" << (results.size() - success_count);

        }

        // Log overall cache hit statistics for the entire batch
        if (hot_cache_) {
            VLOG(1) << "BatchGet completed: num_keys=" << object_keys.size()
                    << " total_cache_hits=" << total_cache_hits;
        } else {
            VLOG(1) << "BatchGet completed for " << object_keys.size()
                    << " keys";
        }
        pt_full.End(std::any_of(results.begin(), results.end(),
                                [](const auto& r) { return !r; })
                        ? -1
                        : 0);
        return results;
    }

    bool Client::RedirectToHotCache(const std::string& key,
                                    Replica::Descriptor& replica) {
        if (!replica.is_memory_replica() || !hot_cache_) {
            return false;
        }

        auto& mem_desc = replica.get_memory_descriptor();
        HotMemBlock* blk = hot_cache_->GetHotKey(key);
        if (blk == nullptr) {
            return false;
        }

        if (mem_desc.buffer_descriptor.size_ != blk->size) {
            LOG(ERROR) << "Cache hit but size mismatch for key: " << key;
            return false;
        }

        mem_desc.buffer_descriptor.transport_endpoint_ =
            (metadata_connstring_ == P2PHANDSHAKE) ? GetTransportEndpoint()
                                                   : local_hostname_;
        mem_desc.buffer_descriptor.buffer_address_ =
            reinterpret_cast<uintptr_t>(blk->addr);
        return true;
    }

    // 方案二被动层：向 master 上报传输失败副本的坏端点（多 submaster 下内存段
    // 全量挂载于所有 submaster，坏端点影响所有 master 的分配，需广播）。
    // best-effort：上报失败仅告警不阻塞主流程，误报由 worker 重挂载时
    // MountSegment 的 erase 自愈兜底。调用方需在延迟指标采集之后调用，
    // 避免上报 RPC 耗时污染延迟统计。
    void Client::ReportFailedEndpointsToMasters(
        const std::set<std::string>& failed_endpoints) {
        if (failed_endpoints.empty()) {
            return;
        }
        const std::vector<std::string> invalid_endpoints(
            failed_endpoints.begin(), failed_endpoints.end());
        std::vector<std::string> report_targets;
        {
            std::lock_guard<std::mutex> addr_lock(
                cvm_submaster_addresses_mutex_);
            report_targets = cvm_submaster_addresses_;
        }
        if (!report_targets.empty()) {
            for (const auto& addr : report_targets) {
                auto r = master_client_.ReportInvalidReplicaEndpointsTo(
                    addr, invalid_endpoints);
                if (!r) {
                    LOG(WARNING)
                        << "report_invalid_endpoints_failed addr=" << addr
                        << " count=" << invalid_endpoints.size()
                        << " error=" << r.error();
                }
            }
        } else {
            auto r =
                master_client_.ReportInvalidReplicaEndpoints(invalid_endpoints);
            if (!r) {
                LOG(WARNING)
                    << "report_invalid_endpoints_failed count="
                    << invalid_endpoints.size() << " error=" << r.error();
            }
        }
    }

    tl::expected<void, ErrorCode> Client::Put(const ObjectKey& key,
                                              std::vector<Slice>& slices,
                                              const ReplicateConfig& config) {
        const uint64_t trace_id = mooncake::logging::CurrentTraceId();
        const bool breakdown_log =
            ShouldSampleStoreTransferDiagnostics(trace_id);
        const auto breakdown_start =
            breakdown_log ? std::chrono::steady_clock::now()
                          : std::chrono::steady_clock::time_point{};
        auto checksum_end = breakdown_start;
        auto prepare_end = breakdown_start;
        auto put_start_end = breakdown_start;
        auto transfer_end = breakdown_start;
        auto put_end_end = breakdown_start;
        auto revoke_end = breakdown_start;

        SpDiag::PerfPoint pt_full(PerfKey::PUT_SINGLE_FULL,
                                  SpDiag::PerfLevel::KEY_MODULE);
        pt_full.Start();
        std::optional<uint64_t> object_checksum;
        if (object_checksum_enabled_) {
            auto checksum_result = ComputeObjectChecksumForSlices(
                key, slices, CalculateSliceSize(slices));
            if (!checksum_result) {
                pt_full.End(-1);
                return tl::unexpected(checksum_result.error());
            }
            object_checksum = *checksum_result;
        }
        if (breakdown_log) checksum_end = std::chrono::steady_clock::now();

        // Prepare slice lengths
        std::vector<size_t> slice_lengths;
        for (size_t i = 0; i < slices.size(); ++i) {
            slice_lengths.emplace_back(slices[i].size);
        }

        ReplicateConfig client_cfg = AttachHostId(config);
        AttachHcaAffinity(client_cfg, slices);
        if (protocol_ == "cxl") {
            client_cfg.preferred_segment = local_hostname_;
        }

        if (hot_cache_) {
            hot_cache_->RemoveHotKey(key);
        }
        if (breakdown_log) prepare_end = std::chrono::steady_clock::now();

        // Start put operation
        SpDiag::PerfPoint pt_start(PerfKey::PUT_SINGLE_PUT_START,
                                   SpDiag::PerfLevel::MODULE);
        pt_start.Start();
        auto start_result =
            master_client_.PutStart(key, slice_lengths, client_cfg);
        if (!start_result &&
            start_result.error() == ErrorCode::OBJECT_ALREADY_EXISTS &&
            healDanglingLocalDiskReplica(key) ==
                DiskReplicaHealResult::kEvicted) {
            start_result =
                master_client_.PutStart(key, slice_lengths, client_cfg);
        }
        pt_start.End(start_result ? 0 : -1);
        if (breakdown_log) put_start_end = std::chrono::steady_clock::now();
        if (!start_result) {
            ErrorCode err = start_result.error();
            if (err == ErrorCode::OBJECT_ALREADY_EXISTS) {
                VLOG(1) << "object_already_exists key=" << key;
                pt_full.End(0);
                return {};
            }
            if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
                LOG(WARNING) << "Failed to start put operation for key=" << key
                             << PUT_NO_SPACE_HELPER_STR;
            } else {
                LOG(ERROR) << "Failed to start put operation for key=" << key
                           << ": " << toString(err);
            }
            pt_full.End(-1);
            return tl::unexpected(err);
        }

        PutStartResult& put_start = start_result.value();
        ReplicaTransferSummary transfer_summary;
        for (const auto& replica : put_start.replicas) {
            transfer_summary.RecordAllocatedReplica(replica);
        }

        // Record Put transfer latency (all replicas)
        auto t0_put = std::chrono::steady_clock::now();

        // We must deal with disk replica first, then the disk putrevoke/putend
        // can be called surely
        if (storage_backend_) {
            for (auto it = put_start.replicas.rbegin();
                 it != put_start.replicas.rend(); ++it) {
                const auto& replica = *it;
                if (replica.is_disk_replica()) {
                    // Store to local file if storage backend is available
                    auto disk_descriptor = replica.get_disk_descriptor();
                    SpDiag::PerfPoint pt_disk(PerfKey::PUT_SINGLE_DISK_WRITE,
                                              SpDiag::PerfLevel::MODULE);
                    pt_disk.Start();
                    PutToLocalFile(key, slices, disk_descriptor);
                    pt_disk.End(0);
                    break;  // Only one disk replica is needed
                }
            }
        }

        std::set<std::string> failed_endpoints;
        const auto* put_layouts = put_start.vsegment_layouts.has_value()
                                      ? &*put_start.vsegment_layouts
                                      : nullptr;
        SpDiag::PerfPoint pt_transfer(PerfKey::PUT_SINGLE_TRANSFER_WRITE,
                                      SpDiag::PerfLevel::MODULE);
        pt_transfer.Start();
        for (const auto& [replica_type, transfer_err] :
             WriteMemoryAndNofReplicas(put_start.replicas, slices,
                                       &failed_endpoints, put_layouts)) {
            if (transfer_err != ErrorCode::OK) {
                transfer_summary.RecordFailure(replica_type, transfer_err);
                continue;
            }
            transfer_summary.RecordSuccess(replica_type);
        }

        if (transfer_summary.allocated_dfs_replicas > 0 &&
            NonDfsTransfersSucceeded(transfer_summary)) {
            std::vector<std::string> dfs_keys;
            std::vector<const std::vector<Slice>*> dfs_slices;
            std::vector<DistributedFSDescriptor> dfs_descriptors;
            for (const auto& replica : put_start.replicas) {
                if (!replica.is_dfs_replica()) {
                    continue;
                }
                dfs_keys.push_back(key);
                dfs_slices.push_back(&slices);
                dfs_descriptors.push_back(replica.get_dfs_descriptor());
            }
            for (auto dfs_result :
                 WriteDfsReplicas(dfs_keys, dfs_slices, dfs_descriptors)) {
                if (dfs_result == ErrorCode::OK) {
                    transfer_summary.RecordSuccess(ReplicaType::DFS);
                } else {
                    transfer_summary.RecordFailure(ReplicaType::DFS,
                                                   dfs_result);
                }
            }
        } else if (transfer_summary.allocated_dfs_replicas > 0) {
            // A DFS replica was allocated but is abandoned because a peer
            // replica failed first. Without this counter the write simply
            // disappears: no error is recorded against DFS and no I/O is
            // attempted. Counted per key, matching SubmitDfsWrites.
            VLOG(1) << "Skipping DFS write, action=skip, key=" << key
                    << ", reason=non_dfs_transfer_failed";
            if (auto* dfs_metric = GetDfsMetricPtr()) {
                dfs_metric->RecordSkippedWrites(1);
            }
        }
        pt_transfer.End(transfer_summary.first_error == ErrorCode::OK ? 0 : -1);

        auto us_put = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - t0_put)
                          .count();
        if (metrics_) {
            metrics_->transfer_metric.put_latency_us.observe(us_put);
        }
        if (breakdown_log) transfer_end = std::chrono::steady_clock::now();

        // 方案二被动层：上报坏端点（不改变写语义——FLEXIBLE 部分成功仍成功）。
        // 放在 put_latency 指标采集之后，避免上报 RPC 耗时污染写延迟统计。
        ReportFailedEndpointsToMasters(failed_endpoints);

        const auto finalize_decision =
            DetermineFinalizeDecision(client_cfg, transfer_summary);

        if (finalize_decision.end_type.has_value()) {
            SpDiag::PerfPoint pt_end(PerfKey::PUT_SINGLE_PUT_END,
                                     SpDiag::PerfLevel::MODULE);
            pt_end.Start();
            auto end_result = master_client_.PutEnd(
                ObjectMeta{key, object_checksum}, *finalize_decision.end_type,
                put_start.operation_id);
            pt_end.End(end_result ? 0 : -1);
            if (!end_result) {
                ErrorCode err = end_result.error();
                LOG(ERROR) << "Failed to end put operation: " << err;
                pt_full.End(-1);
                return tl::unexpected(err);
            }
        }
        if (breakdown_log) put_end_end = std::chrono::steady_clock::now();

        if (finalize_decision.revoke_type.has_value()) {
            SpDiag::PerfPoint pt_revoke(PerfKey::PUT_SINGLE_PUT_REVOKE,
                                        SpDiag::PerfLevel::MODULE);
            pt_revoke.Start();
            auto revoke_result = master_client_.PutRevoke(
                key, *finalize_decision.revoke_type, put_start.operation_id);
            pt_revoke.End(revoke_result ? 0 : -1);
            if (!revoke_result) {
                LOG(ERROR) << "Failed to revoke put operation";
                pt_full.End(-1);
                return tl::unexpected(revoke_result.error());
            }
        }
        if (breakdown_log) revoke_end = std::chrono::steady_clock::now();

        if (!finalize_decision.success) {
            pt_full.End(-1);
            return tl::unexpected(finalize_decision.error);
        }

        if (breakdown_log) {
            auto elapsed_us = [](auto begin, auto end) {
                return std::chrono::duration_cast<std::chrono::microseconds>(
                           end - begin)
                    .count();
            };
            MC_LOG(INFO)
                << "client_put_breakdown trace_id=" << trace_id
                << " key=" << key << " slices=" << slices.size()
                << " replicas=" << start_result.value().replicas.size()
                << " checksum_us=" << elapsed_us(breakdown_start, checksum_end)
                << " prepare_us=" << elapsed_us(checksum_end, prepare_end)
                << " put_start_us=" << elapsed_us(prepare_end, put_start_end)
                << " transfer_us=" << elapsed_us(put_start_end, transfer_end)
                << " put_end_us=" << elapsed_us(transfer_end, put_end_end)
                << " revoke_us=" << elapsed_us(put_end_end, revoke_end)
                << " total_us=" << elapsed_us(breakdown_start, revoke_end)
                << " status=ok";
        }

        pt_full.End(0);
        return {};
    }

    namespace {

    // Every COMPLETE replica in the list is a LOCAL_DISK replica owned by this
    // client, so nothing else can serve reads for the key.
    bool HasOnlyClientLocalDiskReplicas(
        const std::vector<Replica::Descriptor>& replicas,
        const UUID& client_id) {
        bool has_local_disk = false;
        for (const auto& replica : replicas) {
            if (replica.status != ReplicaStatus::COMPLETE) {
                continue;
            }
            if (replica.is_local_disk_replica() &&
                replica.get_local_disk_descriptor().client_id == client_id) {
                has_local_disk = true;
                continue;
            }
            // A completed replica owned elsewhere (memory, remote disk, DFS)
            // can still serve reads, so there is nothing for this client to
            // heal.
            return false;
        }
        return has_local_disk;
    }

    }  // namespace

    Client::DiskReplicaHealResult Client::healDanglingLocalDiskReplica(
        const ObjectKey& key) {
        auto replicas = master_client_.GetReplicaList(key);
        if (!replicas ||
            !HasOnlyClientLocalDiskReplicas(replicas->replicas, client_id_) ||
            !local_disk_probe_fn_) {
            return DiskReplicaHealResult::kNotDangling;
        }
        // A physical SSD wipe can leave master metadata naming a completed
        // entry whose backing file is gone (issue #3709). Reporting success for
        // a Put on top of it would store nothing. Ask the installed probe (the
        // offload file storage owned by RealClient) whether the file is still
        // there. Only a proven-gone answer evicts; present or unknown
        // (transport/config trouble) keeps the old idempotent path, so a
        // healthy replica can never be evicted by mistake.
        const std::optional<bool> file_gone = local_disk_probe_fn_(key);
        if (file_gone == false) {
            return DiskReplicaHealResult::kPresent;
        }
        if (file_gone != true) {
            return DiskReplicaHealResult::kUnknown;
        }
        auto evicted =
            master_client_.EvictDiskReplica(key, ReplicaType::LOCAL_DISK);
        if (!evicted) {
            LOG(WARNING)
                << "Failed to evict dangling LOCAL_DISK replica for key=" << key
                << ": " << toString(evicted.error());
            return DiskReplicaHealResult::kUnknown;
        }
        LOG(WARNING) << "Evicted dangling LOCAL_DISK replica for key=" << key
                     << " (backing file already gone)";
        return DiskReplicaHealResult::kEvicted;
    }

    tl::expected<void, ErrorCode> Client::Upsert(
        const ObjectKey& key, std::vector<Slice>& slices,
        const ReplicateConfig& config, bool enable_upsert_from_diagnostics) {
        const uint64_t trace_id = mooncake::logging::CurrentTraceId();
        const bool breakdown_log =
            enable_upsert_from_diagnostics &&
            mooncake::logging::ShouldSampleHiFreqLog(trace_id);
        const auto breakdown_start =
            breakdown_log ? std::chrono::steady_clock::now()
                          : std::chrono::steady_clock::time_point{};
        auto checksum_end = breakdown_start;
        auto prepare_end = breakdown_start;
        auto query_end = breakdown_start;
        auto transfer_end = breakdown_start;
        auto finalize_end = breakdown_start;
        auto cache_invalidate_end = breakdown_start;

        std::optional<uint64_t> object_checksum;
        if (object_checksum_enabled_) {
            auto checksum_result = ComputeObjectChecksumForSlices(
                key, slices, CalculateSliceSize(slices));
            if (!checksum_result) {
                return tl::unexpected(checksum_result.error());
            }
            object_checksum = *checksum_result;
        }
        if (breakdown_log) checksum_end = std::chrono::steady_clock::now();

        // Prepare slice lengths
        std::vector<size_t> slice_lengths;
        for (size_t i = 0; i < slices.size(); ++i) {
            slice_lengths.emplace_back(slices[i].size);
        }

        ReplicateConfig client_cfg = AttachHostId(config);
        AttachHcaAffinity(client_cfg, slices);
        if (protocol_ == "cxl") {
            client_cfg.preferred_segment = local_hostname_;
        }

        if (hot_cache_) {
            hot_cache_->RemoveHotKey(key);
        }
        if (breakdown_log) prepare_end = std::chrono::steady_clock::now();

        // Start upsert operation
        SpDiag::PerfPoint pt_query(PerfKey::UPSERT_FROM_INTERNAL_QUERY,
                                   SpDiag::PerfLevel::MODULE);
        if (enable_upsert_from_diagnostics) pt_query.Start();
        auto start_result =
            master_client_.UpsertStart(key, slice_lengths, client_cfg);
        if (enable_upsert_from_diagnostics) {
            pt_query.End(start_result ? 0 : -1);
        }
        if (breakdown_log) query_end = std::chrono::steady_clock::now();
        if (!start_result) {
            ErrorCode err = start_result.error();
            if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
                LOG(WARNING)
                    << "Failed to start upsert operation for key=" << key
                    << PUT_NO_SPACE_HELPER_STR;
            } else {
                LOG(ERROR) << "Failed to start upsert operation for key=" << key
                           << ": " << toString(err);
            }
            return tl::unexpected(err);
        }

        ReplicaTransferSummary transfer_summary;
        for (const auto& replica : start_result.value().replicas) {
            transfer_summary.RecordAllocatedReplica(replica);
        }

        // Record transfer latency
        PutStartResult& upsert_start = start_result.value();
        auto t0 = std::chrono::steady_clock::now();

        // Handle disk replicas first
        if (storage_backend_) {
            for (auto it = upsert_start.replicas.rbegin();
                 it != upsert_start.replicas.rend(); ++it) {
                const auto& replica = *it;
                if (replica.is_disk_replica()) {
                    auto disk_descriptor = replica.get_disk_descriptor();
                    PutToLocalFile(key, slices, disk_descriptor);
                    break;
                }
            }
        }

        std::set<std::string> failed_endpoints;
        const auto* upsert_layouts = upsert_start.vsegment_layouts.has_value()
                                         ? &*upsert_start.vsegment_layouts
                                         : nullptr;
        // Transfer to memory and NoF replicas first.
        for (const auto& [replica_type, transfer_err] :
             WriteMemoryAndNofReplicas(upsert_start.replicas, slices,
                                       &failed_endpoints, upsert_layouts)) {
            if (transfer_err != ErrorCode::OK) {
                transfer_summary.RecordFailure(replica_type, transfer_err);
                continue;
            }
            transfer_summary.RecordSuccess(replica_type);
        }

        if (transfer_summary.allocated_dfs_replicas > 0 &&
            NonDfsTransfersSucceeded(transfer_summary)) {
            std::vector<std::string> dfs_keys;
            std::vector<const std::vector<Slice>*> dfs_slices;
            std::vector<DistributedFSDescriptor> dfs_descriptors;
            for (const auto& replica : upsert_start.replicas) {
                if (!replica.is_dfs_replica()) {
                    continue;
                }
                dfs_keys.push_back(key);
                dfs_slices.push_back(&slices);
                dfs_descriptors.push_back(replica.get_dfs_descriptor());
            }
            for (auto dfs_result :
                 WriteDfsReplicas(dfs_keys, dfs_slices, dfs_descriptors)) {
                if (dfs_result == ErrorCode::OK) {
                    transfer_summary.RecordSuccess(ReplicaType::DFS);
                } else {
                    transfer_summary.RecordFailure(ReplicaType::DFS,
                                                   dfs_result);
                }
            }
        } else if (transfer_summary.allocated_dfs_replicas > 0) {
            // Same abandoned-write case as in Put: the DFS replica was
            // allocated but a peer replica failed first, so no I/O is attempted
            // and no DFS error is recorded. Counted per key, matching
            // SubmitDfsWrites.
            VLOG(1) << "Skipping DFS write, action=skip, key=" << key
                    << ", reason=non_dfs_transfer_failed";
            if (auto* dfs_metric = GetDfsMetricPtr()) {
                dfs_metric->RecordSkippedWrites(1);
            }
        }

        auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0)
                      .count();
        if (metrics_) {
            metrics_->transfer_metric.put_latency_us.observe(us);
        }
        if (breakdown_log) transfer_end = std::chrono::steady_clock::now();

        ReportFailedEndpointsToMasters(failed_endpoints);
        const auto finalize_decision =
            DetermineFinalizeDecision(client_cfg, transfer_summary);
        if (finalize_decision.end_type.has_value()) {
            auto end_result = master_client_.UpsertEnd(
                ObjectMeta{key, object_checksum}, *finalize_decision.end_type,
                upsert_start.operation_id);
            if (!end_result) {
                ErrorCode err = end_result.error();
                LOG(ERROR) << "Failed to end upsert operation: " << err;
                return tl::unexpected(err);
            }
        }
        if (finalize_decision.revoke_type.has_value()) {
            auto revoke_result = master_client_.UpsertRevoke(
                key, *finalize_decision.revoke_type, upsert_start.operation_id);
            if (!revoke_result) {
                LOG(ERROR) << "Failed to revoke upsert operation";
                return tl::unexpected(revoke_result.error());
            }
        }
        if (!finalize_decision.success) {
            return tl::unexpected(finalize_decision.error);
        }

        // Success-side invalidation: a concurrent read between the pre-upsert
        // RemoveHotKey() and UpsertEnd could have read the old value and
        // submitted an async hot-cache fill with a still-valid token.
        // Invalidate again now that the new value is committed so that stale
        // fill cannot publish.
        if (hot_cache_) {
            hot_cache_->RemoveHotKey(key);
        }
        if (breakdown_log) {
            cache_invalidate_end = std::chrono::steady_clock::now();
            auto elapsed_us = [](auto begin, auto end) {
                return std::chrono::duration_cast<std::chrono::microseconds>(
                           end - begin)
                    .count();
            };
            MC_LOG(INFO)
                << "upsert_from_breakdown trace_id=" << trace_id
                << " key=" << key << " slices=" << slices.size()
                << " replicas=" << upsert_start.replicas.size()
                << " checksum_us=" << elapsed_us(breakdown_start, checksum_end)
                << " prepare_us=" << elapsed_us(checksum_end, prepare_end)
                << " query_us=" << elapsed_us(prepare_end, query_end)
                << " transfer_us=" << elapsed_us(query_end, transfer_end)
                << " finalize_us=" << elapsed_us(transfer_end, finalize_end)
                << " cache_invalidate_us="
                << elapsed_us(finalize_end, cache_invalidate_end)
                << " total_us="
                << elapsed_us(breakdown_start, cache_invalidate_end)
                << " status=ok";
        }

        return {};
    }

    // TODO: `client.cpp` is too long, consider split it into multiple files
    enum class PutOperationState {
        PENDING,
        MASTER_FAILED,
        TRANSFER_FAILED,
        FINALIZE_FAILED,
        SUCCESS
    };

    class PutOperation {
       public:
        struct PendingTransferRecord {
            ReplicaType replica_type;
            bool is_vsegment;
            TransferFuture future;
            // 方案二被动层：传输失败归因用，目标 worker 的 transport endpoint。
            // memory/nof 副本单端点；vsegment 副本记录传输 plan 解析出的全部
            // 条带成员端点（best-effort：无法定位具体失败成员，误报由 worker
            // 重挂载时 MountSegment 的 erase 自愈兜底）。
            std::vector<std::string> transport_endpoints;

            PendingTransferRecord(ReplicaType type, bool vsegment,
                                  TransferFuture&& transfer_future,
                                  std::vector<std::string> endpoints = {})
                : replica_type(type),
                  is_vsegment(vsegment),
                  future(std::move(transfer_future)),
                  transport_endpoints(std::move(endpoints)) {}
        };

        PutOperation(std::string_view k, const std::vector<Slice>& s)
            : key(k), slices(s) {
            // Initialize with a pending error state to ensure result is always
            // set
            result = tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }

        std::string key;
        std::vector<Slice> slices;
        std::optional<uint64_t> object_checksum;
        std::vector<std::vector<Slice>> batched_slices;

        // Enhanced state tracking
        PutOperationState state = PutOperationState::PENDING;
        tl::expected<void, ErrorCode> result;
        std::vector<Replica::Descriptor> replicas;
        // 方案 C：批次响应带回的去重 vsegment layout 表；同批次的所有 op 共享
        // 同一个 shared_ptr（只在 StartBatchPut/StartBatchUpsert 里构造一次）。
        std::shared_ptr<const std::vector<VSegmentLayoutEntry>>
            vsegment_layouts;
        std::vector<PendingTransferRecord> pending_transfers;

        size_t requested_memory_replicas = 0;
        size_t requested_nof_replicas = 0;
        size_t requested_dfs_replicas = 0;
        ReplicaTransferSummary transfer_summary;

        // 方案二被动层：本操作传输失败副本的目标端点集合。memory/nof 按副本
        // Descriptor 端点归因；vsegment 副本 Descriptor 仅含逻辑位置，由
        // SubmitTransfers/WaitForTransfers 经 PendingTransferRecord.
        // transport_endpoints（传输 plan 全部条带成员端点）best-effort 归因。
        // 批次结束后由调用方聚合上报 master。
        std::set<std::string> failed_endpoints;

        // Error context for debugging
        std::optional<std::string> failure_context;

        // Helper methods for robust state management
        void SetSuccess() {
            state = PutOperationState::SUCCESS;
            result = {};
            failure_context.reset();
        }

        void SetTerminalError(ErrorCode error, PutOperationState terminal_state,
                              const std::string& context = "") {
            state = terminal_state;
            result = tl::unexpected(error);
            if (!context.empty()) {
                failure_context = context;
            }
        }

        void SetError(ErrorCode error, const std::string& context = "") {
            result = tl::unexpected(error);
            if (!context.empty()) {
                failure_context = toString(error) + ": " + context + "; " +
                                  failure_context.value_or("");
            }

            // Update state based on current processing stage
            if (replicas.empty()) {
                state = PutOperationState::MASTER_FAILED;
            } else if (pending_transfers.empty()) {
                state = PutOperationState::TRANSFER_FAILED;
            } else {
                state = PutOperationState::FINALIZE_FAILED;
            }
        }

        void AppendFailureContext(const std::string& context) {
            if (context.empty()) {
                return;
            }
            if (!failure_context.has_value()) {
                failure_context = context;
                return;
            }
            failure_context = *failure_context + "; " + context;
        }

        void InitializeRequestedReplicas(const ReplicateConfig& config) {
            requested_memory_replicas = config.replica_num;
            requested_nof_replicas = config.nof_replica_num;
            requested_dfs_replicas = config.dfs_replica_num;
        }

        ReplicateConfig ToReplicateConfig() const {
            ReplicateConfig config;
            config.replica_num = requested_memory_replicas;
            config.nof_replica_num = requested_nof_replicas;
            config.dfs_replica_num = requested_dfs_replicas;
            return config;
        }

        void RecordAllocatedReplicas() {
            transfer_summary = ReplicaTransferSummary{};
            for (const auto& replica : replicas) {
                transfer_summary.RecordAllocatedReplica(replica);
            }
        }

        bool IsResolved() const { return state != PutOperationState::PENDING; }

        bool IsSuccessful() const {
            return state == PutOperationState::SUCCESS && result.has_value();
        }
    };

    // 聚合一批操作的失败端点（方案二被动层）。独立于类外的自由函数：
    // Client::BatchUpsert 定义在 PutOperation 类之前，无法直接解引用
    // op.failed_endpoints（不完整类型），经由此前向声明的 helper 间接访问。
    void AggregateBatchFailedEndpoints(const std::vector<PutOperation>& ops,
                                       std::set<std::string>& out) {
        for (const auto& op : ops) {
            out.insert(op.failed_endpoints.begin(), op.failed_endpoints.end());
        }
    }

    std::vector<PutOperation> Client::CreatePutOperations(
        const std::vector<ObjectKey>& keys,
        const std::vector<std::vector<Slice>>& batched_slices) {
        SpDiag::PerfPoint pt_create(PerfKey::PUT_BATCH_CREATE_OPS,
                                    SpDiag::PerfLevel::MODULE);
        pt_create.Start();
        std::vector<PutOperation> ops;
        ops.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            ops.emplace_back(keys[i], batched_slices[i]);
        }
        pt_create.End(0);
        return ops;
    }

    void Client::ComputeBatchObjectChecksums(std::vector<PutOperation> & ops) {
        if (!object_checksum_enabled_) {
            return;
        }
        for (auto& op : ops) {
            auto checksum_result = ComputeObjectChecksumForSlices(
                op.key, op.slices, CalculateSliceSize(op.slices));
            if (!checksum_result) {
                op.SetTerminalError(checksum_result.error(),
                                    PutOperationState::MASTER_FAILED,
                                    "Object checksum calculation failed");
                continue;
            }
            op.object_checksum = *checksum_result;
        }
    }

    ReplicateConfig Client::AttachBatchHcaAffinity(
        const ReplicateConfig& config, const std::vector<PutOperation>& ops,
        const std::vector<size_t>& indices) const {
        if (!StoreHcaAffinityEnabled()) return config;
        auto request = config.ForKeySubset(indices);
        if (request.preferred_hca_names || request.per_key_preferred_hca_names)
            return request;
        if (auto infer = MakeHcaAffinity()) {
            std::vector<std::vector<std::string>> hints;
            bool has_hint = false;
            for (auto index : indices) {
                hints.push_back(infer(ops[index].slices));
                has_hint |= !hints.back().empty();
            }
            if (has_hint)
                request.per_key_preferred_hca_names = std::move(hints);
        }
        return request;
    }

    void Client::healDanglingLocalDiskBatchStarts(
        std::vector<PutOperation> & ops,
        const std::vector<size_t>& active_indices,
        const std::vector<size_t>& already_exists,
        const std::vector<std::string>& keys,
        const std::vector<std::vector<uint64_t>>& slice_lengths,
        const ReplicateConfig& config) {
        if (already_exists.empty() || !local_disk_probe_fn_) {
            return;
        }
        std::vector<std::string> candidate_keys;
        candidate_keys.reserve(already_exists.size());
        for (size_t i : already_exists) {
            candidate_keys.emplace_back(keys[i]);
        }
        // One round trip for the whole subset: a healthy idempotent batch pays
        // a single batched query and no evictions, and the per-key work left
        // over is a local filesystem probe.
        auto replica_lists = master_client_.BatchGetReplicaList(candidate_keys);
        if (replica_lists.size() != candidate_keys.size()) {
            return;
        }
        std::vector<std::string> evict_keys;
        std::vector<size_t> evict_positions;
        for (size_t j = 0; j < candidate_keys.size(); ++j) {
            if (!replica_lists[j] ||
                !HasOnlyClientLocalDiskReplicas(replica_lists[j]->replicas,
                                                client_id_)) {
                continue;
            }
            const std::optional<bool> file_gone =
                local_disk_probe_fn_(candidate_keys[j]);
            if (file_gone != true) {
                continue;
            }
            evict_keys.emplace_back(candidate_keys[j]);
            evict_positions.emplace_back(j);
        }
        if (evict_keys.empty()) {
            return;
        }
        auto evicted = master_client_.BatchEvictDiskReplica(
            evict_keys, ReplicaType::LOCAL_DISK);
        if (evicted.size() != evict_keys.size()) {
            return;
        }
        std::vector<std::string> retry_keys;
        std::vector<std::vector<uint64_t>> retry_slices;
        std::vector<size_t> retry_positions;
        std::vector<size_t> retry_config_indices;
        for (size_t j = 0; j < evict_keys.size(); ++j) {
            if (!evicted[j]) {
                LOG(WARNING)
                    << "Failed to evict dangling LOCAL_DISK replica for "
                    << "key=" << evict_keys[j] << ": "
                    << toString(evicted[j].error());
                continue;
            }
            LOG(WARNING) << "Evicted dangling LOCAL_DISK replica for key="
                         << evict_keys[j] << " (backing file already gone)";
            retry_positions.emplace_back(evict_positions[j]);
            retry_config_indices.push_back(already_exists[evict_positions[j]]);
            retry_keys.emplace_back(keys[already_exists[evict_positions[j]]]);
            retry_slices.emplace_back(
                slice_lengths[already_exists[evict_positions[j]]]);
        }
        if (retry_keys.empty()) {
            return;
        }
        const auto retry_config =
            config.group_ids || config.session_ids ||
                    config.residency_affinity_ids ||
                    config.per_key_preferred_hca_names
                ? std::make_optional(config.ForKeySubset(retry_config_indices))
                : std::nullopt;
        auto retry_responses = master_client_.BatchPutStart(
            retry_keys, retry_slices, retry_config ? *retry_config : config);
        if (retry_responses.size() != retry_keys.size()) {
            return;
        }
        std::vector<VSegmentLayoutEntry> retry_layout_entries;
        for (const auto& response : retry_responses) {
            if (response && response->vsegment_layouts.has_value()) {
                MergeVSegmentLayoutTable(retry_layout_entries,
                                         *response->vsegment_layouts);
            }
        }
        std::shared_ptr<const std::vector<VSegmentLayoutEntry>> retry_layouts;
        if (!retry_layout_entries.empty()) {
            retry_layouts = std::make_shared<const std::vector<VSegmentLayoutEntry>>(
                std::move(retry_layout_entries));
        }
        for (size_t r = 0; r < retry_keys.size(); ++r) {
            auto& op = ops[active_indices[already_exists[retry_positions[r]]]];
            if (!retry_responses[r]) {
                // OBJECT_ALREADY_EXISTS again means another writer won the
                // race; the idempotent skip applies. Any other error is
                // reported as is.
                if (retry_responses[r].error() !=
                    ErrorCode::OBJECT_ALREADY_EXISTS) {
                    op.SetTerminalError(retry_responses[r].error(),
                                        PutOperationState::MASTER_FAILED,
                                        "Master failed to start put operation");
                }
                continue;
            }
            op.replicas = std::move(retry_responses[r]->replicas);
            op.vsegment_layouts = retry_layouts;
            op.RecordAllocatedReplicas();
            if (!HasExpectedReplicaAllocation(config, op.transfer_summary)) {
                op.SetTerminalError(ErrorCode::NO_AVAILABLE_HANDLE,
                                    PutOperationState::MASTER_FAILED,
                                    "Allocated replicas do not satisfy "
                                    "requested replica policy");
            }
        }
    }

    void Client::StartBatchPut(std::vector<PutOperation> & ops,
                               const ReplicateConfig& base_config) {
        std::vector<std::string> keys;
        std::vector<std::vector<uint64_t>> slice_lengths;
        std::vector<size_t> active_indices;

        keys.reserve(ops.size());
        slice_lengths.reserve(ops.size());
        active_indices.reserve(ops.size());

        if (hot_cache_) {
            std::vector<std::string> hot_keys;
            hot_keys.reserve(ops.size());
            for (const auto& op : ops) hot_keys.emplace_back(op.key);
            hot_cache_->RemoveHotKeys(hot_keys);
        }

        for (size_t i = 0; i < ops.size(); ++i) {
            const auto& op = ops[i];
            if (op.IsResolved()) {
                continue;
            }
            active_indices.emplace_back(i);
            keys.emplace_back(op.key);

            std::vector<uint64_t> slice_sizes;
            slice_sizes.reserve(op.slices.size());
            for (const auto& slice : op.slices) {
                slice_sizes.emplace_back(slice.size);
            }
            slice_lengths.emplace_back(std::move(slice_sizes));
        }

        if (active_indices.empty()) {
            return;
        }

        std::optional<ReplicateConfig> active_config;
        if (StoreHcaAffinityEnabled()) {
            active_config =
                AttachBatchHcaAffinity(base_config, ops, active_indices);
        } else if (active_indices.size() != ops.size() &&
                   (base_config.group_ids || base_config.session_ids ||
                    base_config.residency_affinity_ids ||
                    base_config.per_key_preferred_hca_names)) {
            active_config = base_config.ForKeySubset(active_indices);
        }
        const auto& config = active_config ? *active_config : base_config;
        SpDiag::PerfPoint pt_batch_start(PerfKey::PUT_BATCH_PUT_START,
                                         SpDiag::PerfLevel::MODULE);
        pt_batch_start.Start();
        auto start_responses =
            master_client_.BatchPutStart(keys, slice_lengths, config);

        // Ensure response size matches request size
        if (start_responses.size() != active_indices.size()) {
            LOG(ERROR) << "BatchPutStart response size mismatch: expected "
                       << active_indices.size() << ", got "
                       << start_responses.size();
            for (size_t index : active_indices) {
                auto& op = ops[index];
                op.SetError(ErrorCode::RPC_FAIL,
                            "BatchPutStart response size mismatch");
            }
            pt_batch_start.End(-1);
            return;
        }

        const bool any_start_succeeded = std::any_of(
            start_responses.begin(), start_responses.end(),
            [](const auto& response) { return response.has_value(); });
        pt_batch_start.End(any_start_succeeded ? 0 : -1);

        std::vector<VSegmentLayoutEntry> merged_layouts;
        for (const auto& response : start_responses) {
            if (response && response->vsegment_layouts.has_value()) {
                MergeVSegmentLayoutTable(merged_layouts,
                                         *response->vsegment_layouts);
            }
        }
        std::shared_ptr<const std::vector<VSegmentLayoutEntry>> batch_layouts;
        if (!merged_layouts.empty()) {
            batch_layouts =
                std::make_shared<const std::vector<VSegmentLayoutEntry>>(
                    std::move(merged_layouts));
        }

        // Process individual responses with robust error handling
        std::vector<size_t> already_exists;
        for (size_t i = 0; i < active_indices.size(); ++i) {
            auto& op = ops[active_indices[i]];
            op.InitializeRequestedReplicas(config);
            if (!start_responses[i]) {
                if (start_responses[i].error() ==
                    ErrorCode::OBJECT_ALREADY_EXISTS) {
                    // Decided after the heal pass below: retried when the
                    // replica proves dangling, or the idempotent skip that
                    // CollectResults already grants.
                    already_exists.emplace_back(i);
                    continue;
                }
                op.SetTerminalError(start_responses[i].error(),
                                    PutOperationState::MASTER_FAILED,
                                    "Master failed to start put operation");
            } else {
                op.replicas = std::move(start_responses[i]->replicas);
                op.vsegment_layouts = batch_layouts;
                op.RecordAllocatedReplicas();
                if (!HasExpectedReplicaAllocation(config,
                                                  op.transfer_summary)) {
                    op.SetTerminalError(ErrorCode::NO_AVAILABLE_HANDLE,
                                        PutOperationState::MASTER_FAILED,
                                        "Allocated replicas do not satisfy "
                                        "requested replica policy");
                    continue;
                }
                // Operation continues to next stage - result remains
                // INTERNAL_ERROR until fully successful
                VLOG(1) << "Successfully started put for key " << op.key
                        << " with " << op.replicas.size() << " replicas";
            }
        }

        healDanglingLocalDiskBatchStarts(ops, active_indices, already_exists,
                                         keys, slice_lengths, config);
        for (size_t i : already_exists) {
            auto& op = ops[active_indices[i]];
            if (!op.IsResolved() && op.replicas.empty()) {
                op.SetTerminalError(ErrorCode::OBJECT_ALREADY_EXISTS,
                                    PutOperationState::MASTER_FAILED,
                                    "Master failed to start put operation");
            }
        }
    }

    void Client::StartBatchUpsert(std::vector<PutOperation> & ops,
                                  const ReplicateConfig& base_config) {
        std::vector<std::string> keys;
        std::vector<std::vector<uint64_t>> slice_lengths;
        std::vector<size_t> active_indices;

        keys.reserve(ops.size());
        slice_lengths.reserve(ops.size());
        active_indices.reserve(ops.size());

        if (hot_cache_) {
            std::vector<std::string> hot_keys;
            hot_keys.reserve(ops.size());
            for (const auto& op : ops) hot_keys.emplace_back(op.key);
            hot_cache_->RemoveHotKeys(hot_keys);
        }

        for (size_t i = 0; i < ops.size(); ++i) {
            const auto& op = ops[i];
            if (op.IsResolved()) {
                continue;
            }
            active_indices.emplace_back(i);
            keys.emplace_back(op.key);

            std::vector<uint64_t> slice_sizes;
            slice_sizes.reserve(op.slices.size());
            for (const auto& slice : op.slices) {
                slice_sizes.emplace_back(slice.size);
            }
            slice_lengths.emplace_back(std::move(slice_sizes));
        }

        if (active_indices.empty()) {
            return;
        }

        std::optional<ReplicateConfig> active_config;
        if (StoreHcaAffinityEnabled()) {
            active_config =
                AttachBatchHcaAffinity(base_config, ops, active_indices);
        } else if (active_indices.size() != ops.size() &&
                   (base_config.group_ids || base_config.session_ids ||
                    base_config.residency_affinity_ids ||
                    base_config.per_key_preferred_hca_names)) {
            active_config = base_config.ForKeySubset(active_indices);
        }
        const auto& config = active_config ? *active_config : base_config;
        auto start_responses =
            master_client_.BatchUpsertStart(keys, slice_lengths, config);

        // Ensure response size matches request size
        if (start_responses.size() != active_indices.size()) {
            LOG(ERROR) << "BatchUpsertStart response size mismatch: expected "
                       << active_indices.size() << ", got "
                       << start_responses.size();
            for (size_t index : active_indices) {
                auto& op = ops[index];
                op.SetError(ErrorCode::RPC_FAIL,
                            "BatchUpsertStart response size mismatch");
            }
            return;
        }

        std::vector<VSegmentLayoutEntry> merged_layouts;
        for (const auto& response : start_responses) {
            if (response && response->vsegment_layouts.has_value()) {
                MergeVSegmentLayoutTable(merged_layouts,
                                         *response->vsegment_layouts);
            }
        }
        std::shared_ptr<const std::vector<VSegmentLayoutEntry>> batch_layouts;
        if (!merged_layouts.empty()) {
            batch_layouts =
                std::make_shared<const std::vector<VSegmentLayoutEntry>>(
                    std::move(merged_layouts));
        }

        // Process individual responses with robust error handling
        for (size_t i = 0; i < active_indices.size(); ++i) {
            auto& op = ops[active_indices[i]];
            op.InitializeRequestedReplicas(config);
            if (!start_responses[i]) {
                op.SetTerminalError(start_responses[i].error(),
                                    PutOperationState::MASTER_FAILED,
                                    "Master failed to start upsert operation");
            } else {
                op.replicas = std::move(start_responses[i]->replicas);
                op.vsegment_layouts = batch_layouts;
                op.RecordAllocatedReplicas();
                if (!HasExpectedReplicaAllocation(config,
                                                  op.transfer_summary)) {
                    op.SetTerminalError(ErrorCode::NO_AVAILABLE_HANDLE,
                                        PutOperationState::MASTER_FAILED,
                                        "Allocated replicas do not satisfy "
                                        "requested replica policy");
                    continue;
                }
                VLOG(1) << "Successfully started upsert for key " << op.key
                        << " with " << op.replicas.size() << " replicas";
            }
        }
    }

    void Client::SubmitTransfers(std::vector<PutOperation> & ops,
                                 bool group_by_target) {
        SpDiag::PerfPoint pt_submit_batch(PerfKey::PUT_BATCH_SUBMIT,
                                          SpDiag::PerfLevel::MODULE);
        pt_submit_batch.Start();
        if (std::all_of(ops.begin(), ops.end(), [](const PutOperation& op) {
                return op.IsResolved();
            })) {
            pt_submit_batch.End(0);
            return;
        }
        if (!transfer_submitter_) {
            LOG(ERROR) << "TransferSubmitter not initialized";
            for (auto& op : ops) {
                if (op.IsResolved()) {
                    continue;
                }
                op.SetTerminalError(ErrorCode::INVALID_PARAMS,
                                    PutOperationState::TRANSFER_FAILED,
                                    "TransferSubmitter not initialized");
            }
            pt_submit_batch.End(-1);
            return;
        }

        struct TargetWriteGroup {
            std::vector<Replica::Descriptor> replicas;
            std::vector<std::vector<Slice>> slices;
            std::vector<PutOperation*> ops;
        };
        std::unordered_map<std::string, TargetWriteGroup> target_groups;

        for (auto& op : ops) {
            // Skip operations that already failed in previous stages
            if (op.IsResolved()) {
                continue;
            }

            // Skip operations that don't have replicas (failed in
            // StartBatchPut)
            if (op.replicas.empty()) {
                op.SetTerminalError(ErrorCode::INTERNAL_ERROR,
                                    PutOperationState::MASTER_FAILED,
                                    "No replicas available for transfer");
                continue;
            }

            // We must deal with disk replica first, then the disk
            // putrevoke/putend can be called surely
            if (storage_backend_) {
                for (auto it = op.replicas.rbegin(); it != op.replicas.rend();
                     ++it) {
                    const auto& replica = *it;
                    if (replica.is_disk_replica()) {
                        auto disk_descriptor = replica.get_disk_descriptor();
                        SpDiag::PerfPoint pt_disk(PerfKey::PUT_BATCH_DISK_WRITE,
                                                  SpDiag::PerfLevel::MODULE);
                        pt_disk.Start();
                        PutToLocalFile(op.key, op.slices, disk_descriptor);
                        pt_disk.End(0);
                        break;  // Only one disk replica is needed
                    }
                }
            }

            for (size_t replica_idx = 0; replica_idx < op.replicas.size();
                 ++replica_idx) {
                const auto& replica = op.replicas[replica_idx];
                if (group_by_target && replica.is_memory_replica() &&
                    CalculateSliceSize(op.slices) ==
                        replica.get_memory_descriptor()
                            .buffer_descriptor.size_) {
                    // A size mismatch would fail the whole group in
                    // submit_batch, so such a replica stays on the per-key path
                    // and fails alone.
                    auto& group = target_groups[replica.get_memory_descriptor()
                                                    .buffer_descriptor
                                                    .transport_endpoint_];
                    group.replicas.push_back(replica);
                    group.slices.push_back(op.slices);
                    group.ops.push_back(&op);
                    continue;
                }
                if (replica.is_memory_replica() || replica.is_nof_replica() ||
                    replica.is_vsegment_replica()) {
                    const auto replica_type =
                        replica.is_memory_replica()     ? ReplicaType::MEMORY
                        : replica.is_vsegment_replica() ? ReplicaType::MEMORY
                                                        : ReplicaType::NOF_SSD;
                    // 方案二被动层：提交时记录目标端点，供失败归因使用
                    std::vector<std::string> transport_endpoints;
                    if (replica.is_memory_replica()) {
                        transport_endpoints.push_back(
                            replica.get_memory_descriptor()
                                .buffer_descriptor.transport_endpoint_);
                    } else if (replica.is_nof_replica()) {
                        transport_endpoints.push_back(
                            replica.get_nof_descriptor()
                                .buffer_descriptor.transport_endpoint_);
                    }
                    std::optional<TransferFuture> submit_result;
                    if (replica.is_vsegment_replica()) {
                        if (!vsegment_transfer_planner_) {
                            op.transfer_summary.RecordFailure(
                                replica_type, ErrorCode::INVALID_PARAMS);
                            op.AppendFailureContext(
                                "VSegmentTransferPlanner not initialized");
                            continue;
                        }
                        std::vector<vsegment::ClientSlice> client_slices;
                        client_slices.reserve(op.slices.size());
                        for (const auto& slice : op.slices) {
                            client_slices.push_back(
                                {reinterpret_cast<uint64_t>(slice.ptr),
                                 slice.size});
                        }
                        SpDiag::PerfPoint pt_plan(PerfKey::VSEGMENT_WRITE_PLAN,
                                                  SpDiag::PerfLevel::MODULE);
                        pt_plan.Start();
                        auto plan = vsegment_transfer_planner_->Plan(
                            replica.get_vsegment_descriptor(), client_slices,
                            op.vsegment_layouts.get());
                        pt_plan.End(plan ? 0 : -1);
                        if (!plan) {
                            op.transfer_summary.RecordFailure(replica_type,
                                                              plan.error);
                            op.AppendFailureContext(plan.detail);
                            continue;
                        }
                        // vsegment 条带成员物理端点，失败时 best-effort 归因
                        for (const auto& subrequest : plan.requests) {
                            transport_endpoints.push_back(subrequest.endpoint);
                        }
                        SpDiag::PerfPoint pt_vsegment_submit(
                            PerfKey::VSEGMENT_WRITE_SUBMIT,
                            SpDiag::PerfLevel::MODULE);
                        pt_vsegment_submit.Start();
                        submit_result = transfer_submitter_->submitVSegment(
                            plan, TransferRequest::WRITE);
                        pt_vsegment_submit.End(submit_result ? 0 : -1);
                    } else if (replica.is_nof_replica()) {
                        auto contiguous_range =
                            GetContiguousSliceRange(op.slices);
                        if (!contiguous_range.has_value()) {
                            std::string failure_context =
                                "NoF transfer requires contiguous slices for "
                                "replica " +
                                std::to_string(replica_idx);
                            op.transfer_summary.RecordFailure(
                                replica_type, ErrorCode::INVALID_PARAMS);
                            op.AppendFailureContext(failure_context);
                            continue;
                        }
                        submit_result = transfer_submitter_->submit(
                            replica, op.slices, TransferRequest::WRITE,
                            contiguous_range->ptr, contiguous_range->size,
                            TransferIntent::kUnspecified);
                    } else {
                        submit_result = transfer_submitter_->submit(
                            replica, op.slices, TransferRequest::WRITE, nullptr,
                            0, TransferIntent::kUnspecified);
                    }

                    if (!submit_result) {
                        std::string failure_context =
                            "Failed to submit transfer for replica " +
                            std::to_string(replica_idx);
                        op.transfer_summary.RecordFailure(
                            replica_type, ErrorCode::TRANSFER_FAIL);
                        op.AppendFailureContext(failure_context);
                        for (const auto& endpoint : transport_endpoints) {
                            op.failed_endpoints.insert(endpoint);
                        }
                        continue;
                    }

                    op.pending_transfers.emplace_back(
                        replica_type, replica.is_vsegment_replica(),
                        std::move(submit_result.value()),
                        std::move(transport_endpoints));
                }
            }

            VLOG(1) << "Submitted " << op.pending_transfers.size()
                    << " transfers for key " << op.key;
        }

        for (auto& [endpoint, group] : target_groups) {
            auto submit_result = transfer_submitter_->submit_batch(
                group.replicas, group.slices, TransferRequest::WRITE,
                TransferIntent::kUnspecified);
            for (auto* op : group.ops) {
                if (!submit_result) {
                    op->transfer_summary.RecordFailure(
                        ReplicaType::MEMORY, ErrorCode::TRANSFER_FAIL);
                    op->AppendFailureContext(
                        "Failed to submit batch transfer to " + endpoint);
                    continue;
                }
                op->pending_transfers.emplace_back(
                    ReplicaType::MEMORY, false, submit_result->share(),
                    std::vector<std::string>{endpoint});
            }
            VLOG(1) << "Submitted batch write of " << group.ops.size()
                    << " replicas to " << endpoint;
        }
        const bool any_error =
            std::any_of(ops.begin(), ops.end(), [](const PutOperation& op) {
                return op.transfer_summary.first_error != ErrorCode::OK;
            });
        pt_submit_batch.End(any_error ? -1 : 0);
    }

    void Client::WaitForTransfers(std::vector<PutOperation> & ops) {
        SpDiag::PerfPoint pt_wait_batch(PerfKey::PUT_BATCH_WAIT,
                                        SpDiag::PerfLevel::MODULE);
        pt_wait_batch.Start();
        for (auto& op : ops) {
            // Skip operations that already failed or completed
            if (op.IsResolved()) {
                continue;
            }

            for (size_t i = 0; i < op.pending_transfers.size(); ++i) {
                auto& pending_transfer = op.pending_transfers[i];
                SpDiag::PerfPoint pt_vsegment_wait(PerfKey::VSEGMENT_WRITE_WAIT,
                                                   SpDiag::PerfLevel::MODULE);
                if (pending_transfer.is_vsegment) pt_vsegment_wait.Start();
                ErrorCode transfer_result = pending_transfer.future.get();
                if (pending_transfer.is_vsegment) {
                    pt_vsegment_wait.End(transfer_result == ErrorCode::OK ? 0
                                                                          : -1);
                }
                if (transfer_result != ErrorCode::OK) {
                    op.transfer_summary.RecordFailure(
                        pending_transfer.replica_type, transfer_result);
                    // 方案二被动层：异步传输失败时按提交时记录的端点归因
                    // （vsegment 为 plan
                    // 解析出的全部条带成员端点，best-effort）
                    for (const auto& endpoint :
                         pending_transfer.transport_endpoints) {
                        op.failed_endpoints.insert(endpoint);
                    }
                    std::string error_context =
                        "Transfer " + std::to_string(i) + " failed";
                    op.AppendFailureContext(error_context);
                } else {
                    op.transfer_summary.RecordSuccess(
                        pending_transfer.replica_type);
                }
            }

            VLOG(1) << "Transfers finished for key " << op.key
                    << ", success(mem="
                    << op.transfer_summary.successful_memory_transfers
                    << ", nof=" << op.transfer_summary.successful_nof_transfers
                    << "), fail(mem="
                    << op.transfer_summary.failed_memory_transfers
                    << ", nof=" << op.transfer_summary.failed_nof_transfers
                    << ")";
        }
        const bool any_error =
            std::any_of(ops.begin(), ops.end(), [](const PutOperation& op) {
                return op.transfer_summary.first_error != ErrorCode::OK;
            });
        pt_wait_batch.End(any_error ? -1 : 0);
    }

    std::vector<ErrorCode> Client::WriteDfsReplicas(
        const std::vector<std::string>& keys,
        const std::vector<const std::vector<Slice>*>& slice_lists,
        const std::vector<DistributedFSDescriptor>& descriptors) {
        if (keys.size() != slice_lists.size() ||
            keys.size() != descriptors.size()) {
            return std::vector<ErrorCode>(keys.size(),
                                          ErrorCode::INVALID_PARAMS);
        }
        if (keys.empty()) {
            return {};
        }
        if (!dfs_storage_backend_) {
            LOG(ERROR) << "DFS backend is unavailable for synchronous write";
            if (auto* dfs_metric = GetDfsMetricPtr()) {
                dfs_metric->RecordWriteErrors(
                    toString(ErrorCode::DFS_SERVICE_UNAVAILABLE),
                    static_cast<int64_t>(keys.size()));
            }
            return std::vector<ErrorCode>(keys.size(),
                                          ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }

        auto* dfs_metric = GetDfsMetricPtr();
        std::vector<ErrorCode> results(keys.size(), ErrorCode::OK);
        std::vector<DfsWriteRequest> requests;
        std::vector<size_t> request_indices;
        // Bytes per request, so the success accounting after BatchWrite does
        // not have to walk the slice lists a second time.
        std::vector<uint64_t> request_bytes;
        std::vector<PinnedBufferPool::Buffer> staging_buffers;
        requests.reserve(keys.size());
        request_indices.reserve(keys.size());
        request_bytes.reserve(keys.size());

        // Staging is the device-to-host copy below; it is timed separately from
        // the BatchWrite call so a slow put can be attributed to the right side
        // of the boundary between them.
        const auto staging_start = std::chrono::steady_clock::now();
        bool staging_performed = false;
        auto runtime_accelerator =
            device::GetAcceleratorRegistry().RuntimeAccelerators();
        for (size_t i = 0; i < keys.size(); ++i) {
            if (slice_lists[i] == nullptr) {
                results[i] = ErrorCode::INVALID_PARAMS;
                if (dfs_metric) {
                    dfs_metric->RecordWriteErrors(
                        toString(ErrorCode::INVALID_PARAMS));
                }
                continue;
            }

            std::vector<Slice> host_slices;
            host_slices.reserve(slice_lists[i]->size());
            uint64_t key_bytes = 0;
            bool staging_succeeded = true;
            for (const auto& slice : *slice_lists[i]) {
                device::PointerInfo info{};
                auto* device = slice.ptr == nullptr
                                   ? nullptr
                                   : runtime_accelerator.FindDeviceForPointer(
                                         slice.ptr, &info);
                if (device == nullptr) {
                    key_bytes += slice.size;
                    host_slices.push_back(slice);
                    continue;
                }

                staging_performed = true;
                device->SetContext(info.device_id);
                auto buffer = pinned_buffer_pool_->Acquire(slice.size);
                if (!device->Copy(buffer.data, slice.ptr, slice.size,
                                  device::CopyDirection::kDeviceToHost)) {
                    LOG(ERROR) << "DFS D2H staging failed, action=stage, key="
                               << keys[i] << ", size=" << slice.size
                               << ", device_id=" << info.device_id;
                    pinned_buffer_pool_->Release(std::move(buffer));
                    results[i] = ErrorCode::TRANSFER_FAIL;
                    staging_succeeded = false;
                    break;
                }
                key_bytes += slice.size;
                host_slices.emplace_back(Slice{buffer.data, slice.size});
                staging_buffers.push_back(std::move(buffer));
            }
            if (!staging_succeeded) {
                // Counted as a DFS write failure: the key was destined for DFS
                // and never got there, even though the backend was never
                // reached.
                if (dfs_metric) {
                    dfs_metric->RecordWriteErrors(
                        toString(ErrorCode::TRANSFER_FAIL));
                }
                continue;
            }

            requests.push_back(DfsWriteRequest{keys[i], descriptors[i],
                                               std::move(host_slices)});
            request_indices.push_back(i);
            request_bytes.push_back(key_bytes);
        }
        if (dfs_metric && staging_performed) {
            dfs_metric->ObserveWriteStaging(elapsed_us_since(staging_start));
        }

        const auto write_start = std::chrono::steady_clock::now();
        auto write_results = dfs_storage_backend_->BatchWrite(requests);
        const auto write_latency_us = elapsed_us_since(write_start);
        int64_t success_keys = 0;
        uint64_t success_bytes = 0;
        if (write_results.size() != requests.size()) {
            LOG(ERROR) << "DFS BatchWrite response size mismatch: expected "
                       << requests.size() << ", got " << write_results.size();
            for (size_t index : request_indices) {
                results[index] = ErrorCode::INTERNAL_ERROR;
            }
            if (dfs_metric && !request_indices.empty()) {
                dfs_metric->RecordWriteErrors(
                    toString(ErrorCode::INTERNAL_ERROR),
                    static_cast<int64_t>(request_indices.size()));
            }
        } else {
            for (size_t i = 0; i < write_results.size(); ++i) {
                results[request_indices[i]] =
                    write_results[i] ? ErrorCode::OK : write_results[i].error();
                if (write_results[i]) {
                    ++success_keys;
                    success_bytes += request_bytes[i];
                } else if (dfs_metric) {
                    dfs_metric->RecordWriteErrors(
                        toString(write_results[i].error()));
                }
            }
        }
        // Latency is per batch, so it is only observed when the batch delivered
        // something; a fully failed batch would otherwise pollute the histogram
        // with error-path timings.
        if (dfs_metric && success_keys > 0) {
            dfs_metric->ObserveWrite(success_keys,
                                     static_cast<int64_t>(success_bytes),
                                     write_latency_us);
        }
        VLOG(1) << "DFS batch write finished, action=write, requested="
                << requests.size() << ", succeeded=" << success_keys
                << ", bytes=" << success_bytes
                << ", latency_us=" << write_latency_us;

        for (auto& buffer : staging_buffers) {
            pinned_buffer_pool_->Release(std::move(buffer));
        }
        return results;
    }

    void Client::SubmitDfsWrites(std::vector<PutOperation> & ops) {
        std::vector<std::string> keys;
        std::vector<const std::vector<Slice>*> slice_lists;
        std::vector<DistributedFSDescriptor> descriptors;
        std::vector<size_t> op_indices;
        auto* dfs_metric = GetDfsMetricPtr();
        int64_t skipped_writes = 0;

        for (size_t i = 0; i < ops.size(); ++i) {
            auto& op = ops[i];
            if (op.IsResolved() ||
                op.transfer_summary.allocated_dfs_replicas == 0) {
                continue;
            }
            // A DFS replica was allocated but is abandoned because a peer
            // replica failed first. Without this counter the write simply
            // disappears: no error is recorded against DFS and no I/O is
            // attempted.
            if (!NonDfsTransfersSucceeded(op.transfer_summary)) {
                ++skipped_writes;
                VLOG(1) << "Skipping DFS write, action=skip, key=" << op.key
                        << ", reason=non_dfs_transfer_failed";
                continue;
            }

            auto dfs_it = std::find_if(op.replicas.begin(), op.replicas.end(),
                                       [](const Replica::Descriptor& replica) {
                                           return replica.is_dfs_replica();
                                       });
            if (dfs_it == op.replicas.end()) {
                op.transfer_summary.RecordFailure(ReplicaType::DFS,
                                                  ErrorCode::INVALID_REPLICA);
                op.AppendFailureContext(
                    "Allocated DFS replica has no descriptor");
                if (dfs_metric) {
                    dfs_metric->RecordWriteErrors(
                        toString(ErrorCode::INVALID_REPLICA));
                }
                continue;
            }
            keys.push_back(op.key);
            slice_lists.push_back(&op.slices);
            descriptors.push_back(dfs_it->get_dfs_descriptor());
            op_indices.push_back(i);
        }
        if (dfs_metric && skipped_writes > 0) {
            dfs_metric->RecordSkippedWrites(skipped_writes);
        }

        auto results = WriteDfsReplicas(keys, slice_lists, descriptors);
        for (size_t i = 0; i < results.size(); ++i) {
            auto& op = ops[op_indices[i]];
            if (results[i] == ErrorCode::OK) {
                op.transfer_summary.RecordSuccess(ReplicaType::DFS);
            } else {
                op.transfer_summary.RecordFailure(ReplicaType::DFS, results[i]);
                op.AppendFailureContext("Synchronous DFS write failed: " +
                                        toString(results[i]));
            }
        }
    }

    void Client::FinalizeBatchPut(std::vector<PutOperation> & ops) {
        struct BatchFinalizeGroup {
            std::vector<std::string> keys;
            std::vector<size_t> indices;
        };

        BatchFinalizeGroup end_all_group;
        BatchFinalizeGroup end_memory_group;
        BatchFinalizeGroup end_nof_group;
        BatchFinalizeGroup revoke_all_group;
        BatchFinalizeGroup revoke_memory_group;
        BatchFinalizeGroup revoke_nof_group;

        std::vector<size_t> pending_finalize_actions(ops.size(), 0);
        std::vector<bool> should_succeed(ops.size(), false);
        std::vector<ErrorCode> terminal_errors(ops.size(), ErrorCode::OK);
        std::vector<std::optional<ErrorCode>> finalize_rpc_errors(ops.size());

        auto add_group_entry = [](BatchFinalizeGroup& group,
                                  const std::string& key, size_t index) {
            group.keys.emplace_back(key);
            group.indices.emplace_back(index);
        };

        auto add_finalize_action =
            [&](const std::optional<ReplicaType>& replica_type, bool is_end,
                const std::string& key, size_t index) {
                if (!replica_type.has_value()) {
                    return;
                }
                switch (*replica_type) {
                    case ReplicaType::ALL:
                        add_group_entry(
                            is_end ? end_all_group : revoke_all_group, key,
                            index);
                        ++pending_finalize_actions[index];
                        break;
                    case ReplicaType::MEMORY:
                        add_group_entry(
                            is_end ? end_memory_group : revoke_memory_group,
                            key, index);
                        ++pending_finalize_actions[index];
                        break;
                    case ReplicaType::NOF_SSD:
                        add_group_entry(
                            is_end ? end_nof_group : revoke_nof_group, key,
                            index);
                        ++pending_finalize_actions[index];
                        break;
                    default:
                        LOG(ERROR)
                            << "Unexpected replica type in batch finalize: "
                            << *replica_type;
                        finalize_rpc_errors[index] = ErrorCode::INVALID_PARAMS;
                        break;
                }
            };

        auto complete_finalize_action = [&](size_t index) {
            if (pending_finalize_actions[index] > 0) {
                --pending_finalize_actions[index];
            }
        };

        for (size_t i = 0; i < ops.size(); ++i) {
            auto& op = ops[i];
            if (op.IsResolved()) {
                if (!op.IsSuccessful() && !op.replicas.empty()) {
                    terminal_errors[i] = op.result.has_value()
                                             ? ErrorCode::INTERNAL_ERROR
                                             : op.result.error();
                    add_finalize_action(ReplicaType::ALL, false, op.key, i);
                }
                continue;
            }
            if (op.replicas.empty()) {
                op.SetTerminalError(ErrorCode::INTERNAL_ERROR,
                                    PutOperationState::MASTER_FAILED,
                                    "Operation has no replicas to finalize");
                continue;
            }

            const auto finalize_decision = DetermineFinalizeDecision(
                op.ToReplicateConfig(), op.transfer_summary);
            should_succeed[i] = finalize_decision.success;
            terminal_errors[i] = finalize_decision.error;
            add_finalize_action(finalize_decision.end_type, true, op.key, i);
            add_finalize_action(finalize_decision.revoke_type, false, op.key,
                                i);
        }

        auto process_end_group = [&](BatchFinalizeGroup& group,
                                     ReplicaType replica_type) {
            if (group.keys.empty()) {
                return;
            }
            std::vector<ObjectMeta> object_metas;
            object_metas.reserve(group.indices.size());
            for (size_t i = 0; i < group.indices.size(); ++i) {
                object_metas.emplace_back(ObjectMeta{
                    group.keys[i], ops[group.indices[i]].object_checksum});
            }
            auto responses =
                master_client_.BatchPutEnd(object_metas, replica_type);
            if (responses.size() != group.keys.size()) {
                for (size_t idx : group.indices) {
                    finalize_rpc_errors[idx] = ErrorCode::RPC_FAIL;
                    complete_finalize_action(idx);
                }
                return;
            }
            for (size_t i = 0; i < responses.size(); ++i) {
                const size_t op_idx = group.indices[i];
                if (!responses[i]) {
                    finalize_rpc_errors[op_idx] = responses[i].error();
                    LOG(ERROR) << "Failed to BatchPutEnd key " << group.keys[i]
                               << ": " << toString(responses[i].error());
                    complete_finalize_action(op_idx);
                    continue;
                }
                complete_finalize_action(op_idx);
            }
        };

        auto process_revoke_group = [&](BatchFinalizeGroup& group,
                                        ReplicaType replica_type) {
            if (group.keys.empty()) {
                return;
            }
            auto responses =
                master_client_.BatchPutRevoke(group.keys, replica_type);
            if (responses.size() != group.keys.size()) {
                for (size_t idx : group.indices) {
                    finalize_rpc_errors[idx] = ErrorCode::RPC_FAIL;
                    complete_finalize_action(idx);
                }
                return;
            }
            for (size_t i = 0; i < responses.size(); ++i) {
                const size_t op_idx = group.indices[i];
                if (!responses[i]) {
                    finalize_rpc_errors[op_idx] = responses[i].error();
                    LOG(ERROR)
                        << "Failed to BatchPutRevoke key " << group.keys[i]
                        << ": " << toString(responses[i].error());
                    complete_finalize_action(op_idx);
                    continue;
                }
                complete_finalize_action(op_idx);
            }
        };

        SpDiag::PerfPoint pt_end(PerfKey::PUT_BATCH_PUT_END,
                                 SpDiag::PerfLevel::MODULE);
        pt_end.Start();
        process_end_group(end_all_group, ReplicaType::ALL);
        process_end_group(end_memory_group, ReplicaType::MEMORY);
        process_end_group(end_nof_group, ReplicaType::NOF_SSD);
        const bool end_error =
            std::any_of(finalize_rpc_errors.begin(), finalize_rpc_errors.end(),
                        [](const auto& error) { return error.has_value(); });
        pt_end.End(end_error ? -1 : 0);

        const size_t errors_before_revoke = static_cast<size_t>(std::count_if(
            finalize_rpc_errors.begin(), finalize_rpc_errors.end(),
            [](const auto& error) { return error.has_value(); }));
        SpDiag::PerfPoint pt_revoke(PerfKey::PUT_BATCH_PUT_REVOKE,
                                    SpDiag::PerfLevel::MODULE);
        pt_revoke.Start();
        process_revoke_group(revoke_all_group, ReplicaType::ALL);
        process_revoke_group(revoke_memory_group, ReplicaType::MEMORY);
        process_revoke_group(revoke_nof_group, ReplicaType::NOF_SSD);
        const size_t errors_after_revoke = static_cast<size_t>(std::count_if(
            finalize_rpc_errors.begin(), finalize_rpc_errors.end(),
            [](const auto& error) { return error.has_value(); }));
        pt_revoke.End(errors_after_revoke > errors_before_revoke ? -1 : 0);

        auto append_finalize_error_context = [&](PutOperation& op,
                                                 size_t index) {
            if (finalize_rpc_errors[index].has_value()) {
                op.AppendFailureContext("Batch finalization RPC failed: " +
                                        toString(*finalize_rpc_errors[index]));
            }
            if (pending_finalize_actions[index] != 0) {
                op.AppendFailureContext(
                    "Operation has unfinished finalize actions");
            }
        };

        for (size_t i = 0; i < ops.size(); ++i) {
            auto& op = ops[i];
            if (op.IsResolved()) {
                if (!op.IsSuccessful()) {
                    append_finalize_error_context(op, i);
                }
                continue;
            }
            if (finalize_rpc_errors[i].has_value() ||
                pending_finalize_actions[i] != 0) {
                if (!should_succeed[i] && terminal_errors[i] != ErrorCode::OK) {
                    append_finalize_error_context(op, i);
                    op.SetTerminalError(
                        terminal_errors[i], PutOperationState::TRANSFER_FAILED,
                        op.failure_context.value_or(
                            "Replica transfer failed before finalize"));
                } else if (finalize_rpc_errors[i].has_value()) {
                    op.SetTerminalError(*finalize_rpc_errors[i],
                                        PutOperationState::FINALIZE_FAILED,
                                        "Batch finalization RPC failed");
                } else {
                    op.SetTerminalError(
                        ErrorCode::INTERNAL_ERROR,
                        PutOperationState::FINALIZE_FAILED,
                        "Operation has unfinished finalize actions");
                }
                continue;
            }
            if (should_succeed[i]) {
                op.SetSuccess();
                continue;
            }
            op.SetTerminalError(terminal_errors[i],
                                PutOperationState::TRANSFER_FAILED,
                                op.failure_context.value_or(
                                    "Replica transfer failed before finalize"));
        }
    }

    void Client::FinalizeBatchUpsert(std::vector<PutOperation> & ops) {
        std::vector<ObjectMeta> successful_object_metas;
        std::vector<size_t> successful_indices;
        std::vector<std::string> failed_keys;
        std::vector<size_t> failed_indices;

        successful_object_metas.reserve(ops.size());
        successful_indices.reserve(ops.size());
        failed_keys.reserve(ops.size());
        failed_indices.reserve(ops.size());

        for (size_t i = 0; i < ops.size(); ++i) {
            auto& op = ops[i];
            if (!op.IsResolved() && !op.replicas.empty() &&
                HasExpectedReplicaAllocation(op.ToReplicateConfig(),
                                             op.transfer_summary) &&
                AllAllocatedTransfersSucceeded(op.transfer_summary)) {
                successful_object_metas.emplace_back(
                    ObjectMeta{op.key, op.object_checksum});
                successful_indices.emplace_back(i);
                continue;
            }

            if (!op.IsResolved() && !op.replicas.empty()) {
                const auto error =
                    op.transfer_summary.first_error == ErrorCode::OK
                        ? ErrorCode::TRANSFER_FAIL
                        : op.transfer_summary.first_error;
                op.SetTerminalError(
                    error, PutOperationState::TRANSFER_FAILED,
                    op.failure_context.value_or(
                        "Replica transfer failed before upsert finalize"));
            }
            if (!op.IsSuccessful() && !op.replicas.empty()) {
                failed_keys.emplace_back(op.key);
                failed_indices.emplace_back(i);
            }
        }

        // Process successful operations
        std::vector<std::string> finalized_keys;
        if (!successful_object_metas.empty()) {
            finalized_keys.reserve(successful_object_metas.size());
            auto end_responses =
                master_client_.BatchUpsertEnd(successful_object_metas);
            if (end_responses.size() != successful_object_metas.size()) {
                LOG(ERROR) << "BatchUpsertEnd response size mismatch: expected "
                           << successful_object_metas.size() << ", got "
                           << end_responses.size();
                for (size_t idx : successful_indices) {
                    ops[idx].SetError(ErrorCode::RPC_FAIL,
                                      "BatchUpsertEnd response size mismatch");
                }
            } else {
                for (size_t i = 0; i < end_responses.size(); ++i) {
                    const size_t op_idx = successful_indices[i];
                    if (!end_responses[i]) {
                        LOG(ERROR) << "Failed to finalize upsert for key "
                                   << successful_object_metas[i].key << ": "
                                   << toString(end_responses[i].error());
                        ops[op_idx].SetError(end_responses[i].error(),
                                             "BatchUpsertEnd failed");
                    } else {
                        ops[op_idx].SetSuccess();
                        finalized_keys.emplace_back(
                            successful_object_metas[i].key);
                        VLOG(1) << "Successfully completed upsert for key "
                                << successful_object_metas[i].key;
                    }
                }
            }
        }

        // Success-side invalidation for finalized upserts only: a concurrent
        // read between StartBatchUpsert()'s pre-invalidation and BatchUpsertEnd
        // could have read the old value and submitted an async hot-cache fill
        // with a still-valid token. Invalidate again now that the new values
        // are committed so those stale fills cannot publish.
        if (hot_cache_ && !finalized_keys.empty()) {
            hot_cache_->RemoveHotKeys(finalized_keys);
        }

        // Process failed operations that need cleanup
        if (!failed_keys.empty()) {
            auto revoke_responses =
                master_client_.BatchUpsertRevoke(failed_keys);
            if (revoke_responses.size() != failed_keys.size()) {
                LOG(ERROR)
                    << "BatchUpsertRevoke response size mismatch: expected "
                    << failed_keys.size() << ", got "
                    << revoke_responses.size();
                for (size_t idx : failed_indices) {
                    ops[idx].SetError(
                        ErrorCode::RPC_FAIL,
                        "BatchUpsertRevoke response size mismatch");
                }
            } else {
                for (size_t i = 0; i < revoke_responses.size(); ++i) {
                    const size_t op_idx = failed_indices[i];
                    if (!revoke_responses[i]) {
                        LOG(ERROR) << "Failed to revoke upsert for key "
                                   << failed_keys[i] << ": "
                                   << toString(revoke_responses[i].error());
                        std::string original_context =
                            ops[op_idx].failure_context.value_or(
                                "unknown error");
                        ops[op_idx].failure_context =
                            original_context + "; revoke also failed";
                    } else {
                        LOG(INFO)
                            << "Successfully revoked failed upsert for key "
                            << failed_keys[i];
                    }
                }
            }
        }

        // Ensure all operations have definitive results
        for (auto& op : ops) {
            if (!op.IsResolved()) {
                op.SetError(ErrorCode::INTERNAL_ERROR,
                            "Operation not resolved after finalization");
                LOG(ERROR) << "Operation for key " << op.key
                           << " was not properly resolved";
            }
        }
    }

    std::vector<tl::expected<void, ErrorCode>> Client::CollectResults(
        const std::vector<PutOperation>& ops) {
        SpDiag::PerfPoint pt_collect(PerfKey::PUT_BATCH_COLLECT_RESULTS,
                                     SpDiag::PerfLevel::MODULE);
        pt_collect.Start();
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(ops.size());

        int no_available_handle_count = 0;
        size_t succeeded_count = 0;
        size_t failed_count = 0;
        for (const auto& op : ops) {
            // With the new structure, result is always set (never nullopt)
            results.emplace_back(op.result);

            // Additional validation and logging for debugging
            if (!op.result.has_value()) {
                // If error == object already exist, consider as ok (Put
                // semantics). UpsertStart never returns this error, so this
                // branch is unreachable for BatchUpsert — kept for BatchPut
                // compatibility.
                if (op.result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
                    results.back() = {};
                    ++succeeded_count;
                    continue;
                }
                if (op.result.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                    no_available_handle_count++;
                    ++failed_count;
                } else {
                    ++failed_count;
                    LOG(ERROR) << "Operation for key " << op.key
                               << " failed: " << toString(op.result.error())
                               << (op.failure_context
                                       ? (" (" + *op.failure_context + ")")
                                       : "");
                }
            } else {
                ++succeeded_count;
                VLOG(1) << "Operation for key " << op.key
                        << " completed successfully";
            }
        }
        if (no_available_handle_count > 0) {
            LOG(WARNING) << "BatchPut failed for " << no_available_handle_count
                         << " keys" << PUT_NO_SPACE_HELPER_STR;
        }
        // 批次级写入恢复转变（成员 last_batch_put_failed_ 注释）：含失败的批次
        // 只置位；此前曾失败后首个全成功批打一条 INFO——与上方逐 key ERROR
        // 构成「失败风暴 → 恢复」的明确收口观测，稳态成功批次静默。
        if (failed_count > 0) {
            last_batch_put_failed_.store(true, std::memory_order_relaxed);
        } else if (succeeded_count > 0 &&
                   last_batch_put_failed_.exchange(false,
                                                   std::memory_order_relaxed)) {
            LOG(INFO) << "Batch put recovered: " << succeeded_count
                      << " keys succeeded after previous failures"
                      << " (client writes are flowing again)";
        }

        pt_collect.End(failed_count == 0 ? 0 : -1);
        return results;
    }

    void Client::StageWriteBuffersForRemoteReplicas(
        std::vector<PutOperation> & ops, const WriteBufferStager& stager) {
        if (!stager) return;

        for (auto& op : ops) {
            if (op.IsResolved() || op.replicas.empty() ||
                std::all_of(op.replicas.begin(), op.replicas.end(),
                            [this](const Replica::Descriptor& replica) {
                                return CanUseLocalMemcpy(replica);
                            })) {
                continue;
            }
            auto staged_slices = stager(op.slices);
            if (!staged_slices) {
                op.SetError(staged_slices.error(),
                            "Failed to stage non-local write buffers");
                continue;
            }
            op.slices = std::move(*staged_slices);
            VLOG(1) << "Staged write buffers for non-local replica, key="
                    << op.key;
        }
    }

    std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const ReplicateConfig& config) {
        return BatchPut(keys, batched_slices, config, {});
    }

    std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const ReplicateConfig& config, const WriteBufferStager& stager,
        bool group_by_target) {
        if (!config.ValidSessionIds(keys.size()) ||
            !config.HasValidKeyHints(keys.size())) {
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        const uint64_t trace_id = mooncake::logging::CurrentTraceId();
        const bool breakdown_log =
            ShouldSampleStoreTransferDiagnostics(trace_id);
        const auto breakdown_start =
            breakdown_log ? std::chrono::steady_clock::now()
                          : std::chrono::steady_clock::time_point{};
        auto prepare_end = breakdown_start;
        auto put_start_end = breakdown_start;
        auto transfer_end = breakdown_start;
        auto put_end_end = breakdown_start;

        SpDiag::PerfPoint pt_full(PerfKey::PUT_BATCH_FULL,
                                  SpDiag::PerfLevel::KEY_MODULE);
        pt_full.Start();
        ReplicateConfig client_cfg = AttachHostId(config);
        if (protocol_ == "cxl") {
            client_cfg.preferred_segment = local_hostname_;
        }
        std::vector<PutOperation> ops =
            CreatePutOperations(keys, batched_slices);
        ComputeBatchObjectChecksums(ops);
        if (breakdown_log) prepare_end = std::chrono::steady_clock::now();
        if (client_cfg.prefer_alloc_in_same_node) {
            if (auto err = ValidatePreferSameNodeWriteConfig(client_cfg)) {
                pt_full.End(-1);
                return std::vector<tl::expected<void, ErrorCode>>(
                    keys.size(), tl::unexpected(*err));
            }
        }
        StartBatchPut(ops, client_cfg);
        StageWriteBuffersForRemoteReplicas(ops, stager);
        if (breakdown_log) put_start_end = std::chrono::steady_clock::now();

            auto t0 = std::chrono::steady_clock::now();
            SubmitTransfers(
                ops, group_by_target || client_cfg.prefer_alloc_in_same_node);
            WaitForTransfers(ops);
            SubmitDfsWrites(ops);
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - t0)
                          .count();
            if (metrics_) {
                metrics_->transfer_metric.batch_put_latency_us.observe(us);
            }
            if (breakdown_log) transfer_end = std::chrono::steady_clock::now();

            FinalizeBatchPut(ops);
            if (breakdown_log) put_end_end = std::chrono::steady_clock::now();
            auto results = CollectResults(ops);
            const bool any_succeeded = std::any_of(
                results.begin(), results.end(),
                [](const auto& result) { return result.has_value(); });
            if (breakdown_log) {
                size_t total_bytes = 0;
                size_t success_count = 0;
                for (const auto& slices : batched_slices) {
                    for (const auto& slice : slices) total_bytes += slice.size;
                }
                for (const auto& result : results) {
                    if (result.has_value()) ++success_count;
                }
                const char* status = success_count == results.size() ? "ok"
                                     : success_count == 0            ? "err"
                                                          : "partial";
                auto elapsed_us = [](auto begin, auto end) {
                    return std::chrono::duration_cast<
                               std::chrono::microseconds>(end - begin)
                        .count();
                };
                MC_LOG(INFO)
                    << "client_batch_put_breakdown trace_id=" << trace_id
                    << " keys=" << keys.size() << " bytes=" << total_bytes
                    << " prepare_us="
                    << elapsed_us(breakdown_start, prepare_end)
                    << " put_start_us="
                    << elapsed_us(prepare_end, put_start_end) << " transfer_us="
                    << elapsed_us(put_start_end, transfer_end)
                    << " put_end_us=" << elapsed_us(transfer_end, put_end_end)
                    << " total_us=" << elapsed_us(breakdown_start, put_end_end)
                    << " success=" << success_count << " status=" << status;
            }
            pt_full.End(any_succeeded ? 0 : -1);
            // 方案二被动层：批次末尾统一上报失败端点（置于 pt_full.End 之后，
            // 避免上报 RPC 耗时污染 PUT_BATCH_FULL 指标）
            std::set<std::string> batch_failed_endpoints;
            for (const auto& op : ops) {
                batch_failed_endpoints.insert(op.failed_endpoints.begin(),
                                              op.failed_endpoints.end());
            }
            ReportFailedEndpointsToMasters(batch_failed_endpoints);
            return results;
        }

        // Defined after PutOperation: std::vector<PutOperation> must be
        // instantiated on a complete type (clang-based compilers diagnose this;
        // GCC defers it).
        std::vector<tl::expected<void, ErrorCode>> Client::BatchUpsert(
            const std::vector<ObjectKey>& keys,
            std::vector<std::vector<Slice>>& batched_slices,
            const ReplicateConfig& config) {
            return BatchUpsert(keys, batched_slices, config, {});
        }

        std::vector<tl::expected<void, ErrorCode>> Client::BatchUpsert(
            const std::vector<ObjectKey>& keys,
            std::vector<std::vector<Slice>>& batched_slices,
            const ReplicateConfig& config, const WriteBufferStager& stager,
            bool group_by_target) {
            if (!config.ValidSessionIds(keys.size()) ||
                !config.HasValidKeyHints(keys.size())) {
                return std::vector<tl::expected<void, ErrorCode>>(
                    keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
            }
            ReplicateConfig client_cfg = AttachHostId(config);
            if (protocol_ == "cxl") {
                client_cfg.preferred_segment = local_hostname_;
            }
            std::vector<PutOperation> ops =
                CreatePutOperations(keys, batched_slices);
            ComputeBatchObjectChecksums(ops);
            if (client_cfg.prefer_alloc_in_same_node) {
                if (auto err = ValidatePreferSameNodeWriteConfig(client_cfg)) {
                    return std::vector<tl::expected<void, ErrorCode>>(
                        keys.size(), tl::unexpected(*err));
                }
            }

            StartBatchUpsert(ops, client_cfg);
            StageWriteBuffersForRemoteReplicas(ops, stager);
            auto t0 = std::chrono::steady_clock::now();
            SubmitTransfers(
                ops, group_by_target || client_cfg.prefer_alloc_in_same_node);
            WaitForTransfers(ops);
            SubmitDfsWrites(ops);
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - t0)
                          .count();
            if (metrics_) {
                metrics_->transfer_metric.batch_put_latency_us.observe(us);
            }

            FinalizeBatchUpsert(ops);
            return CollectResults(ops);
        }

        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        Client::StartBatchPutForSizes(const std::vector<std::string>& keys,
                                      const std::vector<uint64_t>& object_sizes,
                                      const ReplicateConfig& config) {
            std::vector<
                tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
                results(keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
            if (!config.ValidSessionIds(keys.size()) ||
                !config.HasValidKeyHints(keys.size())) {
                return results;
            }
            if (keys.size() != object_sizes.size()) {
                LOG(ERROR) << "StartBatchPutForSizes size mismatch: keys="
                           << keys.size() << ", sizes=" << object_sizes.size();
                return results;
            }

            ReplicateConfig client_cfg = AttachHostId(config);
            if (protocol_ == "cxl") {
                client_cfg.preferred_segment = local_hostname_;
            }

            std::vector<std::vector<Slice>> batched_slices(keys.size());
            for (size_t i = 0; i < keys.size(); ++i) {
                batched_slices[i] = {Slice{nullptr, object_sizes[i]}};
            }
            std::vector<PutOperation> ops =
                CreatePutOperations(keys, batched_slices);
            StartBatchPut(ops, client_cfg);

            for (size_t i = 0; i < ops.size(); ++i) {
                if (ops[i].IsResolved()) {
                    results[i] = tl::unexpected(ops[i].result.error());
                    continue;
                }
                results[i] = std::move(ops[i].replicas);
            }
            return results;
        }

        std::vector<tl::expected<void, ErrorCode>> Client::BatchPutEnd(
            const std::vector<ObjectMeta>& object_metas,
            ReplicaType replica_type) {
            return master_client_.BatchPutEnd(object_metas, replica_type);
        }

        std::vector<tl::expected<void, ErrorCode>> Client::BatchPutRevoke(
            const std::vector<std::string>& keys, ReplicaType replica_type) {
            return master_client_.BatchPutRevoke(keys, replica_type);
        }

        tl::expected<void, ErrorCode> Client::Remove(const ObjectKey& key,
                                                     bool force) {
            if (dfs_storage_backend_ && dfs_storage_backend_->UsesKvcs()) {
                auto results = BatchRemove({key}, force);
                if (results.size() != 1)
                    return tl::unexpected(ErrorCode::INTERNAL_ERROR);
                return std::move(results.front());
            }

            if (hot_cache_) {
                hot_cache_->BumpKeyGeneration(key);
            }
            auto result = master_client_.Remove(key, force);
            // if (storage_backend_) {
            //     storage_backend_->RemoveFile(key);
            // }
            if (!result) {
                return tl::unexpected(result.error());
            }
            if (hot_cache_) {
                hot_cache_->RemoveHotKey(key);
            }
            return {};
        }

        tl::expected<long, ErrorCode> Client::RemoveByRegex(
            const ObjectKey& str, bool force) {
            // KVCS has no provider-side key listing or conditional bulk delete
            // yet. Removing only Master metadata would skip provider cleanup.
            if (dfs_storage_backend_ && dfs_storage_backend_->UsesKvcs())
                return tl::unexpected(ErrorCode::NOT_SUPPORTED);
            if (hot_cache_) {
                hot_cache_->BumpCacheEpoch();
            }

            auto result = master_client_.RemoveByRegex(str, force);
            // if (storage_backend_) {
            //     storage_backend_->RemoveByRegex(str);
            // }
            if (!result) {
                return tl::unexpected(result.error());
            }
            if (result.value() > 0 && hot_cache_) {
                hot_cache_->BumpCacheEpoch();
                hot_cache_->RemoveHotKeysByRegex(str);
            }
            return result.value();
        }

        tl::expected<long, ErrorCode> Client::RemoveAll(bool force) {
            if (dfs_storage_backend_ && dfs_storage_backend_->UsesKvcs())
                return tl::unexpected(ErrorCode::NOT_SUPPORTED);
            if (hot_cache_) {
                hot_cache_->BumpCacheEpoch();
            }

            auto result = master_client_.RemoveAll(force);
            // The master can retain protected objects. Local disk cleanup must
            // use per-object eviction, not an unconditional wipe of this
            // client's files.
            if (result && result.value() > 0 && hot_cache_) {
                hot_cache_->RemoveAllHotKeys();
            }
            return result;
        }

        std::vector<tl::expected<void, ErrorCode>> Client::BatchRemove(
            const std::vector<ObjectKey>& keys, bool force) {
            if (hot_cache_) {
                hot_cache_->BumpKeyGenerations(keys);
            }

            auto results = master_client_.BatchRemove(keys, force);

            const size_t result_count = std::min(keys.size(), results.size());
            std::vector<std::string> removed_keys;
            std::vector<size_t> removed_indices;
            removed_keys.reserve(result_count);
            removed_indices.reserve(result_count);
            for (size_t i = 0; i < result_count; ++i) {
                if (results[i]) {
                    removed_keys.emplace_back(keys[i]);
                    removed_indices.push_back(i);
                }
            }

            if (dfs_storage_backend_ && dfs_storage_backend_->UsesKvcs() &&
                !removed_keys.empty()) {
                const auto deleted =
                    dfs_storage_backend_->BatchDeleteObjects(removed_keys);
                if (deleted.size() != removed_keys.size()) {
                    for (size_t index : removed_indices) {
                        results[index] =
                            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                    }
                } else {
                    for (size_t i = 0; i < deleted.size(); ++i) {
                        if (!deleted[i])
                            results[removed_indices[i]] = deleted[i];
                    }
                }
            }

            if (hot_cache_ && !removed_keys.empty())
                hot_cache_->RemoveHotKeys(removed_keys);

            return results;
        }

        tl::expected<void, ErrorCode> Client::EvictDiskReplica(
            const std::string& key, ReplicaType replica_type) {
            return master_client_.EvictDiskReplica(key, replica_type);
        }

        tl::expected<void, ErrorCode> Client::EvictDiskReplica(
            const std::string& key, const std::string& tenant_id,
            ReplicaType replica_type) {
            return master_client_.EvictDiskReplica(key, tenant_id,
                                                   replica_type);
        }

        tl::expected<bool, ErrorCode> Client::EvictDiskReplicaIfCurrent(
            const std::string& key, const std::string& tenant_id,
            ReplicaType replica_type, ReplicaID expected_replica_id) {
            return master_client_.EvictDiskReplicaIfCurrent(
                key, tenant_id, replica_type, expected_replica_id);
        }

        std::vector<tl::expected<void, ErrorCode>>
        Client::BatchEvictDiskReplica(const std::vector<std::string>& keys,
                                      ReplicaType replica_type) {
            return master_client_.BatchEvictDiskReplica(keys, replica_type);
        }

        std::vector<tl::expected<void, ErrorCode>>
        Client::BatchEvictDiskReplica(const std::vector<std::string>& keys,
                                      const std::string& tenant_id,
                                      ReplicaType replica_type) {
            return master_client_.BatchEvictDiskReplica(keys, tenant_id,
                                                        replica_type);
        }

        std::vector<int> Client::GetNicNumaNodes() const {
            std::set<int> nodes;
            if (!transfer_engine_) return {};
            auto topo = transfer_engine_->getLocalTopology();
            if (!topo) return {};
            for (auto& [name, entry] : topo->getMatrix()) {
                if (name.rfind("cpu:", 0) != 0 || entry.preferred_hca.empty())
                    continue;
                int node = std::stoi(name.substr(4));
                nodes.insert(node);
            }
            return {nodes.begin(), nodes.end()};
        }

        tl::expected<void, ErrorCode> Client::warmup(
            const std::shared_ptr<ClientBufferAllocator>& allocator) {
            if (!transfer_engine_) {
                return tl::unexpected(ErrorCode::INTERNAL_ERROR);
            }
            if (!allocator) {
                LOG(WARNING)
                    << "warmup: no buffer allocator provided, skipping";
                return {};
            }

            // Allocate a buffer from the client's buffer allocator.  The
            // allocator's underlying memory is already registered with the
            // transfer engine during setup_internal, so it can be used directly
            // as the source (WRITE) and destination (READ) for warmup
            // transfers.  BufferHandle is RAII: it deallocates the buffer back
            // to the allocator on destruction, so no explicit
            // register/unregister is needed here.
            constexpr size_t kWarmupBufSize = 4096;
            auto buf_handle = allocator->allocate(kWarmupBufSize);
            if (!buf_handle) {
                LOG(WARNING) << "warmup: failed to allocate warmup buffer";
                return tl::unexpected(ErrorCode::INTERNAL_ERROR);
            }
            std::memset(buf_handle->ptr(), 0, buf_handle->size());

            // Fetch all segment names registered with the master.
            auto segments_result = master_client_.GetAllSegments();
            if (!segments_result) {
                LOG(WARNING) << "warmup: GetAllSegments failed: "
                             << toString(segments_result.error());
                return tl::unexpected(segments_result.error());
            }

            const auto& segments = segments_result.value();
            LOG(INFO) << "warmup: pre-establishing connections to "
                      << segments.size() << " segment(s) for protocol '"
                      << protocol_ << "'";

            // Helper lambda: submit a single transfer request and poll to
            // completion.
            auto submit_and_wait =
                [&](Transport::TransferRequest::OpCode op, SegmentID target_id,
                    const std::string& segment_name) -> bool {
                auto batch_id = transfer_engine_->allocateBatchID(1);
                if (batch_id == INVALID_BATCH_ID) {
                    LOG(WARNING) << "warmup: allocateBatchID failed for '"
                                 << segment_name << "'";
                    return false;
                }
                auto target_meta_data = transfer_engine_->getMetadata();
                auto target_segment =
                    target_meta_data->getSegmentDescByID(target_id);
                Transport::TransferRequest request;
                request.opcode = op;
                request.source =
                    buf_handle->ptr();  // registered buffer carries r/w data
                request.target_id = target_id;
                request.target_offset = target_segment->buffers[0].addr;
                request.length = kWarmupBufSize;

                auto status =
                    transfer_engine_->submitTransfer(batch_id, {request});
                if (!status.ok()) {
                    LOG(WARNING)
                        << "warmup: submitTransfer("
                        << (op == Transport::TransferRequest::WRITE ? "W" : "R")
                        << ") failed for '" << segment_name
                        << "': " << status.message();
                    transfer_engine_->freeBatchID(batch_id);
                    return false;
                }

                // Poll for completion — the connection handshake occurs here.
                Transport::TransferStatus ts;
                for (int poll = 0; poll < 1000; ++poll) {
                    auto s =
                        transfer_engine_->getTransferStatus(batch_id, 0, ts);
                    if (!s.ok()) break;
                    if (ts.s == Transport::COMPLETED ||
                        ts.s == Transport::FAILED || ts.s == Transport::INVALID)
                        break;
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }

                transfer_engine_->freeBatchID(batch_id);
                return ts.s == Transport::COMPLETED;
            };

            size_t success_count = 0;
            for (const auto& segment_name : segments) {
                // Open the segment to resolve its SegmentID (caches the
                // descriptor).
                auto target_id = transfer_engine_->openSegment(segment_name);
                if (target_id == (SegmentHandle)-1) {
                    LOG(WARNING) << "warmup: cannot open segment '"
                                 << segment_name << "'";
                    continue;
                }
                // Skip the local segment — no point self-connecting, and a
                // WRITE would corrupt our own buffer.
                if (target_id == LOCAL_SEGMENT_ID) {
                    continue;
                }

                // Issue a 1-byte WRITE then a 1-byte READ using the registered
                // buffer to exercise both directions of the connection.
                bool ok = submit_and_wait(Transport::TransferRequest::WRITE,
                                          target_id, segment_name);
                ok = submit_and_wait(Transport::TransferRequest::READ,
                                     target_id, segment_name) &&
                     ok;
                if (ok) ++success_count;
            }

            LOG(INFO) << "warmup: completed, " << success_count << "/"
                      << segments.size() << " segments processed";
            return {};
        }

        std::vector<int> Client::GetNumaNodes() const {
            if (!transfer_engine_) return {};
            auto topo = transfer_engine_->getLocalTopology();
            if (!topo) return {};

            std::set<int> nodes;
            for (auto& [name, entry] : topo->getMatrix()) {
                (void)entry;
                if (name.rfind("cpu:", 0) != 0) continue;
                try {
                    nodes.insert(std::stoi(name.substr(4)));
                } catch (const std::exception& e) {
                    LOG(WARNING) << "Ignore invalid NUMA topology entry '"
                                 << name << "': " << e.what();
                }
            }
            return {nodes.begin(), nodes.end()};
        }

        tl::expected<void, ErrorCode> Client::MountSegment(
            const void* buffer, size_t size, const std::string& protocol,
            const std::string& location) {
            auto result =
                MountSegmentAndGetId(buffer, size, protocol, location);
            if (!result) {
                return tl::unexpected(result.error());
            }
            return {};
        }

        tl::expected<void, ErrorCode> Client::UnmountSegmentImpl(
            std::unordered_map<UUID, Segment, boost::hash<UUID>>::iterator it) {
            // 全量 unmount：与全量 mount 对称，向所有 primary submaster 注销
            // segment
            // （单个失败仅告警，不阻断本地 unregister）。单
            // master/直连走原路径。
            std::vector<std::string> unmount_targets;
            {
                std::lock_guard<std::mutex> addr_lock(
                    cvm_submaster_addresses_mutex_);
                unmount_targets = cvm_submaster_addresses_;
            }
            if (!unmount_targets.empty()) {
                for (const auto& addr : unmount_targets) {
                    auto r =
                        master_client_.UnmountSegmentTo(addr, it->second.id);
                    if (!r) {
                        LOG(WARNING)
                            << "unmount_segment_from_submaster_failed addr="
                            << addr << " id=" << it->second.id
                            << " error=" << r.error();
                    }
                }
            } else {
                auto unmount_result =
                    master_client_.UnmountSegment(it->second.id);
                if (!unmount_result) {
                    ErrorCode err = unmount_result.error();
                    LOG(ERROR) << "Failed to unmount segment from master: "
                               << toString(err);
                    return tl::unexpected(err);
                }
            }

            if (gather_service_)
                gather_service_->removeSource(
                    reinterpret_cast<void*>(it->second.base));
            int rc = transfer_engine_->unregisterLocalMemory(
                reinterpret_cast<void*>(it->second.base));
            if (rc != 0) {
                LOG(ERROR)
                    << "Failed to unregister transfer buffer with transfer "
                       "engine ret is "
                    << rc;
                if (rc != ERR_ADDRESS_NOT_REGISTERED) {
                    return tl::unexpected(ErrorCode::INTERNAL_ERROR);
                }
                // Otherwise, the segment is already unregistered from transfer
                // engine, we can continue
            }

            mounted_segments_.erase(it);
            return {};
        }

        tl::expected<void, ErrorCode> Client::UnmountSegment(const void* buffer,
                                                             size_t size) {
            std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
            auto segment = mounted_segments_.end();

            for (auto it = mounted_segments_.begin();
                 it != mounted_segments_.end(); ++it) {
                if (it->second.base == reinterpret_cast<uintptr_t>(buffer) &&
                    it->second.size == size) {
                    segment = it;
                    break;
                }
            }
            if (segment == mounted_segments_.end()) {
                LOG(ERROR) << "segment_not_found base=" << buffer
                           << " size=" << size;
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }

            return UnmountSegmentImpl(segment);
        }

        tl::expected<UUID, ErrorCode> Client::MountSegmentAndGetId(
            const void* buffer, size_t size, const std::string& protocol,
            const std::string& location) {
            auto check_result =
                CheckRegisterMemoryParams(buffer, size, protocol);
            if (!check_result) {
                return tl::unexpected(check_result.error());
            }

            UUID segment_id;
            {
                std::lock_guard<std::mutex> lock(mounted_segments_mutex_);

                // Check if the segment overlaps with any existing segment
                for (auto& it : mounted_segments_) {
                    auto& mtseg = it.second;
                    uintptr_t l1 = reinterpret_cast<uintptr_t>(mtseg.base);
                    uintptr_t r1 = reinterpret_cast<uintptr_t>(mtseg.size) + l1;
                    uintptr_t l2 = reinterpret_cast<uintptr_t>(buffer);
                    uintptr_t r2 = reinterpret_cast<uintptr_t>(size) + l2;
                    if (std::max(l1, l2) < std::min(r1, r2)) {
                        LOG(ERROR) << "segment_overlaps base1=" << mtseg.base
                                   << " size1=" << mtseg.size
                                   << " base2=" << buffer << " size2=" << size;
                        return tl::unexpected(ErrorCode::INVALID_PARAMS);
                    }
                }

                int rc = transfer_engine_->registerLocalMemory(
                    (void*)buffer, size, location, true, true);
                if (rc != 0) {
                    LOG(ERROR) << "register_local_memory_failed base=" << buffer
                               << " size=" << size << ", error=" << rc;
                    return tl::unexpected(ErrorCode::INVALID_PARAMS);
                }

                Segment segment;
                segment.id = generate_uuid();
                segment.name = local_hostname_;
                segment.base = reinterpret_cast<uintptr_t>(buffer);
                segment.size = size;
                segment.protocol = protocol;
                segment.host_id = host_id_;
                if (protocol == "rdma" && StoreHcaAffinityEnabled()) {
                    if (auto topology = transfer_engine_->getLocalTopology()) {
                        segment.locality =
                            DiscoverSegmentLocality(*topology, location);
                    }
                }
                // 方案 a：直接用 location 字符串作为 medium 上报。location 形如
                // "cpu:0" / "cuda:0" / "ssd:0"，planner 的 --required_medium
                // 用相同 字符串匹配即可。支持混合介质集群：不同 segment
                // 可上报不同 medium。 master 的 PublishSegmentOwnerForCvm
                // 优先用此值，缺省回退集群默认。
                segment.medium = location;
                if (metadata_connstring_ == P2PHANDSHAKE) {
                    segment.te_endpoint = transfer_engine_->getLocalIpAndPort();
                } else {
                    segment.te_endpoint = local_hostname_;
                }

                // Only CPU memory on classic RDMA owners enables gather.
                // Service setup failure leaves the existing direct path available.
                if (SupportsGatherRead() && protocol == "rdma" &&
                    !device::GetAcceleratorRegistry()
                         .RuntimeAccelerators()
                         .FindDeviceForPointer(buffer)) {
                    try {
                        if (!gather_service_) {
                            auto service =
                                std::make_unique<store::GatherReadService>(
                                    transfer_engine_, store::GatherReadOptions{},
                                    GetSegmentEndpoint());
                            service->start("0.0.0.0");
                            gather_endpoint_ = buildHostNameWithPort(
                                getHostNameWithoutPort(
                                    transfer_engine_->getLocalIpAndPort()),
                                service->port());
                            gather_service_ = std::move(service);
                            (void)master_client_.PublishGatherEndpoint(
                                GetSegmentEndpoint(), gather_endpoint_);
                        }
                        gather_service_->addSource(
                            const_cast<void*>(buffer), size);
                    } catch (const std::exception& e) {
                        LOG(WARNING) << "Store gather unavailable: " << e.what();
                    }
                }

                // 全量 mount：CVM 多 submaster 模式下，同一 segment 挂到所有
                // primary submaster（任一成功即可），使任何 slot owner
                // 都能本地分配副本； 单 master/直连模式下
                // cvm_submaster_addresses_ 为空，走原 MountSegment。
                std::vector<std::string> mount_targets;
                {
                    std::lock_guard<std::mutex> addr_lock(
                        cvm_submaster_addresses_mutex_);
                    mount_targets = cvm_submaster_addresses_;
                }
                if (!mount_targets.empty()) {
                    bool any_mounted = false;
                    for (const auto& addr : mount_targets) {
                        auto r = master_client_.MountSegmentTo(addr, segment);
                        if (r) {
                            any_mounted = true;
                        } else {
                            LOG(WARNING)
                                << "mount_segment_to_submaster_failed addr="
                                << addr << " id=" << segment.id
                                << " error=" << r.error();
                        }
                    }
                    if (!any_mounted) {
                        if (gather_service_)
                            gather_service_->removeSource(
                                const_cast<void*>(buffer));
                        LOG(ERROR)
                            << "mount_segment_to_all_submasters_failed base="
                            << buffer << " size=" << size;
                        return tl::unexpected(ErrorCode::RPC_FAIL);
                    }
                } else {
                    auto mount_result = master_client_.MountSegment(segment);
                    if (!mount_result) {
                        ErrorCode err = mount_result.error();
                        if (gather_service_)
                            gather_service_->removeSource(
                                const_cast<void*>(buffer));
                        LOG(ERROR)
                            << "mount_segment_to_master_failed base=" << buffer
                            << " size=" << size << ", error=" << err;
                        return tl::unexpected(err);
                    }
                }

                segment_id = segment.id;
                mounted_segments_[segment_id] = segment;
            }

            EnsureStorageControlPlaneStarted();
            return segment_id;
        }

        tl::expected<void, ErrorCode> Client::UnmountSegmentById(
            const UUID& segment_id, uint64_t grace_period_ms,
            std::function<void(const UUID&)> cleanup_callback) {
            std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
            auto segment = mounted_segments_.find(segment_id);
            if (segment == mounted_segments_.end()) {
                LOG(ERROR) << "segment_not_found id="
                           << UuidToString(segment_id);
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }

            if (grace_period_ms == 0) {
                return UnmountSegmentImpl(segment);
            }

            // 全量优雅卸载：与全量 mount/unmount 对称，向所有 primary submaster
            // 发起 优雅卸载。单个失败即返回错误，避免部分 submaster
            // 已进入优雅卸载而其余 未生效导致状态不一致。单
            // master/直连模式走原路径。
            std::vector<std::string> graceful_targets;
            {
                std::lock_guard<std::mutex> addr_lock(
                    cvm_submaster_addresses_mutex_);
                graceful_targets = cvm_submaster_addresses_;
            }
            if (!graceful_targets.empty()) {
                for (const auto& addr : graceful_targets) {
                    auto r = master_client_.GracefulUnmountSegmentTo(
                        addr, segment_id, grace_period_ms);
                    if (!r) {
                        LOG(ERROR) << "Failed to graceful unmount segment from "
                                      "submaster addr="
                                   << addr << " id=" << segment_id
                                   << " error=" << r.error();
                        return tl::unexpected(r.error());
                    }
                }
            } else {
                auto result = master_client_.GracefulUnmountSegment(
                    segment_id, grace_period_ms);
                if (!result) {
                    ErrorCode err = result.error();
                    LOG(ERROR)
                        << "Failed to graceful unmount segment from master: "
                        << toString(err);
                    return tl::unexpected(err);
                }
            }

            gracefully_unmounting_segments_.emplace(segment->first,
                                                    segment->second);
            if (cleanup_callback) {
                graceful_unmount_cleanup_callbacks_[segment->first] =
                    std::move(cleanup_callback);
            }
            mounted_segments_.erase(segment);
            StartGracefulUnmountTimer(segment_id, grace_period_ms);
            return {};
        }

        bool Client::WaitForGracefulUnmountDelay(
            std::chrono::milliseconds delay) {
            std::unique_lock<std::mutex> lock(graceful_unmount_timer_mutex_);
            return graceful_unmount_timer_cv_.wait_for(lock, delay, [this]() {
                return graceful_unmount_timer_stopping_;
            });
        }

        void Client::StartGracefulUnmountTimer(const UUID& segment_id,
                                               uint64_t grace_period_ms) {
            auto delay = std::chrono::milliseconds(grace_period_ms) +
                         std::chrono::seconds(10);
            task_thread_pool_.enqueue([this, segment_id, delay]() {
                if (this->WaitForGracefulUnmountDelay(delay)) {
                    return;
                }
                this->OnGracefulUnmountTimer(segment_id, /*retry_left=*/3);
            });
        }

        void Client::OnGracefulUnmountTimer(const UUID& segment_id,
                                            int retry_left) {
            // Query master to confirm the segment has been removed
            {
                std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
                auto it = gracefully_unmounting_segments_.find(segment_id);
                if (it == gracefully_unmounting_segments_.end()) {
                    // Already cleaned up (e.g. by destructor)
                    return;
                }
            }

            std::vector<std::string> status_targets;
            {
                std::lock_guard<std::mutex> addr_lock(
                    cvm_submaster_addresses_mutex_);
                status_targets = cvm_submaster_addresses_;
            }

            bool removed = false;
            if (!status_targets.empty()) {
                // 多 submaster：仅当所有 primary submaster
                // 均确认移除后才视为完成， 保证优雅卸载在所有 submaster
                // 对称生效后才释放本地 MR 与回调。
                removed = true;
                for (const auto& addr : status_targets) {
                    auto status = master_client_.QuerySegmentStatusByIdTo(
                        addr, segment_id);
                    if (!status) {
                        if (status.error() == ErrorCode::SEGMENT_NOT_FOUND) {
                            continue;  // 该 submaster 已移除
                        }
                        LOG(WARNING)
                            << "Failed to query graceful unmount segment "
                               "status from submaster addr="
                            << addr << " error=" << status.error();
                        removed = false;
                    } else if (status.value() == SegmentStatus::UNDEFINED) {
                        continue;  // 该 submaster 已移除
                    } else {
                        removed = false;
                    }
                }
            } else {
                auto status = master_client_.QuerySegmentStatusById(segment_id);
                if (!status) {
                    if (status.error() == ErrorCode::SEGMENT_NOT_FOUND) {
                        removed = true;
                    } else {
                        LOG(WARNING) << "Failed to query graceful unmount "
                                        "segment status: "
                                     << toString(status.error());
                    }
                } else if (status.value() == SegmentStatus::UNDEFINED) {
                    removed = true;
                }
            }

            if (removed) {
                std::function<void(const UUID&)> cleanup_callback;
                {
                    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
                    auto it = gracefully_unmounting_segments_.find(segment_id);
                    if (it != gracefully_unmounting_segments_.end()) {
                        if (gather_service_)
                            gather_service_->removeSource(
                                reinterpret_cast<void*>(it->second.base));
                        int rc = transfer_engine_->unregisterLocalMemory(
                            reinterpret_cast<void*>(it->second.base));
                        if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
                            LOG(ERROR) << "Failed to unregister TE MR for "
                                          "graceful unmount: "
                                       << rc;
                        }
                        gracefully_unmounting_segments_.erase(it);
                    }
                    auto callback_it =
                        graceful_unmount_cleanup_callbacks_.find(segment_id);
                    if (callback_it !=
                        graceful_unmount_cleanup_callbacks_.end()) {
                        cleanup_callback = std::move(callback_it->second);
                        graceful_unmount_cleanup_callbacks_.erase(callback_it);
                    }
                }
                if (cleanup_callback) {
                    cleanup_callback(segment_id);
                }
                return;
            }

            if (retry_left > 0) {
                try {
                    task_thread_pool_.enqueue([this, segment_id, retry_left]() {
                        if (this->WaitForGracefulUnmountDelay(
                                std::chrono::seconds(10))) {
                            return;
                        }
                        this->OnGracefulUnmountTimer(segment_id,
                                                     retry_left - 1);
                    });
                } catch (const std::runtime_error& e) {
                    VLOG(1)
                        << "Skip graceful unmount retry enqueue: " << e.what();
                }
            } else {
                LOG(WARNING) << "Graceful unmount cleanup timeout for segment "
                             << UuidToString(segment_id);
            }
        }

        tl::expected<void, ErrorCode> Client::RegisterLocalMemory(
            void* addr, size_t length, const std::string& location,
            bool remote_accessible, bool update_metadata) {
            auto check_result =
                CheckRegisterMemoryParams(addr, length, protocol_);
            if (!check_result) {
                return tl::unexpected(check_result.error());
            }
            if (this->transfer_engine_->registerLocalMemory(
                    addr, length, location, remote_accessible,
                    update_metadata) != 0) {
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
            return {};
        }

        tl::expected<void, ErrorCode> Client::unregisterLocalMemory(
            void* addr, bool update_metadata) {
            const int rc =
                transfer_engine_->unregisterLocalMemory(addr, update_metadata);
            if (rc != 0) {
                // Distinguish "not registered" from real failures so callers
                // can tell an already-cleaned buffer apart from a live
                // registration whose deregistration failed.
                return tl::unexpected(rc == ERR_ADDRESS_NOT_REGISTERED
                                          ? ErrorCode::NOT_REGISTERED
                                          : ErrorCode::INVALID_PARAMS);
            }
            return {};
        }

        tl::expected<bool, ErrorCode> Client::IsExist(const std::string& key) {
            auto result = master_client_.ExistKey(key);
            return result;
        }

        tl::expected<bool, ErrorCode> Client::ProbeKey(const std::string& key) {
            auto result = master_client_.ProbeKey(key);
            return result;
        }

        std::vector<tl::expected<bool, ErrorCode>> Client::BatchIsExist(
            const std::vector<std::string>& keys) {
            auto response = master_client_.BatchExistKey(keys);

            // Check if we got the expected number of responses
            if (response.size() != keys.size()) {
                LOG(ERROR) << "BatchExistKey response size mismatch. Expected: "
                           << keys.size() << ", Got: " << response.size();
                // Return vector of RPC_FAIL errors
                std::vector<tl::expected<bool, ErrorCode>> results;
                results.reserve(keys.size());
                for (size_t i = 0; i < keys.size(); ++i) {
                    results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
                }
                return results;
            }

            // Return the response directly as it's already in the correct
            // format
            return response;
        }

        std::vector<tl::expected<bool, ErrorCode>> Client::BatchProbeKey(
            const std::vector<std::string>& keys,
            const GrantLeasePolicy& policy) {
            auto response = master_client_.BatchProbeKey(keys, policy);

            // Check if we got the expected number of responses
            if (response.size() != keys.size()) {
                LOG(ERROR) << "BatchProbeKey response size mismatch. Expected: "
                           << keys.size() << ", Got: " << response.size();
                // Return vector of RPC_FAIL errors
                std::vector<tl::expected<bool, ErrorCode>> results;
                results.reserve(keys.size());
                for (size_t i = 0; i < keys.size(); ++i) {
                    results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
                }
                return results;
            }

            return response;
        }

        WeightRpcResult<WeightRevisionMetadata> Client::BeginWeightImport(
            const BeginWeightImportRequest& request) {
            return master_client_.BeginWeightImport(request);
        }

        WeightRpcResult<WeightRevisionMetadata> Client::CommitWeightImport(
            const CommitWeightImportRequest& request) {
            return master_client_.CommitWeightImport(request);
        }

        WeightRpcResult<WeightRevisionMetadata> Client::AbortWeightImport(
            const AbortWeightImportRequest& request) {
            return master_client_.AbortWeightImport(request);
        }

        WeightRpcResult<WeightRevisionView> Client::GetWeightRevision(
            const GetWeightRevisionRequest& request) {
            return master_client_.GetWeightRevision(request);
        }

        WeightRpcResult<ListWeightRevisionsResponse>
        Client::ListWeightRevisions(const ListWeightRevisionsRequest& request) {
            return master_client_.ListWeightRevisions(request);
        }

        WeightRpcResult<WeightRevisionLease> Client::AcquireWeightRevisionLease(
            const AcquireWeightRevisionLeaseRequest& request) {
            return master_client_.AcquireWeightRevisionLease(request);
        }

        WeightRpcResult<WeightRevisionLease> Client::RenewWeightRevisionLease(
            const RenewWeightRevisionLeaseRequest& request) {
            return master_client_.RenewWeightRevisionLease(request);
        }

        WeightRpcResult<void> Client::ReleaseWeightRevisionLease(
            const ReleaseWeightRevisionLeaseRequest& request) {
            return master_client_.ReleaseWeightRevisionLease(request);
        }

        WeightRpcResult<WeightResidencyOperation>
        Client::StartWeightResidencyOperation(
            const StartWeightResidencyOperationRequest& request) {
            return master_client_.StartWeightResidencyOperation(request);
        }

        WeightRpcResult<WeightResidencyOperation> Client::QueryWeightOperation(
            const QueryWeightOperationRequest& request) {
            return master_client_.QueryWeightOperation(request);
        }

        WeightRpcResult<WeightRevisionMetadata> Client::DeleteWeightRevision(
            const DeleteWeightRevisionRequest& request) {
            return master_client_.DeleteWeightRevision(request);
        }

        void* Client::GetBaseAddr() { return transfer_engine_->getBaseAddr(); }

        tl::expected<void, ErrorCode> Client::MountLocalDiskSegment(
            bool enable_offloading) {
            auto response = master_client_.MountLocalDiskSegment(
                client_id_, enable_offloading);

            if (!response) {
                LOG(ERROR) << "MountLocalDiskSegment failed, error code is "
                           << response.error();
                return response;
            }

            local_disk_mounted_.store(true);
            local_disk_offload_enabled_.store(enable_offloading);
            EnsureStorageControlPlaneStarted();
            return response;
        }

        tl::expected<void, ErrorCode> Client::UnmountLocalDiskSegment() {
            auto response = master_client_.UnmountLocalDiskSegment(client_id_);
            if (!response) {
                LOG(ERROR) << "UnmountLocalDiskSegment failed, error code is "
                           << response.error();
            }
            return response;
        }

        tl::expected<void, ErrorCode> Client::OffloadObjectHeartbeat(
            bool enable_offloading,
            std::vector<OffloadTaskItem>& offloading_objects) {
            auto response = master_client_.OffloadObjectHeartbeat(
                client_id_, enable_offloading);
            if (!response) {
                LOG(ERROR) << "OffloadObjectHeartbeat failed, error code is "
                           << response.error();
                return tl::make_unexpected(response.error());
            }
            offloading_objects = std::move(response.value());
            return {};
        }

        tl::expected<bool, ErrorCode> Client::PollRemoveAll() {
            return master_client_.PollRemoveAll();
        }

        tl::expected<void, ErrorCode> Client::ReportSsdCapacity(
            int64_t ssd_total_capacity_bytes) {
            auto response = master_client_.ReportSsdCapacity(
                client_id_, ssd_total_capacity_bytes);
            if (!response) {
                LOG(ERROR) << "ReportSsdCapacity failed, error code is "
                           << response.error();
                return tl::make_unexpected(response.error());
            }
            ssd_total_capacity_bytes_.store(ssd_total_capacity_bytes);
            return {};
        }

        tl::expected<void, ErrorCode> Client::BatchGetOffloadObject(
            const std::string& transfer_engine_addr,
            const std::vector<std::string>& keys,
            const std::vector<uintptr_t>& pointers,
            const std::unordered_map<std::string, std::vector<Slice>>&
                batch_slices) {
            return BatchGetOffloadObject(transfer_engine_addr, keys, pointers,
                                         batch_slices,
                                         OffloadBufferAccess::kTransferEngine,
                                         TransferIntent::kForegroundGet);
        }

        tl::expected<void, ErrorCode> Client::BatchGetOffloadObject(
            const std::string& transfer_engine_addr,
            const std::vector<std::string>& keys,
            const std::vector<uintptr_t>& pointers,
            const std::unordered_map<std::string, std::vector<Slice>>&
                batch_slices,
            OffloadBufferAccess buffer_access, TransferIntent intent) {
            auto future = transfer_submitter_->submit_batch_get_offload_object(
                transfer_engine_addr, keys, pointers, batch_slices,
                buffer_access, intent);
            if (!future) {
                LOG(ERROR) << "Failed to submit transfer operation";
                return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
            }
            VLOG(1) << "Using transfer strategy: " << future->strategy();
            auto result = future->get();
            if (result != ErrorCode::OK) {
                LOG(ERROR) << "Transfer failed, error code is " << result;
                return tl::make_unexpected(result);
            }
            return {};
        }

        tl::expected<void, ErrorCode> Client::NotifyOffloadSuccess(
            const std::vector<std::string>& keys,
            const std::vector<StorageObjectMetadata>& metadatas) {
            auto response = master_client_.NotifyOffloadSuccess(
                client_id_, keys, metadatas);
            return response;
        }

        tl::expected<void, ErrorCode> Client::NotifyOffloadSuccess(
            const std::vector<OffloadTaskItem>& tasks,
            const std::vector<StorageObjectMetadata>& metadatas) {
            return master_client_.NotifyOffloadSuccess(client_id_, tasks,
                                                       metadatas);
        }

        void Client::SetDfsStorageBackend(
            std::shared_ptr<DistributedStorageBackend> backend) {
            dfs_storage_backend_ = std::move(backend);
            EnsureStorageControlPlaneStarted();
        }

        bool Client::CanReadDfsReplicaIntoHostMemory(
            const Replica::Descriptor& replica) const {
            return replica.is_dfs_replica() && dfs_storage_backend_ &&
                   dfs_storage_backend_->SupportsArbitraryHostReadBuffers();
        }

            std::vector<std::string> Client::GetSubmasterAddresses() const {
                return master_client_.GetSubmasterAddresses();
            }

            tl::expected<void, ErrorCode> Client::MountLocalDiskSegmentTo(
                const std::string& address, bool enable_offloading) {
                auto response = master_client_.MountLocalDiskSegmentTo(
                    address, client_id_, enable_offloading);
                if (!response) {
                    LOG(ERROR)
                        << "MountLocalDiskSegmentTo failed, addr=" << address
                        << " error=" << response.error();
                    return response;
                }
                local_disk_mounted_.store(true);
                local_disk_offload_enabled_.store(enable_offloading);
                EnsureStorageControlPlaneStarted();
                return response;
            }

            tl::expected<void, ErrorCode> Client::UnmountLocalDiskSegmentTo(
                const std::string& address) {
                return master_client_.UnmountLocalDiskSegmentTo(address,
                                                                client_id_);
            }

            tl::expected<void, ErrorCode> Client::OffloadObjectHeartbeatTo(
                const std::string& address, bool enable_offloading,
                std::vector<OffloadTaskItem>& offloading_objects) {
                auto response = master_client_.OffloadObjectHeartbeatTo(
                    address, client_id_, enable_offloading);
                if (!response) {
                    return tl::make_unexpected(response.error());
                }
                offloading_objects = std::move(response.value());
                return {};
            }

            tl::expected<void, ErrorCode> Client::ReportSsdCapacityTo(
                const std::string& address, int64_t ssd_total_capacity_bytes) {
                auto response = master_client_.ReportSsdCapacityTo(
                    address, client_id_, ssd_total_capacity_bytes);
                if (!response) {
                    LOG(WARNING)
                        << "ReportSsdCapacityTo failed, addr=" << address
                        << " error=" << response.error();
                }
                return response;
            }

            tl::expected<bool, ErrorCode> Client::PollRemoveAllTo(
                const std::string& address) {
                return master_client_.PollRemoveAllTo(address, client_id_);
            }

            tl::expected<std::vector<RemoveTaskItem>, ErrorCode>
            Client::RemoveObjectHeartbeatTo(const std::string& address) {
                return master_client_.RemoveObjectHeartbeatTo(address,
                                                              client_id_);
            }

            tl::expected<void, ErrorCode> Client::AckRemoveObjectHeartbeatTo(
                const std::string& address,
                const std::vector<RemoveTaskItem>& tasks) {
                return master_client_.AckRemoveObjectHeartbeatTo(
                    address, client_id_, tasks);
            }

            tl::expected<void, ErrorCode> Client::NotifyOffloadSuccessRouted(
                const std::vector<OffloadTaskItem>& tasks,
                const std::vector<StorageObjectMetadata>& metadatas) {
                // 旧签名重载：无来源信息（source 为空），转发到双投实现。
                std::vector<SourcedOffloadTask> sourced_tasks;
                sourced_tasks.reserve(tasks.size());
                for (const auto& task : tasks) {
                    sourced_tasks.push_back(
                        SourcedOffloadTask{task, std::string()});
                }
                return NotifyOffloadSuccessRouted(sourced_tasks, metadatas);
            }

            tl::expected<void, ErrorCode> Client::NotifyOffloadSuccessRouted(
                const std::vector<SourcedOffloadTask>& tasks,
                const std::vector<StorageObjectMetadata>& metadatas) {
                const auto addresses = master_client_.GetSubmasterAddresses();
                if (addresses.empty()) {
                    // 单 master
                    // 模式（路由表未加载）：走当前连接，与非定向等价。
                    std::vector<OffloadTaskItem> plain_tasks;
                    plain_tasks.reserve(tasks.size());
                    for (const auto& st : tasks) {
                        plain_tasks.push_back(st.task);
                    }
                    return master_client_.NotifyOffloadSuccess(
                        client_id_, plain_tasks, metadatas);
                }

                // 多 submaster：双投清理协议。按目标地址分组：
                // - 成功通知投 owner（建副本）；source 非空且 != owner 时双投
                //   source（master 侧"仅清理"语义）。
                // - NACK（data_size<0）只投 source（任务状态只在 source 上）。
                // - owner 未解析投 source；source 也空才回退当前连接。
                if (tasks.size() != metadatas.size()) {
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
                std::map<std::string,
                         std::pair<std::vector<OffloadTaskItem>,
                                   std::vector<StorageObjectMetadata>>>
                    grouped;
                std::vector<OffloadTaskItem> fallback_tasks;
                std::vector<StorageObjectMetadata> fallback_metadatas;
                size_t dual_delivery_count = 0;
                size_t unresolved_count = 0;
                const auto add_to_group =
                    [&grouped](const std::string& addr,
                               const OffloadTaskItem& task,
                               const StorageObjectMetadata& metadata) {
                        auto& [g_tasks, g_metas] = grouped[addr];
                        g_tasks.push_back(task);
                        g_metas.push_back(metadata);
                    };
                for (size_t i = 0; i < tasks.size(); ++i) {
                    const auto& st = tasks[i];
                    const bool is_nack = metadatas[i].data_size < 0;
                    auto owner = master_client_.ResolveSubmasterFor(
                        st.task.tenant_id, st.task.key);
                    const auto& source = st.source_address;
                    if (is_nack) {
                        // NACK：清理语义，任务状态只在 source 上。
                        if (!source.empty()) {
                            add_to_group(source, st.task, metadatas[i]);
                        } else {
                            fallback_tasks.push_back(st.task);
                            fallback_metadatas.push_back(metadatas[i]);
                        }
                        continue;
                    }
                    if (owner) {
                        add_to_group(*owner, st.task, metadatas[i]);
                        if (!source.empty() && source != *owner) {
                            // 双投：slot 已从 source 迁往 owner，source
                            // 上的任务 状态（源副本 refcnt
                            // pin）需要"仅清理"通知闭环。
                            add_to_group(source, st.task, metadatas[i]);
                            ++dual_delivery_count;
                        }
                    } else if (!source.empty()) {
                        // owner 未解析：投 source。source 侧 slot
                        // 校验决定建副本 （路由表滞后，slot 实际还在
                        // source）或清理（已迁移）。
                        add_to_group(source, st.task, metadatas[i]);
                        ++unresolved_count;
                    } else {
                        fallback_tasks.push_back(st.task);
                        fallback_metadatas.push_back(metadatas[i]);
                        ++unresolved_count;
                    }
                }
                if (dual_delivery_count > 0 || unresolved_count > 0) {
                    VLOG(1)
                        << "NotifyOffloadSuccessRouted: tasks=" << tasks.size()
                        << " dual_delivery=" << dual_delivery_count
                        << " unresolved=" << unresolved_count;
                }

                std::optional<ErrorCode> first_error;
                for (const auto& [address, group] : grouped) {
                    const auto& [g_tasks, g_metas] = group;
                    auto result = master_client_.NotifyOffloadSuccessTo(
                        address, client_id_, g_tasks, g_metas);
                    if (!result) {
                        LOG(WARNING)
                            << "NotifyOffloadSuccessTo failed, addr=" << address
                            << " error=" << result.error()
                            << " tasks=" << g_tasks.size();
                        if (!first_error) {
                            first_error = result.error();
                        }
                    }
                }
                if (!fallback_tasks.empty()) {
                    auto result = master_client_.NotifyOffloadSuccess(
                        client_id_, fallback_tasks, fallback_metadatas);
                    if (!result && !first_error) {
                        first_error = result.error();
                    }
                }
                if (first_error) {
                    return tl::make_unexpected(*first_error);
                }
                return {};
            }

            tl::expected<void, ErrorCode> Client::PromotionObjectHeartbeat(
                std::vector<PromotionTaskItem> & promotion_objects) {
                auto response =
                    master_client_.PromotionObjectHeartbeat(client_id_);
                if (!response) {
                    return tl::make_unexpected(response.error());
                }
                promotion_objects = std::move(response.value());
                return {};
            }

            tl::expected<void, ErrorCode> Client::RegisterPrefetchTask(
                const std::string& object_key, const std::string& tenant_id) {
                // Default (empty) means this client's own tenant; a delegated
                // prefetch carries the requester's tenant explicitly.
                return master_client_.RegisterPrefetchTask(
                    client_id_, object_key,
                    tenant_id.empty() ? master_client_.tenant_id() : tenant_id);
            }

                tl::expected<void, ErrorCode>
                Client::PromotionObjectHeartbeatTo(
                    const std::string& address,
                    std::vector<PromotionTaskItem>& promotion_objects) {
                    auto response = master_client_.PromotionObjectHeartbeatTo(
                        address, client_id_);
                    if (!response) {
                        return tl::make_unexpected(response.error());
                    }
                    promotion_objects = std::move(response.value());
                    return {};
                }

                tl::expected<std::vector<RemoveTaskItem>, ErrorCode>
                Client::RemoveObjectHeartbeat(const UUID& client_id) {
                    return master_client_.RemoveObjectHeartbeat(client_id);
                }

                tl::expected<void, ErrorCode> Client::AckRemoveObjectHeartbeat(
                    const UUID& client_id,
                    const std::vector<RemoveTaskItem>& tasks) {
                    return master_client_.AckRemoveObjectHeartbeat(client_id,
                                                                   tasks);
                }

                tl::expected<PromotionAllocStartResponse, ErrorCode>
                Client::PromotionAllocStart(
                    const std::string& key, uint64_t size,
                    const std::vector<std::string>& preferred_segments) {
                    return master_client_.PromotionAllocStart(
                        client_id_, key, size, preferred_segments);
                }

                tl::expected<PromotionAllocStartResponse, ErrorCode>
                Client::PromotionAllocStart(
                    const std::string& key, const std::string& tenant_id,
                    uint64_t size,
                    const std::vector<std::string>& preferred_segments) {
                    return master_client_.PromotionAllocStart(
                        client_id_, key, tenant_id, size, preferred_segments);
                }

                tl::expected<void, ErrorCode> Client::NotifyPromotionSuccess(
                    const std::string& key) {
                    return master_client_.NotifyPromotionSuccess(client_id_,
                                                                 key);
                }

                tl::expected<void, ErrorCode> Client::NotifyPromotionSuccess(
                    const std::string& key, const std::string& tenant_id) {
                    return master_client_.NotifyPromotionSuccess(
                        client_id_, key, tenant_id);
                }

                tl::expected<void, ErrorCode> Client::NotifyPromotionFailure(
                    const std::string& key) {
                    return master_client_.NotifyPromotionFailure(client_id_,
                                                                 key);
                }

                tl::expected<void, ErrorCode> Client::NotifyPromotionFailure(
                    const std::string& key, const std::string& tenant_id) {
                    return master_client_.NotifyPromotionFailure(
                        client_id_, key, tenant_id);
                }

                tl::expected<PromotionAllocStartResponse, ErrorCode>
                Client::PromotionAllocStartTo(
                    const std::string& address, const std::string& key,
                    const std::string& tenant_id, uint64_t size,
                    const std::vector<std::string>& preferred_segments) {
                    return master_client_.PromotionAllocStartTo(
                        address, client_id_, key, tenant_id, size,
                        preferred_segments);
                }

                tl::expected<void, ErrorCode> Client::NotifyPromotionSuccessTo(
                    const std::string& address, const std::string& key,
                    const std::string& tenant_id) {
                    return master_client_.NotifyPromotionSuccessTo(
                        address, client_id_, key, tenant_id);
                }

                tl::expected<void, ErrorCode> Client::NotifyPromotionFailureTo(
                    const std::string& address, const std::string& key,
                    const std::string& tenant_id) {
                    return master_client_.NotifyPromotionFailureTo(
                        address, client_id_, key, tenant_id);
                }

                ErrorCode Client::PromotionWrite(
                    const Replica::Descriptor& memory_descriptor,
                    std::vector<Slice>& slices) {
                    return TransferWrite(memory_descriptor, slices);
                }

                tl::expected<UUID, ErrorCode> Client::CreateCopyTask(
                    const std::string& key,
                    const std::vector<std::string>& targets) {
                    return master_client_.CreateCopyTask(key, targets);
                }

                tl::expected<UUID, ErrorCode> Client::CreateCopyTask(
                    const std::string& key, const std::string& tenant_id,
                    const std::vector<std::string>& targets) {
                    return master_client_.CreateCopyTask(key, tenant_id,
                                                         targets);
                }

                tl::expected<UUID, ErrorCode> Client::CreateMoveTask(
                    const std::string& key, const std::string& source,
                    const std::string& target) {
                    return master_client_.CreateMoveTask(key, source, target);
                }

                tl::expected<UUID, ErrorCode> Client::CreateMoveTask(
                    const std::string& key, const std::string& tenant_id,
                    const std::string& source, const std::string& target) {
                    return master_client_.CreateMoveTask(key, tenant_id, source,
                                                         target);
                }

                tl::expected<void, ErrorCode> Client::ExecuteReplicaTransfer(
                    const std::string& key, const std::string& action_name,
                    std::function<tl::expected<void, ErrorCode>()> end_fn,
                    std::function<tl::expected<void, ErrorCode>()> revoke_fn,
                    const Replica::Descriptor& source,
                    const std::vector<Replica::Descriptor>& targets,
                    const std::vector<VSegmentLayoutEntry>* layouts) {
                    auto revoke_lambda = [&]() {
                        auto revoke_result = revoke_fn();
                        if (!revoke_result.has_value()) {
                            LOG(WARNING)
                                << "action=replica_" << action_name
                                << "_revoke_failed"
                                << ", key=" << key
                                << ", error_code=" << revoke_result.error();
                        }
                    };

                    if (!source.is_memory_replica() &&
                        !source.is_vsegment_replica()) {
                        LOG(ERROR)
                            << "action=replica_" << action_name << "_failed"
                            << ", key=" << key
                            << ", error=invalid_replica_type";
                        revoke_lambda();
                        return tl::unexpected(ErrorCode::INVALID_PARAMS);
                    }

                    // A local memory source can be transferred zero-copy. A
                    // vsegment source is remote and striped, so read it into
                    // registered staging memory first.
                    if (source.is_memory_replica() &&
                        !IsReplicaOnLocalMemory(source)) {
                        LOG(ERROR)
                            << "action=replica_" << action_name << "_failed"
                            << ", key=" << key
                            << ", error=source_replica_not_in_local_memory";
                        revoke_lambda();
                        return tl::unexpected(
                            ErrorCode::REPLICA_NOT_IN_LOCAL_MEMORY);
                    }

                    std::optional<BufferHandle> staging;
                    std::vector<Slice> slices;
                    if (source.is_memory_replica()) {
                        const auto& buffer_descriptor =
                            source.get_memory_descriptor().buffer_descriptor;
                        void* buffer = reinterpret_cast<void*>(
                            buffer_descriptor.buffer_address_);
                        slices =
                            split_into_slices(buffer, buffer_descriptor.size_);
                    } else {
                        if (!replica_transfer_staging_allocator_) {
                            LOG(ERROR)
                                << "action=replica_" << action_name << "_failed"
                                << ", key=" << key
                                << ", error=staging_allocator_unavailable";
                            revoke_lambda();
                            return tl::unexpected(
                                ErrorCode::NO_AVAILABLE_HANDLE);
                        }
                        const auto length =
                            source.get_vsegment_descriptor().length;
                        staging = replica_transfer_staging_allocator_->allocate(
                            length);
                        if (!staging) {
                            revoke_lambda();
                            return tl::unexpected(
                                ErrorCode::NO_AVAILABLE_HANDLE);
                        }
                        slices = split_into_slices(*staging);
                        if (TransferRead(source, slices, TransferIntent::kMigration, layouts) !=
                            ErrorCode::OK) {
                            revoke_lambda();
                            return tl::unexpected(ErrorCode::TRANSFER_FAIL);
                        }
                    }

                    // Transfer to each target
                    for (const auto& target : targets) {
                        if (TransferWrite(target, slices,
                                          TransferIntent::kMigration, nullptr,
                                          layouts) !=
                            ErrorCode::OK) {
                            revoke_lambda();
                            return tl::unexpected(ErrorCode::TRANSFER_FAIL);
                        }
                    }

                    // Call end function to finalize
                    auto end_result = end_fn();
                    if (!end_result.has_value()) {
                        revoke_lambda();
                        return tl::unexpected(end_result.error());
                    }

                    return {};
                }

                tl::expected<void, ErrorCode> Client::Copy(
                    const std::string& key, const std::string& source,
                    const std::vector<std::string>& targets) {
                    return Copy(key, master_client_.tenant_id(), source,
                                targets);
                }

                tl::expected<void, ErrorCode> Client::Copy(
                    const std::string& key, const std::string& tenant_id,
                    const std::string& source,
                    const std::vector<std::string>& targets) {
                    return Copy(key, tenant_id, source, targets, UUID{}, 0);
                }

                tl::expected<void, ErrorCode> Client::Copy(
                    const std::string& key, const std::string& tenant_id,
                    const std::string& source,
                    const std::vector<std::string>& targets,
                    const UUID& dynamic_replication_lease_id,
                    uint64_t dynamic_replication_version_epoch) {
                    LOG(INFO) << "action=replica_copy_start" << ", key=" << key
                              << ", targets_count=" << targets.size();

                    // Call CopyStart first - it validates existence and
                    // allocates replicas
                    auto start_result =
                        dynamic_replication_lease_id == UUID{}
                            ? master_client_.CopyStart(key, tenant_id, source,
                                                       targets)
                            : master_client_.DynamicReplicaCopyStart(
                                  key, tenant_id, source, targets,
                                  dynamic_replication_lease_id,
                                  dynamic_replication_version_epoch);
                    if (!start_result.has_value()) {
                        ErrorCode error = start_result.error();
                        if (dynamic_replication_lease_id != UUID{} &&
                            error == ErrorCode::OBJECT_ALREADY_EXISTS) {
                            LOG(INFO) << "action=replica_copy_skipped"
                                      << ", key=" << key
                                      << ", info=target_replicas_already_exist";
                            return {};
                        }
                        LOG(ERROR) << "action=replica_copy_failed"
                                   << ", key=" << key << ", source=" << source
                                   << ", error=copy_start_failed"
                                   << ", error_code=" << error;
                        return tl::unexpected(error);
                    }

                    const auto& response = start_result.value();
                    if (response.targets.empty()) {
                        LOG(INFO)
                            << "action=replica_copy_skipped" << ", key=" << key
                            << ", info=target_replicas_already_exist";
                        // Target replicas already exist, consider it success
                        auto copy_end_result =
                            dynamic_replication_lease_id == UUID{}
                                ? master_client_.CopyEnd(key, tenant_id)
                                : master_client_.DynamicReplicaCopyEnd(
                                      key, tenant_id,
                                      dynamic_replication_lease_id,
                                      dynamic_replication_version_epoch);
                        if (!copy_end_result.has_value()) {
                            ErrorCode error = copy_end_result.error();
                            LOG(ERROR)
                                << "action=replica_copy_failed"
                                << ", key=" << key << ", error=copy_end_failed"
                                << ", error_code=" << error;
                            return tl::unexpected(error);
                        }
                        return {};
                    }

                    auto copy_end = [&]() {
                        return dynamic_replication_lease_id == UUID{}
                                   ? master_client_.CopyEnd(key, tenant_id)
                                   : master_client_.DynamicReplicaCopyEnd(
                                         key, tenant_id,
                                         dynamic_replication_lease_id,
                                         dynamic_replication_version_epoch);
                    };
                    auto copy_revoke = [&]() {
                        return dynamic_replication_lease_id == UUID{}
                                   ? master_client_.CopyRevoke(key, tenant_id)
                                   : master_client_.DynamicReplicaCopyRevoke(
                                         key, tenant_id,
                                         dynamic_replication_lease_id,
                                         dynamic_replication_version_epoch);
                    };
                    auto result = ExecuteReplicaTransfer(
                        key, "copy", copy_end, copy_revoke, response.source,
                        response.targets,
                        response.vsegment_layouts.has_value()
                            ? &*response.vsegment_layouts
                            : nullptr);

                    if (result.has_value()) {
                        LOG(INFO)
                            << "action=replica_copy_success" << ", key=" << key
                            << ", target_count=" << response.targets.size();
                    }

                    return result;
                }

                tl::expected<void, ErrorCode> Client::Move(
                    const std::string& key, const std::string& source,
                    const std::string& target) {
                    return Move(key, master_client_.tenant_id(), source,
                                target);
                }

                tl::expected<void, ErrorCode> Client::Move(
                    const std::string& key, const std::string& tenant_id,
                    const std::string& source, const std::string& target) {
                    LOG(INFO) << "action=replica_move_start" << ", key=" << key
                              << ", source_segment=" << source
                              << ", target_segment=" << target;

                    // Call MoveStart first - it validates existence and
                    // allocates replica if needed
                    auto move_start_result = master_client_.MoveStart(
                        key, tenant_id, source, target);
                    if (!move_start_result.has_value()) {
                        ErrorCode error = move_start_result.error();
                        LOG(ERROR)
                            << "action=replica_move_failed" << ", key=" << key
                            << ", error=move_start_failed"
                            << ", error_code=" << error;
                        // MoveStart already validated existence, so we just
                        // return the error
                        return tl::unexpected(error);
                    }

                    const auto& response = move_start_result.value();
                    if (!response.target.has_value()) {
                        LOG(INFO)
                            << "action=replica_move_skipped" << ", key=" << key
                            << ", info=target_replica_already_exists";
                        // Target already exists, consider it success
                        auto move_end_result =
                            master_client_.MoveEnd(key, tenant_id);
                        if (!move_end_result.has_value()) {
                            ErrorCode error = move_end_result.error();
                            LOG(ERROR)
                                << "action=replica_move_failed"
                                << ", key=" << key << ", error=move_end_failed"
                                << ", error_code=" << error;
                            return tl::unexpected(error);
                        }
                        return {};
                    }

                    std::vector<Replica::Descriptor> targets = {
                        response.target.value()};

                    auto result = ExecuteReplicaTransfer(
                        key, "move",
                        [&]() {
                            return master_client_.MoveEnd(key, tenant_id);
                        },
                        [&]() {
                            return master_client_.MoveRevoke(key, tenant_id);
                        },
                        response.source, targets,
                        response.vsegment_layouts.has_value()
                            ? &*response.vsegment_layouts
                            : nullptr);

                    if (result.has_value()) {
                        LOG(INFO)
                            << "action=replica_move_success" << ", key=" << key
                            << ", source_segment=" << source
                            << ", target_segment=" << target;
                    }

                    return result;
                }

                tl::expected<QueryTaskResponse, ErrorCode> Client::QueryTask(
                    const UUID& task_id) {
                    return master_client_.QueryTask(task_id);
                }

                tl::expected<std::vector<TaskAssignment>, ErrorCode>
                Client::FetchTasks(size_t batch_size) {
                    return master_client_.FetchTasks(batch_size);
                }

                tl::expected<void, ErrorCode> Client::MarkTaskToComplete(
                    const TaskCompleteRequest& update_request) {
                    return master_client_.MarkTaskToComplete(update_request);
                }

                ErrorCode Client::PrepareStorageBackend(
                    const std::string& storage_root_dir,
                    const std::string& fsdir, bool enable_eviction,
                    uint64_t quota_bytes) {
                    // Initialize storage backend
                    auto backend_result = StorageBackend::Create(
                        storage_root_dir, fsdir, enable_eviction);
                    if (!backend_result) {
                        LOG(ERROR) << "Failed to create storage backend: "
                                   << backend_result.error();
                        return backend_result.error();
                    }
                    // Initialize into a local first and only publish to
                    // storage_backend_ after a successful Init(): users of
                    // storage_backend_ only null-check it, so it must never
                    // point to a backend whose Init() failed (using it before
                    // successful Init() is undefined behavior). If Init()
                    // throws, stack unwinding destroys the local and
                    // storage_backend_ stays clean.
                    auto backend = std::move(backend_result.value());
                    auto init_result = backend->Init(quota_bytes);
                    if (!init_result) {
                        LOG(ERROR)
                            << "Failed to initialize StorageBackend. Error: "
                            << init_result.error()
                            << ". The backend will be unusable.";
                        return init_result.error();
                    }
                    storage_backend_ = std::move(backend);
                    return ErrorCode::OK;
                }

                void Client::PutToLocalFile(
                    const std::string& key, const std::vector<Slice>& slices,
                    const DiskDescriptor& disk_descriptor) {
                    if (!storage_backend_) return;

                    size_t total_size = 0;
                    for (const auto& slice : slices) {
                        total_size += slice.size;
                    }

                    std::string path = disk_descriptor.file_path;

                    // Synchronous D2H staging + copy into std::string.
                    // Done on the calling thread to guarantee GPU buffers are
                    // still valid (BatchPut has not yet returned to Python, so
                    // blocks are not reused).
                    std::string value;
                    value.reserve(total_size);
                    auto runtime_accelerator =
                        device::GetAcceleratorRegistry().RuntimeAccelerators();

                    for (const auto& slice : slices) {
                        device::PointerInfo info{};
                        auto* device = runtime_accelerator.FindDeviceForPointer(
                            slice.ptr, &info);
                        if (device) {
                            device->SetContext(info.device_id);
                            auto buf = pinned_buffer_pool_->Acquire(slice.size);
                            if (!device->Copy(
                                    buf.data, slice.ptr, slice.size,
                                    device::CopyDirection::kDeviceToHost)) {
                                LOG(ERROR) << "D2H copy failed for key: " << key
                                           << ", triggering PutRevoke for disk "
                                              "replica";
                                pinned_buffer_pool_->Release(std::move(buf));
                                // Must revoke to avoid phantom replica in
                                // master
                                auto revoke_result = master_client_.PutRevoke(
                                    key, ReplicaType::DISK);
                                if (!revoke_result) {
                                    LOG(ERROR) << "Failed to revoke put "
                                                  "operation for key: "
                                               << key;
                                }
                                return;
                            }
                            value.append(buf.data, slice.size);
                            pinned_buffer_pool_->Release(std::move(buf));
                        } else {
                            value.append(static_cast<char*>(slice.ptr),
                                         slice.size);
                        }
                    }

                    // Async StoreObject + PutEnd (unchanged from original)
                    const uint64_t trace_id =
                        mooncake::logging::CurrentTraceId();
                    write_thread_pool_.enqueue([this,
                                                backend = storage_backend_, key,
                                                value = std::move(value), path,
                                                trace_id] {
                        mooncake::logging::ScopedTraceId trace(trace_id);
                        ReplicaType replica_type = ReplicaType::DISK;
                        // Store the object
                        auto store_result = backend->StoreObject(
                            path, value, key,
                            [this, replica_type](
                                const std::vector<std::string>& evicted_keys)
                                -> tl::expected<void, ErrorCode> {
                                auto evict_results =
                                    master_client_.BatchEvictDiskReplica(
                                        evicted_keys, replica_type);
                                if (evict_results.size() !=
                                    evicted_keys.size()) {
                                    return tl::make_unexpected(
                                        ErrorCode::INTERNAL_ERROR);
                                }
                                for (size_t i = 0; i < evict_results.size();
                                     ++i) {
                                    if (!evict_results[i]) {
                                        if (evict_results[i].error() ==
                                            ErrorCode::OBJECT_NOT_FOUND) {
                                            VLOG(1) << "Master no longer "
                                                       "tracks evicted key: "
                                                    << evicted_keys[i];
                                            continue;
                                        }
                                        LOG(WARNING)
                                            << "Failed to notify master about "
                                               "evicted key: "
                                            << evicted_keys[i] << ", error: "
                                            << evict_results[i].error();
                                        return tl::make_unexpected(
                                            evict_results[i].error());
                                    }
                                }
                                return {};
                            });

                        if (!store_result) {
                            // If storage failed, revoke the put operation
                            LOG(ERROR)
                                << "Failed to store object for key: " << key;
                            auto revoke_result =
                                master_client_.PutRevoke(key, replica_type);
                            if (!revoke_result) {
                                LOG(ERROR) << "Failed to revoke put operation "
                                              "for key: "
                                           << key;
                            }
                            return;
                        }

                        // If storage succeeded, end the put operation
                        auto end_result = master_client_.PutEnd(
                            ObjectMeta{key, std::nullopt}, replica_type);
                        if (!end_result) {
                            LOG(ERROR)
                                << "Failed to end put operation for key: "
                                << key;
                        }
                    });
                }

tl::expected<TransferFuture, ErrorCode> Client::SubmitSingleTransfer(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code, TransferIntent intent) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (replica_descriptor.is_vsegment_replica())
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    std::optional<TransferFuture> future;
    if (replica_descriptor.is_nof_replica()) {
        auto contiguous_range = GetContiguousSliceRange(slices);
        if (!contiguous_range.has_value()) {
            LOG(ERROR) << "NoF transfer requires contiguous slices";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        future = transfer_submitter_->submit(replica_descriptor, slices,
                                             op_code, contiguous_range->ptr,
                                             contiguous_range->size, intent);
    } else {
        future = transfer_submitter_->submit(replica_descriptor, slices,
                                             op_code, nullptr, 0, intent);
    }
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return tl::unexpected(ErrorCode::TRANSFER_FAIL);
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return std::move(*future);
}

ErrorCode Client::TransferData(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code, TransferIntent intent,
    std::set<std::string>* failed_endpoints,
    const std::vector<VSegmentLayoutEntry>* layouts) {
    if (!transfer_submitter_) return ErrorCode::INVALID_PARAMS;
    std::optional<TransferFuture> future;
    std::vector<std::string> endpoints;
    if (replica_descriptor.is_vsegment_replica()) {
        if (!vsegment_transfer_planner_) return ErrorCode::INVALID_PARAMS;
        std::vector<vsegment::ClientSlice> client_slices;
        client_slices.reserve(slices.size());
        for (const auto& slice : slices) {
            client_slices.push_back(
                {reinterpret_cast<uint64_t>(slice.ptr), slice.size});
        }
        auto plan = vsegment_transfer_planner_->Plan(
            replica_descriptor.get_vsegment_descriptor(), client_slices, layouts);
        if (!plan) return plan.error;
        for (const auto& request : plan.requests) endpoints.push_back(request.endpoint);
        future = transfer_submitter_->submitVSegment(plan, op_code);
    } else {
        auto submitted = SubmitSingleTransfer(replica_descriptor, slices, op_code, intent);
        if (!submitted) return submitted.error();
        future.emplace(std::move(*submitted));
    }
    if (!future) {
        if (failed_endpoints) failed_endpoints->insert(endpoints.begin(), endpoints.end());
        return ErrorCode::TRANSFER_FAIL;
    }
    auto result = future->get();
    if (result != ErrorCode::OK && failed_endpoints)
        failed_endpoints->insert(endpoints.begin(), endpoints.end());
    return result;
}

std::optional<TransferFuture> Client::SubmitRangeRead(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    return transfer_submitter_->submitRangeRead(
        replica_descriptor, slices, src_offset, TransferIntent::kForegroundGet);
}

std::optional<TransferFuture> Client::SubmitRangeWrite(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t dst_offset) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    return transfer_submitter_->submitRangeWrite(
        replica_descriptor, slices, dst_offset, TransferIntent::kUnspecified);
}

std::vector<tl::expected<int64_t, ErrorCode>> Client::BatchTransferReadRanges(
    const std::vector<Replica::Descriptor>& replicas,
    const std::vector<std::vector<Slice>>& slices,
    const std::vector<std::vector<uint64_t>>& src_offsets,
    TransferIntent intent, std::span<const QueryResult* const> query_results) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        replicas.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (replicas.size() != slices.size() ||
        replicas.size() != src_offsets.size() ||
        (!query_results.empty() && query_results.size() != replicas.size())) {
        LOG(ERROR) << "BatchTransferReadRanges size mismatch: replicas="
                   << replicas.size() << ", slices=" << slices.size()
                   << ", offsets=" << src_offsets.size();
        return results;
    }

    size_t fragment_count = 0;
    for (const auto& entry : slices) {
        fragment_count += entry.size();
    }

    auto select_replica = MakeReadReplicaSelector();
    // Every fragment of every entry goes into one scatter submit so the
    // transport sees the whole layer at once instead of one transfer per key.
    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(replicas.size());
    for (size_t i = 0; i < replicas.size(); ++i) {
        if (slices[i].size() != src_offsets[i].size()) {
            LOG(ERROR) << "BatchTransferReadRanges fragment count mismatch, "
                       << "entry=" << i << ", slices=" << slices[i].size()
                       << ", offsets=" << src_offsets[i].size();
            continue;  // results[i] stays INVALID_PARAMS
        }
        if (!replicas[i].is_memory_replica()) {
            LOG(ERROR) << "Range read requires a memory replica, entry=" << i;
            continue;
        }

        Replica::Descriptor selected;
        const auto* selected_replica = &replicas[i];
        int64_t transferred = 0;
        for (size_t j = 0; j < slices[i].size(); ++j) {
            if (!query_results.empty() && query_results[i]) {
                if (select_replica(query_results[i]->replicas,
                                   std::span<const Slice>(&slices[i][j], 1),
                                   selected) != ErrorCode::OK ||
                    !selected.is_memory_replica()) {
                    entry_errors[i] = ErrorCode::INVALID_REPLICA;
                    break;
                }
                selected_replica = &selected;
            }
            const auto& handle =
                selected_replica->get_memory_descriptor().buffer_descriptor;
            builder.Add(TransferRequest::READ, handle, slices[i][j],
                        src_offsets[i][j], &entry_errors[i]);
            transferred += static_cast<int64_t>(slices[i][j].size);
        }
        results[i] = entry_errors[i]
                         ? tl::expected<int64_t, ErrorCode>(
                               tl::unexpected(*entry_errors[i]))
                         : tl::expected<int64_t, ErrorCode>(transferred);
    }

    CompleteScatterRanges(*this, builder, results, entry_errors,
                          "Failed to submit batch range read",
                          "Range read failed");
    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>>
Client::BatchTransferReadOffloadRanges(
    const std::string& transfer_engine_addr,
    const std::vector<uint64_t>& remote_bases,
    const std::vector<size_t>& remote_sizes,
    const std::vector<std::vector<Slice>>& slices,
    const std::vector<std::vector<uint64_t>>& src_offsets) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        remote_bases.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (remote_bases.size() != remote_sizes.size() ||
        remote_bases.size() != slices.size() ||
        remote_bases.size() != src_offsets.size()) {
        LOG(ERROR) << "BatchTransferReadOffloadRanges size mismatch: bases="
                   << remote_bases.size() << ", sizes=" << remote_sizes.size()
                   << ", slices=" << slices.size()
                   << ", offsets=" << src_offsets.size();
        return results;
    }

    size_t fragment_count = 0;
    for (const auto& entry : slices) {
        fragment_count += entry.size();
    }

    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(remote_bases.size());
    for (size_t i = 0; i < remote_bases.size(); ++i) {
        if (slices[i].size() != src_offsets[i].size()) {
            LOG(ERROR)
                << "BatchTransferReadOffloadRanges fragment count mismatch, "
                << "entry=" << i << ", slices=" << slices[i].size()
                << ", offsets=" << src_offsets[i].size();
            continue;
        }
        int64_t transferred = 0;
        for (size_t j = 0; j < slices[i].size(); ++j) {
            builder.Add(TransferRequest::READ, transfer_engine_addr,
                        remote_bases[i], remote_sizes[i], slices[i][j],
                        src_offsets[i][j], &entry_errors[i]);
            transferred += static_cast<int64_t>(slices[i][j].size);
        }
        results[i] = transferred;  // optimistic; corrected on await
    }

    if (builder.empty()) {
        return results;
    }

    auto operation = SubmitScatterNative(builder.ranges());
    if (!operation) {
        LOG(ERROR) << "Failed to submit batch offload range read";
        for (auto& result : results) {
            if (result.has_value()) {
                result = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
        }
        return results;
    }
    const auto status = operation->wait();
    if (!status.ok()) {
        LOG(ERROR) << "Batch offload range read scatter operation failed: "
                   << status.ToString();
        MarkScatterOperationFailure(results, entry_errors, status);
    }

    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value() || !entry_errors[i].has_value()) {
            continue;
        }
        LOG(ERROR) << "Offload range read failed, entry=" << i
                   << ", error=" << static_cast<int>(entry_errors[i].value());
        results[i] = tl::unexpected(entry_errors[i].value());
    }
    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>> Client::BatchTransferWriteRanges(
    const std::vector<std::vector<Replica::Descriptor>>& replicas_per_entry,
    const std::vector<std::vector<Slice>>& slices,
    const std::vector<std::vector<uint64_t>>& dst_offsets,
    TransferIntent intent) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        replicas_per_entry.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (replicas_per_entry.size() != slices.size() ||
        replicas_per_entry.size() != dst_offsets.size()) {
        LOG(ERROR) << "BatchTransferWriteRanges size mismatch: entries="
                   << replicas_per_entry.size() << ", slices=" << slices.size()
                   << ", offsets=" << dst_offsets.size();
        return results;
    }

    size_t fragment_count = 0;
    for (size_t i = 0; i < replicas_per_entry.size(); ++i) {
        for (const auto& replica : replicas_per_entry[i]) {
            if (replica.is_memory_replica()) {
                fragment_count += slices[i].size();
            }
        }
    }

    // One scatter submit covers every replica of every entry, so replication
    // fans out inside a single transfer instead of one submit per fragment.
    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(
        replicas_per_entry.size());
    for (size_t i = 0; i < replicas_per_entry.size(); ++i) {
        if (slices[i].size() != dst_offsets[i].size()) {
            LOG(ERROR) << "BatchTransferWriteRanges fragment count mismatch, "
                       << "entry=" << i << ", slices=" << slices[i].size()
                       << ", offsets=" << dst_offsets[i].size();
            continue;  // results[i] stays INVALID_PARAMS
        }

        // Logical bytes: counted once per fragment, not per replica.
        int64_t transferred = 0;
        for (const auto& slice : slices[i]) {
            transferred += static_cast<int64_t>(slice.size);
        }
        bool submitted = false;
        for (const auto& replica : replicas_per_entry[i]) {
            if (!replica.is_memory_replica()) {
                continue;
            }
            const auto& handle =
                replica.get_memory_descriptor().buffer_descriptor;
            for (size_t j = 0; j < slices[i].size(); ++j) {
                builder.Add(TransferRequest::WRITE, handle, slices[i][j],
                            dst_offsets[i][j], &entry_errors[i]);
                submitted = true;
            }
        }
        if (!submitted) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        results[i] = transferred;  // optimistic; corrected on await
    }

    if (builder.empty()) {
        return results;
    }

    auto operation = SubmitScatterNative(builder.ranges(), intent);
    if (!operation) {
        LOG(ERROR) << "Failed to submit batch range write";
        for (auto& result : results) {
            if (result.has_value()) {
                result = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
        }
        return results;
    }
    const auto status = operation->wait();
    if (!status.ok()) {
        LOG(ERROR) << "Batch range write scatter operation failed: "
                   << status.ToString();
        MarkScatterOperationFailure(results, entry_errors, status);
    }

    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value() || !entry_errors[i].has_value()) {
            continue;
        }
        LOG(ERROR) << "Range write failed, entry=" << i
                   << ", error=" << static_cast<int>(entry_errors[i].value());
        results[i] = tl::unexpected(entry_errors[i].value());
    }
    return results;
}


                ErrorCode Client::TransferReadInternal(
                    const Replica::Descriptor& replica_descriptor,
                    std::vector<Slice>& slices,
                    uint64_t src_offset,
                    const std::vector<VSegmentLayoutEntry>* layouts) {
                    if (!transfer_submitter_) {
                        LOG(ERROR) << "TransferSubmitter not initialized";
                        return ErrorCode::INVALID_PARAMS;
                    }

                    if (replica_descriptor.is_vsegment_replica()) {
                        if (!vsegment_transfer_planner_)
                            return ErrorCode::INVALID_PARAMS;
                        auto replica =
                            replica_descriptor.get_vsegment_descriptor();
                        uint64_t length = 0;
                        std::vector<vsegment::ClientSlice> client_slices;
                        client_slices.reserve(slices.size());
                        for (const auto& slice : slices) {
                            if (slice.size >
                                std::numeric_limits<uint64_t>::max() - length)
                                return ErrorCode::INVALID_PARAMS;
                            length += slice.size;
                            client_slices.push_back(
                                {reinterpret_cast<uint64_t>(slice.ptr),
                                 slice.size});
                        }
                        if (src_offset > replica.length ||
                            length > replica.length - src_offset)
                            return ErrorCode::INVALID_PARAMS;
                        if (src_offset > std::numeric_limits<uint64_t>::max() -
                                             replica.logical_offset)
                            return ErrorCode::INVALID_PARAMS;
                        replica.logical_offset += src_offset;
                        replica.length = length;
                        SpDiag::PerfPoint pt_vsegment_plan(
                            PerfKey::VSEGMENT_READ_PLAN,
                            SpDiag::PerfLevel::MODULE);
                        pt_vsegment_plan.Start();
                        auto plan = vsegment_transfer_planner_->Plan(
                            replica, client_slices, layouts);
                        pt_vsegment_plan.End(plan ? 0 : -1);
                        if (!plan) return plan.error;
                        SpDiag::PerfPoint pt_vsegment_submit(
                            PerfKey::VSEGMENT_READ_SUBMIT,
                            SpDiag::PerfLevel::MODULE);
                        pt_vsegment_submit.Start();
                        auto future = transfer_submitter_->submitVSegment(
                            plan, TransferRequest::READ);
                        pt_vsegment_submit.End(future ? 0 : -1);
                        if (!future) return ErrorCode::TRANSFER_FAIL;
                        SpDiag::PerfPoint pt_vsegment_wait(
                            PerfKey::VSEGMENT_READ_WAIT,
                            SpDiag::PerfLevel::MODULE);
                        pt_vsegment_wait.Start();
                        const ErrorCode result = future->get();
                        pt_vsegment_wait.End(result == ErrorCode::OK ? 0 : -1);
                        return result;
                    }

                    auto future = SubmitRangeRead(replica_descriptor, slices, src_offset);
                    if (!future) {
                        LOG(ERROR) << "Failed to submit range read operation";
                        return ErrorCode::TRANSFER_FAIL;
                    }

                    VLOG(1)
                        << "Using transfer strategy: " << future->strategy();

                    return future->get();
                }

                std::vector<std::pair<ReplicaType, ErrorCode>>
                Client::WriteMemoryAndNofReplicas(
                    const std::vector<Replica::Descriptor>& replicas,
                    std::vector<Slice>& slices,
                    std::set<std::string>* failed_endpoints,
                    const std::vector<VSegmentLayoutEntry>* layouts) {
                    // Preserve concurrent submission for direct memory and NoF;
                    // VSegment uses its expanded layout-aware transfer plan.
                    std::vector<std::pair<ReplicaType,
                        tl::expected<TransferFuture, ErrorCode>>> pending;
                    std::vector<std::pair<ReplicaType, ErrorCode>> results;
                    for (const auto& replica : replicas) {
                        if (replica.is_vsegment_replica()) {
                            results.emplace_back(
                                ReplicaType::MEMORY,
                                TransferData(replica, slices, TransferRequest::WRITE,
                                             TransferIntent::kUnspecified,
                                             failed_endpoints, layouts));
                        } else if (replica.is_memory_replica() ||
                                   replica.is_nof_replica()) {
                            pending.emplace_back(
                                replica.is_memory_replica() ? ReplicaType::MEMORY
                                                            : ReplicaType::NOF_SSD,
                                SubmitSingleTransfer(replica, slices,
                                                     TransferRequest::WRITE));
                        }
                    }
                    for (auto& [type, future] : pending) {
                        results.emplace_back(type, future ? future->get() : future.error());
                    }
                    return results;
                }

                ErrorCode Client::TransferWrite(
                    const Replica::Descriptor& replica_descriptor,
                    std::vector<Slice>& slices, TransferIntent intent,
                    std::set<std::string>* failed_endpoints,
                    const std::vector<VSegmentLayoutEntry>* layouts) {
                    return TransferData(replica_descriptor, slices,
                                        TransferRequest::WRITE, intent,
                                        failed_endpoints, layouts);
                }

                ErrorCode Client::TransferWriteRange(
                    const Replica::Descriptor& replica_descriptor,
                    std::vector<Slice>& slices, uint64_t dst_offset) {
                    auto future = SubmitRangeWrite(replica_descriptor, slices,
                                                   dst_offset);
                    if (!future) {
                        LOG(ERROR) << "Failed to submit range write operation";
                        return ErrorCode::TRANSFER_FAIL;
                    }

                    VLOG(1)
                        << "Using transfer strategy: " << future->strategy();
                    return future->get();
                }

                ErrorCode Client::TransferRead(
                    const Replica::Descriptor& replica_descriptor,
                    std::vector<Slice>& slices, TransferIntent intent,
                    const std::vector<VSegmentLayoutEntry>* layouts) {
                    SpDiag::PerfPoint pt_read(PerfKey::GET_SINGLE_TRANSFER_READ,
                                             SpDiag::PerfLevel::KEY_MODULE);
                    pt_read.Start();
                    size_t total_size = 0;
                    if (replica_descriptor.is_memory_replica()) {
                        auto& mem_desc =
                            replica_descriptor.get_memory_descriptor();
                        total_size = mem_desc.buffer_descriptor.size_;
                    } else if (replica_descriptor.is_nof_replica()) {
                        auto& nof_desc =
                            replica_descriptor.get_nof_descriptor();
                        total_size = nof_desc.buffer_descriptor.size_;
                    } else if (replica_descriptor.is_disk_replica()) {
                        auto& disk_desc =
                            replica_descriptor.get_disk_descriptor();
                        total_size = disk_desc.object_size;
                    } else if (replica_descriptor.is_local_disk_replica()) {
                        auto& disk_desc =
                            replica_descriptor.get_local_disk_descriptor();
                        total_size = disk_desc.object_size;
                    } else if (replica_descriptor.is_dfs_replica()) {
                        total_size =
                            replica_descriptor.get_dfs_descriptor().object_size;
                    } else if (replica_descriptor.is_vsegment_replica()) {
                        total_size =
                            replica_descriptor.get_vsegment_descriptor().length;
                    }

                    size_t slices_size = CalculateSliceSize(slices);
                    if (slices_size < total_size) {
                        LOG(ERROR) << "Slice size " << slices_size
                                   << " is smaller than total "
                                   << "size " << total_size;
                        pt_read.End(-1);
                        return ErrorCode::INVALID_PARAMS;
                    }

                    if (replica_descriptor.is_dfs_replica()) {
                        LOG(ERROR) << "DFS reads require object key context";
                        return ErrorCode::INVALID_REPLICA;
                    }

                    const ErrorCode read_result = TransferData(
                        replica_descriptor, slices, TransferRequest::READ, intent,
                        nullptr, layouts);
                    pt_read.End(read_result == ErrorCode::OK ? 0 : -1);
                    return read_result;
                }

                ErrorCode Client::ReadDfsReplica(
                    const std::string& key,
                    const Replica::Descriptor& replica_descriptor,
                    std::vector<Slice>& slices) {
                    if (!replica_descriptor.is_dfs_replica()) {
                        return ErrorCode::INVALID_REPLICA;
                    }
                    if (!dfs_storage_backend_) {
                        LOG(ERROR) << "DFS backend is not initialized";
                        if (auto* dfs_metric = GetDfsMetricPtr()) {
                            dfs_metric->RecordReadErrors(
                                toString(ErrorCode::DFS_SERVICE_UNAVAILABLE));
                        }
                        return ErrorCode::DFS_SERVICE_UNAVAILABLE;
                    }

                    auto* dfs_metric = GetDfsMetricPtr();
                    const auto& desc = replica_descriptor.get_dfs_descriptor();
                    std::vector<DfsReadRequest> requests{
                        DfsReadRequest{key, desc, slices}};
                    const auto read_start = std::chrono::steady_clock::now();
                    auto results = dfs_storage_backend_->BatchRead(requests);
                    const auto read_latency_us = elapsed_us_since(read_start);
                    if (results.size() != 1) {
                        LOG(ERROR)
                            << "DFS BatchRead response size mismatch for key "
                            << key;
                        if (dfs_metric) {
                            dfs_metric->RecordReadErrors(
                                toString(ErrorCode::INTERNAL_ERROR));
                        }
                        return ErrorCode::INTERNAL_ERROR;
                    }
                    if (!results[0]) {
                        LOG(ERROR)
                            << "DFS read failed, action=read, key=" << key
                            << ", offset=" << desc.offset
                            << ", size=" << desc.object_size
                            << ", latency_us=" << read_latency_us
                            << ", error=" << results[0].error();
                        if (dfs_metric) {
                            dfs_metric->RecordReadErrors(
                                toString(results[0].error()));
                        }
                        return results[0].error();
                    }
                    if (dfs_metric) {
                        dfs_metric->ObserveRead(
                            1, static_cast<int64_t>(desc.object_size),
                            read_latency_us);
                    }
                    VLOG(1) << "DFS read finished, action=read, key=" << key
                            << ", offset=" << desc.offset
                            << ", size=" << desc.object_size
                            << ", latency_us=" << read_latency_us;
                    return ErrorCode::OK;
                }

                void Client::ReconcileMissingKvcsReplica(
                    const std::string& key, const QueryResult& query_result,
                    const Replica::Descriptor& replica_descriptor) {
                    if (!dfs_storage_backend_ ||
                        !dfs_storage_backend_->UsesKvcs() ||
                        !replica_descriptor.is_dfs_replica()) {
                        return;
                    }
                    if (!query_result.resolved_tenant_id.has_value()) {
                        LOG(WARNING)
                            << "Cannot reconcile missing KVCS replica without "
                               "resolved tenant, key="
                            << key;
                        return;
                    }

                    auto evicted = master_client_.EvictDiskReplicaIfCurrent(
                        key, *query_result.resolved_tenant_id, ReplicaType::DFS,
                        replica_descriptor.id);
                    if (!evicted) {
                        LOG(WARNING)
                            << "Failed to reconcile missing KVCS replica, key="
                            << key << ", replica_id=" << replica_descriptor.id
                            << ", error=" << toString(evicted.error());
                    } else if (!*evicted) {
                        VLOG(1)
                            << "KVCS replica was replaced before miss "
                               "reconciliation, key="
                            << key << ", replica_id=" << replica_descriptor.id;
                    }
                }

                std::vector<tl::expected<void, ErrorCode>>
                Client::BatchReadFileReplicaRanges(
                    const std::vector<std::string>& keys,
                    const std::vector<Replica::Descriptor>& replicas,
                    const std::vector<Slice>& windows,
                    const std::vector<uint64_t>& value_offsets) {
                    std::vector<tl::expected<void, ErrorCode>> results(
                        keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
                    if (keys.size() != replicas.size() ||
                        keys.size() != windows.size() ||
                        keys.size() != value_offsets.size()) {
                        LOG(ERROR)
                            << "BatchReadFileReplicaRanges size mismatch";
                        return results;
                    }

                    std::vector<DfsReadRequest> dfs_requests;
                    std::vector<size_t> dfs_indexes;
                    for (size_t i = 0; i < keys.size(); ++i) {
                        if (windows[i].size >
                            static_cast<size_t>(
                                std::numeric_limits<int64_t>::max())) {
                            continue;
                        }
                        if (replicas[i].is_disk_replica()) {
                            if (!storage_backend_) {
                                LOG(ERROR)
                                    << "Storage backend is not initialized";
                                results[i] =
                                    tl::unexpected(ErrorCode::INTERNAL_ERROR);
                                continue;
                            }
                            if (value_offsets[i] >
                                static_cast<uint64_t>(
                                    std::numeric_limits<int64_t>::max())) {
                                continue;
                            }
                            std::vector<Slice> slices{windows[i]};
                            auto loaded = storage_backend_->LoadObject(
                                replicas[i].get_disk_descriptor().file_path,
                                slices, static_cast<int64_t>(windows[i].size),
                                static_cast<int64_t>(value_offsets[i]));
                            results[i] = loaded
                                             ? tl::expected<void, ErrorCode>{}
                                             : tl::unexpected(loaded.error());
                            continue;
                        }
                        if (!replicas[i].is_dfs_replica()) {
                            results[i] =
                                tl::unexpected(ErrorCode::INVALID_REPLICA);
                            continue;
                        }
                        if (!dfs_storage_backend_) {
                            LOG(ERROR) << "DFS backend is not initialized";
                            results[i] = tl::unexpected(
                                ErrorCode::DFS_SERVICE_UNAVAILABLE);
                            if (auto* dfs_metric = GetDfsMetricPtr()) {
                                dfs_metric->RecordReadErrors(toString(
                                    ErrorCode::DFS_SERVICE_UNAVAILABLE));
                            }
                            continue;
                        }
                        dfs_requests.push_back(
                            DfsReadRequest{keys[i],
                                           replicas[i].get_dfs_descriptor(),
                                           {windows[i]},
                                           value_offsets[i],
                                           true});
                        dfs_indexes.push_back(i);
                        results[i] = {};
                    }

                    if (dfs_requests.empty()) return results;

                    // Same counters as Get/BatchGet. Bytes are the window that
                    // was read, not the full object: a layer transfers only
                    // that window.
                    auto* dfs_metric = GetDfsMetricPtr();
                    const auto dfs_read_start =
                        std::chrono::steady_clock::now();
                    auto dfs_results =
                        dfs_storage_backend_->BatchRead(dfs_requests);
                    const auto dfs_read_latency_us =
                        elapsed_us_since(dfs_read_start);
                    if (dfs_results.size() != dfs_requests.size()) {
                        LOG(ERROR)
                            << "DFS BatchRead response size mismatch: expected "
                            << dfs_requests.size() << ", got "
                            << dfs_results.size();
                        for (size_t index : dfs_indexes) {
                            results[index] =
                                tl::unexpected(ErrorCode::INTERNAL_ERROR);
                        }
                        if (dfs_metric) {
                            dfs_metric->RecordReadErrors(
                                toString(ErrorCode::INTERNAL_ERROR),
                                static_cast<int64_t>(dfs_indexes.size()));
                        }
                        return results;
                    }

                    int64_t dfs_success_keys = 0;
                    int64_t dfs_success_bytes = 0;
                    for (size_t j = 0; j < dfs_results.size(); ++j) {
                        const size_t index = dfs_indexes[j];
                        if (!dfs_results[j]) {
                            LOG(ERROR) << "DFS range read failed, "
                                          "action=range_read, key="
                                       << keys[index]
                                       << ", error=" << dfs_results[j].error();
                            results[index] =
                                tl::unexpected(dfs_results[j].error());
                            if (dfs_metric) {
                                dfs_metric->RecordReadErrors(
                                    toString(dfs_results[j].error()));
                            }
                            continue;
                        }
                        ++dfs_success_keys;
                        dfs_success_bytes +=
                            static_cast<int64_t>(windows[index].size);
                    }
                    if (dfs_metric && dfs_success_keys > 0) {
                        dfs_metric->ObserveRead(dfs_success_keys,
                                                dfs_success_bytes,
                                                dfs_read_latency_us);
                    }
                    return results;
                }

                ErrorCode Client::TransferReadRange(
                    const Replica::Descriptor& replica_descriptor,
                    std::vector<Slice>& slices, uint64_t src_offset,
                    const std::vector<VSegmentLayoutEntry>* layouts) {
                    return TransferReadInternal(replica_descriptor, slices,
                                                src_offset, layouts);
                }

                void Client::PollAndDispatchTasks() {
                    if (task_running_.load()) {
                        auto fetch_result = FetchTasks(kTaskBatchSize);
                        if (fetch_result.has_value()) {
                            const auto& tasks = fetch_result.value();
                            if (!tasks.empty()) {
                                LOG(INFO) << "action=task_poll_success"
                                          << ", task_count=" << tasks.size();
                                for (const auto& task_assignment : tasks) {
                                    SubmitTask(task_assignment);
                                }
                            }
                        } else {
                            ErrorCode error = fetch_result.error();
                            // Only log if it's not an RPC failure (which is
                            // expected during connection failures)
                            if (error != ErrorCode::RPC_FAIL) {
                                LOG(WARNING) << "action=task_poll_failed"
                                             << ", error_code=" << error;
                            }
                        }
                    }
                }

                void Client::TaskPollThreadMain() {
                    const auto poll_interval = std::chrono::milliseconds(1000);

                    while (task_poll_running_.load()) {
                        PollAndDispatchTasks();
                        std::this_thread::sleep_for(poll_interval);
                    }
                }

                void Client::SubmitTask(const TaskAssignment& assignment) {
                    if (!task_running_.load()) {
                        LOG(WARNING) << "action=task_rejected"
                                     << ", task_id=" << assignment.id
                                     << ", reason=executor_stopped";
                        return;
                    }

                    // Construct ClientTask from TaskAssignment
                    ClientTask client_task;
                    client_task.assignment = assignment;
                    client_task.retry_count = 0;

                    task_thread_pool_.enqueue(
                        [this, client_task]() { ExecuteTask(client_task); });
                }

                void Client::ExecuteTask(const ClientTask& client_task) {
                    const auto& assignment = client_task.assignment;
                    ErrorCode result = ErrorCode::OK;

                    try {
                        switch (assignment.type) {
                            case TaskType::REPLICA_COPY: {
                                ReplicaCopyPayload payload;
                                struct_json::from_json(payload,
                                                       assignment.payload);
                                const UUID dynamic_lease_id{
                                    payload.dynamic_replication_lease_id_high,
                                    payload.dynamic_replication_lease_id_low};
                                auto copy_result = Copy(
                                    payload.key, payload.tenant_id,
                                    payload.source, payload.targets,
                                    dynamic_lease_id,
                                    payload.dynamic_replication_version_epoch);
                                if (copy_result.has_value()) {
                                    result = ErrorCode::OK;
                                } else {
                                    result = copy_result.error();
                                }
                                break;
                            }
                            case TaskType::REPLICA_MOVE: {
                                ReplicaMovePayload payload;
                                struct_json::from_json(payload,
                                                       assignment.payload);
                                auto move_result =
                                    Move(payload.key, payload.tenant_id,
                                         payload.source, payload.target);
                                if (move_result.has_value()) {
                                    result = ErrorCode::OK;
                                } else {
                                    result = move_result.error();
                                }
                                break;
                            }
                            default:
                                LOG(ERROR) << "action=task_execution_failed"
                                           << ", task_id=" << assignment.id
                                           << ", error=unknown_task_type"
                                           << ", task_type=" << assignment.type;
                                result = ErrorCode::INVALID_PARAMS;
                                break;
                        }
                    } catch (const std::exception& e) {
                        LOG(ERROR) << "action=task_execution_failed"
                                   << ", task_id=" << assignment.id
                                   << ", error=exception"
                                   << ", exception=" << e.what();
                        result = ErrorCode::INTERNAL_ERROR;
                    }

                    if (result == ErrorCode::OK) {
                        TaskCompleteRequest complete_request;
                        complete_request.id = assignment.id;
                        complete_request.status = TaskStatus::SUCCESS;
                        complete_request.message =
                            "Task completed successfully";
                        auto complete_result =
                            master_client_.MarkTaskToComplete(complete_request);
                        if (!complete_result.has_value()) {
                            LOG(WARNING)
                                << "action=task_complete_failed"
                                << ", task_id=" << assignment.id
                                << ", error_code=" << complete_result.error();
                        }
                    } else {
                        uint32_t current_retry_count = client_task.retry_count;
                        // Only retry on allocation failures
                        // (NO_AVAILABLE_HANDLE) Other errors (e.g.,
                        // OBJECT_NOT_FOUND, REPLICA_NOT_FOUND) should not be
                        // retried
                        bool should_retry =
                            (result == ErrorCode::NO_AVAILABLE_HANDLE) &&
                            (current_retry_count <
                             assignment.max_retry_attempts);

                        if (should_retry) {
                            ClientTask retry_task = client_task;
                            retry_task.increment_retry();

                            const auto retry_delay = std::chrono::milliseconds(
                                50 * (current_retry_count + 1));
                            std::this_thread::sleep_for(retry_delay);

                            LOG(WARNING)
                                << "action=task_execution_failed_retry"
                                << ", task_id=" << assignment.id
                                << ", error_code=" << result
                                << ", retry_count=" << current_retry_count
                                << ", max_retry_count="
                                << assignment.max_retry_attempts
                                << ", will_retry=true"
                                << ", retry_delay=" << retry_delay.count()
                                << "ms";

                            task_thread_pool_.enqueue([this, retry_task]() {
                                ExecuteTask(retry_task);
                            });
                        } else {
                            LOG(ERROR)
                                << "action=task_execution_failed"
                                << ", task_id=" << assignment.id
                                << ", error_code=" << result
                                << ", retry_count=" << current_retry_count
                                << ", max_retry_count="
                                << assignment.max_retry_attempts;
                            TaskCompleteRequest complete_request;
                            complete_request.id = assignment.id;
                            complete_request.status = TaskStatus::FAILED;
                            complete_request.message =
                                toString(result) + " (max retries reached: " +
                                std::to_string(assignment.max_retry_attempts) +
                                ")";
                            auto complete_result =
                                master_client_.MarkTaskToComplete(
                                    complete_request);
                            if (!complete_result.has_value()) {
                                LOG(WARNING) << "action=task_complete_failed"
                                             << ", task_id=" << assignment.id
                                             << ", error_code="
                                             << complete_result.error();
                            }
                        }
                    }
                }

                void Client::HeartbeatAllSubmasters() {
                    std::vector<std::string> targets;
                    {
                        std::lock_guard<std::mutex> lock(
                            cvm_submaster_addresses_mutex_);
                        targets = cvm_submaster_addresses_;
                    }
                    if (targets.empty()) {
                        return;  // 单 master/直连模式，无多 submaster 心跳
                    }

                    // 去重：当前 active master 由主循环 master_client_.Ping()
                    // 单独 ping， 这里跳过，避免同一 submaster 被重复 ping。
                    const std::string active_address =
                        master_client_.GetCurrentAddress();

                    for (const auto& addr : targets) {
                        if (addr == active_address) {
                            continue;
                        }
                        auto ping_result = master_client_.PingTo(addr);
                        if (!ping_result) {
                            VLOG(1) << "HeartbeatAllSubmasters: ping submaster "
                                       "failed addr="
                                    << addr << " error=" << ping_result.error();
                            continue;
                        }
                        if (ping_result.value().client_status ==
                            ClientStatus::NEED_REMOUNT) {
                            // 该 submaster 侧没有本 client 的
                            // segment（可能被误判过期卸载）， 重新 mount
                            // 所有本地 segment 到该 submaster，恢复其分配能力。
                            LOG(INFO) << "HeartbeatAllSubmasters: submaster "
                                      << addr << " requires remount";
                            std::lock_guard<std::mutex> lock(
                                mounted_segments_mutex_);
                            for (const auto& [seg_id, segment] :
                                 mounted_segments_) {
                                auto r = master_client_.MountSegmentTo(addr,
                                                                       segment);
                                if (!r) {
                                    LOG(WARNING) << "remount_segment_to_"
                                                    "submaster_failed addr="
                                                 << addr << " id=" << seg_id
                                                 << " error=" << r.error();
                                }
                            }
                        }
                    }
                }

                void Client::RefreshSubmasterAddresses() {
                    // 仅在 CVM 多 submaster 模式下有意义（有已知 namespace）。
                    if (cvm_cluster_namespace_.empty()) {
                        return;
                    }

                    std::vector<cvm::MasterRegistration> masters;
                    ViewVersionId version = 0;
                    ErrorCode err = cvm::EtcdViewStore::LoadAllMasters(
                        cvm_cluster_namespace_, masters, version);
                    if (err != ErrorCode::OK) {
                        LOG(WARNING) << "RefreshSubmasterAddresses: "
                                        "LoadAllMasters failed: "
                                     << err;
                        return;
                    }

                    std::vector<std::string> new_addresses;
                    for (const auto& reg : masters) {
                        if (reg.role == static_cast<int32_t>(
                                            cvm::MasterRole::kPrimary) &&
                            !reg.address.empty()) {
                            new_addresses.push_back(reg.address);
                        }
                    }
                    std::sort(new_addresses.begin(), new_addresses.end());
                    // 去重：与 ConnectToCvmSubmasters 保持一致，避免重复
                    // primary 注册导致 全量 mount 对同一 submaster 重复调用。
                    new_addresses.erase(
                        std::unique(new_addresses.begin(), new_addresses.end()),
                        new_addresses.end());

                    // 若本轮扫描为空（etcd 抖动或所有 primary
                    // 异常），保持现有地址列表不变
                    // （sticky），避免短暂异常清空地址列表导致心跳与全量 mount
                    // 退化。
                    if (new_addresses.empty()) {
                        LOG(WARNING) << "RefreshSubmasterAddresses: no primary "
                                        "masters found, "
                                        "keeping existing addresses";
                        return;
                    }

                    // 对比现有地址，找出新增/移除的
                    // submaster；同时用最新结果替换地址列表。
                    std::vector<std::string> added;
                    std::vector<std::string> removed;
                    {
                        std::lock_guard<std::mutex> lock(
                            cvm_submaster_addresses_mutex_);
                        for (const auto& addr : new_addresses) {
                            if (std::find(cvm_submaster_addresses_.begin(),
                                          cvm_submaster_addresses_.end(),
                                          addr) ==
                                cvm_submaster_addresses_.end()) {
                                added.push_back(addr);
                            }
                        }
                        for (const auto& addr : cvm_submaster_addresses_) {
                            if (std::find(new_addresses.begin(),
                                          new_addresses.end(),
                                          addr) == new_addresses.end()) {
                                removed.push_back(addr);
                            }
                        }
                        cvm_submaster_addresses_ = std::move(new_addresses);
                    }

                    // 对新增的 submaster 全量 mount 所有本地
                    // segment，使其具备分配能力。
                    if (!added.empty()) {
                        std::lock_guard<std::mutex> lock(
                            mounted_segments_mutex_);
                        for (const auto& addr : added) {
                            LOG(INFO)
                                << "RefreshSubmasterAddresses: new submaster "
                                << addr << ", mounting all local segments";
                            for (const auto& [seg_id, segment] :
                                 mounted_segments_) {
                                auto r = master_client_.MountSegmentTo(addr,
                                                                       segment);
                                if (!r) {
                                    LOG(WARNING) << "mount_segment_to_new_"
                                                    "submaster_failed addr="
                                                 << addr << " id=" << seg_id
                                                 << " error=" << r.error();
                                }
                            }
                            // LOCAL_DISK 段对称补挂：新 submaster 上线后其 slot
                            // 的 offload 决策需要本 client 的
                            // LocalDiskSegment；不补挂则首个心跳周期 内该
                            // master 的任务拉取会
                            // SEGMENT_NOT_FOUND（靠定向恢复兜底，
                            // 延迟一个心跳周期）。补挂后重报容量，恢复其 SSD
                            // 记账分母。
                            if (local_disk_mounted_.load()) {
                                const bool enable_offloading =
                                    local_disk_offload_enabled_.load();
                                auto r = master_client_.MountLocalDiskSegmentTo(
                                    addr, client_id_, enable_offloading);
                                if (!r) {
                                    LOG(WARNING) << "mount_local_disk_segment_"
                                                    "to_new_submaster_failed"
                                                 << " addr=" << addr
                                                 << " error=" << r.error();
                                } else {
                                    const int64_t capacity =
                                        ssd_total_capacity_bytes_.load();
                                    if (capacity > 0) {
                                        (void)
                                            master_client_.ReportSsdCapacityTo(
                                                addr, client_id_, capacity);
                                    }
                                }
                            }
                        }
                    }

                    // 对被移除的 submaster 全量 unmount，保证 segment
                    // 生命周期对称闭合。 单个失败仅告警不阻断（其 lease
                    // 最终会过期回收）。
                    if (!removed.empty()) {
                        std::lock_guard<std::mutex> lock(
                            mounted_segments_mutex_);
                        for (const auto& addr : removed) {
                            LOG(INFO)
                                << "RefreshSubmasterAddresses: submaster "
                                   "removed "
                                << addr << ", unmounting all local segments";
                            for (const auto& [seg_id, segment] :
                                 mounted_segments_) {
                                auto r = master_client_.UnmountSegmentTo(
                                    addr, segment.id);
                                if (!r) {
                                    LOG(WARNING)
                                        << "unmount_segment_from_removed_"
                                           "submaster_failed addr="
                                        << addr << " id=" << seg_id
                                        << " error=" << r.error();
                                }
                            }
                            // LOCAL_DISK 段对称卸载：被移除的 submaster
                            // 不再是任何 slot 的
                            // owner，其容量记账若不清理将永久泄漏（重复计数会污染
                            // SSD_FREE_RATIO_FIRST 分配策略）。
                            if (local_disk_mounted_.load()) {
                                auto r =
                                    master_client_.UnmountLocalDiskSegmentTo(
                                        addr, client_id_);
                                if (!r) {
                                    LOG(WARNING)
                                        << "unmount_local_disk_segment_from_"
                                           "removed_submaster"
                                        << "_failed addr=" << addr
                                        << " error=" << r.error();
                                }
                            }
                        }
                    }
                }

                void Client::StorageHeartbeatThreadMain() {
                    // How many failed pings before reconnecting via the HA
                    // coordinator
                    const int max_ping_fail_count = 3;
                    // How long to wait for next ping after success
                    const int success_ping_interval_ms = 1000;
                    // How long to wait for next ping after failure
                    const int fail_ping_interval_ms = 1000;
                    // Increment after a ping failure, reset after a ping
                    // success
                    int ping_fail_count = 0;

                    auto remount_segment = [this]() {
                        // This lock must be held until the remount rpc is
                        // finished, otherwise there will be corner cases, e.g.,
                        // a segment is unmounted successfully first, and then
                        // remounted again in this thread.
                        std::lock_guard<std::mutex> lock(
                            mounted_segments_mutex_);
                        std::vector<Segment> segments;
                        for (auto it : mounted_segments_) {
                            auto& segment = it.second;
                            segments.emplace_back(segment);
                        }
                        auto remount_result =
                            master_client_.ReMountSegment(segments);
                        if (!remount_result) {
                            ErrorCode err = remount_result.error();
                            LOG(ERROR) << "Failed to remount segments: " << err;
                        }
                        // Re-publish Transfer Engine segment descriptors to the
                        // HTTP metadata server.  When Master (which hosts the
                        // HTTP metadata server in the same process) is killed
                        // and restarted, all in-memory KV entries are lost.
                        // ReMountSegment above only restores Master-side
                        // allocation state; it does NOT write back the
                        // transport-level segment descriptors.  Without this,
                        // remote peers get HTTP 404 when querying our segment
                        // descriptor and data transfers fail.
                        auto metadata = transfer_engine_->getMetadata();
                        if (metadata) {
                            int rc = metadata->updateLocalSegmentDesc();
                            if (rc != 0) {
                                LOG(ERROR)
                                    << "Failed to re-publish segment "
                                       "descriptor "
                                    << "to metadata server, rc=" << rc
                                    << ", will retry in next heartbeat cycle";
                                segment_desc_publish_pending_.store(true);
                            } else {
                                segment_desc_publish_pending_.store(false);
                            }
                            // Also re-publish RPC meta entry
                            // (mooncake/rpc_meta/<hostname>). Remote peers need
                            // this to locate our RDMA RPC port for handshake.
                            // Like segment descriptors, this entry is lost when
                            // the HTTP metadata server is cleared on Master
                            // restart.
                            rc = metadata->rePublishRpcMetaEntry(
                                local_hostname_);
                            if (rc != 0) {
                                LOG(ERROR)
                                    << "Failed to re-publish RPC meta entry "
                                    << "to metadata server, rc=" << rc
                                    << ", will retry in next heartbeat cycle";
                                rpc_meta_publish_pending_.store(true);
                            } else {
                                rpc_meta_publish_pending_.store(false);
                            }
                        }
                        // Note: LOCAL_DISK segment remount is NOT done here.
                        // It is handled by FileStorage::Heartbeat() when it
                        // detects SEGMENT_NOT_FOUND, which also triggers
                        // ScanMeta to re-register offloaded object metadata.
                    };
                    // Use another thread to remount segments to avoid blocking
                    // the ping thread
                    std::future<void> remount_segment_future;

                    while (storage_heartbeat_running_.load()) {
                        // Join the remount segment thread if it is ready
                        if (remount_segment_future.valid() &&
                            remount_segment_future.wait_for(
                                std::chrono::seconds(0)) ==
                                std::future_status::ready) {
                            remount_segment_future = std::future<void>();
                        }

                        auto ping_result =
                            metrics_
                                ? metrics_->master_heartbeat_metric.ObservePing(
                                      [this] { return master_client_.Ping(); })
                                : master_client_.Ping();
                        // CVM 多 submaster 心跳：向所有 primary submaster 定向
                        // ping 保活， 避免各 submaster 因收不到本 client 的
                        // ping 而误判过期并卸载 segment。
                        HeartbeatAllSubmasters();

                        if (ping_result) {
                            // Ephemeral gather discovery survives Master restart/failover
                            // through the existing owner heartbeat, not snapshot metadata.
                            std::string gather_endpoint;
                            {
                                std::lock_guard lock(mounted_segments_mutex_);
                                gather_endpoint = gather_endpoint_;
                            }
                            if (!gather_endpoint.empty())
                                (void)master_client_.PublishGatherEndpoint(
                                    GetSegmentEndpoint(), gather_endpoint);
                            // Reset ping failure count
                            ping_fail_count = 0;
                            last_ping_success_.store(true);
                            auto& ping_response = ping_result.value();
                            if (ping_response.client_status ==
                                    ClientStatus::NEED_REMOUNT &&
                                !remount_segment_future.valid()) {
                                // Ensure at most one remount segment thread is
                                // running
                                remount_segment_future = std::async(
                                    std::launch::async, remount_segment);
                            } else if (segment_desc_publish_pending_.load() &&
                                       !remount_segment_future.valid()) {
                                // Previous remount succeeded but
                                // updateLocalSegmentDesc() failed (e.g.
                                // transient HTTP error).  Retry it directly
                                // without re-running ReMountSegment.
                                auto metadata = transfer_engine_->getMetadata();
                                if (metadata) {
                                    int rc = metadata->updateLocalSegmentDesc();
                                    if (rc != 0) {
                                        LOG(ERROR) << "Retry: failed to "
                                                      "re-publish segment "
                                                   << "descriptor to metadata "
                                                      "server, rc="
                                                   << rc;
                                    } else {
                                        LOG(INFO) << "Retry: successfully "
                                                     "re-published "
                                                  << "segment descriptor to "
                                                     "metadata server";
                                        segment_desc_publish_pending_.store(
                                            false);
                                    }
                                }
                            } else if (rpc_meta_publish_pending_.load() &&
                                       !remount_segment_future.valid()) {
                                // Previous remount succeeded but
                                // rePublishRpcMetaEntry() failed.  Retry it
                                // directly.
                                auto metadata = transfer_engine_->getMetadata();
                                if (metadata) {
                                    int rc = metadata->rePublishRpcMetaEntry(
                                        local_hostname_);
                                    if (rc != 0) {
                                        LOG(ERROR) << "Retry: failed to "
                                                      "re-publish RPC "
                                                   << "meta entry to metadata "
                                                      "server, rc="
                                                   << rc;
                                    } else {
                                        LOG(INFO) << "Retry: successfully "
                                                     "re-published "
                                                  << "RPC meta entry to "
                                                     "metadata server";
                                        rpc_meta_publish_pending_.store(false);
                                    }
                                }
                            }

                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(
                                    success_ping_interval_ms));
                            continue;
                        }

                        ping_fail_count++;
                        last_ping_success_.store(false);
                        if (ping_fail_count < max_ping_fail_count) {
                            LOG(ERROR) << "Failed to ping master";
                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(
                                    fail_ping_interval_ms));
                            continue;
                        }

                        // Exceeded ping failure threshold. Reconnect based on
                        // mode.
                        if (leader_coordinator_) {
                            LOG(ERROR) << "Failed to ping master for "
                                       << ping_fail_count
                                       << " times; fetching latest master view "
                                          "and reconnecting";
                            auto current_view =
                                leader_coordinator_->ReadCurrentView();
                            if (!current_view) {
                                LOG(ERROR) << "Failed to get new master view: "
                                           << toString(current_view.error());
                                std::this_thread::sleep_for(
                                    std::chrono::milliseconds(
                                        fail_ping_interval_ms));
                                continue;
                            }
                            if (!current_view.value().has_value()) {
                                LOG(WARNING)
                                    << "No active master view is published yet";
                                std::this_thread::sleep_for(
                                    std::chrono::milliseconds(
                                        fail_ping_interval_ms));
                                continue;
                            }

                            const auto& next_view =
                                current_view.value().value();
                            auto err = SwitchLeader(next_view);
                            if (err != ErrorCode::OK) {
                                LOG(ERROR) << "Failed to connect to master "
                                           << next_view.leader_address << ": "
                                           << toString(err);
                                std::this_thread::sleep_for(
                                    std::chrono::milliseconds(
                                        fail_ping_interval_ms));
                                continue;
                            }

                            LOG(INFO) << "Reconnected to master "
                                      << next_view.leader_address;
                            ping_fail_count = 0;
                        } else if (!cvm_cluster_namespace_.empty()) {
                            // CVM multi-submaster mode: the connected submaster
                            // died or became unreachable. Re-discover live
                            // submasters from the /cvm/ registry and reconnect
                            // to another one.
                            LOG(ERROR) << "Failed to ping master for "
                                       << ping_fail_count
                                       << " times (CVM submaster mode); "
                                          "re-discovering "
                                          "submasters from etcd";
                            auto err = ConnectToCvmSubmasters(etcd_connstring_);
                            if (err != ErrorCode::OK) {
                                std::this_thread::sleep_for(
                                    std::chrono::milliseconds(
                                        fail_ping_interval_ms));
                                continue;
                            }
                            ping_fail_count = 0;
                        } else {
                            const std::string current_master_address =
                                direct_master_address_;
                            LOG(ERROR) << "Failed to ping master for "
                                       << ping_fail_count
                                       << " times (non-HA); reconnecting to "
                                       << current_master_address;
                            auto err =
                                ConnectMasterEndpoint(current_master_address);
                            if (err != ErrorCode::OK) {
                                LOG(ERROR) << "Reconnect failed to "
                                           << current_master_address << ": "
                                           << toString(err);
                                std::this_thread::sleep_for(
                                    std::chrono::milliseconds(
                                        fail_ping_interval_ms));
                                continue;
                            }
                            LOG(INFO) << "Reconnected to master "
                                      << current_master_address;
                            last_ping_success_.store(true);
                            ping_fail_count = 0;
                        }
                    }
                    // Explicitly wait for the remount segment thread to finish
                    if (remount_segment_future.valid()) {
                        remount_segment_future.wait();
                    }
                }

                ErrorCode Client::FindFirstCompleteReplica(
                    const std::vector<Replica::Descriptor>& replica_list,
                    Replica::Descriptor& replica) const {
                    // Find the first complete replica
                    for (size_t i = 0; i < replica_list.size(); ++i) {
                        if (replica_list[i].status == ReplicaStatus::COMPLETE) {
                            replica = replica_list[i];
                            return ErrorCode::OK;
                        }
                    }

                    // No complete replica found
                    return ErrorCode::INVALID_REPLICA;
                }

                tl::expected<Replica::Descriptor, ErrorCode>
                Client::GetPreferredReplica(
                    const std::vector<Replica::Descriptor>& replica_list) {
                    if (replica_list.empty()) {
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                    if (mounted_segments_.empty() || replica_list.size() == 1) {
                        return replica_list[0];
                    }

                    std::unordered_set<std::string> local_endpoints;
                    {
                        std::lock_guard<std::mutex> lock(
                            mounted_segments_mutex_);
                        for (const auto& [segment_id, segment] :
                             mounted_segments_) {
                            local_endpoints.insert(segment.te_endpoint);
                        }
                    }

                    // Prefer local MEMORY replicas first
                    for (const auto& rep : replica_list) {
                        if (rep.is_memory_replica()) {
                            const auto& mem_desc = rep.get_memory_descriptor();
                            const std::string& endpoint =
                                mem_desc.buffer_descriptor.transport_endpoint_;
                            if (local_endpoints.count(endpoint)) {
                                return rep;
                            }
                        }
                    }

                    // Then prefer local NOF_SSD replicas
                    for (const auto& rep : replica_list) {
                        if (rep.is_nof_replica()) {
                            const auto& nof_desc = rep.get_nof_descriptor();
                            const std::string& endpoint =
                                nof_desc.buffer_descriptor.transport_endpoint_;
                            if (local_endpoints.count(endpoint)) {
                                return rep;
                            }
                        }
                    }

                    return replica_list[0];
                }

                ErrorCode Client::InitLocalHotCache() {
                    hot_cache_handler_.reset();
                    UnregisterLocalHotCacheMemory();
                    hot_cache_.reset();
                    admission_sketch_.reset();

                    if (object_checksum_enabled_) {
                        return ErrorCode::OK;
                    }

                    const auto config = LocalHotCacheConfig::FromEnvironment();
                    if (config.total_size_bytes == 0) {
                        return ErrorCode::OK;
                    }

                    size_t thread_num = 2;

                    // Enable hot cache
                    {
                        hot_cache_ = std::make_shared<LocalHotCache>(
                            config.total_size_bytes, config.block_size_bytes,
                            config.use_shm);
                        // Check if cache initialization was successful
                        if (hot_cache_->GetCacheSize() == 0) {
                            LOG(ERROR)
                                << "Local hot cache creation failed: no blocks "
                                   "allocated. "
                                << "total_cache=" << config.total_size_bytes;
                            hot_cache_.reset();
                            hot_cache_handler_.reset();
                            admission_sketch_.reset();
                            return ErrorCode::INVALID_PARAMS;
                        }

                        // For shm-backed (HugeTLB) hot caches,
                        // ShmHelper::allocate rounds the mapping up to hugepage
                        // granularity while GetTotalSize() reports the logical
                        // size, so registering the logical size could end an MR
                        // inside the last huge page (madvise MADV_DONTFORK then
                        // fails with EINVAL). Register the full segment
                        // instead, same rule as
                        // RealClient::register_buffer_internal.
                        size_t hot_cache_reg_size = hot_cache_->GetTotalSize();
                        if (auto shm_seg = hot_cache_->GetShmSegment()) {
                            hot_cache_reg_size = shm_seg->size;
                        }
                        int rc = transfer_engine_->registerLocalMemory(
                            hot_cache_->GetBaseAddress(), hot_cache_reg_size,
                            kWildcardLocation, true, true);
                        if (rc != 0) {
                            LOG(ERROR)
                                << "Failed to register local hot cache memory "
                                   "with transfer "
                                   "engine, base="
                                << hot_cache_->GetBaseAddress()
                                << ", size=" << hot_cache_->GetTotalSize()
                                << ", ret=" << rc;
                            hot_cache_.reset();
                            hot_cache_handler_.reset();
                            admission_sketch_.reset();
                            hot_cache_memory_registered_ = false;
                            return ErrorCode::INVALID_PARAMS;
                        }
                        hot_cache_memory_registered_ = true;

                        LOG(INFO)
                            << "Local hot cache enabled with cache size="
                            << config.total_size_bytes
                            << ", block size=" << config.block_size_bytes
                            << ", block amount=" << hot_cache_->GetCacheSize()
                            << ", shm=" << (config.use_shm ? "on" : "off")
                            << ", transfer engine registered=on";
                        // Create async handler with 2 worker threads
                        hot_cache_handler_ =
                            std::make_unique<LocalHotCacheHandler>(hot_cache_,
                                                                   thread_num);
                        admission_sketch_ = std::make_unique<CountMinSketch>();
                        admission_threshold_ = config.admission_threshold;
                    }
                    return ErrorCode::OK;
                }

                void Client::UnregisterLocalHotCacheMemory() {
                    if (!(hot_cache_ && hot_cache_memory_registered_)) {
                        return;
                    }
                    if (!transfer_engine_) {
                        hot_cache_memory_registered_ = false;
                        return;
                    }

                    int rc = transfer_engine_->unregisterLocalMemory(
                        hot_cache_->GetBaseAddress(), true);
                    if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
                        LOG(ERROR) << "Failed to unregister local hot cache "
                                      "memory from transfer "
                                      "engine, base="
                                   << hot_cache_->GetBaseAddress()
                                   << ", size=" << hot_cache_->GetTotalSize()
                                   << ", ret=" << rc;
                    }
                    hot_cache_memory_registered_ = false;
                }

                void Client::ProcessSlicesAsync(
                    const std::string& key, const std::vector<Slice>& slices,
                    const Replica::Descriptor& replica) {
                    if (!(hot_cache_ && replica.is_memory_replica())) {
                        return;
                    }

                    // Skip local data unless hot cache is in shm mode (shared
                    // with dummy clients who need local data cached for
                    // zero-copy access).
                    if (!hot_cache_->IsShm() &&
                        IsReplicaOnLocalMemory(replica)) {
                        return;
                    }

                    // The writeback copies out of the read destination with a
                    // host memcpy (LocalHotCacheHandler::SubmitPutTask), so a
                    // slice that landed in device memory (e.g. batch_get_into
                    // into CUDA buffers) would fault the reader.
                    // Device-resident results are not cached through this path.
                    auto runtime_accelerator =
                        device::GetAcceleratorRegistry().RuntimeAccelerators();
                    size_t device_dest_slices = 0;
                    // Identify TE transfer slices (non-local) and submit async
                    // put tasks
                    for (size_t i = 0; i < slices.size(); ++i) {
                        if (runtime_accelerator.FindDeviceForPointer(
                                slices[i].ptr) != nullptr) {
                            ++device_dest_slices;
                            continue;
                        }
                        if (!hot_cache_handler_->SubmitPutTask(key,
                                                               slices[i])) {
                            LOG(ERROR) << "Failed to submit hot cache put task "
                                          "for key="
                                       << key << " slice_idx=" << i;
                            return;
                        }
                    }
                    if (device_dest_slices > 0) {
                        LOG(INFO) << "Skipped hot-cache writeback for "
                                  << device_dest_slices
                                  << " device-resident slice(s), key=" << key;
                    }
                }

                bool Client::IsReplicaOnLocalMemory(
                    const Replica::Descriptor& replica) {
                    if (!replica.is_memory_replica()) {
                        return false;
                    }
                    const auto replica_transfer_endpoint =
                        replica.get_memory_descriptor()
                            .buffer_descriptor.transport_endpoint_;
                    // Fast paths first: lock-free comparisons against this
                    // client's own endpoint/hostname.
                    if (metadata_connstring_ == P2PHANDSHAKE) {
                        if (replica_transfer_endpoint ==
                            GetTransportEndpoint()) {
                            return true;
                        }
                    } else if (local_hostname_ == replica_transfer_endpoint) {
                        return true;
                    }
                    // Fallback: the replica's endpoint may address one of this
                    // client's mounted segments by segment name or TE endpoint
                    // (e.g. when the writer published the segment under a name
                    // different from local_hostname_).
                    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
                    for (const auto& [segment_id, segment] :
                         mounted_segments_) {
                        if (replica_transfer_endpoint == segment.name ||
                            replica_transfer_endpoint == segment.te_endpoint) {
                            return true;
                        }
                    }
                    return false;
                }

                tl::expected<std::vector<std::string>, ErrorCode>
                Client::GetOffloadEndpoints() {
                    return master_client_.GetOffloadEndpoints();
                }

                tl::expected<void, ErrorCode> Client::BatchPushOffloadObject(
                    const std::string& requester_te_addr,
                    const std::vector<std::string>& keys,
                    const std::vector<uint64_t>& src_pointers,
                    const std::vector<std::vector<OffloadDstSlice>>&
                        dst_slices) {
                    (void)requester_te_addr;
                    (void)keys;
                    (void)src_pointers;
                    (void)dst_slices;
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }

                std::optional<store::GatherReadOperation>
                Client::SubmitGatherRead(
                    const std::string& endpoint,
                    const std::vector<store::GatherReadRange>& ranges,
                    void* destination, size_t capacity) {
                    if (!SupportsGatherRead() || endpoint.empty())
                        return std::nullopt;
                    std::shared_ptr<GatherPeer> peer_ptr;
                    {
                        std::lock_guard lock(gather_clients_mutex_);
                        auto& slot = gather_clients_[endpoint];
                        if (!slot) slot = std::make_shared<GatherPeer>();
                        peer_ptr = slot;
                    }
                    auto& peer = *peer_ptr;
                    std::lock_guard lock(peer.mutex);
                    const auto now = std::chrono::steady_clock::now();
                    if (now >= peer.refresh_at) {
                        auto discovered =
                            master_client_.ResolveGatherEndpoint(endpoint);
                        const std::string resolved = discovered ? *discovered : "";
                        if (resolved != peer.endpoint) peer.clients.clear();
                        peer.endpoint = resolved;
                        peer.refresh_at = now +
                                           std::chrono::seconds(
                                               resolved.empty() ? 5 : 30);
                    }
                    if (peer.endpoint.empty()) return std::nullopt;
                    auto& clients = peer.clients;
                    std::erase_if(clients, [](const auto& client) {
                        return client->retired();
                    });
                    auto it = std::find_if(
                        clients.begin(), clients.end(),
                        [](const auto& client) { return client->available(); });
                    if (it == clients.end()) {
                        if (clients.size() >= 64) return std::nullopt;
                        try {
                            clients.push_back(
                                std::make_unique<store::GatherReadClient>(
                                    transfer_engine_, peer.endpoint,
                                    std::chrono::seconds(60), endpoint));
                        } catch (const std::exception& e) {
                            peer.refresh_at = now + std::chrono::seconds(5);
                            peer.endpoint.clear();
                            clients.clear();
                            LOG(WARNING)
                                << "Store gather connection unavailable: "
                                << e.what();
                            return std::nullopt;
                        }
                        it = std::prev(clients.end());
                    }
                    // The synchronous get_into_ranges caller owns the destination and its
                    // registration until all operations (including recovery fences) finish.
                    return (*it)->submitGatherRead(ranges, destination,
                                                   capacity, transfer_engine_);
                }
            }  // namespace mooncake
