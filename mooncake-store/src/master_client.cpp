#include "master_client.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>

#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <future>
#include <optional>
#include <ostream>
#include <string>
#include <sys/syscall.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>
#include <ylt/util/tl/expected.hpp>

#include "mooncake_logging.h"
#include "mutex.h"
#include "rpc_service.h"
#include "store_diagnostics.h"
#include "types.h"
#include "common/scoped_vlog_timer.h"
#include "etcd_helper.h"
#include "cvm/etcd_view_store.h"
#include "cvm/slot_hash.h"
#include "partition/kv_hash_map.h"
#include "master_metric_manager.h"
#include "version.h"

#define SPDIAG_PERF_DEF_FILE "mooncake_perf_points.def"
#define SPDIAG_PROGRAM_NAME "mooncake_store"

#include <optional>
#include "spdiag/auto_perf.h"

#if defined(MOONCAKE_RPC_SUBMIT_DIAG_BUILD) && \
    !defined(MOONCAKE_RPC_SUBMIT_TRACE_VERSION)
#error "RPC diagnostics require the build-local instrumented coro_rpc headers"
#endif

namespace mooncake {

namespace {

std::optional<uint16_t> ParsePartitionSlot(const std::string& partition_id) {
    uint32_t value = 0;
    const char* begin = partition_id.data();
    const char* end = begin + partition_id.size();
    const auto [ptr, error] = std::from_chars(begin, end, value);
    if (partition_id.empty() || error != std::errc{} || ptr != end ||
        value >= cvm::SlotCount()) {
        return std::nullopt;
    }
    return static_cast<uint16_t>(value);
}

}  // namespace

template <auto Method>
struct RpcNameTraits;

template <>
struct RpcNameTraits<&WrappedMasterService::ExistKey> {
    static constexpr const char* value = "ExistKey";
};

template <>
struct RpcNameTraits<&WrappedMasterService::ProbeKey> {
    static constexpr const char* value = "ProbeKey";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchExistKey> {
    static constexpr const char* value = "BatchExistKey";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchProbeKey> {
    static constexpr const char* value = "BatchProbeKey";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetReplicaList> {
    static constexpr const char* value = "GetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetReplicaListForAdmin> {
    static constexpr const char* value = "GetReplicaListForAdmin";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchGetReplicaListForAdmin> {
    static constexpr const char* value = "BatchGetReplicaListForAdmin";
};

template <>
struct RpcNameTraits<&WrappedMasterService::VSegmentPutStart> {
    static constexpr const char* value = "VSegmentPutStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::VSegmentPutEnd> {
    static constexpr const char* value = "VSegmentPutEnd";
};

template <>
struct RpcNameTraits<&WrappedMasterService::VSegmentPutRevoke> {
    static constexpr const char* value = "VSegmentPutRevoke";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CalcCacheStats> {
    static constexpr const char* value = "CalcCacheStats";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchQueryIp> {
    static constexpr const char* value = "BatchQueryIp";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchReplicaClear> {
    static constexpr const char* value = "BatchReplicaClear";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetReplicaListByRegex> {
    static constexpr const char* value = "GetReplicaListByRegex";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchGetReplicaList> {
    static constexpr const char* value = "BatchGetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedMasterService::PutStart> {
    static constexpr const char* value = "PutStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchPutStart> {
    static constexpr const char* value = "BatchPutStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::PutEnd> {
    static constexpr const char* value = "PutEnd";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchPutEnd> {
    static constexpr const char* value = "BatchPutEnd";
};

template <>
struct RpcNameTraits<&WrappedMasterService::PutRevoke> {
    static constexpr const char* value = "PutRevoke";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchPutRevoke> {
    static constexpr const char* value = "BatchPutRevoke";
};

template <>
struct RpcNameTraits<&WrappedMasterService::ReportInvalidReplicaEndpoints> {
    static constexpr const char* value = "ReportInvalidReplicaEndpoints";
};

template <>
struct RpcNameTraits<&WrappedMasterService::UpsertStart> {
    static constexpr const char* value = "UpsertStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchUpsertStart> {
    static constexpr const char* value = "BatchUpsertStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::UpsertEnd> {
    static constexpr const char* value = "UpsertEnd";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchUpsertEnd> {
    static constexpr const char* value = "BatchUpsertEnd";
};

template <>
struct RpcNameTraits<&WrappedMasterService::UpsertRevoke> {
    static constexpr const char* value = "UpsertRevoke";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchUpsertRevoke> {
    static constexpr const char* value = "BatchUpsertRevoke";
};

template <>
struct RpcNameTraits<&WrappedMasterService::Remove> {
    static constexpr const char* value = "Remove";
};

template <>
struct RpcNameTraits<&WrappedMasterService::RemoveByRegex> {
    static constexpr const char* value = "RemoveByRegex";
};

template <>
struct RpcNameTraits<&WrappedMasterService::RemoveAll> {
    static constexpr const char* value = "RemoveAll";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchRemove> {
    static constexpr const char* value = "BatchRemove";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MountSegment> {
    static constexpr const char* value = "MountSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MountNoFSegment> {
    static constexpr const char* value = "MountNoFSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::ReMountSegment> {
    static constexpr const char* value = "ReMountSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::ReMountNoFSegment> {
    static constexpr const char* value = "ReMountNoFSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::UnmountSegment> {
    static constexpr const char* value = "UnmountSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GracefulUnmountSegment> {
    static constexpr const char* value = "GracefulUnmountSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::UnmountNoFSegment> {
    static constexpr const char* value = "UnmountNoFSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetAllNoFSegments> {
    static constexpr const char* value = "GetAllNoFSegments";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetAllSegmentsForAdmin> {
    static constexpr const char* value = "GetAllSegmentsForAdmin";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetNoFSegmentsByName> {
    static constexpr const char* value = "GetNoFSegmentsByName";
};

template <>
struct RpcNameTraits<&WrappedMasterService::Ping> {
    static constexpr const char* value = "Ping";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetFsdir> {
    static constexpr const char* value = "GetFsdir";
};

template <>
struct RpcNameTraits<&WrappedMasterService::QuerySegmentStatusById> {
    static constexpr const char* value = "QuerySegmentStatusById";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetStorageConfig> {
    static constexpr const char* value = "GetStorageConfig";
};

template <>
struct RpcNameTraits<&WrappedMasterService::ServiceReady> {
    static constexpr const char* value = "ServiceReady";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MountLocalDiskSegment> {
    static constexpr const char* value = "MountLocalDiskSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::UnmountLocalDiskSegment> {
    static constexpr const char* value = "UnmountLocalDiskSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::OffloadObjectHeartbeat> {
    static constexpr const char* value = "OffloadObjectHeartbeat";
};

template <>
struct RpcNameTraits<&WrappedMasterService::ReportSsdCapacity> {
    static constexpr const char* value = "ReportSsdCapacity";
};

template <>
struct RpcNameTraits<&WrappedMasterService::NotifyOffloadSuccess> {
    static constexpr const char* value = "NotifyOffloadSuccess";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetOffloadEndpoints> {
    static constexpr const char* value = "GetOffloadEndpoints";
};

template <>
struct RpcNameTraits<&WrappedMasterService::PromotionObjectHeartbeat> {
    static constexpr const char* value = "PromotionObjectHeartbeat";
};
template <>
struct RpcNameTraits<&WrappedMasterService::RemoveObjectHeartbeat> {
    static constexpr const char* value = "RemoveObjectHeartbeat";
};

template <>
struct RpcNameTraits<&WrappedMasterService::AckRemoveObjectHeartbeat> {
    static constexpr const char* value = "AckRemoveObjectHeartbeat";
};

template <>
struct RpcNameTraits<&WrappedMasterService::PromotionAllocStart> {
    static constexpr const char* value = "PromotionAllocStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::RegisterPrefetchTask> {
    static constexpr const char* value = "RegisterPrefetchTask";
};

template <>
struct RpcNameTraits<&WrappedMasterService::NotifyPromotionSuccess> {
    static constexpr const char* value = "NotifyPromotionSuccess";
};

template <>
struct RpcNameTraits<&WrappedMasterService::NotifyPromotionFailure> {
    static constexpr const char* value = "NotifyPromotionFailure";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CopyStart> {
    static constexpr const char* value = "CopyStart";
};
template <>
struct RpcNameTraits<&WrappedMasterService::DynamicReplicaCopyStart> {
    static constexpr const char* value = "DynamicReplicaCopyStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CopyEnd> {
    static constexpr const char* value = "CopyEnd";
};
template <>
struct RpcNameTraits<&WrappedMasterService::DynamicReplicaCopyEnd> {
    static constexpr const char* value = "DynamicReplicaCopyEnd";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CopyRevoke> {
    static constexpr const char* value = "CopyRevoke";
};
template <>
struct RpcNameTraits<&WrappedMasterService::DynamicReplicaCopyRevoke> {
    static constexpr const char* value = "DynamicReplicaCopyRevoke";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MoveStart> {
    static constexpr const char* value = "MoveStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MoveEnd> {
    static constexpr const char* value = "MoveEnd";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MoveRevoke> {
    static constexpr const char* value = "MoveRevoke";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CreateCopyTask> {
    static constexpr const char* value = "CreateCopyTask";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CreateMoveTask> {
    static constexpr const char* value = "CreateMoveTask";
};

template <>
struct RpcNameTraits<&WrappedMasterService::QueryTask> {
    static constexpr const char* value = "QueryTask";
};

template <>
struct RpcNameTraits<&WrappedMasterService::FetchTasks> {
    static constexpr const char* value = "FetchTasks";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MarkTaskToComplete> {
    static constexpr const char* value = "MarkTaskToComplete";
};

template <>
struct RpcNameTraits<&WrappedMasterService::EvictDiskReplica> {
    static constexpr const char* value = "EvictDiskReplica";
};

template <>
struct RpcNameTraits<&WrappedMasterService::EvictDiskReplicaIfCurrent> {
    static constexpr const char* value = "EvictDiskReplicaIfCurrent";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchEvictDiskReplica> {
    static constexpr const char* value = "BatchEvictDiskReplica";
};

template <>
struct RpcNameTraits<&WrappedMasterService::PollRemoveAll> {
    static constexpr const char* value = "PollRemoveAll";
};

#define DEFINE_WEIGHT_RPC_NAME(method)                    \
    template <>                                           \
    struct RpcNameTraits<&WrappedMasterService::method> { \
        static constexpr const char* value = #method;     \
    }

namespace {

bool RpcSubmitDiagnosticsRequested() {
    static const bool enabled = [] {
        const bool requested =
            Environ::GetBool("MC_STORE_RPC_SUBMIT_DIAG", false);
#ifndef MOONCAKE_RPC_SUBMIT_TRACE_VERSION
        if (requested) {
            LOG(WARNING) << "MC_STORE_RPC_SUBMIT_DIAG requires rebuilding with "
                            "-DMC_ENABLE_RPC_SUBMIT_DIAG=ON";
        }
#endif
        return requested;
    }();
    return enabled;
}

// These boundaries deliberately surround the pool callback, not TCP packets.
// The pool may await the response before returning, depending on reuse_limit.
struct RpcPoolCallTiming {
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    TimePoint begin{};
    TimePoint callback_enter{};
    TimePoint submit_return{};
    TimePoint pool_return{};
    uint64_t caller_tid = 0;
    uint64_t callback_tid = 0;
    uint64_t submit_tid = 0;
    uint64_t pool_return_tid = 0;
    uint32_t client_id = 0;
    // get_client_id() is copied from pool configuration, not a connection ID.
    // These identify live objects within this process, not socket generations.
    uintptr_t client_object = 0;
    uintptr_t executor_object = 0;
    bool callback_entered = false;
    bool submit_returned = false;
#ifdef MOONCAKE_RPC_SUBMIT_TRACE_VERSION
    std::optional<coro_rpc::mooncake_submit_trace_t> submit_trace;
#endif

    void Start() {
        caller_tid = static_cast<uint64_t>(::syscall(SYS_gettid));
        begin = Clock::now();
    }

    void Finish() {
        pool_return = Clock::now();
        pool_return_tid = static_cast<uint64_t>(::syscall(SYS_gettid));
    }
};

std::ostream& operator<<(std::ostream& output,
                         const RpcPoolCallTiming& timing) {
    const auto us = [](auto begin, auto end) {
        return std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                     begin)
            .count();
    };
    const auto ns = [](auto time) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   time.time_since_epoch())
            .count();
    };
    // -1 means the boundary was not reached (for example, connection failure).
    // Successful phase durations add up to pool_call_us, modulo us rounding.
    output << " phase_timing=pool_callback_v1"
           << " pool_call_us=" << us(timing.begin, timing.pool_return)
           << " pool_acquire_dispatch_us="
           << (timing.callback_entered ? us(timing.begin, timing.callback_enter)
                                       : -1)
           << " client_submit_us="
           << (timing.submit_returned
                   ? us(timing.callback_enter, timing.submit_return)
                   : -1)
           << " pool_after_submit_us="
           << (timing.submit_returned
                   ? us(timing.submit_return, timing.pool_return)
                   : -1)
           << " pool_callback_entered=" << timing.callback_entered
           << " client_submit_returned=" << timing.submit_returned
           << " rpc_client_id=" << timing.client_id
           << " rpc_client_object=" << timing.client_object
           << " rpc_executor_object=" << timing.executor_object
           << " caller_tid=" << timing.caller_tid
           << " callback_tid=" << timing.callback_tid
           << " submit_tid=" << timing.submit_tid
           << " pool_return_tid=" << timing.pool_return_tid
           << " rpc_begin_steady_ns=" << ns(timing.begin)
           << " rpc_end_steady_ns=" << ns(timing.pool_return);
#ifdef MOONCAKE_RPC_SUBMIT_TRACE_VERSION
    if (timing.submit_trace) {
        const auto& trace = *timing.submit_trace;
        output
            << " b_wrapper_us="
            << (trace.Has(coro_rpc::mooncake_submit_trace_t::Begin)
                    ? us(timing.callback_enter,
                         trace.times[coro_rpc::mooncake_submit_trace_t::Begin])
                    : -1)
            << trace;
    } else {
        output << " b_diag=off";
    }
#else
    output << " b_diag="
           << (RpcSubmitDiagnosticsRequested() ? "not_built" : "off");
#endif
    return output;
}

template <auto ServiceMethod, typename... Args>
auto MeasureRpcClientSubmit(coro_rpc::coro_rpc_client& client,
                            RpcPoolCallTiming& timing, Args&&... args)
    -> decltype(client.send_request<ServiceMethod>(
        std::forward<Args>(args)...)) {
    timing.callback_enter = RpcPoolCallTiming::Clock::now();
    timing.callback_entered = true;
    timing.callback_tid = static_cast<uint64_t>(::syscall(SYS_gettid));
    timing.client_id = client.get_client_id();
    timing.client_object = reinterpret_cast<uintptr_t>(&client);
    timing.executor_object =
        reinterpret_cast<uintptr_t>(&client.get_executor());

    // Await only the outer submission coroutine. Return its response Lazy
    // unchanged so the pool retains its existing response/connection lifetime.
    // This interval includes preparation, write serialization and recv startup;
    // it is not a measurement of async_write alone or of network RTT.
    auto submission = [&] {
#ifdef MOONCAKE_RPC_SUBMIT_TRACE_VERSION
        if (RpcSubmitDiagnosticsRequested()) {
            coro_rpc::request_config_t config;
            config.mooncake_submit_trace = &timing.submit_trace.emplace();
            return client.send_request<ServiceMethod>(
                std::move(config), std::forward<Args>(args)...);
        }
#endif
        return client.send_request<ServiceMethod>(std::forward<Args>(args)...);
    }();
    auto response = co_await std::move(submission);
    timing.submit_return = RpcPoolCallTiming::Clock::now();
#ifdef MOONCAKE_RPC_SUBMIT_TRACE_VERSION
    if (timing.submit_trace) {
        timing.submit_trace->Mark(coro_rpc::mooncake_submit_trace_t::Done,
                                  timing.submit_return);
    }
#endif
    timing.submit_returned = true;
    timing.submit_tid = static_cast<uint64_t>(::syscall(SYS_gettid));
    co_return std::move(response);
}

}  // namespace

DEFINE_WEIGHT_RPC_NAME(BeginWeightImport);
DEFINE_WEIGHT_RPC_NAME(CommitWeightImport);
DEFINE_WEIGHT_RPC_NAME(AbortWeightImport);
DEFINE_WEIGHT_RPC_NAME(GetWeightRevision);
DEFINE_WEIGHT_RPC_NAME(ListWeightRevisions);
DEFINE_WEIGHT_RPC_NAME(AcquireWeightRevisionLease);
DEFINE_WEIGHT_RPC_NAME(RenewWeightRevisionLease);
DEFINE_WEIGHT_RPC_NAME(ReleaseWeightRevisionLease);
DEFINE_WEIGHT_RPC_NAME(StartWeightResidencyOperation);
DEFINE_WEIGHT_RPC_NAME(QueryWeightOperation);
DEFINE_WEIGHT_RPC_NAME(DeleteWeightRevision);

#undef DEFINE_WEIGHT_RPC_NAME

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> MasterClient::invoke_rpc_with_client_pool(
    const std::shared_ptr<RpcClientPool::ClientPool>& client_pool,
    Args&&... args) {
    RpcDrainGuard::ScopedCall inflight(rpc_drain_);
    if (!inflight.ok()) {
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    // Increment RPC counter
    if (metrics_) {
        metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
    }

    auto start_time = std::chrono::steady_clock::now();
    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>> {
            auto ret = co_await client_pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client& client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                LOG(ERROR) << "Client not available";
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                if (result.error().code == coro_rpc::errc::timed_out) {
                    LOG(ERROR) << "RPC call timed out: " << result.error().msg;
                    co_return tl::make_unexpected(ErrorCode::RPC_TIMEOUT);
                }
                LOG(ERROR) << "RPC call failed: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            if (metrics_) {
                auto end_time = std::chrono::steady_clock::now();
                auto latency =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        end_time - start_time);
                metrics_->rpc_latency.observe(
                    {RpcNameTraits<ServiceMethod>::value}, latency.count());
            }
            co_return result->result();
        }());
}

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> MasterClient::invoke_rpc_with_pool(
    RpcClientPool& client_accessor, Args&&... args) {
    return invoke_rpc_with_client_pool<ServiceMethod, ReturnType>(
        client_accessor.GetClientPool(), std::forward<Args>(args)...);
}

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> MasterClient::invoke_rpc(Args&&... args) {
    return invoke_rpc_with_pool<ServiceMethod, ReturnType>(
        client_accessor_, std::forward<Args>(args)...);
}

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> MasterClient::invoke_rpc_to(
    const std::string& address, Args&&... args) {
    auto pool = targeted_accessor_.GetOrCreateClientPool(address);
    return invoke_rpc_with_client_pool<ServiceMethod, ReturnType>(
        pool, std::forward<Args>(args)...);
}

template <auto ServiceMethod, typename ResultType, typename... Args>
std::vector<tl::expected<ResultType, ErrorCode>> MasterClient::invoke_batch_rpc(
    size_t input_size, Args&&... args) {
    RpcDrainGuard::ScopedCall inflight(rpc_drain_);
    if (!inflight.ok()) {
        return std::vector<tl::expected<ResultType, ErrorCode>>(
            input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
    }
    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    const bool breakdown_log = ShouldSampleStoreTransferDiagnostics(trace_id);
    const auto total_start = breakdown_log
                                 ? std::chrono::steady_clock::now()
                                 : std::chrono::steady_clock::time_point{};
    auto pool = client_accessor_.GetClientPool();
    const auto pool_end = breakdown_log
                              ? std::chrono::steady_clock::now()
                              : std::chrono::steady_clock::time_point{};
    uint64_t rpc_call_us = 0;
    uint64_t result_get_us = 0;
    uint64_t result_parse_us = 0;
    RpcPoolCallTiming pool_timing;
    bool rpc_transport_ok = false;

    // Increment RPC counter
    if (metrics_) {
        metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
    }

    auto start_time = std::chrono::steady_clock::now();
    auto rpc_result = async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<
                  std::vector<tl::expected<ResultType, ErrorCode>>> {
            // 巨尾归因（二）：连接池取用（含首连/重连）+ 请求提交。
            // PerfPoint 驻留协程帧，跨 co_await 挂起按墙钟时间计量。
            SpDiag::PerfPoint pt_send(PerfKey::CVM_RPC_BATCH_SEND,
                                      SpDiag::PerfLevel::MODULE);
            pt_send.Start();
            if (breakdown_log) pool_timing.Start();
            auto ret = co_await pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client& client) {
                    if (breakdown_log) {
                        return MeasureRpcClientSubmit<ServiceMethod>(
                            client, pool_timing, std::forward<Args>(args)...);
                    }
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            pt_send.End(ret.has_value() ? 0 : -1);
            if (breakdown_log) {
                pool_timing.Finish();
                rpc_call_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        pool_timing.pool_return - pool_end)
                        .count();
            }
            if (!ret.has_value()) {
                LOG(ERROR) << "Client not available";
                co_return std::vector<tl::expected<ResultType, ErrorCode>>(
                    input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
            }
            // 巨尾归因（三）：等待响应（网络 RTT + 服务端 handler 整窗）。
            SpDiag::PerfPoint pt_result(PerfKey::CVM_RPC_BATCH_RESULT_GET,
                                        SpDiag::PerfLevel::MODULE);
            pt_result.Start();
            const auto result_get_start =
                breakdown_log ? std::chrono::steady_clock::now()
                              : std::chrono::steady_clock::time_point{};
            auto result = co_await std::move(ret.value());
            pt_result.End(result ? 0 : -1);
            if (breakdown_log) {
                result_get_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - result_get_start)
                        .count();
            }
            if (!result) {
                const ErrorCode err_code =
                    (result.error().code == coro_rpc::errc::timed_out)
                        ? ErrorCode::RPC_TIMEOUT
                        : ErrorCode::RPC_FAIL;
                LOG(ERROR) << "Batch RPC call failed: " << result.error().msg;
                std::vector<tl::expected<ResultType, ErrorCode>> error_results;
                error_results.reserve(input_size);
                for (size_t i = 0; i < input_size; ++i) {
                    error_results.emplace_back(tl::make_unexpected(err_code));
                }
                co_return error_results;
            }
            rpc_transport_ok = true;
            if (metrics_) {
                auto end_time = std::chrono::steady_clock::now();
                auto latency =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        end_time - start_time);
                metrics_->rpc_latency.observe(
                    {RpcNameTraits<ServiceMethod>::value}, latency.count());
            }
            const auto result_parse_start =
                breakdown_log ? std::chrono::steady_clock::now()
                              : std::chrono::steady_clock::time_point{};
            auto response = result->result();
            if (breakdown_log) {
                result_parse_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - result_parse_start)
                        .count();
            }
            co_return std::move(response);
        }());
    if (breakdown_log) {
        MC_LOG(INFO) << "master_rpc_client_breakdown method="
                     << RpcNameTraits<ServiceMethod>::value
                     << " pool_lookup_us="
                     << std::chrono::duration_cast<std::chrono::microseconds>(
                            pool_end - total_start)
                            .count()
                     << " rpc_call_us=" << rpc_call_us
                     << " result_get_us=" << result_get_us
                     << " result_parse_us=" << result_parse_us << pool_timing
                     << " total_us="
                     << std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - total_start)
                            .count()
                     << " status=" << (rpc_transport_ok ? "ok" : "rpc_fail");
    }
    return rpc_result;
}

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> MasterClient::InvokeRoutedWithSlotRetry(
    RoutedCallTiming* timing, const std::string& tenant_id,
    const std::string& key, uint32_t rpc_stat_key, Args... args) {
    const auto t_routed_start = std::chrono::steady_clock::now();
    // 可选的 SpDiag 统计（首段 RPC = 发起 ylt 请求到拿到结果的时延，与
    // timing->rpc_first_us 同区间）：仅显式传入 PerfKey 的调用方记录
    // （Query 传 QUERY_RPC_CALL），默认 0 不记录——本模板被 7 个 routed
    // RPC 共享，不得污染彼此统计。PerfPoint 驻留协程帧按墙钟计量。
    std::optional<SpDiag::PerfPoint> pt_rpc;
    if (rpc_stat_key != 0) {
        pt_rpc.emplace(rpc_stat_key, SpDiag::PerfLevel::MODULE);
        pt_rpc->Start();
    }
    auto result = invoke_rpc<ServiceMethod, ReturnType>(args...);
    if (pt_rpc) {
        pt_rpc->End(result ? 0 : -1);
    }
    if (timing) {
        timing->rpc_first_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - t_routed_start)
                .count();
    }

    if (!result && (result.error() == ErrorCode::SLOT_NOT_OWNED ||
                    result.error() == ErrorCode::STALE_ROUTE)) {
        // 环已变化（成员增删）或分区路由 epoch 过期（owner 推导两模型
        // 切换的瞬态 / vsegment manager 尚未随心跳安装）：都是路由态过期，
        // 刷新路由 + 重切到新 owner，重试一次。
        // 节流告警（防刷屏）：重试本身高频（启动瞬态/切换窗口），仅首条
        // 立即打、其后至多 60s 一条并带窗口内累计数，绝不逐次打印。
        static std::atomic<uint64_t> route_retry_count{0};
        static std::atomic<int64_t> last_route_retry_warn_ms{0};
        route_retry_count.fetch_add(1, std::memory_order_relaxed);
        const int64_t now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
        int64_t last_ms =
            last_route_retry_warn_ms.load(std::memory_order_relaxed);
        if ((last_ms == 0 || now_ms - last_ms >= 60000) &&
            last_route_retry_warn_ms.compare_exchange_strong(
                last_ms, now_ms, std::memory_order_relaxed)) {
            const uint64_t suppressed =
                route_retry_count.exchange(0, std::memory_order_relaxed);
            LOG(WARNING) << "routed RPC hit stale routing (SLOT_NOT_OWNED/"
                         << "STALE_ROUTE), retried after refresh: "
                         << suppressed << " retries since last notice";
        }
        // 重试整窗（含 RefreshSubmasterRouting 同步 etcd 全表重载 +
        // SwitchToSubmaster 建连 + 二次 RPC）。正常≈0，飙升即迁移动荡。
        // 与上面 60s 节流 WARNING 互补：日志看拐点，本点看分布。
        SpDiag::PerfPoint pt_retry(PerfKey::CVM_ROUTE_STALE_RETRY,
                                   SpDiag::PerfLevel::KEY_MODULE);
        pt_retry.Start();
        const auto t_refresh = std::chrono::steady_clock::now();
        const bool refresh_ok = RefreshSubmasterRouting() == ErrorCode::OK;
        if (timing) {
            timing->route_refresh_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - t_refresh)
                    .count();
            ++timing->retry_count;
        }
        if (refresh_ok && SwitchToSubmaster(tenant_id, key) == ErrorCode::OK) {
            if (timing) {
                timing->retry_switch_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - t_refresh)
                        .count() -
                    timing->route_refresh_us;
            }
            const auto t_retry = std::chrono::steady_clock::now();
            auto retry_result = invoke_rpc<ServiceMethod, ReturnType>(args...);
            if (timing) {
                timing->rpc_retry_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - t_retry)
                        .count();
            }
            pt_retry.End(retry_result ? 0 : -1);
            return retry_result;
        }
        pt_retry.End(-1);
    } else if (!result && result.error() == ErrorCode::SLOT_MIGRATING) {
        // owner 已 expected 但元数据尚未 import/replay 完成：环不变，退避
        // （有界）后原地重试，等待元数据就绪。
        // 迁移窗口期退避总时长（含 sleep + 重试 RPC），DEBUG 级：
        // 仅迁移窗口可见，平时零流量。
        SpDiag::PerfPoint pt_backoff(PerfKey::CVM_ROUTE_BACKOFF,
                                     SpDiag::PerfLevel::DEBUG);
        pt_backoff.Start();
        constexpr int kMaxRetries = 3;
        const auto t_backoff = std::chrono::steady_clock::now();
        for (int i = 0; i < kMaxRetries; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20 << i));
            auto attempt = invoke_rpc<ServiceMethod, ReturnType>(args...);
            if (timing) {
                ++timing->backoff_count;
            }
            if (!attempt || attempt.error() != ErrorCode::SLOT_MIGRATING) {
                if (timing) {
                    timing->backoff_us =
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - t_backoff)
                            .count();
                }
                pt_backoff.End(attempt ? 0 : -1);
                return attempt;
            }
            result = std::move(attempt);
        }
        if (timing) {
            timing->backoff_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - t_backoff)
                    .count();
        }
        pt_backoff.End(-1);  // 耗尽重试仍 MIGRATING
    }

    return result;
}

template <typename ResultType, typename SendGroupFn>
void MasterClient::RetryBatchOnStaleRoute(
    const std::string& tenant_id, const std::vector<std::string>& keys,
    std::vector<tl::expected<ResultType, ErrorCode>>& results,
    SendGroupFn send_group) {
    // 与 InvokeRoutedWithSlotRetry 同源的路由自愈，粒度从单 key 扩展为
    // 失败子集：批量分组 RPC 的逐 key 结果由服务端逐 key 判定，路由瞬态
    // （迁移窗口/成员切换/auto-plan 尚未收敛）只会命中批内一部分 key。
    // 只重发失败 key（成功 key 的结果绝不被二次写入），有界 1 次。
    // 收集四类可重试失败：
    //   - STALE_ROUTE / SLOT_NOT_OWNED：路由过期 → 刷新路由 + 重分组重发；
    //   - SLOT_MIGRATING：owner 已 expected 但元数据未就绪（迁移窗口）→
    //     轻量退避一轮后与上类共用同一次重发（单 key 路径的 3 次退避在
    //     batch 聚合下收敛为 1 次，秒级导入完成即成功，未完成靠上层重试）；
    //   - UNAVAILABLE_IN_CURRENT_STATUS：首轮空分组占位（无 owner，可能
    //     为 rank 空缺瞬态）→ 刷新后重分组可修复；服务端若真返回同码，
    //     重发后仍失败保持终态，语义不变。
    if (keys.size() != results.size()) {
        return;  // 规模不匹配属调用方缺陷：保持首轮结果，不重试
    }
    std::vector<std::string> failed_keys;
    std::vector<size_t> orig_indices;
    bool has_migrating = false;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            const ErrorCode err = results[i].error();
            if (err == ErrorCode::STALE_ROUTE ||
                err == ErrorCode::SLOT_NOT_OWNED ||
                err == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS) {
                failed_keys.push_back(keys[i]);
                orig_indices.push_back(i);
            } else if (err == ErrorCode::SLOT_MIGRATING) {
                has_migrating = true;
                failed_keys.push_back(keys[i]);
                orig_indices.push_back(i);
            }
        }
    }
    if (failed_keys.empty()) {
        return;
    }

    // 迁移窗口轻量退避（仅含 MIGRATING 时）：给 slot import/OpLog replay
    // 一个就绪窗口再重发，避免立即重发必然再次 MIGRATING。
    if (has_migrating) {
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
    }

    // 节流告警：与单 key 路径同款（首条立即、其后 60s 一条 + 窗口累计），
    // 迁移/切换窗口内批量重试高频，绝不逐批打印。计数器按模板实例隔离，
    // 实例数由批结果类型决定（有界）。
    static std::atomic<uint64_t> batch_route_retry_count{0};
    static std::atomic<int64_t> last_batch_route_retry_warn_ms{0};
    batch_route_retry_count.fetch_add(1, std::memory_order_relaxed);
    const int64_t now_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    int64_t last_ms =
        last_batch_route_retry_warn_ms.load(std::memory_order_relaxed);
    if ((last_ms == 0 || now_ms - last_ms >= 60000) &&
        last_batch_route_retry_warn_ms.compare_exchange_strong(
            last_ms, now_ms, std::memory_order_relaxed)) {
        const uint64_t suppressed =
            batch_route_retry_count.exchange(0, std::memory_order_relaxed);
        LOG(WARNING) << "batch RPC hit routing transient (STALE_ROUTE/"
                     << "SLOT_NOT_OWNED/SLOT_MIGRATING/no-owner), retried "
                        "failed keys after refresh: "
                     << suppressed << " batches since last notice";
    }

    // 打点复用单 key 路径的 CVM_ROUTE_STALE_RETRY：正常≈0，飙升即迁移动荡；
    // 子集重新分组沿用 GroupKeysBySubmaster（自带 RouteGroup 子打点）。
    SpDiag::PerfPoint pt_retry(PerfKey::CVM_ROUTE_STALE_RETRY,
                               SpDiag::PerfLevel::KEY_MODULE);
    pt_retry.Start();
    if (RefreshSubmasterRouting() != ErrorCode::OK) {
        // 刷新失败（etcd 瞬态）：保持首轮错误；后台 5s 路由刷新线程兜底。
        pt_retry.End(-1);
        return;
    }
    auto groups = GroupKeysBySubmaster(failed_keys, tenant_id);
    bool all_ok = true;
    for (auto& [submaster, local_indices] : groups) {
        if (submaster.empty()) {
            // 刷新后仍无 owner（rank 空缺/整段迁移中）：保持原终态错误。
            all_ok = false;
            continue;
        }
        std::vector<std::string> group_keys;
        std::vector<size_t> group_orig;
        group_keys.reserve(local_indices.size());
        group_orig.reserve(local_indices.size());
        for (size_t local : local_indices) {
            group_keys.push_back(failed_keys[local]);
            group_orig.push_back(orig_indices[local]);
        }
        if (!send_group(submaster, std::move(group_keys), group_orig)) {
            all_ok = false;
        }
    }
    pt_retry.End(all_ok ? 0 : -1);
}

MasterClient::~MasterClient() {
    // Never release the pool under a suspended request coroutine (#3909).
    // 30s is generous: every request carries its own RPC timeout.
    if (!rpc_drain_.drain_for(std::chrono::seconds(30))) {
        LOG(ERROR) << "MasterClient teardown: RPCs still in flight after "
                      "30s drain; releasing the pool regardless";
    }
}

void MasterClient::EnableHaConnectionPolicy() {
    MutexLocker lock(&connect_mutex_);
    if (!client_addr_param_.empty()) {
        ha_control_client_accessor_.GetOrCreateClientPool(client_addr_param_);
    }
    ha_connection_policy_enabled_.store(true, std::memory_order_release);
}

#define DEFINE_WEIGHT_CLIENT_METHOD(method, request_type, result_type)       \
    WeightRpcResult<result_type> MasterClient::method(                       \
        const request_type& request) {                                       \
        return invoke_rpc<&WrappedMasterService::method,                     \
                          tl::expected<result_type, WeightManagementError>>( \
            request, tenant_id_.value());                                    \
    }

DEFINE_WEIGHT_CLIENT_METHOD(BeginWeightImport, BeginWeightImportRequest,
                            WeightRevisionMetadata)
DEFINE_WEIGHT_CLIENT_METHOD(CommitWeightImport, CommitWeightImportRequest,
                            WeightRevisionMetadata)
DEFINE_WEIGHT_CLIENT_METHOD(AbortWeightImport, AbortWeightImportRequest,
                            WeightRevisionMetadata)
DEFINE_WEIGHT_CLIENT_METHOD(GetWeightRevision, GetWeightRevisionRequest,
                            WeightRevisionView)
DEFINE_WEIGHT_CLIENT_METHOD(ListWeightRevisions, ListWeightRevisionsRequest,
                            ListWeightRevisionsResponse)
DEFINE_WEIGHT_CLIENT_METHOD(AcquireWeightRevisionLease,
                            AcquireWeightRevisionLeaseRequest,
                            WeightRevisionLease)
DEFINE_WEIGHT_CLIENT_METHOD(RenewWeightRevisionLease,
                            RenewWeightRevisionLeaseRequest,
                            WeightRevisionLease)
DEFINE_WEIGHT_CLIENT_METHOD(StartWeightResidencyOperation,
                            StartWeightResidencyOperationRequest,
                            WeightResidencyOperation)
DEFINE_WEIGHT_CLIENT_METHOD(QueryWeightOperation, QueryWeightOperationRequest,
                            WeightResidencyOperation)
DEFINE_WEIGHT_CLIENT_METHOD(DeleteWeightRevision, DeleteWeightRevisionRequest,
                            WeightRevisionMetadata)

#undef DEFINE_WEIGHT_CLIENT_METHOD

WeightRpcResult<void> MasterClient::ReleaseWeightRevisionLease(
    const ReleaseWeightRevisionLeaseRequest& request) {
    return invoke_rpc<&WrappedMasterService::ReleaseWeightRevisionLease,
                      tl::expected<void, WeightManagementError>>(
        request, tenant_id_.value());
}

void MasterClient::WarmupRpcPool() {
    auto pool = client_accessor_.GetClientPool();
    if (!pool) {
        return;
    }
    if (!Environ::Get().GetYltRpcPoolWarmupEnabled(true)) {
        LOG(INFO) << "Master RPC pool warmup disabled by "
                  << "MC_YLT_RPC_POOL_WARMUP";
        return;
    }
    const size_t target_connections =
        Environ::Get().GetYltRpcPoolWarmupConnections(
            pool->get_pool_config().max_connection);
    if (target_connections == 0) {
        LOG(INFO) << "Master RPC pool warmup disabled: target_connections=0";
        return;
    }

    LOG(INFO) << "Warming up master RPC pool to " << target_connections
              << " connection(s), max_connection="
              << pool->get_pool_config().max_connection;
    std::vector<std::future<bool>> futures;
    futures.reserve(target_connections);
    for (size_t i = 0; i < target_connections; ++i) {
        futures.emplace_back(std::async(std::launch::async, [this]() {
            auto result =
                invoke_rpc<&WrappedMasterService::ServiceReady, std::string>();
            return result.has_value();
        }));
    }

    size_t ok_count = 0;
    for (auto& future : futures) {
        if (future.get()) {
            ++ok_count;
        }
    }
    LOG(INFO) << "Master RPC pool warmup completed: " << ok_count << "/"
              << target_connections << " succeeded";
}

ErrorCode MasterClient::Connect(const std::string& master_addr) {
    ScopedVLogTimer timer(1, "MasterClient::Connect");
    timer.LogRequest("master_addr=", master_addr);

    MutexLocker lock(&connect_mutex_);
    const bool use_ha_control_pool =
        ha_connection_policy_enabled_.load(std::memory_order_acquire);
    if (use_ha_control_pool) {
        ha_probe_client_accessor_.GetOrCreateClientPool(master_addr);
    } else {
        client_accessor_.GetOrCreateClientPool(master_addr);
    }
    // The client pool does not have native connection check method, so we need
    // to use custom ServiceReady API.
    auto result =
        use_ha_control_pool
            ? invoke_rpc_with_pool<&WrappedMasterService::ServiceReady,
                                   std::string>(ha_probe_client_accessor_)
            : invoke_rpc_with_pool<&WrappedMasterService::ServiceReady,
                                   std::string>(client_accessor_);
    if (!result.has_value()) {
        timer.LogResponse("error_code=", result.error());
        return result.error();
    }
    // Check if server version matches client version
    std::string server_version = result.value();
    std::string client_version = GetMooncakeStoreVersion();
    if (server_version != client_version) {
        LOG(ERROR) << "Version mismatch: server=" << server_version
                   << " client=" << client_version;
        timer.LogResponse("error_code=", ErrorCode::INVALID_VERSION);
        return ErrorCode::INVALID_VERSION;
    }
    if (use_ha_control_pool) {
        // Only move foreground traffic after the fast control-plane probe has
        // established that the new leader is ready and version-compatible.
        client_accessor_.GetOrCreateClientPool(master_addr);
        ha_control_client_accessor_.GetOrCreateClientPool(master_addr);
    }
    client_addr_param_ = master_addr;
    WarmupRpcPool();
    timer.LogResponse("error_code=", ErrorCode::OK);
    return ErrorCode::OK;
}

ErrorCode MasterClient::LoadRoutingFromEtcd(
    const std::string& etcd_endpoints, const std::string& cluster_namespace) {
    if (etcd_endpoints.empty() || cluster_namespace.empty()) {
        LOG(ERROR) << "LoadRoutingFromEtcd requires non-empty etcd_endpoints "
                   << "and cluster_namespace";
        return ErrorCode::INVALID_PARAMS;
    }

    ErrorCode err = EtcdHelper::ConnectToEtcdStoreClient(etcd_endpoints);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect etcd for partition routing: " << err;
        return err;
    }

    err = partition_router_.LoadFromEtcdSnapshot(cluster_namespace);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to load partition routing snapshot from etcd "
                     << "(namespace=" << cluster_namespace << "): " << err;
        return err;
    }
    {
        std::lock_guard<std::mutex> lock(routing_config_mutex_);
        routing_cluster_namespace_ = cluster_namespace;
    }
    LOG(INFO) << "Loaded partition routing from etcd: namespace="
              << cluster_namespace << " entries=" << partition_router_.Size();
    return ErrorCode::OK;
}

ErrorCode MasterClient::RefreshSubmasterRouting() {
    std::string cluster_namespace;
    {
        std::lock_guard<std::mutex> lock(routing_config_mutex_);
        cluster_namespace = routing_cluster_namespace_;
    }
    if (cluster_namespace.empty()) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    return partition_router_.LoadFromEtcdSnapshot(cluster_namespace);
}

std::optional<std::string> MasterClient::ResolveSubmaster(
    const std::string& key) const {
    const uint16_t slot = partition::KvHashMap::Compute(tenant_id_, key);
    auto submaster = partition_router_.ResolveSubmaster(slot);
    if (submaster) {
        LOG(INFO) << "ResolveSubmaster hit: tenant=" << tenant_id_.value()
                  << " key=" << key << " slot=" << slot
                  << " submaster=" << *submaster;
    } else {
        LOG(WARNING) << "ResolveSubmaster miss: tenant=" << tenant_id_.value()
                     << " key=" << key << " slot=" << slot
                     << " (routing not loaded or slot has no owner)";
    }
    return submaster;
}

std::optional<std::string> MasterClient::ResolveSubmasterFor(
    const std::string& tenant_id, const std::string& key) const {
    // 路由表未加载（单 master 模式）：直接 nullopt，调用方回退非定向。
    // miss 不打日志——批量分组场景由调用方聚合计数，避免逐 key 刷屏。
    if (partition_router_.Size() == 0) {
        return std::nullopt;
    }
    const TenantId tenant(tenant_id);
    return partition_router_.ResolveSubmasterQuiet(
        partition::KvHashMap::Compute(tenant, key));
}

void MasterClient::SwitchToSubmasterByAddress(const std::string& address) {
    client_accessor_.GetOrCreateClientPool(address);
}

ErrorCode MasterClient::SwitchToSubmaster(const std::string& tenant_id,
                                          const std::string& key) {
    // 路由打点（读写共键）：status 0=hit 切换(含建连) / 2=miss 失败。
    // 单 master 表空路径不激活（未 Start 的点析构干净），避免零流量稀释；
    // miss 率 = badCount/tickCount，延迟分布即多 submaster 路由开销
    // （含 GetOrCreateClientPool 新 submaster 首次建连成本）。
    SpDiag::PerfPoint pt_switch(PerfKey::CVM_ROUTE_SWITCH,
                                SpDiag::PerfLevel::KEY_MODULE);
    // Routing not loaded (single-master mode): keep the current connection so
    // existing behavior is preserved. Check Size() before ResolveSubmaster to
    // avoid its per-miss WARNING log firing on every single-key request.
    if (partition_router_.Size() == 0) {
        return ErrorCode::OK;
    }
    pt_switch.Start();

    const TenantId tenant(tenant_id);
    const uint16_t slot = partition::KvHashMap::Compute(tenant, key);
    auto submaster = partition_router_.ResolveSubmaster(slot);
    if (!submaster) {
        LOG(WARNING) << "SwitchToSubmaster miss: tenant=" << tenant_id
                     << " key=" << key << " slot=" << slot
                     << " (slot has no owner in routing table)";
        pt_switch.End(2);
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    SwitchToSubmasterByAddress(*submaster);
    // 每次单 key RPC 一条：随 QPS 刷屏，降为 VLOG(1)。
    // 命中延迟分布已由 CVM_ROUTE_SWITCH 打点覆盖，日志仅调试用。
    VLOG(1) << "SwitchToSubmaster: tenant=" << tenant_id << " key=" << key
            << " slot=" << slot << " -> submaster=" << *submaster;
    pt_switch.End(0);
    return ErrorCode::OK;
}

tl::expected<std::string, ErrorCode> MasterClient::ResolveVSegmentSubmaster(
    const std::string& partition_id) {
    std::string cluster_namespace;
    {
        std::lock_guard<std::mutex> lock(routing_config_mutex_);
        cluster_namespace = routing_cluster_namespace_;
    }
    // No routing configuration means an intentional legacy single-master
    // deployment. An empty slot table alone is not sufficient evidence: it
    // also occurs during startup and failed refreshes.
    if (cluster_namespace.empty()) return std::string{};

    if (auto slot = ParsePartitionSlot(partition_id)) {
        auto target = partition_router_.ResolveSubmaster(*slot);
        if (!target) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        return *target;
    }

    partition::PartitionRoute route;
    ViewVersionId version = 0;
    auto error = cvm::EtcdViewStore::LoadPartitionRoute(
        cluster_namespace, partition_id, route, version);
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    if (route.partition_id.partition_id != partition_id ||
        route.owner_submaster_id.empty() ||
        route.state !=
            static_cast<int32_t>(partition::PartitionState::kActive)) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    return route.owner_submaster_id;
}

std::map<std::string, std::vector<size_t>> MasterClient::GroupKeysBySubmaster(
    const std::vector<std::string>& keys, const std::string& tenant_id) {
    // 巨尾归因（残差段）：BatchQuery 总时长减去 RouteRefresh/RpcSend/ResultGet
    // 的残差主要由本函数承担（slot→owner 分组 + 描述日志构建）。
    // status: 0=全部命中 / 2=存在无 owner 的 key（与 RouteSwitch miss=2
    // 对齐）。
    SpDiag::PerfPoint pt_group(PerfKey::CVM_RPC_BATCH_ROUTE_GROUP,
                               SpDiag::PerfLevel::MODULE);
    pt_group.Start();
    std::map<std::string, std::vector<size_t>> groups;
    const TenantId tenant(tenant_id);
    bool has_miss = false;
    for (size_t i = 0; i < keys.size(); ++i) {
        const uint16_t slot = partition::KvHashMap::Compute(tenant, keys[i]);
        auto submaster = partition_router_.ResolveSubmaster(slot);
        if (!submaster) has_miss = true;
        groups[submaster.value_or("")].push_back(i);
    }

    // 每批一条的分组描述日志会随 QPS 刷屏且拖慢热路径（含在 RouteGroup
    // 打点窗口内），降为 VLOG(1)；描述串构建同样用 VLOG_IS_ON 门控，
    // 关闭时零开销。
    if (VLOG_IS_ON(1)) {
        std::string group_desc;
        for (const auto& [submaster, indices] : groups) {
            if (!group_desc.empty()) {
                group_desc += ", ";
            }
            group_desc +=
                (submaster.empty() ? std::string("<none>") : submaster);
            group_desc += ":" + std::to_string(indices.size());
        }
        VLOG(1) << "GroupKeysBySubmaster: tenant=" << tenant_id
                << " total_keys=" << keys.size() << " groups=" << groups.size()
                << " [" << group_desc << "]";
    }
    pt_group.End(has_miss ? 2 : 0);
    return groups;
}

tl::expected<bool, ErrorCode> MasterClient::ExistKey(
    const std::string& object_key) {
    ScopedVLogTimer timer(1, "MasterClient::ExistKey");
    timer.LogRequest("object_key=", object_key);

    ErrorCode switch_err = SwitchToSubmaster(tenant_id_.value(), object_key);
    if (switch_err != ErrorCode::OK) {
        timer.LogResponse("error_code=", switch_err);
        return tl::make_unexpected(switch_err);
    }

    auto result =
        InvokeRoutedWithSlotRetry<&WrappedMasterService::ExistKey, bool>(
            /*timing=*/nullptr, tenant_id_.value(), object_key,
            /*rpc_stat_key=*/0, object_key, tenant_id_.value());
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> MasterClient::BatchExistKey(
    const std::vector<std::string>& object_keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchExistKey");
    timer.LogRequest("keys_count=", object_keys.size());

    // Single-master mode (routing not loaded): use the original batch path.
    if (partition_router_.Size() == 0) {
        auto result =
            invoke_batch_rpc<&WrappedMasterService::BatchExistKey, bool>(
                object_keys.size(), object_keys, tenant_id_.value());
        timer.LogResponse("result=", result.size(), " keys");
        return result;
    }

    std::vector<tl::expected<bool, ErrorCode>> results(
        object_keys.size(),
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));

    auto groups = GroupKeysBySubmaster(object_keys, tenant_id_.value());
    for (auto& [submaster, indices] : groups) {
        if (submaster.empty()) {
            LOG(WARNING) << "BatchExistKey: " << indices.size()
                         << " key(s) have no submaster, marked unavailable";
            continue;
        }
        SwitchToSubmasterByAddress(submaster);

        std::vector<std::string> group_keys;
        group_keys.reserve(indices.size());
        for (size_t idx : indices) {
            group_keys.push_back(object_keys[idx]);
        }

        auto group_result =
            invoke_batch_rpc<&WrappedMasterService::BatchExistKey, bool>(
                group_keys.size(), group_keys, tenant_id_.value());
        for (size_t j = 0; j < indices.size(); ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }

    // 路由自愈：STALE_ROUTE/SLOT_NOT_OWNED/SLOT_MIGRATING/无 owner 的
    // 失败子集刷新路由后重发一次（有界）；仍失败保持终态错误。
    RetryBatchOnStaleRoute<bool>(
        tenant_id_.value(), object_keys, results,
        [this, &results, tid = tenant_id_.value()](
            const std::string& submaster, std::vector<std::string> group_keys,
            const std::vector<size_t>& group_orig) {
            SwitchToSubmasterByAddress(submaster);
            auto group_result =
                invoke_batch_rpc<&WrappedMasterService::BatchExistKey, bool>(
                    group_keys.size(), group_keys, tid);
            if (group_result.size() != group_orig.size()) {
                return false;
            }
            bool all_ok = true;
            for (size_t j = 0; j < group_orig.size(); ++j) {
                results[group_orig[j]] = std::move(group_result[j]);
                if (!results[group_orig[j]]) {
                    all_ok = false;
                }
            }
            return all_ok;
        });

    timer.LogResponse("result=", results.size(), " keys");
    return results;
}

tl::expected<bool, ErrorCode> MasterClient::ProbeKey(
    const std::string& object_key) {
    ScopedVLogTimer timer(1, "MasterClient::ProbeKey");
    timer.LogRequest("object_key=", object_key);

    auto result = invoke_rpc<&WrappedMasterService::ProbeKey, bool>(
        object_key, tenant_id_.value());
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> MasterClient::BatchProbeKey(
    const std::vector<std::string>& object_keys,
    const GrantLeasePolicy& policy) {
    ScopedVLogTimer timer(1, "MasterClient::BatchProbeKey");
    timer.LogRequest("keys_count=", object_keys.size());

    auto result = invoke_batch_rpc<&WrappedMasterService::BatchProbeKey, bool>(
        object_keys.size(), policy, object_keys, tenant_id_.value());
    timer.LogResponse("result=", result.size(), " keys");
    return result;
}

tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
MasterClient::CalcCacheStats() {
    return invoke_rpc<&WrappedMasterService::CalcCacheStats,
                      MasterMetricManager::CacheHitStatDict>();
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
MasterClient::BatchQueryIp(const std::vector<UUID>& client_ids) {
    ScopedVLogTimer timer(1, "MasterClient::BatchQueryIp");
    timer.LogRequest("client_ids_count=", client_ids.size());

    auto result = invoke_rpc<
        &WrappedMasterService::BatchQueryIp,
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>>(
        client_ids);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<std::string>, ErrorCode>
MasterClient::BatchReplicaClear(const std::vector<std::string>& object_keys,
                                const UUID& client_id,
                                const std::string& segment_name) {
    ScopedVLogTimer timer(1, "MasterClient::BatchReplicaClear");
    timer.LogRequest("object_keys_count=", object_keys.size(),
                     ", client_id=", client_id,
                     ", segment_name=", segment_name);
    auto result = invoke_rpc<&WrappedMasterService::BatchReplicaClear,
                             std::vector<std::string>>(
        object_keys, client_id, segment_name, tenant_id_.value());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
MasterClient::GetReplicaListByRegex(const std::string& str) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaListByRegex");
    timer.LogRequest("Regex=", str);

    auto result = invoke_rpc<
        &WrappedMasterService::GetReplicaListByRegex,
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>>(
        str, tenant_id_.value());

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<GetReplicaListResponse, ErrorCode> MasterClient::GetReplicaList(
    const std::string& object_key) {
    return GetReplicaList(object_key, tenant_id_.value());
}

vsegment::VSegmentPutStartResult MasterClient::VSegmentPutStart(
    const std::string& partition_id, uint64_t route_epoch,
    const std::string& operation_id, uint64_t length,
    const std::string& profile_name) {
    auto invoke =
        [&]() -> tl::expected<vsegment::VSegmentPutStartResult, ErrorCode> {
        auto target = ResolveVSegmentSubmaster(partition_id);
        if (!target) return tl::make_unexpected(target.error());
        if (target->empty()) {
            return invoke_rpc<&WrappedMasterService::VSegmentPutStart,
                              vsegment::VSegmentPutStartResult>(
                partition_id, route_epoch, operation_id, length, profile_name);
        }
        return invoke_rpc_to<&WrappedMasterService::VSegmentPutStart,
                             vsegment::VSegmentPutStartResult>(
            *target, partition_id, route_epoch, operation_id, length,
            profile_name);
    };
    auto result = invoke();
    if (result) return std::move(result.value());
    return {result.error(), operation_id, {}, "vsegment PutStart RPC failed"};
}

ErrorCode MasterClient::VSegmentPutEnd(const VSegmentDescriptor& replica,
                                       uint64_t route_epoch,
                                       const std::string& operation_id,
                                       const std::string& object_id) {
    auto invoke = [&]() -> tl::expected<ErrorCode, ErrorCode> {
        auto target = ResolveVSegmentSubmaster(replica.partition_id);
        if (!target) return tl::make_unexpected(target.error());
        if (target->empty()) {
            return invoke_rpc<&WrappedMasterService::VSegmentPutEnd, ErrorCode>(
                replica, route_epoch, operation_id, object_id);
        }
        return invoke_rpc_to<&WrappedMasterService::VSegmentPutEnd, ErrorCode>(
            *target, replica, route_epoch, operation_id, object_id);
    };
    auto result = invoke();
    return result ? result.value() : result.error();
}

ErrorCode MasterClient::VSegmentPutRevoke(const std::string& partition_id,
                                          const std::string& vsegment_id,
                                          uint64_t route_epoch,
                                          const std::string& operation_id) {
    auto invoke = [&]() -> tl::expected<ErrorCode, ErrorCode> {
        auto target = ResolveVSegmentSubmaster(partition_id);
        if (!target) return tl::make_unexpected(target.error());
        if (target->empty()) {
            return invoke_rpc<&WrappedMasterService::VSegmentPutRevoke,
                              ErrorCode>(partition_id, vsegment_id, route_epoch,
                                         operation_id);
        }
        return invoke_rpc_to<&WrappedMasterService::VSegmentPutRevoke,
                             ErrorCode>(*target, partition_id, vsegment_id,
                                        route_epoch, operation_id);
    };
    auto result = invoke();
    return result ? result.value() : result.error();
}

tl::expected<GetReplicaListResponse, ErrorCode> MasterClient::GetReplicaList(
    const std::string& object_key, const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaList");
    timer.LogRequest("object_key=", object_key, ", tenant_id=", tenant_id);

    // query 逐段归因（对齐 get_into_breakdown 风格）：switch（路由解析+建连）
    // 与 routed（RPC 含重试/退避）分段。与 invoke_rpc 内层的
    // master_rpc_client_breakdown（pool_lookup/rpc_call/result_get）互补。
    const auto t0 = std::chrono::steady_clock::now();
    // SpDiag 统计：Query 三段下钻（与 query_rpc_breakdown trace 字段同区
    // 间）——RouteSwitch（路由解析/切换）、RpcCall（InvokeRoutedWithSlotRetry
    // 内首段 RPC，见其 rpc_stat_key 参数）、RpcTotal（路由+RPC 全程）。统
    // 计值与 trace 的 switch_us/rpc_first_us/total_us 可直接交叉对账。
    SpDiag::PerfPoint pt_total(PerfKey::QUERY_RPC_TOTAL,
                               SpDiag::PerfLevel::MODULE);
    pt_total.Start();
    int64_t switch_us = 0;
    RoutedCallTiming timing;

    ErrorCode switch_err;
    {
        SpDiag::PerfPoint pt_switch(PerfKey::QUERY_ROUTE_SWITCH,
                                    SpDiag::PerfLevel::MODULE);
        pt_switch.Start();
        const auto t_switch = std::chrono::steady_clock::now();
        switch_err = SwitchToSubmaster(tenant_id, object_key);
        switch_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - t_switch)
                        .count();
        pt_switch.End(switch_err == ErrorCode::OK ? 0 : -1);
    }
    if (switch_err != ErrorCode::OK) {
        timer.LogResponse("error_code=", switch_err);
        pt_total.End(-1);
        MC_LOG(INFO) << "query_rpc_breakdown key[" << object_key
                     << "] switch_us[" << switch_us << "] rpc_first_us[0]"
                     << "] total_us["
                     << std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count()
                     << "] retry_count[0] backoff_count[0] status[switch_fail_"
                     << static_cast<int>(switch_err) << "]";
        return tl::make_unexpected(switch_err);
    }

    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    auto result =
        InvokeRoutedWithSlotRetry<&WrappedMasterService::GetReplicaList,
                                  GetReplicaListResponse>(
            &timing, tenant_id, object_key,
            /*rpc_stat_key=*/static_cast<uint32_t>(PerfKey::QUERY_RPC_CALL),
            object_key, tenant_id, trace_id, client_id_);
    timer.LogResponseExpected(result);

    const char* status = "ok";
    if (!result && timing.backoff_count > 0) {
        status = "backoff_exhausted";
    } else if (!result) {
        status = "rpc_fail";
    } else if (timing.retry_count > 0) {
        status = "retry_ok";
    } else if (timing.backoff_count > 0) {
        status = "backoff_ok";
    }
    MC_LOG(INFO) << "query_rpc_breakdown key[" << object_key << "] switch_us["
                 << switch_us << "] rpc_first_us[" << timing.rpc_first_us
                 << "] route_refresh_us[" << timing.route_refresh_us
                 << "] retry_switch_us[" << timing.retry_switch_us
                 << "] rpc_retry_us[" << timing.rpc_retry_us << "] backoff_us["
                 << timing.backoff_us << "] total_us["
                 << std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - t0)
                        .count()
                 << "] retry_count[" << timing.retry_count << "] backoff_count["
                 << timing.backoff_count << "] status[" << status << "]";
    pt_total.End(result ? 0 : -1);
    return result;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterClient::BatchGetReplicaList(const std::vector<std::string>& object_keys) {
    return BatchGetReplicaList(object_keys, tenant_id_.value());
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterClient::BatchGetReplicaList(const std::vector<std::string>& object_keys,
                                  const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::BatchGetReplicaList");
    timer.LogRequest("keys_count=", object_keys.size(),
                     ", tenant_id=", tenant_id);

    // Single-master mode (routing not loaded): use the original batch path.
    if (partition_router_.Size() == 0) {
        const uint64_t trace_id = mooncake::logging::CurrentTraceId();
        auto result =
            invoke_batch_rpc<&WrappedMasterService::BatchGetReplicaList,
                             GetReplicaListResponse>(object_keys.size(),
                                                     object_keys, tenant_id,
                                                     trace_id, client_id_);
        timer.LogResponse("result=", result.size(), " operations");
        return result;
    }

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> results(
        object_keys.size(),
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));

    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    auto groups = GroupKeysBySubmaster(object_keys, tenant_id);
    for (auto& [submaster, indices] : groups) {
        if (submaster.empty()) {
            LOG(WARNING) << "BatchGetReplicaList: " << indices.size()
                         << " key(s) have no submaster, marked unavailable";
            continue;
        }
        SwitchToSubmasterByAddress(submaster);

        std::vector<std::string> group_keys;
        group_keys.reserve(indices.size());
        for (size_t idx : indices) {
            group_keys.push_back(object_keys[idx]);
        }

        auto group_result =
            invoke_batch_rpc<&WrappedMasterService::BatchGetReplicaList,
                             GetReplicaListResponse>(
                group_keys.size(), group_keys, tenant_id, trace_id, client_id_);
        for (size_t j = 0; j < indices.size(); ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }

    // 路由自愈：STALE_ROUTE/SLOT_NOT_OWNED/SLOT_MIGRATING/无 owner 的
    // 失败子集刷新路由后重发一次（有界）；仍失败保持终态错误。
    RetryBatchOnStaleRoute<GetReplicaListResponse>(
        tenant_id, object_keys, results,
        [this, &results, tenant_id, trace_id](
            const std::string& submaster, std::vector<std::string> group_keys,
            const std::vector<size_t>& group_orig) {
            SwitchToSubmasterByAddress(submaster);
            auto group_result =
                invoke_batch_rpc<&WrappedMasterService::BatchGetReplicaList,
                                 GetReplicaListResponse>(group_keys.size(),
                                                         group_keys, tenant_id,
                                                         trace_id, client_id_);
            if (group_result.size() != group_orig.size()) {
                return false;
            }
            bool all_ok = true;
            for (size_t j = 0; j < group_orig.size(); ++j) {
                results[group_orig[j]] = std::move(group_result[j]);
                if (!results[group_orig[j]]) {
                    all_ok = false;
                }
            }
            return all_ok;
        });

    timer.LogResponse("result=", results.size(), " operations");
    return results;
}

tl::expected<GetReplicaListResponse, ErrorCode>
MasterClient::GetReplicaListReadOnly(const std::string& object_key) {
    return GetReplicaListReadOnly(object_key, tenant_id_.value());
}

tl::expected<GetReplicaListResponse, ErrorCode>
MasterClient::GetReplicaListReadOnly(const std::string& object_key,
                                     const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaListReadOnly");
    timer.LogRequest("object_key=", object_key, ", tenant_id=", tenant_id);

    auto result = invoke_rpc<&WrappedMasterService::GetReplicaListForAdmin,
                             GetReplicaListResponse>(object_key, tenant_id);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterClient::BatchGetReplicaListReadOnly(
    const std::vector<std::string>& object_keys) {
    return BatchGetReplicaListReadOnly(object_keys, tenant_id_.value());
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterClient::BatchGetReplicaListReadOnly(
    const std::vector<std::string>& object_keys, const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::BatchGetReplicaListReadOnly");
    timer.LogRequest("keys_count=", object_keys.size(),
                     ", tenant_id=", tenant_id);

    auto result =
        invoke_batch_rpc<&WrappedMasterService::BatchGetReplicaListForAdmin,
                         GetReplicaListResponse>(object_keys.size(),
                                                 object_keys, tenant_id);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<PutStartResult, ErrorCode>
MasterClient::PutStart(const std::string& key,
                       const std::vector<size_t>& slice_lengths,
                       const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::PutStart");
    timer.LogRequest("key=", key, ", slice_count=", slice_lengths.size());

    ErrorCode switch_err = SwitchToSubmaster(tenant_id_.value(), key);
    if (switch_err != ErrorCode::OK) {
        timer.LogResponse("error_code=", switch_err);
        return tl::make_unexpected(switch_err);
    }

    uint64_t total_slice_length = 0;
    for (const auto& slice_length : slice_lengths) {
        total_slice_length += slice_length;
    }

    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    auto result = InvokeRoutedWithSlotRetry<&WrappedMasterService::PutStart,
                                            PutStartResult>(
        /*timing=*/nullptr, tenant_id_.value(), key, /*rpc_stat_key=*/0,
        client_id_, key, total_slice_length, config, tenant_id_.value(),
        trace_id);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<PutStartResult, ErrorCode>>
MasterClient::BatchPutStart(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<uint64_t>>& slice_lengths,
    const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutStart");
    timer.LogRequest("keys_count=", keys.size());

    std::vector<uint64_t> total_slice_lengths;
    total_slice_lengths.reserve(slice_lengths.size());
    for (const auto& per_key_lengths : slice_lengths) {
        uint64_t total_slice_length = 0;
        for (const auto& slice_length : per_key_lengths) {
            total_slice_length += slice_length;
        }
        total_slice_lengths.emplace_back(total_slice_length);
    }

    const uint64_t trace_id = mooncake::logging::CurrentTraceId();

    // Single-master mode (routing not loaded): use the original batch path.
    if (partition_router_.Size() == 0) {
        auto result = invoke_batch_rpc<&WrappedMasterService::BatchPutStart,
                                       PutStartResult>(
            keys.size(), client_id_, keys, total_slice_lengths, config,
            tenant_id_.value(), trace_id);
        timer.LogResponse("result=", result.size(), " operations");
        return result;
    }

    std::vector<tl::expected<PutStartResult, ErrorCode>> results(
        keys.size(),
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));

    auto groups = GroupKeysBySubmaster(keys, tenant_id_.value());
    for (auto& [submaster, indices] : groups) {
        if (submaster.empty()) {
            LOG(WARNING) << "BatchPutStart: " << indices.size()
                         << " key(s) have no submaster, marked unavailable";
            continue;
        }
        SwitchToSubmasterByAddress(submaster);

        std::vector<std::string> group_keys;
        std::vector<uint64_t> group_total_slice_lengths;
        group_keys.reserve(indices.size());
        group_total_slice_lengths.reserve(indices.size());
        for (size_t idx : indices) {
            group_keys.push_back(keys[idx]);
            group_total_slice_lengths.push_back(total_slice_lengths[idx]);
        }

        auto group_result =
            invoke_batch_rpc<&WrappedMasterService::BatchPutStart,
                             PutStartResult>(group_keys.size(), client_id_,
                                             group_keys,
                                             group_total_slice_lengths, config,
                                             tenant_id_.value(), trace_id);
        for (size_t j = 0; j < indices.size(); ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }

    // 路由自愈：STALE_ROUTE/SLOT_NOT_OWNED/SLOT_MIGRATING/无 owner 的
    // 失败子集刷新路由后重发一次（有界）；仍失败保持终态错误
    // （Client::StartBatchPut 收到后走 SetTerminalError，不再有下一轮）。
    RetryBatchOnStaleRoute<PutStartResult>(
        tenant_id_.value(), keys, results,
        [this, &results, &config, &total_slice_lengths, trace_id](
            const std::string& submaster, std::vector<std::string> group_keys,
            const std::vector<size_t>& group_orig) {
            SwitchToSubmasterByAddress(submaster);
            std::vector<uint64_t> group_total_slice_lengths;
            group_total_slice_lengths.reserve(group_orig.size());
            for (size_t idx : group_orig) {
                group_total_slice_lengths.push_back(total_slice_lengths[idx]);
            }
            auto group_result =
                invoke_batch_rpc<&WrappedMasterService::BatchPutStart,
                                 PutStartResult>(
                    group_keys.size(), client_id_, group_keys,
                    group_total_slice_lengths, config, tenant_id_.value(),
                    trace_id);
            if (group_result.size() != group_orig.size()) {
                return false;
            }
            bool all_ok = true;
            for (size_t j = 0; j < group_orig.size(); ++j) {
                results[group_orig[j]] = std::move(group_result[j]);
                if (!results[group_orig[j]]) {
                    all_ok = false;
                }
            }
            return all_ok;
        });

    timer.LogResponse("result=", results.size(), " operations");
    return results;
}

tl::expected<void, ErrorCode> MasterClient::PutEnd(
    const ObjectMeta& object_meta, ReplicaType replica_type,
    const std::string& operation_id) {
    ScopedVLogTimer timer(1, "MasterClient::PutEnd");
    timer.LogRequest("key=", object_meta.key);

    ErrorCode switch_err =
        SwitchToSubmaster(tenant_id_.value(), object_meta.key);
    if (switch_err != ErrorCode::OK) {
        timer.LogResponse("error_code=", switch_err);
        return tl::make_unexpected(switch_err);
    }

    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    auto result =
        InvokeRoutedWithSlotRetry<&WrappedMasterService::PutEnd, void>(
            /*timing=*/nullptr, tenant_id_.value(), object_meta.key,
            /*rpc_stat_key=*/0, client_id_, object_meta, replica_type,
            tenant_id_.value(), trace_id, operation_id);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutEnd(
    const std::vector<ObjectMeta>& object_metas, ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutEnd");
    timer.LogRequest("keys_count=", object_metas.size());

    const uint64_t trace_id = mooncake::logging::CurrentTraceId();

    // Single-master mode (routing not loaded): use the original batch path.
    if (partition_router_.Size() == 0) {
        auto result =
            invoke_batch_rpc<&WrappedMasterService::BatchPutEnd, void>(
                object_metas.size(), client_id_, object_metas, replica_type,
                tenant_id_.value(), trace_id);
        timer.LogResponse("result=", result.size(), " operations");
        return result;
    }

    std::vector<tl::expected<void, ErrorCode>> results(
        object_metas.size(),
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));

    std::vector<std::string> keys;
    keys.reserve(object_metas.size());
    for (const auto& meta : object_metas) {
        keys.push_back(meta.key);
    }

    auto groups = GroupKeysBySubmaster(keys, tenant_id_.value());
    for (auto& [submaster, indices] : groups) {
        if (submaster.empty()) {
            LOG(WARNING) << "BatchPutEnd: " << indices.size()
                         << " key(s) have no submaster, marked unavailable";
            continue;
        }
        SwitchToSubmasterByAddress(submaster);

        std::vector<ObjectMeta> group_object_metas;
        group_object_metas.reserve(indices.size());
        for (size_t idx : indices) {
            group_object_metas.push_back(object_metas[idx]);
        }

        auto group_result =
            invoke_batch_rpc<&WrappedMasterService::BatchPutEnd, void>(
                group_object_metas.size(), client_id_, group_object_metas,
                replica_type, tenant_id_.value(), trace_id);
        for (size_t j = 0; j < indices.size(); ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }

    // 路由自愈：STALE_ROUTE/SLOT_NOT_OWNED/SLOT_MIGRATING/无 owner 的
    // 失败子集刷新路由后重发一次（有界）；仍失败保持终态错误。
    RetryBatchOnStaleRoute<void>(
        tenant_id_.value(), keys, results,
        [this, &results, &object_metas, replica_type, trace_id](
            const std::string& submaster, std::vector<std::string> group_keys,
            const std::vector<size_t>& group_orig) {
            SwitchToSubmasterByAddress(submaster);
            std::vector<ObjectMeta> group_object_metas;
            group_object_metas.reserve(group_orig.size());
            for (size_t idx : group_orig) {
                group_object_metas.push_back(object_metas[idx]);
            }
            auto group_result =
                invoke_batch_rpc<&WrappedMasterService::BatchPutEnd, void>(
                    group_object_metas.size(), client_id_, group_object_metas,
                    replica_type, tenant_id_.value(), trace_id);
            if (group_result.size() != group_orig.size()) {
                return false;
            }
            bool all_ok = true;
            for (size_t j = 0; j < group_orig.size(); ++j) {
                results[group_orig[j]] = std::move(group_result[j]);
                if (!results[group_orig[j]]) {
                    all_ok = false;
                }
            }
            return all_ok;
        });

    timer.LogResponse("result=", results.size(), " operations");
    return results;
}

tl::expected<void, ErrorCode> MasterClient::PutRevoke(
    const std::string& key, ReplicaType replica_type,
    const std::string& operation_id) {
    ScopedVLogTimer timer(1, "MasterClient::PutRevoke");
    timer.LogRequest("key=", key);

    ErrorCode switch_err = SwitchToSubmaster(tenant_id_.value(), key);
    if (switch_err != ErrorCode::OK) {
        timer.LogResponse("error_code=", switch_err);
        return tl::make_unexpected(switch_err);
    }

    auto result =
        InvokeRoutedWithSlotRetry<&WrappedMasterService::PutRevoke, void>(
            /*timing=*/nullptr, tenant_id_.value(), key, /*rpc_stat_key=*/0,
            client_id_, key, replica_type, tenant_id_.value(), operation_id);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutRevoke(
    const std::vector<std::string>& keys, ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutRevoke");
    timer.LogRequest("keys_count=", keys.size());

    // Single-master mode (routing not loaded): use the original batch path.
    if (partition_router_.Size() == 0) {
        auto result =
            invoke_batch_rpc<&WrappedMasterService::BatchPutRevoke, void>(
                keys.size(), client_id_, keys, replica_type,
                tenant_id_.value());
        timer.LogResponse("result=", result.size(), " operations");
        return result;
    }

    std::vector<tl::expected<void, ErrorCode>> results(
        keys.size(),
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));

    auto groups = GroupKeysBySubmaster(keys, tenant_id_.value());
    for (auto& [submaster, indices] : groups) {
        if (submaster.empty()) {
            LOG(WARNING) << "BatchPutRevoke: " << indices.size()
                         << " key(s) have no submaster, marked unavailable";
            continue;
        }
        SwitchToSubmasterByAddress(submaster);

        std::vector<std::string> group_keys;
        group_keys.reserve(indices.size());
        for (size_t idx : indices) {
            group_keys.push_back(keys[idx]);
        }

        auto group_result =
            invoke_batch_rpc<&WrappedMasterService::BatchPutRevoke, void>(
                group_keys.size(), client_id_, group_keys, replica_type,
                tenant_id_.value());
        for (size_t j = 0; j < indices.size(); ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }

    // 路由自愈：STALE_ROUTE/SLOT_NOT_OWNED/SLOT_MIGRATING/无 owner 的
    // 失败子集刷新路由后重发一次（有界）；仍失败保持终态错误。
    RetryBatchOnStaleRoute<void>(
        tenant_id_.value(), keys, results,
        [this, &results, replica_type](const std::string& submaster,
                                       std::vector<std::string> group_keys,
                                       const std::vector<size_t>& group_orig) {
            SwitchToSubmasterByAddress(submaster);
            auto group_result =
                invoke_batch_rpc<&WrappedMasterService::BatchPutRevoke, void>(
                    group_keys.size(), client_id_, group_keys, replica_type,
                    tenant_id_.value());
            if (group_result.size() != group_orig.size()) {
                return false;
            }
            bool all_ok = true;
            for (size_t j = 0; j < group_orig.size(); ++j) {
                results[group_orig[j]] = std::move(group_result[j]);
                if (!results[group_orig[j]]) {
                    all_ok = false;
                }
            }
            return all_ok;
        });

    timer.LogResponse("result=", results.size(), " operations");
    return results;
}

tl::expected<PutStartResult, ErrorCode> MasterClient::UpsertStart(
    const std::string& key, const std::vector<size_t>& slice_lengths,
    const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::UpsertStart");
    timer.LogRequest("key=", key, ", slice_count=", slice_lengths.size());

    ErrorCode switch_err = SwitchToSubmaster(tenant_id_.value(), key);
    if (switch_err != ErrorCode::OK) {
        timer.LogResponse("error_code=", switch_err);
        return tl::make_unexpected(switch_err);
    }

    uint64_t total_slice_length = 0;
    for (const auto& slice_length : slice_lengths) {
        total_slice_length += slice_length;
    }

    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    auto result = InvokeRoutedWithSlotRetry<&WrappedMasterService::UpsertStart,
                                            PutStartResult>(
        /*timing=*/nullptr, tenant_id_.value(), key, /*rpc_stat_key=*/0,
        client_id_, key, total_slice_length, config, tenant_id_.value(),
        trace_id);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<PutStartResult, ErrorCode>>
MasterClient::BatchUpsertStart(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<uint64_t>>& slice_lengths,
    const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::BatchUpsertStart");
    timer.LogRequest("keys_count=", keys.size());

    std::vector<uint64_t> total_slice_lengths;
    total_slice_lengths.reserve(slice_lengths.size());
    for (const auto& key_slices : slice_lengths) {
        uint64_t total = 0;
        for (const auto& sl : key_slices) {
            total += sl;
        }
        total_slice_lengths.emplace_back(total);
    }

    // Single-master mode (routing not loaded): use the original batch path.
    if (partition_router_.Size() == 0) {
        auto result = invoke_batch_rpc<&WrappedMasterService::BatchUpsertStart,
                                       PutStartResult>(
            keys.size(), client_id_, keys, total_slice_lengths, config,
            tenant_id_.value());
        timer.LogResponse("result=", result.size(), " operations");
        return result;
    }

    std::vector<tl::expected<PutStartResult, ErrorCode>> results(
        keys.size(),
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));

    auto groups = GroupKeysBySubmaster(keys, tenant_id_.value());
    for (auto& [submaster, indices] : groups) {
        if (submaster.empty()) {
            LOG(WARNING) << "BatchUpsertStart: " << indices.size()
                         << " key(s) have no submaster, marked unavailable";
            continue;
        }
        SwitchToSubmasterByAddress(submaster);

        std::vector<std::string> group_keys;
        std::vector<uint64_t> group_total_slice_lengths;
        group_keys.reserve(indices.size());
        group_total_slice_lengths.reserve(indices.size());
        for (size_t idx : indices) {
            group_keys.push_back(keys[idx]);
            group_total_slice_lengths.push_back(total_slice_lengths[idx]);
        }

        auto group_result =
            invoke_batch_rpc<&WrappedMasterService::BatchUpsertStart,
                             PutStartResult>(
                group_keys.size(), client_id_, group_keys,
                group_total_slice_lengths, config, tenant_id_.value());
        for (size_t j = 0; j < indices.size(); ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }

    // 路由自愈：STALE_ROUTE/SLOT_NOT_OWNED/SLOT_MIGRATING/无 owner 的
    // 失败子集刷新路由后重发一次（有界）；仍失败保持终态错误。
    RetryBatchOnStaleRoute<PutStartResult>(
        tenant_id_.value(), keys, results,
        [this, &results, &total_slice_lengths, &config](
            const std::string& submaster, std::vector<std::string> group_keys,
            const std::vector<size_t>& group_orig) {
            SwitchToSubmasterByAddress(submaster);
            std::vector<uint64_t> group_total_slice_lengths;
            group_total_slice_lengths.reserve(group_orig.size());
            for (size_t idx : group_orig) {
                group_total_slice_lengths.push_back(total_slice_lengths[idx]);
            }
            auto group_result =
                invoke_batch_rpc<&WrappedMasterService::BatchUpsertStart,
                                 PutStartResult>(
                    group_keys.size(), client_id_, group_keys,
                    group_total_slice_lengths, config, tenant_id_.value());
            if (group_result.size() != group_orig.size()) {
                return false;
            }
            bool all_ok = true;
            for (size_t j = 0; j < group_orig.size(); ++j) {
                results[group_orig[j]] = std::move(group_result[j]);
                if (!results[group_orig[j]]) {
                    all_ok = false;
                }
            }
            return all_ok;
        });

    timer.LogResponse("result=", results.size(), " operations");
    return results;
}

tl::expected<void, ErrorCode> MasterClient::UpsertEnd(
    const ObjectMeta& object_meta, ReplicaType replica_type,
    const std::string& operation_id) {
    ScopedVLogTimer timer(1, "MasterClient::UpsertEnd");
    timer.LogRequest("key=", object_meta.key);

    ErrorCode switch_err =
        SwitchToSubmaster(tenant_id_.value(), object_meta.key);
    if (switch_err != ErrorCode::OK) {
        timer.LogResponse("error_code=", switch_err);
        return tl::make_unexpected(switch_err);
    }

    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    auto result =
        InvokeRoutedWithSlotRetry<&WrappedMasterService::UpsertEnd, void>(
            /*timing=*/nullptr, tenant_id_.value(), object_meta.key,
            /*rpc_stat_key=*/0, client_id_, object_meta, replica_type,
            tenant_id_.value(), trace_id, operation_id);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchUpsertEnd(
    const std::vector<ObjectMeta>& object_metas) {
    ScopedVLogTimer timer(1, "MasterClient::BatchUpsertEnd");
    timer.LogRequest("keys_count=", object_metas.size());

    // Single-master mode (routing not loaded): use the original batch path.
    if (partition_router_.Size() == 0) {
        auto result =
            invoke_batch_rpc<&WrappedMasterService::BatchUpsertEnd, void>(
                object_metas.size(), client_id_, object_metas,
                tenant_id_.value());
        timer.LogResponse("result=", result.size(), " operations");
        return result;
    }

    std::vector<tl::expected<void, ErrorCode>> results(
        object_metas.size(),
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));

    std::vector<std::string> keys;
    keys.reserve(object_metas.size());
    for (const auto& meta : object_metas) {
        keys.push_back(meta.key);
    }

    auto groups = GroupKeysBySubmaster(keys, tenant_id_.value());
    for (auto& [submaster, indices] : groups) {
        if (submaster.empty()) {
            LOG(WARNING) << "BatchUpsertEnd: " << indices.size()
                         << " key(s) have no submaster, marked unavailable";
            continue;
        }
        SwitchToSubmasterByAddress(submaster);

        std::vector<ObjectMeta> group_object_metas;
        group_object_metas.reserve(indices.size());
        for (size_t idx : indices) {
            group_object_metas.push_back(object_metas[idx]);
        }

        auto group_result =
            invoke_batch_rpc<&WrappedMasterService::BatchUpsertEnd, void>(
                group_object_metas.size(), client_id_, group_object_metas,
                tenant_id_.value());
        for (size_t j = 0; j < indices.size(); ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }

    // 路由自愈：STALE_ROUTE/SLOT_NOT_OWNED/SLOT_MIGRATING/无 owner 的
    // 失败子集刷新路由后重发一次（有界）；仍失败保持终态错误。
    RetryBatchOnStaleRoute<void>(
        tenant_id_.value(), keys, results,
        [this, &results, &object_metas](const std::string& submaster,
                                        std::vector<std::string> group_keys,
                                        const std::vector<size_t>& group_orig) {
            SwitchToSubmasterByAddress(submaster);
            std::vector<ObjectMeta> group_object_metas;
            group_object_metas.reserve(group_orig.size());
            for (size_t idx : group_orig) {
                group_object_metas.push_back(object_metas[idx]);
            }
            auto group_result =
                invoke_batch_rpc<&WrappedMasterService::BatchUpsertEnd, void>(
                    group_object_metas.size(), client_id_, group_object_metas,
                    tenant_id_.value());
            if (group_result.size() != group_orig.size()) {
                return false;
            }
            bool all_ok = true;
            for (size_t j = 0; j < group_orig.size(); ++j) {
                results[group_orig[j]] = std::move(group_result[j]);
                if (!results[group_orig[j]]) {
                    all_ok = false;
                }
            }
            return all_ok;
        });

    timer.LogResponse("result=", results.size(), " operations");
    return results;
}

tl::expected<void, ErrorCode> MasterClient::UpsertRevoke(
    const std::string& key, ReplicaType replica_type,
    const std::string& operation_id) {
    ScopedVLogTimer timer(1, "MasterClient::UpsertRevoke");
    timer.LogRequest("key=", key);

    ErrorCode switch_err = SwitchToSubmaster(tenant_id_.value(), key);
    if (switch_err != ErrorCode::OK) {
        timer.LogResponse("error_code=", switch_err);
        return tl::make_unexpected(switch_err);
    }

    const uint64_t trace_id = mooncake::logging::CurrentTraceId();
    auto result =
        InvokeRoutedWithSlotRetry<&WrappedMasterService::UpsertRevoke, void>(
            /*timing=*/nullptr, tenant_id_.value(), key, /*rpc_stat_key=*/0,
            client_id_, key, replica_type, tenant_id_.value(), trace_id,
            operation_id);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchUpsertRevoke(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchUpsertRevoke");
    timer.LogRequest("keys_count=", keys.size());

    // Single-master mode (routing not loaded): use the original batch path.
    if (partition_router_.Size() == 0) {
        auto result =
            invoke_batch_rpc<&WrappedMasterService::BatchUpsertRevoke, void>(
                keys.size(), client_id_, keys, tenant_id_.value());
        timer.LogResponse("result=", result.size(), " operations");
        return result;
    }

    std::vector<tl::expected<void, ErrorCode>> results(
        keys.size(),
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));

    auto groups = GroupKeysBySubmaster(keys, tenant_id_.value());
    for (auto& [submaster, indices] : groups) {
        if (submaster.empty()) {
            LOG(WARNING) << "BatchUpsertRevoke: " << indices.size()
                         << " key(s) have no submaster, marked unavailable";
            continue;
        }
        SwitchToSubmasterByAddress(submaster);

        std::vector<std::string> group_keys;
        group_keys.reserve(indices.size());
        for (size_t idx : indices) {
            group_keys.push_back(keys[idx]);
        }

        auto group_result =
            invoke_batch_rpc<&WrappedMasterService::BatchUpsertRevoke, void>(
                group_keys.size(), client_id_, group_keys, tenant_id_.value());
        for (size_t j = 0; j < indices.size(); ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }

    // 路由自愈：STALE_ROUTE/SLOT_NOT_OWNED/SLOT_MIGRATING/无 owner 的
    // 失败子集刷新路由后重发一次（有界）；仍失败保持终态错误。
    RetryBatchOnStaleRoute<void>(
        tenant_id_.value(), keys, results,
        [this, &results](const std::string& submaster,
                         std::vector<std::string> group_keys,
                         const std::vector<size_t>& group_orig) {
            SwitchToSubmasterByAddress(submaster);
            auto group_result =
                invoke_batch_rpc<&WrappedMasterService::BatchUpsertRevoke,
                                 void>(group_keys.size(), client_id_,
                                       group_keys, tenant_id_.value());
            if (group_result.size() != group_orig.size()) {
                return false;
            }
            bool all_ok = true;
            for (size_t j = 0; j < group_orig.size(); ++j) {
                results[group_orig[j]] = std::move(group_result[j]);
                if (!results[group_orig[j]]) {
                    all_ok = false;
                }
            }
            return all_ok;
        });

    timer.LogResponse("result=", results.size(), " operations");
    return results;
}

tl::expected<void, ErrorCode> MasterClient::Remove(const std::string& key,
                                                   bool force) {
    ScopedVLogTimer timer(1, "MasterClient::Remove");
    timer.LogRequest("key=", key, ", force=", force);

    ErrorCode switch_err = SwitchToSubmaster(tenant_id_.value(), key);
    if (switch_err != ErrorCode::OK) {
        timer.LogResponse("error_code=", switch_err);
        return tl::make_unexpected(switch_err);
    }

    auto result =
        InvokeRoutedWithSlotRetry<&WrappedMasterService::Remove, void>(
            /*timing=*/nullptr, tenant_id_.value(), key, /*rpc_stat_key=*/0,
            key, force, tenant_id_.value());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> MasterClient::RemoveByRegex(
    const std::string& str, bool force) {
    ScopedVLogTimer timer(1, "MasterClient::RemoveByRegex");
    timer.LogRequest("key=", str, ", force=", force);

    auto result = invoke_rpc<&WrappedMasterService::RemoveByRegex, long>(
        str, force, tenant_id_.value());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> MasterClient::RemoveAll(bool force) {
    ScopedVLogTimer timer(1, "MasterClient::RemoveAll");
    timer.LogRequest("action=remove_all_objects, force=", force);

    auto result = invoke_rpc<&WrappedMasterService::RemoveAll, long>(
        force, tenant_id_.value());
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchRemove(
    const std::vector<std::string>& keys, bool force) {
    ScopedVLogTimer timer(1, "MasterClient::BatchRemove");
    timer.LogRequest("keys_count=", keys.size(), ", force=", force);

    // Single-master mode (routing not loaded): use the original batch path.
    if (partition_router_.Size() == 0) {
        auto result =
            invoke_batch_rpc<&WrappedMasterService::BatchRemove, void>(
                keys.size(), keys, force, tenant_id_.value());
        timer.LogResponse("result=", result.size(), " operations");
        return result;
    }

    std::vector<tl::expected<void, ErrorCode>> results(
        keys.size(),
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));

    auto groups = GroupKeysBySubmaster(keys, tenant_id_.value());
    for (auto& [submaster, indices] : groups) {
        if (submaster.empty()) {
            LOG(WARNING) << "BatchRemove: " << indices.size()
                         << " key(s) have no submaster, marked unavailable";
            continue;
        }
        SwitchToSubmasterByAddress(submaster);

        std::vector<std::string> group_keys;
        group_keys.reserve(indices.size());
        for (size_t idx : indices) {
            group_keys.push_back(keys[idx]);
        }

        auto group_result =
            invoke_batch_rpc<&WrappedMasterService::BatchRemove, void>(
                group_keys.size(), group_keys, force, tenant_id_.value());
        for (size_t j = 0; j < indices.size(); ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }

    // 路由自愈：STALE_ROUTE/SLOT_NOT_OWNED/SLOT_MIGRATING/无 owner 的
    // 失败子集刷新路由后重发一次（有界）；仍失败保持终态错误。
    RetryBatchOnStaleRoute<void>(
        tenant_id_.value(), keys, results,
        [this, &results, force](const std::string& submaster,
                                std::vector<std::string> group_keys,
                                const std::vector<size_t>& group_orig) {
            SwitchToSubmasterByAddress(submaster);
            auto group_result =
                invoke_batch_rpc<&WrappedMasterService::BatchRemove, void>(
                    group_keys.size(), group_keys, force, tenant_id_.value());
            if (group_result.size() != group_orig.size()) {
                return false;
            }
            bool all_ok = true;
            for (size_t j = 0; j < group_orig.size(); ++j) {
                results[group_orig[j]] = std::move(group_result[j]);
                if (!results[group_orig[j]]) {
                    all_ok = false;
                }
            }
            return all_ok;
        });

    timer.LogResponse("result=", results.size(), " operations");
    return results;
}

tl::expected<void, ErrorCode> MasterClient::MountSegment(
    const Segment& segment) {
    ScopedVLogTimer timer(1, "MasterClient::MountSegment");
    timer.LogRequest("base=", segment.base, ", size=", segment.size,
                     ", name=", segment.name, ", id=", segment.id,
                     ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::MountSegment, void>(
        segment, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MountSegmentTo(
    const std::string& address, const Segment& segment) {
    ScopedVLogTimer timer(1, "MasterClient::MountSegmentTo");
    timer.LogRequest("address=", address, ", id=", segment.id,
                     ", client_id=", client_id_);

    auto result = invoke_rpc_to<&WrappedMasterService::MountSegment, void>(
        address, segment, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MountNoFSegment(
    const NoFSegment& segment) {
    ScopedVLogTimer timer(1, "MasterClient::MountNofSegment");
    timer.LogRequest("NoF segment mount: ", "base=", segment.base,
                     ", size=", segment.size, ", name=", segment.name,
                     ", id=", segment.id, ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::MountNoFSegment, void>(
        segment, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::ReMountSegment(
    const std::vector<Segment>& segments) {
    ScopedVLogTimer timer(1, "MasterClient::ReMountSegment");
    timer.LogRequest("segments_num=", segments.size(),
                     ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::ReMountSegment, void>(
        segments, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::ReMountNoFSegment(
    const std::vector<NoFSegment>& segments) {
    ScopedVLogTimer timer(1, "MasterClient::ReMountNofSegment");
    timer.LogRequest("NoF segment remount: ", "segments_num=", segments.size(),
                     ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::ReMountNoFSegment, void>(
        segments, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::UnmountSegment(
    const UUID& segment_id) {
    ScopedVLogTimer timer(1, "MasterClient::UnmountSegment");
    timer.LogRequest("segment_id=", segment_id, ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::UnmountSegment, void>(
        segment_id, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::UnmountSegmentTo(
    const std::string& address, const UUID& segment_id) {
    ScopedVLogTimer timer(1, "MasterClient::UnmountSegmentTo");
    timer.LogRequest("address=", address, ", segment_id=", segment_id,
                     ", client_id=", client_id_);

    auto result = invoke_rpc_to<&WrappedMasterService::UnmountSegment, void>(
        address, segment_id, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::GracefulUnmountSegment(
    const UUID& segment_id, uint64_t grace_period_ms) {
    ScopedVLogTimer timer(1, "MasterClient::GracefulUnmountSegment");
    timer.LogRequest("segment_id=", segment_id, ", client_id=", client_id_,
                     ", grace_period_ms=", grace_period_ms);

    auto result =
        invoke_rpc<&WrappedMasterService::GracefulUnmountSegment, void>(
            segment_id, client_id_, grace_period_ms);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::GracefulUnmountSegmentTo(
    const std::string& address, const UUID& segment_id,
    uint64_t grace_period_ms) {
    ScopedVLogTimer timer(1, "MasterClient::GracefulUnmountSegmentTo");
    timer.LogRequest("address=", address, ", segment_id=", segment_id,
                     ", client_id=", client_id_,
                     ", grace_period_ms=", grace_period_ms);

    auto result =
        invoke_rpc_to<&WrappedMasterService::GracefulUnmountSegment, void>(
            address, segment_id, client_id_, grace_period_ms);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::UnmountNoFSegment(
    const UUID& segment_id) {
    ScopedVLogTimer timer(1, "MasterClient::UnmountNoFSegment");
    timer.LogRequest("NoF segment unmount: ", "segment_id=", segment_id,
                     ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::UnmountNoFSegment, void>(
        segment_id, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<NoFSegment>, ErrorCode>
MasterClient::GetAllNoFSegments() {
    ScopedVLogTimer timer(1, "MasterClient::GetAllNoFSegments");
    timer.LogRequest("Get all NoF segments, client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::GetAllNoFSegments,
                             std::vector<NoFSegment>>();
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<std::string>, ErrorCode>
MasterClient::GetAllSegments() {
    ScopedVLogTimer timer(1, "MasterClient::GetAllSegments");
    timer.LogRequest("Get all segments, client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::GetAllSegmentsForAdmin,
                             std::vector<std::string>>();
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode>
MasterClient::GetNoFSegmentsByName(const std::string& segment_name) {
    ScopedVLogTimer timer(1, "MasterClient::GetNoFSegmentsByName");
    timer.LogRequest("segment_name=", segment_name, ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::GetNoFSegmentsByName,
                             std::vector<NoFSegmentOwnerInfo>>(segment_name);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<PingResponse, ErrorCode> MasterClient::Ping() {
    ScopedVLogTimer timer(1, "MasterClient::Ping");
    timer.LogRequest("client_id=", client_id_);

    std::shared_ptr<RpcClientPool::ClientPool> ping_pool;
    {
        MutexLocker lock(&connect_mutex_);
        ping_pool =
            ha_connection_policy_enabled_.load(std::memory_order_acquire)
                ? ha_control_client_accessor_.GetClientPool()
                : client_accessor_.GetClientPool();
    }
    auto result =
        invoke_rpc_with_client_pool<&WrappedMasterService::Ping, PingResponse>(
            ping_pool, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

std::string MasterClient::GetCurrentAddress() const {
    return client_accessor_.GetAddress();
}

std::vector<std::string> MasterClient::GetSubmasterAddresses() const {
    return partition_router_.GetAllPrimaryAddresses();
}

tl::expected<PingResponse, ErrorCode> MasterClient::PingTo(
    const std::string& address) {
    ScopedVLogTimer timer(1, "MasterClient::PingTo");
    timer.LogRequest("address=", address, ", client_id=", client_id_);

    auto result = invoke_rpc_to<&WrappedMasterService::Ping, PingResponse>(
        address, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::string, ErrorCode> MasterClient::GetFsdir() {
    ScopedVLogTimer timer(1, "MasterClient::GetFsdir");
    timer.LogRequest("action=get_fsdir");

    auto result = invoke_rpc<&WrappedMasterService::GetFsdir, std::string>();
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<SegmentStatus, ErrorCode> MasterClient::QuerySegmentStatusById(
    const UUID& segment_id) {
    ScopedVLogTimer timer(1, "MasterClient::QuerySegmentStatusById");
    timer.LogRequest("segment_id=", segment_id);

    auto result = invoke_rpc<&WrappedMasterService::QuerySegmentStatusById,
                             SegmentStatus>(segment_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<SegmentStatus, ErrorCode> MasterClient::QuerySegmentStatusByIdTo(
    const std::string& address, const UUID& segment_id) {
    ScopedVLogTimer timer(1, "MasterClient::QuerySegmentStatusByIdTo");
    timer.LogRequest("address=", address, ", segment_id=", segment_id);

    auto result = invoke_rpc_to<&WrappedMasterService::QuerySegmentStatusById,
                                SegmentStatus>(address, segment_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<GetStorageConfigResponse, ErrorCode>
MasterClient::GetStorageConfig() {
    ScopedVLogTimer timer(1, "MasterClient::GetStorageConfig");
    timer.LogRequest("action=get_storage_config");

    auto result = invoke_rpc<&WrappedMasterService::GetStorageConfig,
                             GetStorageConfigResponse>();
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MountLocalDiskSegment(
    const UUID& client_id, bool enable_offloading) {
    ScopedVLogTimer timer(1, "MasterClient::MountLocalDiskSegment");
    timer.LogRequest("client_id=", client_id,
                     ", enable_offloading=", enable_offloading);

    auto result =
        invoke_rpc<&WrappedMasterService::MountLocalDiskSegment, void>(
            client_id, enable_offloading);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::UnmountLocalDiskSegment(
    const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::UnmountLocalDiskSegment");
    timer.LogRequest("client_id=", client_id);

    auto result =
        invoke_rpc<&WrappedMasterService::UnmountLocalDiskSegment, void>(
            client_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MountLocalDiskSegmentTo(
    const std::string& address, const UUID& client_id, bool enable_offloading) {
    ScopedVLogTimer timer(1, "MasterClient::MountLocalDiskSegmentTo");
    timer.LogRequest("address=", address, ", client_id=", client_id,
                     ", enable_offloading=", enable_offloading);

    auto result =
        invoke_rpc_to<&WrappedMasterService::MountLocalDiskSegment, void>(
            address, client_id, enable_offloading);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::UnmountLocalDiskSegmentTo(
    const std::string& address, const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::UnmountLocalDiskSegmentTo");
    timer.LogRequest("address=", address, ", client_id=", client_id);

    auto result =
        invoke_rpc_to<&WrappedMasterService::UnmountLocalDiskSegment, void>(
            address, client_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<UUID, ErrorCode> MasterClient::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    return CreateCopyTask(key, tenant_id_.value(), targets);
}

tl::expected<UUID, ErrorCode> MasterClient::CreateCopyTask(
    const std::string& key, const std::string& tenant_id,
    const std::vector<std::string>& targets) {
    ScopedVLogTimer timer(1, "MasterClient::CreateCopyTask");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id,
                     ", targets_size=", targets.size());

    auto result = invoke_rpc<&WrappedMasterService::CreateCopyTask, UUID>(
        key, tenant_id, targets);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<UUID, ErrorCode> MasterClient::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    return CreateMoveTask(key, tenant_id_.value(), source, target);
}

tl::expected<UUID, ErrorCode> MasterClient::CreateMoveTask(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::string& target) {
    ScopedVLogTimer timer(1, "MasterClient::CreateMoveTask");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id,
                     ", source=", source, ", target=", target);

    auto result = invoke_rpc<&WrappedMasterService::CreateMoveTask, UUID>(
        key, tenant_id, source, target);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
MasterClient::OffloadObjectHeartbeat(const UUID& client_id,
                                     bool enable_offloading) {
    ScopedVLogTimer timer(1, "MasterClient::OffloadObjectHeartbeat");
    timer.LogRequest("client_id=", client_id,
                     ", enable_offloading=", enable_offloading);

    auto result =
        invoke_rpc<&WrappedMasterService::OffloadObjectHeartbeat,
                   std::vector<OffloadTaskItem>>(client_id, enable_offloading);
    return result;
}

tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
MasterClient::OffloadObjectHeartbeatTo(const std::string& address,
                                       const UUID& client_id,
                                       bool enable_offloading) {
    ScopedVLogTimer timer(1, "MasterClient::OffloadObjectHeartbeatTo");
    timer.LogRequest("address=", address, ", client_id=", client_id,
                     ", enable_offloading=", enable_offloading);

    auto result = invoke_rpc_to<&WrappedMasterService::OffloadObjectHeartbeat,
                                std::vector<OffloadTaskItem>>(
        address, client_id, enable_offloading);
    return result;
}

tl::expected<bool, ErrorCode> MasterClient::PollRemoveAll() {
    ScopedVLogTimer timer(1, "MasterClient::PollRemoveAll");
    timer.LogRequest("client_id=", client_id_);

    auto result =
        invoke_rpc<&WrappedMasterService::PollRemoveAll, bool>(client_id_);
    timer.LogResponse("should_remove_all=",
                      result.has_value() ? result.value() : false);
    return result;
}

tl::expected<bool, ErrorCode> MasterClient::PollRemoveAllTo(
    const std::string& address, const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::PollRemoveAllTo");
    timer.LogRequest("address=", address, ", client_id=", client_id);

    auto result = invoke_rpc_to<&WrappedMasterService::PollRemoveAll, bool>(
        address, client_id);
    timer.LogResponse("should_remove_all=",
                      result.has_value() ? result.value() : false);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::ReportSsdCapacity(
    const UUID& client_id, int64_t ssd_total_capacity_bytes) {
    ScopedVLogTimer timer(1, "MasterClient::ReportSsdCapacity");
    timer.LogRequest("client_id=", client_id,
                     ", ssd_total_capacity_bytes=", ssd_total_capacity_bytes);
    return invoke_rpc<&WrappedMasterService::ReportSsdCapacity, void>(
        client_id, ssd_total_capacity_bytes);
}

tl::expected<void, ErrorCode> MasterClient::ReportSsdCapacityTo(
    const std::string& address, const UUID& client_id,
    int64_t ssd_total_capacity_bytes) {
    ScopedVLogTimer timer(1, "MasterClient::ReportSsdCapacityTo");
    timer.LogRequest("address=", address, ", client_id=", client_id,
                     ", ssd_total_capacity_bytes=", ssd_total_capacity_bytes);
    return invoke_rpc_to<&WrappedMasterService::ReportSsdCapacity, void>(
        address, client_id, ssd_total_capacity_bytes);
}

tl::expected<void, ErrorCode> MasterClient::ReportInvalidReplicaEndpoints(
    const std::vector<std::string>& endpoints) {
    ScopedVLogTimer timer(1, "MasterClient::ReportInvalidReplicaEndpoints");
    timer.LogRequest("endpoints_count=", endpoints.size());
    return invoke_rpc<&WrappedMasterService::ReportInvalidReplicaEndpoints,
                      void>(endpoints);
}

tl::expected<void, ErrorCode> MasterClient::ReportInvalidReplicaEndpointsTo(
    const std::string& address, const std::vector<std::string>& endpoints) {
    ScopedVLogTimer timer(1, "MasterClient::ReportInvalidReplicaEndpointsTo");
    timer.LogRequest("address=", address,
                     ", endpoints_count=", endpoints.size());
    return invoke_rpc_to<&WrappedMasterService::ReportInvalidReplicaEndpoints,
                         void>(address, endpoints);
}

tl::expected<void, ErrorCode> MasterClient::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    std::vector<OffloadTaskItem> tasks;
    tasks.reserve(keys.size());
    for (const auto& key : keys) {
        tasks.push_back(OffloadTaskItem{
            .tenant_id = tenant_id_.value(), .key = key, .size = 0});
    }
    return NotifyOffloadSuccess(client_id, tasks, metadatas);
}

tl::expected<void, ErrorCode> MasterClient::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
    const std::vector<StorageObjectMetadata>& metadatas) {
    ScopedVLogTimer timer(1, "MasterClient::NotifyOffloadSuccess");
    timer.LogRequest("client_id=", client_id, ", tasks_count=", tasks.size(),
                     ", metadatas_count=", metadatas.size());

    auto result = invoke_rpc<&WrappedMasterService::NotifyOffloadSuccess, void>(
        client_id, tasks, metadatas);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::NotifyOffloadSuccessTo(
    const std::string& address, const UUID& client_id,
    const std::vector<OffloadTaskItem>& tasks,
    const std::vector<StorageObjectMetadata>& metadatas) {
    ScopedVLogTimer timer(1, "MasterClient::NotifyOffloadSuccessTo");
    timer.LogRequest("address=", address, ", client_id=", client_id,
                     ", tasks_count=", tasks.size(),
                     ", metadatas_count=", metadatas.size());

    auto result =
        invoke_rpc_to<&WrappedMasterService::NotifyOffloadSuccess, void>(
            address, client_id, tasks, metadatas);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<std::string>, ErrorCode>
MasterClient::GetOffloadEndpoints() {
    ScopedVLogTimer timer(1, "MasterClient::GetOffloadEndpoints");
    timer.LogRequest("action=get_offload_endpoints");

    auto result = invoke_rpc<&WrappedMasterService::GetOffloadEndpoints,
                             std::vector<std::string>>();
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<PromotionTaskItem>, ErrorCode>
MasterClient::PromotionObjectHeartbeat(const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::PromotionObjectHeartbeat");
    timer.LogRequest("client_id=", client_id);
    return invoke_rpc<&WrappedMasterService::PromotionObjectHeartbeat,
                      std::vector<PromotionTaskItem>>(client_id);
}

tl::expected<std::vector<PromotionTaskItem>, ErrorCode>
MasterClient::PromotionObjectHeartbeatTo(const std::string& address,
                                         const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::PromotionObjectHeartbeatTo");
    timer.LogRequest("address=", address, ", client_id=", client_id);
    return invoke_rpc_to<&WrappedMasterService::PromotionObjectHeartbeat,
                         std::vector<PromotionTaskItem>>(address, client_id);
}

tl::expected<std::vector<RemoveTaskItem>, ErrorCode>
MasterClient::RemoveObjectHeartbeat(const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::RemoveObjectHeartbeat");
    timer.LogRequest("client_id=", client_id.first, ":", client_id.second);
    return invoke_rpc<&WrappedMasterService::RemoveObjectHeartbeat,
                      std::vector<RemoveTaskItem>>(client_id);
}

tl::expected<std::vector<RemoveTaskItem>, ErrorCode>
MasterClient::RemoveObjectHeartbeatTo(const std::string& address,
                                      const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::RemoveObjectHeartbeatTo");
    timer.LogRequest("address=", address, ", client_id=", client_id.first, ":",
                     client_id.second);
    return invoke_rpc_to<&WrappedMasterService::RemoveObjectHeartbeat,
                         std::vector<RemoveTaskItem>>(address, client_id);
}

tl::expected<void, ErrorCode> MasterClient::AckRemoveObjectHeartbeat(
    const UUID& client_id, const std::vector<RemoveTaskItem>& tasks) {
    ScopedVLogTimer timer(1, "MasterClient::AckRemoveObjectHeartbeat");
    timer.LogRequest("client_id=", client_id.first, ":", client_id.second,
                     " tasks=", tasks.size());
    return invoke_rpc<&WrappedMasterService::AckRemoveObjectHeartbeat, void>(
        client_id, tasks);
}

tl::expected<void, ErrorCode> MasterClient::AckRemoveObjectHeartbeatTo(
    const std::string& address, const UUID& client_id,
    const std::vector<RemoveTaskItem>& tasks) {
    ScopedVLogTimer timer(1, "MasterClient::AckRemoveObjectHeartbeatTo");
    timer.LogRequest("address=", address, ", client_id=", client_id.first, ":",
                     client_id.second, " tasks=", tasks.size());
    return invoke_rpc_to<&WrappedMasterService::AckRemoveObjectHeartbeat, void>(
        address, client_id, tasks);
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
MasterClient::PromotionAllocStart(
    const UUID& client_id, const std::string& key, uint64_t size,
    const std::vector<std::string>& preferred_segments) {
    return PromotionAllocStart(client_id, key, tenant_id_.value(), size,
                               preferred_segments);
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
MasterClient::PromotionAllocStart(
    const UUID& client_id, const std::string& key, const std::string& tenant_id,
    uint64_t size, const std::vector<std::string>& preferred_segments) {
    ScopedVLogTimer timer(1, "MasterClient::PromotionAllocStart");
    timer.LogRequest("client_id=", client_id, ", key=", key,
                     ", tenant_id=", tenant_id, ", size=", size,
                     ", preferred_count=", preferred_segments.size());
    auto result = invoke_rpc<&WrappedMasterService::PromotionAllocStart,
                             PromotionAllocStartResponse>(
        client_id, key, tenant_id, size, preferred_segments);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::NotifyPromotionSuccess(
    const UUID& client_id, const std::string& key) {
    return NotifyPromotionSuccess(client_id, key, tenant_id_.value());
}

tl::expected<void, ErrorCode> MasterClient::RegisterPrefetchTask(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::RegisterPrefetchTask");
    timer.LogRequest("client_id=", client_id, ", key=", key);
    auto result = invoke_rpc<&WrappedMasterService::RegisterPrefetchTask, void>(
        client_id, key, tenant_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::NotifyPromotionSuccess(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::NotifyPromotionSuccess");
    timer.LogRequest("client_id=", client_id, ", key=", key,
                     ", tenant_id=", tenant_id);
    auto result =
        invoke_rpc<&WrappedMasterService::NotifyPromotionSuccess, void>(
            client_id, key, tenant_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::NotifyPromotionFailure(
    const UUID& client_id, const std::string& key) {
    return NotifyPromotionFailure(client_id, key, tenant_id_.value());
}

tl::expected<void, ErrorCode> MasterClient::NotifyPromotionFailure(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::NotifyPromotionFailure");
    timer.LogRequest("client_id=", client_id, ", key=", key,
                     ", tenant_id=", tenant_id);
    auto result =
        invoke_rpc<&WrappedMasterService::NotifyPromotionFailure, void>(
            client_id, key, tenant_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
MasterClient::PromotionAllocStartTo(
    const std::string& address, const UUID& client_id, const std::string& key,
    const std::string& tenant_id, uint64_t size,
    const std::vector<std::string>& preferred_segments) {
    ScopedVLogTimer timer(1, "MasterClient::PromotionAllocStartTo");
    timer.LogRequest("address=", address, ", client_id=", client_id,
                     ", key=", key, ", tenant_id=", tenant_id, ", size=", size,
                     ", preferred_count=", preferred_segments.size());
    auto result = invoke_rpc_to<&WrappedMasterService::PromotionAllocStart,
                                PromotionAllocStartResponse>(
        address, client_id, key, tenant_id, size, preferred_segments);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::NotifyPromotionSuccessTo(
    const std::string& address, const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::NotifyPromotionSuccessTo");
    timer.LogRequest("address=", address, ", client_id=", client_id,
                     ", key=", key, ", tenant_id=", tenant_id);
    auto result =
        invoke_rpc_to<&WrappedMasterService::NotifyPromotionSuccess, void>(
            address, client_id, key, tenant_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::NotifyPromotionFailureTo(
    const std::string& address, const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::NotifyPromotionFailureTo");
    timer.LogRequest("address=", address, ", client_id=", client_id,
                     ", key=", key, ", tenant_id=", tenant_id);
    auto result =
        invoke_rpc_to<&WrappedMasterService::NotifyPromotionFailure, void>(
            address, client_id, key, tenant_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<CopyStartResponse, ErrorCode> MasterClient::CopyStart(
    const std::string& key, const std::string& src_segment,
    const std::vector<std::string>& tgt_segments) {
    return CopyStart(key, tenant_id_.value(), src_segment, tgt_segments);
}

tl::expected<CopyStartResponse, ErrorCode> MasterClient::CopyStart(
    const std::string& key, const std::string& tenant_id,
    const std::string& src_segment,
    const std::vector<std::string>& tgt_segments) {
    ScopedVLogTimer timer(1, "MasterClient::CopyStart");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id,
                     ", src_segment=", src_segment,
                     ", tgt_segments_count=", tgt_segments.size());

    auto result =
        invoke_rpc<&WrappedMasterService::CopyStart, CopyStartResponse>(
            client_id_, key, tenant_id, src_segment, tgt_segments);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<CopyStartResponse, ErrorCode>
MasterClient::DynamicReplicaCopyStart(
    const std::string& key, const std::string& tenant_id,
    const std::string& src_segment,
    const std::vector<std::string>& tgt_segments,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch) {
    ScopedVLogTimer timer(1, "MasterClient::DynamicReplicaCopyStart");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id,
                     ", src_segment=", src_segment,
                     ", tgt_segments_count=", tgt_segments.size());

    auto result = invoke_rpc<&WrappedMasterService::DynamicReplicaCopyStart,
                             CopyStartResponse>(
        client_id_, key, tenant_id, src_segment, tgt_segments,
        dynamic_replication_lease_id, dynamic_replication_version_epoch);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<QueryTaskResponse, ErrorCode> MasterClient::QueryTask(
    const UUID& task_id) {
    ScopedVLogTimer timer(1, "MasterClient::QueryTask");
    timer.LogRequest("task_id=", task_id);

    auto result =
        invoke_rpc<&WrappedMasterService::QueryTask, QueryTaskResponse>(
            task_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::CopyEnd(const std::string& key) {
    return CopyEnd(key, tenant_id_.value());
}

tl::expected<void, ErrorCode> MasterClient::CopyEnd(
    const std::string& key, const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::CopyEnd");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id);

    auto result = invoke_rpc<&WrappedMasterService::CopyEnd, void>(
        client_id_, key, tenant_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::DynamicReplicaCopyEnd(
    const std::string& key, const std::string& tenant_id,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch) {
    ScopedVLogTimer timer(1, "MasterClient::DynamicReplicaCopyEnd");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id);

    auto result =
        invoke_rpc<&WrappedMasterService::DynamicReplicaCopyEnd, void>(
            client_id_, key, tenant_id, dynamic_replication_lease_id,
            dynamic_replication_version_epoch);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> MasterClient::FetchTasks(
    size_t batch_size) {
    ScopedVLogTimer timer(1, "MasterClient::FetchTasks");
    timer.LogRequest("client_id=", client_id_, ", batch_size=", batch_size);
    auto result =
        invoke_rpc<&WrappedMasterService::FetchTasks,
                   std::vector<TaskAssignment>>(client_id_, batch_size);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::CopyRevoke(const std::string& key) {
    return CopyRevoke(key, tenant_id_.value());
}

tl::expected<void, ErrorCode> MasterClient::CopyRevoke(
    const std::string& key, const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::CopyRevoke");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id);

    auto result = invoke_rpc<&WrappedMasterService::CopyRevoke, void>(
        client_id_, key, tenant_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::DynamicReplicaCopyRevoke(
    const std::string& key, const std::string& tenant_id,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch) {
    ScopedVLogTimer timer(1, "MasterClient::DynamicReplicaCopyRevoke");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id);

    auto result =
        invoke_rpc<&WrappedMasterService::DynamicReplicaCopyRevoke, void>(
            client_id_, key, tenant_id, dynamic_replication_lease_id,
            dynamic_replication_version_epoch);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<MoveStartResponse, ErrorCode> MasterClient::MoveStart(
    const std::string& key, const std::string& src_segment,
    const std::string& tgt_segment) {
    return MoveStart(key, tenant_id_.value(), src_segment, tgt_segment);
}

tl::expected<MoveStartResponse, ErrorCode> MasterClient::MoveStart(
    const std::string& key, const std::string& tenant_id,
    const std::string& src_segment, const std::string& tgt_segment) {
    ScopedVLogTimer timer(1, "MasterClient::MoveStart");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id,
                     ", src_segment=", src_segment,
                     ", tgt_segment=", tgt_segment);

    auto result =
        invoke_rpc<&WrappedMasterService::MoveStart, MoveStartResponse>(
            client_id_, key, tenant_id, src_segment, tgt_segment);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MoveEnd(const std::string& key) {
    return MoveEnd(key, tenant_id_.value());
}

tl::expected<void, ErrorCode> MasterClient::MoveEnd(
    const std::string& key, const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::MoveEnd");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id);

    auto result = invoke_rpc<&WrappedMasterService::MoveEnd, void>(
        client_id_, key, tenant_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MoveRevoke(const std::string& key) {
    return MoveRevoke(key, tenant_id_.value());
}

tl::expected<void, ErrorCode> MasterClient::MoveRevoke(
    const std::string& key, const std::string& tenant_id) {
    ScopedVLogTimer timer(1, "MasterClient::MoveRevoke");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id);

    auto result = invoke_rpc<&WrappedMasterService::MoveRevoke, void>(
        client_id_, key, tenant_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MarkTaskToComplete(
    const TaskCompleteRequest& task_update) {
    ScopedVLogTimer timer(1, "MasterClient::MarkTaskToComplete");
    timer.LogRequest("client_id=", client_id_, ", task_id=", task_update.id);
    auto result = invoke_rpc<&WrappedMasterService::MarkTaskToComplete, void>(
        client_id_, task_update);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::EvictDiskReplica(
    const std::string& key, ReplicaType replica_type) {
    return EvictDiskReplica(key, tenant_id_.value(), replica_type);
}

tl::expected<void, ErrorCode> MasterClient::EvictDiskReplica(
    const std::string& key, const std::string& tenant_id,
    ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "MasterClient::EvictDiskReplica");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id,
                     ", replica_type=", replica_type);

    auto result = invoke_rpc<&WrappedMasterService::EvictDiskReplica, void>(
        client_id_, key, tenant_id, replica_type);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<bool, ErrorCode> MasterClient::EvictDiskReplicaIfCurrent(
    const std::string& key, const std::string& tenant_id,
    ReplicaType replica_type, ReplicaID expected_replica_id) {
    ScopedVLogTimer timer(1, "MasterClient::EvictDiskReplicaIfCurrent");
    timer.LogRequest("key=", key, ", tenant_id=", tenant_id,
                     ", replica_type=", replica_type,
                     ", expected_replica_id=", expected_replica_id);

    auto result =
        invoke_rpc<&WrappedMasterService::EvictDiskReplicaIfCurrent, bool>(
            client_id_, key, tenant_id, replica_type, expected_replica_id);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchEvictDiskReplica(
    const std::vector<std::string>& keys, ReplicaType replica_type) {
    return BatchEvictDiskReplica(keys, tenant_id_.value(), replica_type);
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchEvictDiskReplica(
    const std::vector<std::string>& keys, const std::string& tenant_id,
    ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "MasterClient::BatchEvictDiskReplica");
    timer.LogRequest("keys_count=", keys.size(), ", tenant_id=", tenant_id,
                     ", replica_type=", replica_type);

    // Single-master mode (routing not loaded): use the original batch path.
    if (partition_router_.Size() == 0) {
        auto result =
            invoke_batch_rpc<&WrappedMasterService::BatchEvictDiskReplica,
                             void>(keys.size(), client_id_, keys, tenant_id,
                                   replica_type);
        timer.LogResponse("result=", result.size(), " operations");
        return result;
    }

    std::vector<tl::expected<void, ErrorCode>> results(
        keys.size(),
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));

    auto groups = GroupKeysBySubmaster(keys, tenant_id);
    for (auto& [submaster, indices] : groups) {
        if (submaster.empty()) {
            LOG(WARNING) << "BatchEvictDiskReplica: " << indices.size()
                         << " key(s) have no submaster, marked unavailable";
            continue;
        }
        SwitchToSubmasterByAddress(submaster);

        std::vector<std::string> group_keys;
        group_keys.reserve(indices.size());
        for (size_t idx : indices) {
            group_keys.push_back(keys[idx]);
        }

        auto group_result =
            invoke_batch_rpc<&WrappedMasterService::BatchEvictDiskReplica,
                             void>(group_keys.size(), client_id_, group_keys,
                                   tenant_id, replica_type);
        for (size_t j = 0; j < indices.size(); ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }

    // 路由自愈：STALE_ROUTE/SLOT_NOT_OWNED/SLOT_MIGRATING/无 owner 的
    // 失败子集刷新路由后重发一次（有界）；仍失败保持终态错误。
    // tenant_id 为显式入参（调用方按 task 携带 tenant 定向），捕获引用
    // 在同步执行窗口内有效。
    RetryBatchOnStaleRoute<void>(
        tenant_id, keys, results,
        [this, &results, &tenant_id, replica_type](
            const std::string& submaster, std::vector<std::string> group_keys,
            const std::vector<size_t>& group_orig) {
            SwitchToSubmasterByAddress(submaster);
            auto group_result =
                invoke_batch_rpc<&WrappedMasterService::BatchEvictDiskReplica,
                                 void>(group_keys.size(), client_id_,
                                       group_keys, tenant_id, replica_type);
            if (group_result.size() != group_orig.size()) {
                return false;
            }
            bool all_ok = true;
            for (size_t j = 0; j < group_orig.size(); ++j) {
                results[group_orig[j]] = std::move(group_result[j]);
                if (!results[group_orig[j]]) {
                    all_ok = false;
                }
            }
            return all_ok;
        });

    timer.LogResponse("result=", results.size(), " operations");
    return results;
}

}  // namespace mooncake
