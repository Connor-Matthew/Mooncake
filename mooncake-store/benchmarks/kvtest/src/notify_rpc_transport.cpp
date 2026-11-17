#include "notify_rpc_transport.h"

#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <utility>

namespace mooncake::kvtest {
namespace {

using RpcClientPool = coro_io::client_pool<coro_rpc::coro_rpc_client>;

struct CallResult {
    bool ok{false};
    NotifyRpcResponse response;
    NotifyRpcTiming timing;
    std::string error;
};

async_simple::coro::Lazy<CallResult> CallNotify(
    std::shared_ptr<RpcClientPool> pool, const NotifyRpcRequest &request) {
    CallResult call;
    const auto dispatch_start = std::chrono::steady_clock::now();
    auto submitted = co_await pool->send_request(
        [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client &client) {
            return client.send_request<&NotifyRpcService::Notify>(request);
        });
    call.timing.pool_dispatch_ms =
        std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - dispatch_start)
            .count();
    if (!submitted) {
        call.error = "YLT RPC connection unavailable";
        co_return call;
    }
    call.timing.dispatched = true;

    const auto response_start = std::chrono::steady_clock::now();
    auto result = co_await std::move(submitted.value());
    call.timing.await_response_ms =
        std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - response_start)
            .count();
    if (!result) {
        call.error = result.error().msg;
        co_return call;
    }
    call.timing.responded = true;
    call.response = std::move(result.value().result());
    call.ok = call.response.accepted;
    if (!call.ok) call.error = "notify queue rejected request";
    co_return call;
}

}  // namespace

NotifyRpcService::NotifyRpcService(Handler handler)
    : handler_(std::move(handler)) {}

NotifyRpcResponse NotifyRpcService::Notify(NotifyRpcRequest request) {
    return {handler_ && handler_(std::move(request))};
}

struct NotifyRpcServer::Impl {
    Impl(int port, size_t io_threads, NotifyRpcService::Handler handler)
        : service(std::move(handler)),
          server(std::max<size_t>(1, io_threads), port, "0.0.0.0") {
        server.register_handler<&NotifyRpcService::Notify>(&service);
    }

    NotifyRpcService service;
    coro_rpc::coro_rpc_server server;
    bool started{false};
};

NotifyRpcServer::NotifyRpcServer(int port, size_t io_threads,
                                 NotifyRpcService::Handler handler)
    : impl_(std::make_unique<Impl>(port, io_threads, std::move(handler))) {}

NotifyRpcServer::~NotifyRpcServer() { Stop(); }

bool NotifyRpcServer::Start(std::string *error) {
    if (impl_->started) return true;
    try {
        auto started = impl_->server.async_start();
        if (started.hasResult()) {
            auto ec = std::move(started).get();
            if (error) *error = ec.message();
            return false;
        }
        impl_->started = true;
        return true;
    } catch (const std::exception &e) {
        if (error) *error = e.what();
        return false;
    }
}

void NotifyRpcServer::Stop() {
    if (!impl_ || !impl_->started) return;
    impl_->server.stop();
    impl_->started = false;
}

struct NotifyRpcClient::Impl {
    Impl(size_t connections_per_peer, int reuse_limit, int timeout_ms) {
        RpcClientPool::pool_config config;
        config.max_connection = static_cast<uint32_t>(
            std::max<size_t>(1, connections_per_peer));
        config.connect_retry_count = 0;
        config.reuse_limit = reuse_limit;
        config.host_alive_detect_duration = std::chrono::milliseconds(0);
        config.client_config.connect_timeout_duration =
            std::chrono::milliseconds(timeout_ms);
        config.client_config.request_timeout_duration =
            std::chrono::milliseconds(timeout_ms);
        pools = std::make_shared<
            coro_io::client_pools<coro_rpc::coro_rpc_client>>(config);
    }

    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>> pools;
};

NotifyRpcClient::NotifyRpcClient(size_t connections_per_peer,
                                 int reuse_limit, int timeout_ms)
    : impl_(std::make_unique<Impl>(connections_per_peer, reuse_limit,
                                   timeout_ms)) {}

NotifyRpcClient::~NotifyRpcClient() = default;

bool NotifyRpcClient::Notify(const std::string &endpoint,
                             const NotifyRpcRequest &request,
                             NotifyRpcTiming *timing, std::string *error) {
    try {
        const auto lookup_start = std::chrono::steady_clock::now();
        auto pool = impl_->pools->at(endpoint);
        const double lookup_ms =
            std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - lookup_start)
                .count();
        CallResult result = async_simple::coro::syncAwait(
            CallNotify(std::move(pool), request));
        result.timing.pool_lookup_ms = lookup_ms;
        result.timing.pool_found = true;
        if (timing) *timing = result.timing;
        if (!result.ok && error) *error = std::move(result.error);
        return result.ok;
    } catch (const std::exception &e) {
        if (error) *error = e.what();
        return false;
    }
}

}  // namespace mooncake::kvtest
