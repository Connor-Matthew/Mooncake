#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace mooncake::kvtest {

struct NotifyRpcRequest {
    std::vector<std::string> keys;
    uint64_t size{0};
    int sender{0};
};

struct NotifyRpcResponse {
    bool accepted{false};
};

class NotifyRpcService {
   public:
    using Handler = std::function<bool(NotifyRpcRequest)>;

    explicit NotifyRpcService(Handler handler);
    NotifyRpcResponse Notify(NotifyRpcRequest request);

   private:
    Handler handler_;
};

struct NotifyRpcTiming {
    double pool_lookup_ms{0};
    double pool_dispatch_ms{0};
    double await_response_ms{0};
    bool pool_found{false};
    bool dispatched{false};
    bool responded{false};
};

class NotifyRpcServer {
   public:
    NotifyRpcServer(int port, size_t io_threads,
                    NotifyRpcService::Handler handler);
    ~NotifyRpcServer();

    NotifyRpcServer(const NotifyRpcServer &) = delete;
    NotifyRpcServer &operator=(const NotifyRpcServer &) = delete;

    bool Start(std::string *error);
    void Stop();

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

class NotifyRpcClient {
   public:
    NotifyRpcClient(size_t connections_per_peer, int reuse_limit,
                    int timeout_ms);
    ~NotifyRpcClient();

    NotifyRpcClient(const NotifyRpcClient &) = delete;
    NotifyRpcClient &operator=(const NotifyRpcClient &) = delete;

    bool Notify(const std::string &endpoint, const NotifyRpcRequest &request,
                NotifyRpcTiming *timing, std::string *error);

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake::kvtest
