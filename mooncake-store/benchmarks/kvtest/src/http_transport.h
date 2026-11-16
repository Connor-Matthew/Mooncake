#pragma once

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace mooncake::kvtest {

struct HttpResponse {
    int status{200};
    std::string content_type{"text/plain"};
    std::string body{"ok"};
};

using HttpHandler = std::function<HttpResponse(
    const std::string &method, const std::string &path,
    const std::string &body)>;

class HttpServer {
   public:
    HttpServer(int port, size_t connection_threads, HttpHandler handler);
    ~HttpServer();

    bool Start(std::string *error);
    void Stop();

   private:
    void AcceptLoop();
    void HandleConnection(std::shared_ptr<asio::ip::tcp::socket> socket);

    int port_;
    HttpHandler handler_;
    asio::io_context io_;
    std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
    std::thread accept_thread_;
    struct ConnectionEntry {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> done;
    };
    std::mutex connections_mutex_;
    std::vector<ConnectionEntry> connections_;
    std::atomic<bool> running_{false};
};

// HTTP/1.1 keep-alive client used by the pipeline writer for notifications.
// One exclusive connection per send; up to two idle connections are retained
// per peer. Not thread-safe internally only in the sense that the pool is
// mutex-guarded; each connection is used by one sender worker at a time.
class NotifyHttpClient {
   public:
    explicit NotifyHttpClient(int timeout_seconds = 3);
    ~NotifyHttpClient();

    NotifyHttpClient(const NotifyHttpClient &) = delete;
    NotifyHttpClient &operator=(const NotifyHttpClient &) = delete;

    bool Send(const std::string &peer, const std::string &method,
              const std::string &path, const std::string &body,
              std::string *error);

    uint64_t Created() const;
    uint64_t Reused() const;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Legacy one-shot POST helper (opens a fresh connection per call).
bool HttpPost(const std::string &url, const std::string &path,
              const std::string &body, std::string *error);

}  // namespace mooncake::kvtest
