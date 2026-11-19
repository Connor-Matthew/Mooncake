#pragma once

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
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
    void DoAccept();

    int port_;
    size_t connection_threads_;
    HttpHandler handler_;
    asio::io_context io_;
    std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
    std::vector<std::thread> io_threads_;
    std::atomic<bool> running_{false};
};

}  // namespace mooncake::kvtest
