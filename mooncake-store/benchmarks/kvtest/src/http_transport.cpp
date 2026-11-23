#include "http_transport.h"

#include <asio/connect.hpp>
#include <asio/read.hpp>
#include <asio/read_until.hpp>
#include <asio/steady_timer.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#endif

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <istream>
#include <map>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake::kvtest {
namespace {

#ifdef _WIN32
using SocketHandle = SOCKET;
#else
using SocketHandle = int;
#endif

void SetSocketTimeout(SocketHandle socket, int option, int seconds) {
#ifdef _WIN32
    // Winsock SO_SNDTIMEO/SO_RCVTIMEO take a DWORD in milliseconds, not a
    // struct timeval (passing a timeval makes the kernel read its first 4
    // bytes as a few milliseconds and every request times out immediately).
    DWORD timeout_ms = static_cast<DWORD>(seconds) * 1000;
    setsockopt(socket, SOL_SOCKET, option,
               reinterpret_cast<const char *>(&timeout_ms),
               sizeof(timeout_ms));
#else
    timeval timeout{seconds, 0};
    setsockopt(socket, SOL_SOCKET, option, &timeout, sizeof(timeout));
#endif
}

struct Endpoint {
    std::string host;
    std::string port;
};

bool ParseUrl(const std::string &url, Endpoint *endpoint, std::string *error) {
    std::string value = url;
    constexpr const char *prefix = "http://";
    if (value.rfind(prefix, 0) == 0)
        value.erase(0, std::char_traits<char>::length(prefix));
    const auto slash = value.find('/');
    if (slash != std::string::npos) value.resize(slash);
    const auto colon = value.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == value.size()) {
        if (error) *error = "invalid HTTP peer URL: " + url;
        return false;
    }
    endpoint->host = value.substr(0, colon);
    endpoint->port = value.substr(colon + 1);
    return true;
}

std::string Trim(std::string value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back())))
        value.pop_back();
    size_t start = 0;
    while (start < value.size() &&
           std::isspace(static_cast<unsigned char>(value[start])))
        ++start;
    return value.substr(start);
}

std::string Lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return value;
}

const char *ReasonPhrase(int status) {
    switch (status) {
        case 200:
            return "OK";
        case 400:
            return "Bad Request";
        case 404:
            return "Not Found";
        case 409:
            return "Conflict";
        case 413:
            return "Payload Too Large";
        case 500:
            return "Internal Server Error";
        case 503:
            return "Service Unavailable";
        default:
            return "Error";
    }
}

bool ReadBody(asio::ip::tcp::socket &socket, asio::streambuf &buffer,
              size_t length, std::string *body) {
    body->assign(length, '\0');
    if (length == 0) return true;
    std::istream input(&buffer);
    size_t copied = 0;
    while (copied < length && buffer.size() > 0) {
        input.read(body->data() + copied,
                   static_cast<std::streamsize>(length - copied));
        const auto read = static_cast<size_t>(input.gcount());
        if (read == 0) break;
        copied += read;
    }
    if (copied < length) {
        asio::read(socket, asio::buffer(body->data() + copied,
                                        length - copied));
    }
    return true;
}

}  // namespace

HttpServer::HttpServer(int port, size_t connection_threads,
                       HttpHandler handler)
    : port_(port), handler_(std::move(handler)) {
    // Kept for API compatibility: connections are served by one thread each
    // (like the Python ThreadingTCPServer) so HTTP/1.1 keep-alive idle
    // connections cannot starve a small fixed pool.
    (void)connection_threads;
}

HttpServer::~HttpServer() { Stop(); }

bool HttpServer::Start(std::string *error) {
    try {
        acceptor_ = std::make_unique<asio::ip::tcp::acceptor>(io_);
        acceptor_->open(asio::ip::tcp::v4());
        acceptor_->set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor_->bind(
            asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port_));
        acceptor_->listen();
        // AcceptLoop used to block in a synchronous accept().  On Linux,
        // cancel()/close() from Stop() did not reliably wake that call, so
        // shutdown could hang in accept_thread_.join() until another client
        // happened to connect.  A non-blocking accept loop gives Stop() a
        // bounded (10 ms) join latency without requiring a second /stop.
        acceptor_->non_blocking(true);
        running_.store(true, std::memory_order_release);
        accept_thread_ = std::thread([this] { AcceptLoop(); });
        return true;
    } catch (const std::exception &e) {
        if (error) *error = e.what();
        return false;
    }
}

void HttpServer::Stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) return;
    if (acceptor_) {
        asio::error_code ec;
        acceptor_->cancel(ec);
        acceptor_->close(ec);
    }
    io_.stop();
    if (accept_thread_.joinable()) accept_thread_.join();
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        for (auto &connection : connections_) {
            if (connection.thread.joinable()) connection.thread.join();
        }
        connections_.clear();
    }
}

void HttpServer::AcceptLoop() {
    while (running_.load(std::memory_order_acquire)) {
        auto socket = std::make_shared<asio::ip::tcp::socket>(io_);
        asio::error_code ec;
        acceptor_->accept(*socket, ec);
        if (ec) {
            if (!running_.load(std::memory_order_acquire)) return;
            if (ec == asio::error::would_block || ec == asio::error::try_again) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            continue;
        }
        // Stop may race with a successful accept.  Do not enqueue one last
        // request after shutdown has begun.
        if (!running_.load(std::memory_order_acquire)) {
            socket->close(ec);
            return;
        }
        socket->non_blocking(false, ec);
        if (ec) {
            socket->close(ec);
            continue;
        }
        socket->set_option(asio::ip::tcp::no_delay(true), ec);
        SetSocketTimeout(socket->native_handle(), SO_RCVTIMEO, 5);
        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            // Reap finished keep-alive connections so the entry list stays
            // bounded over long runs.
            for (auto it = connections_.begin(); it != connections_.end();) {
                if (it->done->load(std::memory_order_acquire)) {
                    if (it->thread.joinable()) it->thread.join();
                    it = connections_.erase(it);
                } else {
                    ++it;
                }
            }
            auto done = std::make_shared<std::atomic<bool>>(false);
            connections_.push_back(
                {std::thread([this, socket, done] {
                     HandleConnection(socket);
                     done->store(true, std::memory_order_release);
                 }),
                 done});
        }
    }
}

void HttpServer::HandleConnection(
    std::shared_ptr<asio::ip::tcp::socket> socket) {
    try {
        asio::streambuf buffer;
        for (;;) {
            if (!running_.load(std::memory_order_acquire)) break;
            asio::error_code read_error;
            asio::read_until(*socket, buffer, "\r\n\r\n", read_error);
            if (read_error) break;

            std::istream input(&buffer);
            std::string request_line;
            std::getline(input, request_line);
            request_line = Trim(request_line);
            std::istringstream request_stream(request_line);
            std::string method;
            std::string path;
            std::string version;
            request_stream >> method >> path >> version;
            if (method.empty() || path.empty()) break;

            size_t content_length = 0;
            bool keep_alive = version != "HTTP/1.0";
            std::string line;
            bool bad_header = false;
            while (std::getline(input, line) && line != "\r") {
                const auto colon = line.find(':');
                if (colon == std::string::npos) continue;
                const std::string name =
                    Lower(Trim(line.substr(0, colon)));
                const std::string value =
                    Lower(Trim(line.substr(colon + 1)));
                if (name == "content-length") {
                    try {
                        content_length = static_cast<size_t>(std::stoull(value));
                    } catch (...) {
                        bad_header = true;
                    }
                } else if (name == "connection") {
                    if (value == "close") keep_alive = false;
                    if (value == "keep-alive") keep_alive = true;
                }
            }
            if (bad_header) break;

            constexpr size_t kMaxRequestBody = 4 * 1024 * 1024;
            if (content_length > kMaxRequestBody) {
                static constexpr char response[] =
                    "HTTP/1.1 413 Payload Too Large\r\n"
                    "Content-Length: 0\r\nConnection: close\r\n\r\n";
                asio::write(*socket, asio::buffer(response, sizeof(response) - 1));
                break;
            }

            std::string body;
            ReadBody(*socket, buffer, content_length, &body);

            HttpResponse response;
            if (method.empty() || path.empty()) {
                response = {400, "text/plain", "bad request"};
            } else {
                response = handler_(method, path, body);
            }
            std::ostringstream output;
            output << "HTTP/1.1 " << response.status << ' '
                   << ReasonPhrase(response.status) << "\r\n"
                   << "Content-Type: " << response.content_type << "\r\n"
                   << "Content-Length: " << response.body.size() << "\r\n"
                   << "Connection: " << (keep_alive ? "keep-alive" : "close")
                   << "\r\n\r\n"
                   << response.body;
            asio::write(*socket, asio::buffer(output.str()));
            if (!keep_alive) break;
        }
    } catch (...) {
    }
    asio::error_code ec;
    socket->shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    socket->close(ec);
}

// ---------------------------------------------------------------------------
// NotifyHttpClient
// ---------------------------------------------------------------------------

struct NotifyHttpClient::Impl {
    struct Conn {
        asio::io_context io;
        asio::ip::tcp::socket socket{io};
        bool connected{false};
    };

    explicit Impl(int timeout_seconds)
        : timeout_s(std::max(1, timeout_seconds)) {}

    ~Impl() {
        std::lock_guard<std::mutex> lock(mutex);
        for (auto &[peer, connections] : idle) {
            for (auto &conn : connections) Close(conn.get());
        }
        idle.clear();
    }

    void Close(Conn *conn) {
        if (conn == nullptr) return;
        asio::error_code ec;
        conn->socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
        conn->socket.close(ec);
        conn->connected = false;
    }

    std::unique_ptr<Conn> Checkout(const std::string &peer) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto &connections = idle[peer];
            if (!connections.empty()) {
                auto conn = std::move(connections.back());
                connections.pop_back();
                reused.fetch_add(1, std::memory_order_relaxed);
                return conn;
            }
        }
        created.fetch_add(1, std::memory_order_relaxed);
        return std::make_unique<Conn>();
    }

    void Return(const std::string &peer, std::unique_ptr<Conn> conn) {
        std::lock_guard<std::mutex> lock(mutex);
        auto &connections = idle[peer];
        if (connections.size() < 2) {
            connections.push_back(std::move(conn));
        } else {
            Close(conn.get());
        }
    }

    bool Connect(Conn *conn, const Endpoint &endpoint, std::string *error) {
        try {
            asio::ip::tcp::resolver resolver(conn->io);
            asio::error_code resolve_error;
            const auto endpoints =
                resolver.resolve(endpoint.host, endpoint.port, resolve_error);
            if (resolve_error) {
                if (error) *error = "resolve: " + resolve_error.message();
                return false;
            }
#ifdef _WIN32
            // Local Windows validation builds (Zig/libc++ + standalone Asio)
            // reject the async_connect timer path with a std::thread
            // system_error; a blocking connect is sufficient there. The
            // production Linux path keeps the bounded async connect below.
            asio::error_code connect_error;
            asio::connect(conn->socket, endpoints, connect_error);
            if (connect_error) {
                if (error) *error = "connect: " + connect_error.message();
                return false;
            }
#else
            asio::steady_timer timer(conn->io);
            asio::error_code connect_error = asio::error::would_block;
            bool timed_out = false;
            timer.expires_after(std::chrono::seconds(timeout_s));
            timer.async_wait([&](const asio::error_code &timer_error) {
                if (!timer_error) {
                    timed_out = true;
                    asio::error_code ignored;
                    conn->socket.close(ignored);
                }
            });
            asio::async_connect(
                conn->socket, endpoints,
                [&](const asio::error_code &ec,
                    const asio::ip::tcp::endpoint &) {
                    connect_error = ec;
                    timer.cancel();
                });
            conn->io.run();
            conn->io.restart();
            if (timed_out) {
                if (error) *error = "connect: timeout";
                return false;
            }
            if (connect_error) {
                if (error) *error = "connect: " + connect_error.message();
                return false;
            }
#endif
            asio::error_code option_error;
            conn->socket.set_option(asio::ip::tcp::no_delay(true),
                                    option_error);
            SetSocketTimeout(conn->socket.native_handle(), SO_SNDTIMEO,
                             timeout_s);
            SetSocketTimeout(conn->socket.native_handle(), SO_RCVTIMEO,
                             timeout_s);
            conn->connected = true;
            return true;
        } catch (const std::exception &e) {
            if (error) *error = e.what();
            return false;
        }
    }

    std::mutex mutex;
    std::map<std::string, std::vector<std::unique_ptr<Conn>>> idle;
    std::atomic<uint64_t> created{0};
    std::atomic<uint64_t> reused{0};
    int timeout_s;
};

NotifyHttpClient::NotifyHttpClient(int timeout_seconds)
    : impl_(std::make_unique<Impl>(timeout_seconds)) {}

NotifyHttpClient::~NotifyHttpClient() = default;

uint64_t NotifyHttpClient::Created() const {
    return impl_->created.load(std::memory_order_relaxed);
}

uint64_t NotifyHttpClient::Reused() const {
    return impl_->reused.load(std::memory_order_relaxed);
}

bool NotifyHttpClient::Send(const std::string &peer,
                            const std::string &method,
                            const std::string &path,
                            const std::string &body, std::string *error) {
    Endpoint endpoint;
    if (!ParseUrl(peer, &endpoint, error)) return false;
    std::unique_ptr<Impl::Conn> conn = impl_->Checkout(peer);
    if (!conn) return false;

    bool ok = false;
    bool reusable = false;
    std::string stage = "connect";
    try {
        if (!conn->connected &&
            !impl_->Connect(conn.get(), endpoint, error)) {
            impl_->Close(conn.get());
            return false;
        }
        stage = "write";
        std::ostringstream request;
        request << method << ' ' << path << " HTTP/1.1\r\n"
                << "Host: " << endpoint.host << ':' << endpoint.port << "\r\n"
                << "Content-Type: application/json\r\n"
                << "Content-Length: " << body.size() << "\r\n"
                << "Connection: keep-alive\r\n\r\n"
                << body;
        asio::write(conn->socket, asio::buffer(request.str()));

        stage = "read-headers";
        asio::streambuf response;
        asio::read_until(conn->socket, response, "\r\n\r\n");
        std::istream input(&response);
        std::string status_line;
        std::getline(input, status_line);
        std::string http_version;
        int status = 0;
        {
            std::istringstream status_stream(status_line);
            status_stream >> http_version >> status;
        }
        size_t content_length = 0;
        bool close = false;
        std::string line;
        while (std::getline(input, line) && line != "\r") {
            const auto colon = line.find(':');
            if (colon == std::string::npos) continue;
            const std::string name = Lower(Trim(line.substr(0, colon)));
            const std::string value = Lower(Trim(line.substr(colon + 1)));
            if (name == "content-length") {
                try {
                    content_length = static_cast<size_t>(std::stoull(value));
                } catch (...) {
                }
            } else if (name == "connection" && value == "close") {
                close = true;
            }
        }
        std::string response_body;
        if (content_length > 0) {
            stage = "read-body";
            ReadBody(conn->socket, response, content_length, &response_body);
        }
        if (status == 200) {
            ok = true;
            reusable = !close;
        } else if (error) {
            *error = "HTTP status " + std::to_string(status);
        }
    } catch (const std::exception &e) {
        if (error) *error = stage + ": " + e.what();
    }
    if (reusable) {
        impl_->Return(peer, std::move(conn));
    } else {
        impl_->Close(conn.get());
    }
    return ok;
}

bool HttpPost(const std::string &url, const std::string &path,
              const std::string &body, std::string *error) {
    NotifyHttpClient client(3);
    return client.Send(url, "POST", path, body, error);
}

}  // namespace mooncake::kvtest
