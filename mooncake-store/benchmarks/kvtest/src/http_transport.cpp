#include "http_transport.h"

#include <asio/read.hpp>
#include <asio/read_until.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>

#include <algorithm>
#include <cctype>
#include <istream>
#include <ostream>
#include <sstream>
#include <utility>

namespace mooncake::kvtest {
namespace {

constexpr size_t kMaxRequestBody = 4 * 1024 * 1024;

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

class HttpSession : public std::enable_shared_from_this<HttpSession> {
   public:
    HttpSession(asio::ip::tcp::socket socket, HttpHandler handler)
        : socket_(std::move(socket)), handler_(std::move(handler)) {}

    void Start() { ReadHeaders(); }

   private:
    void ReadHeaders() {
        auto self = shared_from_this();
        asio::async_read_until(
            socket_, buffer_, "\r\n\r\n",
            [self](const asio::error_code &ec, size_t) {
                if (!ec) self->ParseHeaders();
            });
    }

    void ParseHeaders() {
        std::istream input(&buffer_);
        std::string request_line;
        std::getline(input, request_line);
        std::istringstream request_stream(Trim(request_line));
        request_stream >> method_ >> path_ >> version_;

        content_length_ = 0;
        bool connection_close = false;
        bool connection_keep_alive = false;
        std::string line;
        while (std::getline(input, line) && line != "\r") {
            const auto colon = line.find(':');
            if (colon == std::string::npos) continue;
            const std::string name = Lower(Trim(line.substr(0, colon)));
            const std::string value = Lower(Trim(line.substr(colon + 1)));
            if (name == "content-length") {
                try {
                    content_length_ = static_cast<size_t>(std::stoull(value));
                } catch (...) {
                    WriteResponse({400, "text/plain", "bad content-length"},
                                  false);
                    return;
                }
            } else if (name == "connection") {
                connection_close = value == "close";
                connection_keep_alive = value == "keep-alive";
            }
        }

        keep_alive_ = version_ == "HTTP/1.1" ? !connection_close
                                               : connection_keep_alive;
        if (method_.empty() || path_.empty()) {
            WriteResponse({400, "text/plain", "bad request"}, false);
            return;
        }
        if (content_length_ > kMaxRequestBody) {
            WriteResponse({413, "text/plain", ""}, false);
            return;
        }
        if (buffer_.size() >= content_length_) {
            ProcessRequest();
            return;
        }
        const size_t remaining = content_length_ - buffer_.size();
        auto self = shared_from_this();
        asio::async_read(socket_, buffer_, asio::transfer_exactly(remaining),
                         [self](const asio::error_code &ec, size_t) {
                             if (!ec) self->ProcessRequest();
                         });
    }

    void ProcessRequest() {
        std::string body(content_length_, '\0');
        if (content_length_ != 0) {
            std::istream input(&buffer_);
            input.read(body.data(),
                       static_cast<std::streamsize>(content_length_));
            if (static_cast<size_t>(input.gcount()) != content_length_) return;
        }

        HttpResponse response;
        try {
            response = handler_(method_, path_, body);
        } catch (...) {
            response = {500, "text/plain", "handler failure"};
            keep_alive_ = false;
        }
        WriteResponse(std::move(response), keep_alive_);
    }

    void WriteResponse(HttpResponse response, bool keep_alive) {
        keep_alive_ = keep_alive;
        std::ostringstream output;
        output << "HTTP/1.1 " << response.status
               << (response.status == 200 ? " OK" : " ERROR") << "\r\n"
               << "Content-Type: " << response.content_type << "\r\n"
               << "Content-Length: " << response.body.size() << "\r\n"
               << "Connection: " << (keep_alive_ ? "keep-alive" : "close")
               << "\r\n\r\n"
               << response.body;
        response_ = output.str();
        auto self = shared_from_this();
        asio::async_write(
            socket_, asio::buffer(response_),
            [self](const asio::error_code &ec, size_t) {
                if (ec || !self->keep_alive_) return;
                self->method_.clear();
                self->path_.clear();
                self->version_.clear();
                self->response_.clear();
                self->ReadHeaders();
            });
    }

    asio::ip::tcp::socket socket_;
    HttpHandler handler_;
    asio::streambuf buffer_;
    std::string method_;
    std::string path_;
    std::string version_;
    std::string response_;
    size_t content_length_{0};
    bool keep_alive_{false};
};

}  // namespace

HttpServer::HttpServer(int port, size_t connection_threads,
                       HttpHandler handler)
    : port_(port),
      connection_threads_(std::max<size_t>(1, connection_threads)),
      handler_(std::move(handler)) {}

HttpServer::~HttpServer() { Stop(); }

bool HttpServer::Start(std::string *error) {
    try {
        io_.restart();
        acceptor_ = std::make_unique<asio::ip::tcp::acceptor>(io_);
        acceptor_->open(asio::ip::tcp::v4());
        acceptor_->set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor_->bind(
            asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port_));
        acceptor_->listen();
        running_.store(true, std::memory_order_release);
        DoAccept();
        io_threads_.reserve(connection_threads_);
        for (size_t i = 0; i < connection_threads_; ++i)
            io_threads_.emplace_back([this] { io_.run(); });
        return true;
    } catch (const std::exception &e) {
        if (error) *error = e.what();
        Stop();
        return false;
    }
}

void HttpServer::Stop() {
    const bool was_running =
        running_.exchange(false, std::memory_order_acq_rel);
    if (!was_running && io_threads_.empty() && !acceptor_) return;
    if (acceptor_) {
        asio::error_code ec;
        acceptor_->cancel(ec);
        acceptor_->close(ec);
    }
    io_.stop();
    for (auto &thread : io_threads_) {
        if (thread.joinable()) thread.join();
    }
    io_threads_.clear();
    acceptor_.reset();
}

void HttpServer::DoAccept() {
    acceptor_->async_accept(
        [this](const asio::error_code &ec, asio::ip::tcp::socket socket) {
            if (!ec && running_.load(std::memory_order_acquire)) {
                asio::error_code option_error;
                socket.set_option(asio::ip::tcp::no_delay(true), option_error);
                if (!option_error)
                    std::make_shared<HttpSession>(std::move(socket), handler_)
                        ->Start();
            }
            if (running_.load(std::memory_order_acquire)) DoAccept();
        });
}

}  // namespace mooncake::kvtest
