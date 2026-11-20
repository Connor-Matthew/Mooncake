#include "notify_codec.h"

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include <cctype>
#include <cstdio>
#include <sstream>

namespace mooncake::kvtest {
namespace {

bool IsUnreserved(unsigned char c) {
    return std::isalnum(c) != 0 || c == '-' || c == '_' || c == '.' ||
           c == '~';
}

int HexValue(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

void AddParam(NotifyBatch *out, const std::string &name,
              const std::string &value) {
    if (name == "key") {
        out->keys.push_back(value);
    } else if (name == "keys") {
        out->keys.push_back(value);
    } else if (name == "writer_id" || name == "sender") {
        try {
            out->sender = std::stoi(value);
        } catch (...) {
        }
    } else if (name == "size") {
        try {
            out->size = std::stoull(value);
        } catch (...) {
        }
    } else if (name == "warmup") {
        out->warmup = value == "1" || value == "true";
    }
}

}  // namespace

std::string UrlEncode(const std::string &value) {
    std::string out;
    out.reserve(value.size());
    for (unsigned char c : value) {
        if (IsUnreserved(c)) {
            out.push_back(static_cast<char>(c));
        } else {
            char buffer[4];
            std::snprintf(buffer, sizeof(buffer), "%%%02X", c);
            out += buffer;
        }
    }
    return out;
}

bool UrlDecode(const std::string &value, std::string *out) {
    if (out == nullptr) return false;
    out->clear();
    for (size_t i = 0; i < value.size(); ++i) {
        const char c = value[i];
        if (c == '+') {
            out->push_back(' ');
        } else if (c == '%') {
            if (i + 2 >= value.size()) return false;
            const int high = HexValue(value[i + 1]);
            const int low = HexValue(value[i + 2]);
            if (high < 0 || low < 0) return false;
            out->push_back(static_cast<char>((high << 4) | low));
            i += 2;
        } else {
            out->push_back(c);
        }
    }
    return true;
}

std::string EncodeNotifyJson(const NotifyBatch &batch) {
    Json::Value root(Json::objectValue);
    root["action"] = "";
    root["sender"] = batch.sender;
    root["size"] = static_cast<Json::UInt64>(batch.size);
    root["warmup"] = batch.warmup ? 1 : 0;
    Json::Value keys(Json::arrayValue);
    for (const auto &key : batch.keys) keys.append(key);
    root["keys"] = std::move(keys);
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

std::string EncodeNotifyQuery(const NotifyBatch &batch,
                              const std::string &path) {
    std::ostringstream out;
    out << path;
    bool first = true;
    auto separator = [&]() {
        out << (first ? '?' : '&');
        first = false;
    };
    for (const auto &key : batch.keys) {
        separator();
        out << "key=" << UrlEncode(key);
    }
    separator();
    out << "writer_id=" << batch.sender;
    separator();
    out << "size=" << batch.size;
    separator();
    out << "warmup=" << (batch.warmup ? 1 : 0);
    return out.str();
}

bool DecodeNotifyJson(const std::string &body, NotifyBatch *out,
                      std::string *error) {
    if (out == nullptr) return false;
    Json::CharReaderBuilder reader;
    Json::Value root;
    std::string parse_error;
    std::istringstream input(body);
    if (!Json::parseFromStream(reader, input, &root, &parse_error)) {
        if (error) *error = "invalid JSON notify: " + parse_error;
        return false;
    }
    NotifyBatch batch;
    batch.sender = root.get("sender", root.get("writer_id", 0)).asInt();
    batch.size = static_cast<uint64_t>(
        root.get("size", static_cast<Json::UInt64>(0)).asUInt64());
    if (root.isMember("warmup")) {
        const auto &warmup = root["warmup"];
        batch.warmup = warmup.isBool() ? warmup.asBool()
                                       : warmup.asInt() != 0;
    }
    if (root.isMember("keys")) {
        for (const auto &key : root["keys"]) {
            batch.keys.push_back(key.asString());
        }
    } else if (root.isMember("key")) {
        batch.keys.push_back(root["key"].asString());
    }
    if (batch.keys.empty()) {
        if (error) *error = "notify JSON has no keys";
        return false;
    }
    *out = std::move(batch);
    return true;
}

bool DecodeNotifyQuery(const std::string &path_with_query, NotifyBatch *out,
                       std::string *error) {
    if (out == nullptr) return false;
    const auto query_pos = path_with_query.find('?');
    if (query_pos == std::string::npos) {
        if (error) *error = "notify request has no query string";
        return false;
    }
    NotifyBatch batch;
    std::stringstream input(path_with_query.substr(query_pos + 1));
    std::string pair;
    while (std::getline(input, pair, '&')) {
        if (pair.empty()) continue;
        const auto equals = pair.find('=');
        const std::string raw_name =
            equals == std::string::npos ? pair : pair.substr(0, equals);
        const std::string raw_value =
            equals == std::string::npos ? "" : pair.substr(equals + 1);
        std::string name;
        std::string value;
        if (!UrlDecode(raw_name, &name) || !UrlDecode(raw_value, &value)) {
            if (error) *error = "invalid percent-encoding in notify query";
            return false;
        }
        AddParam(&batch, name, value);
    }
    if (batch.keys.empty()) {
        if (error) *error = "notify query has no keys";
        return false;
    }
    *out = std::move(batch);
    return true;
}

bool DecodeNotify(const std::string &path_with_query,
                  const std::string &body, const std::string &format,
                  NotifyBatch *out, std::string *error) {
    if (format == "json") return DecodeNotifyJson(body, out, error);
    if (format == "query") return DecodeNotifyQuery(path_with_query, out, error);
    // "both": prefer a JSON body, otherwise parse the query string.
    if (!body.empty() && DecodeNotifyJson(body, out, nullptr)) return true;
    return DecodeNotifyQuery(path_with_query, out, error);
}

}  // namespace mooncake::kvtest
