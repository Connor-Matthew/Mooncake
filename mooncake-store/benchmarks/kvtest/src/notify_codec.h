#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace mooncake::kvtest {

// One writer batch delivered to a reader. `arrival_us` is filled by the
// receiving process and never serialized.
struct NotifyBatch {
    std::vector<std::string> keys;
    int sender{0};
    uint64_t size{0};
    bool warmup{false};
    int64_t arrival_us{0};
};

std::string EncodeNotifyJson(const NotifyBatch &batch);
std::string EncodeNotifyQuery(const NotifyBatch &batch,
                              const std::string &path = "/notify");

bool DecodeNotifyJson(const std::string &body, NotifyBatch *out,
                      std::string *error);
bool DecodeNotifyQuery(const std::string &path_with_query, NotifyBatch *out,
                       std::string *error);

// format: "both" (JSON body first, then query), "json", or "query".
bool DecodeNotify(const std::string &path_with_query,
                  const std::string &body, const std::string &format,
                  NotifyBatch *out, std::string *error);

std::string UrlEncode(const std::string &value);
bool UrlDecode(const std::string &value, std::string *out);

}  // namespace mooncake::kvtest
