#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

namespace mooncake::kvtest {

// Python-client compatible key: "{prefix}*{scenario}*{index:08d}" when a
// scenario is configured, otherwise "{prefix}*{index:08d}".
inline std::string MakeKey(const std::string &prefix, const std::string &scenario,
                           uint64_t index) {
    std::string key = prefix;
    if (!scenario.empty()) {
        key += '*';
        key += scenario;
    }
    key += '*';
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%08llu",
                  static_cast<unsigned long long>(index));
    key += buffer;
    return key;
}

inline std::vector<std::string> MakeKeyRange(const std::string &prefix,
                                            const std::string &scenario,
                                            uint64_t begin, uint64_t end) {
    std::vector<std::string> keys;
    if (end <= begin) return keys;
    keys.reserve(static_cast<size_t>(end - begin));
    for (uint64_t index = begin; index < end; ++index) {
        keys.push_back(MakeKey(prefix, scenario, index));
    }
    return keys;
}

}  // namespace mooncake::kvtest
