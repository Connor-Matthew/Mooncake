#include "maintenance.h"

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <span>
#include <string>
#include <vector>

#include "buffers.h"
#include "keys.h"

namespace mooncake::kvtest {
namespace {

std::vector<std::string> KeyRange(const Config &config) {
    const uint64_t begin = config.key_start;
    const uint64_t end =
        std::min<uint64_t>(config.EffectiveKeyEnd(), config.key_count);
    return MakeKeyRange(config.key_prefix, config.key_scenario, begin, end);
}

void WriteSummary(const Config &config, const Json::Value &root) {
    std::filesystem::create_directories(config.output_dir);
    std::ofstream output(std::filesystem::path(config.output_dir) /
                         "summary.json");
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "  ";
    output << Json::writeString(builder, root) << '\n';
}

}  // namespace

int RunPrefill(const Config &config, StoreAdapter &store, std::string *error) {
    if (config.data_sizes.size() != 1) {
        if (error) *error = "prefill requires exactly one data_sizes entry";
        return 1;
    }
    const uint64_t size = config.data_sizes.front();
    ValueSource value;
    if (!value.Load(config.value_seed, config.value_file, {size}, error)) {
        return 1;
    }
    const char *data = value.Data(size);
    if (data == nullptr) {
        if (error) *error = "value source produced no data";
        return 1;
    }
    const std::vector<std::string> keys = KeyRange(config);
    if (keys.empty()) {
        if (error) *error = "prefill key range is empty";
        return 1;
    }

    const size_t batch =
        static_cast<size_t>(std::max(1, config.prefill_batch));
    uint64_t existing = 0;
    uint64_t written = 0;
    uint64_t failed = 0;
    for (size_t offset = 0; offset < keys.size(); offset += batch) {
        const size_t count = std::min(batch, keys.size() - offset);
        std::vector<std::string> chunk(keys.begin() + offset,
                                       keys.begin() + offset + count);
        const std::vector<int> existence = store.BatchIsExist(chunk);
        if (existence.size() != chunk.size()) {
            if (error) *error = "batchIsExist returned an unexpected size";
            failed += chunk.size();
            break;
        }
        std::vector<std::string> missing;
        missing.reserve(chunk.size());
        for (size_t i = 0; i < chunk.size(); ++i) {
            if (existence[i] > 0) {
                ++existing;
            } else {
                missing.push_back(chunk[i]);
            }
        }
        if (missing.empty()) continue;
        std::vector<std::span<const char>> values(
            missing.size(),
            std::span<const char>(data, static_cast<size_t>(size)));
        const int rc = store.PutBatch(missing, values);
        if (rc == 0) {
            written += missing.size();
        } else {
            failed += missing.size();
        }
    }

    Json::Value root(Json::objectValue);
    root["mode"] = "prefill";
    root["instance_id"] = config.instance_id;
    root["total"] = static_cast<Json::UInt64>(keys.size());
    root["existing"] = static_cast<Json::UInt64>(existing);
    root["written"] = static_cast<Json::UInt64>(written);
    root["failed"] = static_cast<Json::UInt64>(failed);
    root["data_size"] = static_cast<Json::UInt64>(size);
    WriteSummary(config, root);
    std::cout << "[kvtest] prefill total=" << keys.size()
              << " existing=" << existing << " written=" << written
              << " failed=" << failed << '\n';
    if (failed != 0) {
        if (error) *error = "prefill failed for " + std::to_string(failed) +
                            " keys";
        return 1;
    }
    return 0;
}

int RunCleanup(const Config &config, StoreAdapter &store, std::string *error) {
    Json::Value root(Json::objectValue);
    root["mode"] = "cleanup";
    root["instance_id"] = config.instance_id;
    root["scope"] = config.scope;
    if (config.scope == "all") {
        const long cleared = store.RemoveAll(true);
        root["cleared"] = static_cast<Json::Int64>(cleared);
        WriteSummary(config, root);
        std::cout << "[kvtest] cleanup scope=all cleared=" << cleared << '\n';
        if (cleared < 0) {
            if (error) *error = "removeAll failed";
            return 1;
        }
        return 0;
    }

    const std::vector<std::string> keys = KeyRange(config);
    if (keys.empty()) {
        if (error) *error = "cleanup key range is empty";
        return 1;
    }
    constexpr size_t kChunk = 1024;
    uint64_t removed = 0;
    uint64_t not_found = 0;
    uint64_t failed = 0;
    for (size_t offset = 0; offset < keys.size(); offset += kChunk) {
        const size_t count = std::min(kChunk, keys.size() - offset);
        std::vector<std::string> chunk(keys.begin() + offset,
                                       keys.begin() + offset + count);
        const std::vector<int> existence = store.BatchIsExist(chunk);
        const std::vector<int> statuses = store.BatchRemove(chunk, true);
        if (existence.size() != chunk.size() ||
            statuses.size() != chunk.size()) {
            failed += chunk.size();
            continue;
        }
        for (size_t i = 0; i < chunk.size(); ++i) {
            if (existence[i] == 0) {
                ++not_found;
            } else if (existence[i] == 1 && statuses[i] == 0) {
                ++removed;
            } else {
                ++failed;
            }
        }
    }

    root["keys_attempted"] = static_cast<Json::UInt64>(keys.size());
    root["keys_removed"] = static_cast<Json::UInt64>(removed);
    root["keys_not_found"] = static_cast<Json::UInt64>(not_found);
    root["keys_failed"] = static_cast<Json::UInt64>(failed);
    WriteSummary(config, root);
    std::cout << "[kvtest] cleanup scope=range attempted=" << keys.size()
              << " removed=" << removed << " not_found=" << not_found
              << " failed=" << failed << '\n';
    if (failed != 0) {
        if (error) {
            *error = "cleanup failed for " + std::to_string(failed) + " keys";
        }
        return 1;
    }
    return 0;
}

}  // namespace mooncake::kvtest
