// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_TRANSPORT_UB_PARAMS_H_
#define TENT_TRANSPORT_UB_PARAMS_H_

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

struct UbParams {
    // UB is opt-in until a real URMA provider and NIC topology are present.
    bool enable = false;
    std::vector<std::string> device_filter;
    BondingOptions bonding;
    bool numa_affinity = false;
    bool electrical_network = true;

    uint32_t worker_count = 6;
    uint32_t poller_count = 2;
    uint32_t jfc_per_context = 6;
    uint32_t jetty_per_endpoint = 6;
    uint32_t max_endpoints = 65536;
    size_t slice_size = 64 * 1024;
    size_t max_slices_per_task = 64 * 1024;
    uint32_t max_retries = 8;
    uint32_t slice_timeout_ms = 5000;
    uint32_t endpoint_cooldown_ms = 1000;
    // After the most recent post, JFC pollers keep polling instead of
    // sleeping, and one posting worker spins on the queue instead of waiting
    // on the condition variable, for this long. Sparse foreground transfers
    // then skip a scheduler wakeup on both the post and the completion side.
    // 0 restores the sleep/wait-only behaviour.
    uint32_t poll_idle_spin_us = 1000;
    uint32_t post_idle_spin_us = 1000;

    bool enable_bandwidth_estimation = true;
    bool enable_notifications = false;

    // Reads every UB setting from transports/ub/*. Numeric settings are
    // deliberately read as signed JSON integers first so a negative value is
    // rejected rather than silently converted to an unsigned default.
    static Status FromConfig(const Config& config, UbParams& output) {
        UbParams parsed;

        CHECK_STATUS(readBool(config, "transports/ub/enable", parsed.enable,
                              parsed.enable));
        CHECK_STATUS(readDeviceFilter(config, parsed.device_filter));
        CHECK_STATUS(readBool(config, "transports/ub/numa_affinity", false,
                              parsed.numa_affinity));
        CHECK_STATUS(readBool(config, "transports/ub/electrical_network", true,
                              parsed.electrical_network));
        CHECK_STATUS(readBondingMode(config, parsed.bonding.mode));
        CHECK_STATUS(readBondingLevel(config, parsed.bonding.level));
        CHECK_STATUS(readTransportMode(config,
                                       parsed.bonding.transport_mode));
        CHECK_STATUS(readPositive(config, "transports/ub/worker_count",
                                  parsed.worker_count, parsed.worker_count));
        CHECK_STATUS(readPositive(config, "transports/ub/poller_count",
                                  parsed.poller_count, parsed.poller_count));
        CHECK_STATUS(readPositive(config, "transports/ub/jfc_per_context",
                                  parsed.jfc_per_context,
                                  parsed.jfc_per_context));
        CHECK_STATUS(readPositive(config, "transports/ub/jetty_per_endpoint",
                                  parsed.jetty_per_endpoint,
                                  parsed.jetty_per_endpoint));
        CHECK_STATUS(readPositive(config, "transports/ub/max_endpoints",
                                  parsed.max_endpoints, parsed.max_endpoints));

        uint64_t slice_size = parsed.slice_size;
        CHECK_STATUS(readPositive(config, "transports/ub/slice_size",
                                  slice_size, slice_size));
        if (slice_size > std::numeric_limits<uint32_t>::max()) {
            return Status::InvalidArgument(
                "transports/ub/slice_size exceeds the URMA SGE length limit");
        }
        parsed.slice_size = static_cast<size_t>(slice_size);

        uint64_t max_slices_per_task = parsed.max_slices_per_task;
        CHECK_STATUS(readPositive(config, "transports/ub/max_slices_per_task",
                                  max_slices_per_task, max_slices_per_task));
        if (max_slices_per_task > std::numeric_limits<size_t>::max()) {
            return Status::InvalidArgument(
                "transports/ub/max_slices_per_task exceeds size_t");
        }
        parsed.max_slices_per_task = static_cast<size_t>(max_slices_per_task);

        CHECK_STATUS(readNonNegative(config, "transports/ub/max_retries",
                                     parsed.max_retries, parsed.max_retries));
        CHECK_STATUS(readPositive(config, "transports/ub/slice_timeout_ms",
                                  parsed.slice_timeout_ms,
                                  parsed.slice_timeout_ms));
        CHECK_STATUS(readPositive(config, "transports/ub/endpoint_cooldown_ms",
                                  parsed.endpoint_cooldown_ms,
                                  parsed.endpoint_cooldown_ms));
        CHECK_STATUS(readNonNegative(config, "transports/ub/poll_idle_spin_us",
                                     parsed.poll_idle_spin_us,
                                     parsed.poll_idle_spin_us));
        CHECK_STATUS(readNonNegative(config, "transports/ub/post_idle_spin_us",
                                     parsed.post_idle_spin_us,
                                     parsed.post_idle_spin_us));
        if (parsed.poll_idle_spin_us > kMaxIdleSpinUs ||
            parsed.post_idle_spin_us > kMaxIdleSpinUs) {
            return Status::InvalidArgument(
                "transports/ub/*_idle_spin_us must not exceed 1000000");
        }
        CHECK_STATUS(readBool(config,
                              "transports/ub/enable_bandwidth_estimation",
                              parsed.enable_bandwidth_estimation,
                              parsed.enable_bandwidth_estimation));
        CHECK_STATUS(readBool(config, "transports/ub/enable_notifications",
                              parsed.enable_notifications,
                              parsed.enable_notifications));

        output = std::move(parsed);
        return Status::OK();
    }

   private:
    static std::string normalizeBondingToken(std::string token) {
        const auto first = token.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) return {};
        const auto last = token.find_last_not_of(" \t\r\n");
        token = token.substr(first, last - first + 1);
        std::transform(token.begin(), token.end(), token.begin(),
                       [](unsigned char c) {
                           return static_cast<char>(std::toupper(c));
                       });
        std::replace(token.begin(), token.end(), '_', '-');
        return token;
    }

    static Status readBondingMode(const Config& config, BondingMode& output) {
        static constexpr const char* kKey = "transports/ub/bonding_mode";
        const json value = config.get<json>(kKey, json());
        if (value.is_null()) return Status::OK();
        if (!value.is_string()) {
            return Status::InvalidArgument(std::string(kKey) +
                                           " must be a string");
        }
        const auto token = normalizeBondingToken(value.get<std::string>());
        if (token == "BALANCE") {
            output = BondingMode::kBalance;
        } else if (token == "STANDALONE") {
            output = BondingMode::kStandalone;
        } else if (token == "ACTIVE-BACKUP") {
            output = BondingMode::kActiveBackup;
        } else {
            return Status::InvalidArgument(
                std::string(kKey) +
                " must be BALANCE, STANDALONE, or ACTIVE-BACKUP");
        }
        return Status::OK();
    }

    static Status readBondingLevel(const Config& config,
                                   BondingLevel& output) {
        static constexpr const char* kKey = "transports/ub/bonding_level";
        const json value = config.get<json>(kKey, json());
        if (value.is_null()) return Status::OK();
        if (!value.is_string()) {
            return Status::InvalidArgument(std::string(kKey) +
                                           " must be a string");
        }
        const auto token = normalizeBondingToken(value.get<std::string>());
        if (token == "IODIE") {
            output = BondingLevel::kIodie;
        } else if (token == "PORT") {
            output = BondingLevel::kPort;
        } else {
            return Status::InvalidArgument(std::string(kKey) +
                                           " must be IODIE or PORT");
        }
        return Status::OK();
    }

    static Status readTransportMode(const Config& config,
                                    TransportMode& output) {
        static constexpr const char* kKey = "transports/ub/trans_mode";
        const json value = config.get<json>(kKey, json());
        if (value.is_null()) return Status::OK();
        if (!value.is_string()) {
            return Status::InvalidArgument(std::string(kKey) +
                                           " must be a string");
        }
        const auto token = normalizeBondingToken(value.get<std::string>());
        if (token == "RC") {
            output = TransportMode::kRc;
        } else if (token == "RM") {
            output = TransportMode::kRm;
        } else if (token == "UM") {
            output = TransportMode::kUm;
        } else {
            return Status::InvalidArgument(std::string(kKey) +
                                           " must be UM, RC, or RM");
        }
        return Status::OK();
    }

    static constexpr uint32_t kMaxIdleSpinUs = 1'000'000;

    template <typename T>
    static Status readPositive(const Config& config, const char* key,
                               T default_value, T& output) {
        const json value = config.get<json>(key, json());
        if (value.is_null()) {
            output = default_value;
            return Status::OK();
        }
        if (!value.is_number_integer()) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a positive integer");
        }

        int64_t signed_value = 0;
        try {
            signed_value = value.get<int64_t>();
        } catch (...) {
            return Status::InvalidArgument(std::string(key) +
                                           " is outside the supported range");
        }
        if (signed_value <= 0 ||
            static_cast<uint64_t>(signed_value) >
                static_cast<uint64_t>(std::numeric_limits<T>::max())) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a positive integer in "
                                           "the supported range");
        }
        output = static_cast<T>(signed_value);
        return Status::OK();
    }

    template <typename T>
    static Status readNonNegative(const Config& config, const char* key,
                                  T default_value, T& output) {
        const json value = config.get<json>(key, json());
        if (value.is_null()) {
            output = default_value;
            return Status::OK();
        }
        if (!value.is_number_integer()) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a non-negative integer");
        }
        int64_t signed_value = 0;
        try {
            signed_value = value.get<int64_t>();
        } catch (...) {
            return Status::InvalidArgument(std::string(key) +
                                           " is outside the supported range");
        }
        if (signed_value < 0 ||
            static_cast<uint64_t>(signed_value) >
                static_cast<uint64_t>(std::numeric_limits<T>::max())) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a non-negative integer "
                                           "in the supported range");
        }
        output = static_cast<T>(signed_value);
        return Status::OK();
    }

    static Status readBool(const Config& config, const char* key,
                           bool default_value, bool& output) {
        const json value = config.get<json>(key, json());
        if (value.is_null()) {
            output = default_value;
            return Status::OK();
        }
        if (!value.is_boolean()) {
            return Status::InvalidArgument(std::string(key) +
                                           " must be a boolean");
        }
        output = value.get<bool>();
        return Status::OK();
    }

    static Status readDeviceFilter(const Config& config,
                                   std::vector<std::string>& output) {
        static constexpr const char* kKey = "transports/ub/device_filter";
        const json value = config.get<json>(kKey, json());
        if (value.is_null()) {
            output.clear();
            return Status::OK();
        }

        std::vector<std::string> parsed;
        if (value.is_string()) {
            parsed.push_back(value.get<std::string>());
        } else if (value.is_array()) {
            for (const auto& entry : value) {
                if (!entry.is_string()) {
                    return Status::InvalidArgument(
                        "transports/ub/device_filter entries must be strings");
                }
                parsed.push_back(entry.get<std::string>());
            }
        } else {
            return Status::InvalidArgument(
                "transports/ub/device_filter must be a string or string "
                "array");
        }

        for (const auto& entry : parsed) {
            if (entry.empty()) {
                return Status::InvalidArgument(
                    "transports/ub/device_filter entries must not be empty");
            }
        }
        output = std::move(parsed);
        return Status::OK();
    }
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PARAMS_H_
