// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace mooncake::tent::ub {

inline constexpr uint8_t kUnboundChip = 0xff;

struct ChipBinding {
    uint8_t src{kUnboundChip};
    uint8_t dst{kUnboundChip};
    bool valid() const {
        return src != 0 && dst != 0 && src != kUnboundChip &&
               dst != kUnboundChip;
    }
};

inline ChipBinding selectChipBinding(bool electrical, uint8_t local,
                                     uint8_t remote) {
    return {electrical ? remote : local, remote};
}

// Build only on the host owning the memory. Sorted physical packages use the
// same one-based numbering as classic TE. Unknown topology stays unbound.
inline uint8_t localLocationChip(const std::string& location) {
    static const auto chips = [] {
        std::map<int, int> packages;
        std::set<int> ids;
        std::error_code error;
        std::filesystem::directory_iterator nodes(
            "/sys/devices/system/node", error);
        if (!error) {
            const std::filesystem::directory_iterator end;
            for (; !error && nodes != end; nodes.increment(error)) {
                const auto& entry = *nodes;
                const auto name = entry.path().filename().string();
                if (name.rfind("node", 0) != 0) continue;
                try {
                    size_t consumed = 0;
                    const int node = std::stoi(name.substr(4), &consumed);
                    if (consumed != name.size() - 4 || node < 0) continue;
                    int cpu = -1, package = -1;
                    std::ifstream(entry.path() / "cpulist") >> cpu;
                    if (cpu < 0) continue;
                    std::ifstream("/sys/devices/system/cpu/cpu" +
                                  std::to_string(cpu) +
                                  "/topology/physical_package_id") >> package;
                    if (package < 0) continue;
                    packages[node] = package;
                    ids.insert(package);
                } catch (const std::exception&) {
                    continue;
                }
            }
        }
        std::map<int, uint8_t> result;
        if (ids.size() >= kUnboundChip) return result;
        std::map<int, uint8_t> numbering;
        uint8_t chip = 1;
        for (int id : ids) numbering[id] = chip++;
        for (const auto& [node, id] : packages) result[node] = numbering[id];
        return result;
    }();
    if (location.rfind("cpu:", 0) != 0) return kUnboundChip;
    try {
        size_t consumed = 0;
        const int node = std::stoi(location.substr(4), &consumed);
        if (consumed != location.size() - 4) return kUnboundChip;
        const auto found = chips.find(node);
        return found == chips.end() ? kUnboundChip : found->second;
    } catch (const std::exception&) {
        return kUnboundChip;
    }
}

struct ChipRegion {
    uint64_t size{0};
    uint8_t chip_id{kUnboundChip};
};

// A WR crossing different chips cannot be represented by one affinity hint.
inline uint8_t chipForRange(const std::vector<ChipRegion>& regions,
                            uint64_t offset, uint64_t length) {
    if (length == 0) return kUnboundChip;
    uint8_t chip = kUnboundChip;
    bool found = false;
    for (const auto& region : regions) {
        if (offset >= region.size) {
            offset -= region.size;
            continue;
        }
        if (found && chip != region.chip_id) return kUnboundChip;
        chip = region.chip_id;
        found = true;
        const auto available = region.size - offset;
        if (length <= available) return chip;
        length -= available;
        offset = 0;
    }
    return kUnboundChip;
}

}  // namespace mooncake::tent::ub
