// Copyright 2024 KVCache.AI
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

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "transport/kunpeng_transport/urma/urma_compat.h"

namespace mooncake {
namespace {

TEST(UrmaCompatTest, ResolvesOptionalFunctionsOnceAcrossThreads) {
    std::vector<std::thread> threads;
    for (int i = 0; i < 16; ++i) {
        threads.emplace_back([] { initUrmaCompat(); });
    }
    for (auto& thread : threads) thread.join();

    // The pair must be published atomically: either the new API is entirely
    // available or the compatibility layer entirely falls back.
    EXPECT_EQ(g_urma_get_rjetty == nullptr, g_urma_put_rjetty == nullptr);
    EXPECT_EQ(g_urma_get_seg_ctx == nullptr, g_urma_put_seg_ctx == nullptr);
#ifdef MOONCAKE_MOCK_URMA
    EXPECT_NE(g_urma_get_rjetty, nullptr);
    EXPECT_NE(g_urma_put_rjetty, nullptr);
    EXPECT_NE(g_urma_get_seg_ctx, nullptr);
    EXPECT_NE(g_urma_put_seg_ctx, nullptr);
#endif
}

TEST(UrmaCompatTest, ValidatesDelegatedRjettyEncodingAndLength) {
    std::vector<unsigned char> decoded;
    std::string error;

    EXPECT_FALSE(decodeUrmaRjettyBlob("0G", decoded, error));
    EXPECT_FALSE(error.empty());

    const std::string short_blob((sizeof(urma_rjetty_t) - 1) * 2, '0');
    EXPECT_FALSE(decodeUrmaRjettyBlob(short_blob, decoded, error));
    EXPECT_FALSE(error.empty());

    const std::string valid_blob(sizeof(urma_rjetty_t) * 2, '0');
    EXPECT_TRUE(decodeUrmaRjettyBlob(valid_blob, decoded, error));
    EXPECT_EQ(decoded.size(), sizeof(urma_rjetty_t));
    EXPECT_TRUE(error.empty());
}

TEST(UrmaCompatTest, ValidatesLegacyAndDelegatedSegmentEncoding) {
    std::vector<unsigned char> decoded;
    std::string error;

    EXPECT_FALSE(decodeUrmaSegBlob("0G", decoded, error));
    EXPECT_FALSE(error.empty());

    const std::string short_blob((sizeof(urma_seg_t) - 1) * 2, '0');
    EXPECT_FALSE(decodeUrmaSegBlob(short_blob, decoded, error));
    EXPECT_FALSE(error.empty());

    // A base urma_seg_t is the legacy format and remains supported.
    const std::string legacy_blob(sizeof(urma_seg_t) * 2, '0');
    EXPECT_TRUE(decodeUrmaSegBlob(legacy_blob, decoded, error));
    EXPECT_EQ(decoded.size(), sizeof(urma_seg_t));

    // New UMDK versions append opaque driver data. It must be kept intact.
    constexpr size_t kExtensionSize = 16;
    const std::string delegated_blob((sizeof(urma_seg_t) + kExtensionSize) * 2,
                                     '0');
    EXPECT_TRUE(decodeUrmaSegBlob(delegated_blob, decoded, error));
    EXPECT_EQ(decoded.size(), sizeof(urma_seg_t) + kExtensionSize);
    EXPECT_TRUE(error.empty());
}

}  // namespace
}  // namespace mooncake
