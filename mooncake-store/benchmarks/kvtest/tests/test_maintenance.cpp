#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include "fake_store.h"
#include "keys.h"
#include "maintenance.h"

namespace mooncake::kvtest {
namespace {

Config MakeConfig(const std::string &mode, uint64_t keys,
                  const std::string &name) {
    Config config;
    config.mode = mode;
    config.role = "client";
    config.key_prefix = "mc_bench";
    config.key_scenario = "basic";
    config.key_count = keys;
    config.data_sizes = {64};
    config.prefill_batch = 4;
    config.output_dir =
        (std::filesystem::path(::testing::TempDir()) / ("kvtest_" + name))
            .string();
    return config;
}

bool SummaryExists(const Config &config) {
    return std::filesystem::exists(
        std::filesystem::path(config.output_dir) / "summary.json");
}

std::string SummaryText(const Config &config) {
    std::ifstream input(std::filesystem::path(config.output_dir) /
                        "summary.json");
    return std::string((std::istreambuf_iterator<char>(input)),
                       std::istreambuf_iterator<char>());
}

}  // namespace

TEST(MaintenanceTest, PrefillWritesMissingKeysOnly) {
    FakeStore store;
    Config config = MakeConfig("prefill", 10, "prefill");
    std::string error;
    ASSERT_EQ(RunPrefill(config, store, &error), 0) << error;
    EXPECT_EQ(store.data.size(), 10u);
    EXPECT_TRUE(SummaryExists(config));
    EXPECT_NE(SummaryText(config).find("\"mode\""), std::string::npos);

    // A second pass must not rewrite existing keys.
    const auto before = store.data.begin()->first;
    ASSERT_EQ(RunPrefill(config, store, &error), 0) << error;
    EXPECT_EQ(store.data.size(), 10u);
    EXPECT_EQ(store.data.begin()->first, before);
}

TEST(MaintenanceTest, CleanupRangeRemovesKeys) {
    FakeStore store;
    Config config = MakeConfig("cleanup", 10, "cleanup_range");
    config.scope = "range";
    std::string error;
    ASSERT_EQ(RunPrefill(config, store, &error), 0) << error;
    ASSERT_EQ(store.data.size(), 10u);
    ASSERT_EQ(RunCleanup(config, store, &error), 0) << error;
    EXPECT_TRUE(store.data.empty());
}

TEST(MaintenanceTest, CleanupAllClearsStore) {
    FakeStore store;
    Config config = MakeConfig("cleanup", 10, "cleanup_all");
    config.scope = "all";
    std::string error;
    ASSERT_EQ(RunPrefill(config, store, &error), 0) << error;
    ASSERT_EQ(store.data.size(), 10u);
    ASSERT_EQ(RunCleanup(config, store, &error), 0) << error;
    EXPECT_TRUE(store.data.empty());
}

TEST(MaintenanceTest, PrefillUsesPythonCompatibleKeys) {
    FakeStore store;
    Config config = MakeConfig("prefill", 2, "prefill_keys");
    std::string error;
    ASSERT_EQ(RunPrefill(config, store, &error), 0) << error;
    EXPECT_NE(store.data.find("mc_bench*basic*00000000"), store.data.end());
    EXPECT_NE(store.data.find("mc_bench*basic*00000001"), store.data.end());
}

}  // namespace mooncake::kvtest
