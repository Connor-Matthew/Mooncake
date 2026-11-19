#include <gtest/gtest.h>

#include <filesystem>
#include <span>
#include <string>
#include <vector>

#include "basic.h"
#include "buffers.h"
#include "fake_store.h"
#include "keys.h"

namespace mooncake::kvtest {
namespace {

Config MakeConfig(const std::string &operation, int threads, uint64_t keys) {
    Config config;
    config.mode = "basic";
    config.role = "client";
    config.operation = operation;
    config.num_threads = threads;
    config.num_total_threads = threads;
    config.key_count = keys;
    config.data_sizes = {64};
    config.target_qps = 0.0;
    config.batch_keys_count = 1;
    config.verify_level = "full";
    config.verify_sample_bytes = 64;
    config.verify_sample_step_bytes = 64;
    return config;
}

std::string MetricsDir(const std::string &name) {
    return (std::filesystem::path(::testing::TempDir()) /
            ("kvtest_basic_" + name))
        .string();
}

void Prefill(FakeStore &store, const Config &config, uint64_t count) {
    std::vector<char> value(static_cast<size_t>(config.data_sizes.front()));
    FillPatternBytes(value.data(), value.size(), config.value_seed);
    for (uint64_t i = 0; i < count; ++i) {
        const std::string key =
            MakeKey(config.key_prefix, config.key_scenario, i);
        ASSERT_EQ(store.Put(
                      key, std::span<const char>(value.data(), value.size())),
                  0);
    }
}

const MetricSnapshot *FindSnapshot(const std::vector<MetricSnapshot> &snaps,
                                   const std::string &name) {
    for (const auto &snapshot : snaps) {
        if (snapshot.name == name) return &snapshot;
    }
    return nullptr;
}

}  // namespace

TEST(BasicRunnerTest, PutOnePass) {
    FakeStore store;
    MetricsCollector metrics(0, 3000, MetricsDir("put"));
    metrics.Start();
    const Config config = MakeConfig("put", 2, 10);
    BasicRunner runner(config, store, metrics);
    std::string error;
    ASSERT_TRUE(runner.Prepare(&error)) << error;
    ASSERT_TRUE(runner.Start(&error)) << error;

    EXPECT_EQ(store.data.size(), 10u);
    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *put = FindSnapshot(snapshots, "put");
    ASSERT_NE(put, nullptr);
    EXPECT_EQ(put->total, 10u);
    EXPECT_EQ(put->success, 10u);
    metrics.Stop();
}

TEST(BasicRunnerTest, GetAfterPrefill) {
    FakeStore store;
    MetricsCollector metrics(0, 3000, MetricsDir("get"));
    metrics.Start();
    const Config config = MakeConfig("get", 2, 8);
    Prefill(store, config, 8);
    BasicRunner runner(config, store, metrics);
    std::string error;
    ASSERT_TRUE(runner.Prepare(&error)) << error;
    ASSERT_TRUE(runner.Start(&error)) << error;

    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *get = FindSnapshot(snapshots, "get");
    ASSERT_NE(get, nullptr);
    EXPECT_EQ(get->total, 8u);
    EXPECT_EQ(get->success, 8u);
    metrics.Stop();
}

TEST(BasicRunnerTest, DurationModeLoopsReads) {
    FakeStore store;
    MetricsCollector metrics(0, 3000, MetricsDir("duration"));
    metrics.Start();
    Config config = MakeConfig("get", 1, 4);
    config.duration = 0.05;
    Prefill(store, config, 4);
    BasicRunner runner(config, store, metrics);
    std::string error;
    ASSERT_TRUE(runner.Prepare(&error)) << error;
    ASSERT_TRUE(runner.Start(&error)) << error;

    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *get = FindSnapshot(snapshots, "get");
    ASSERT_NE(get, nullptr);
    EXPECT_GE(get->total, 4u);
    EXPECT_EQ(get->success, get->total);
    metrics.Stop();
}

TEST(BasicRunnerTest, MixedPutGet) {
    FakeStore store;
    MetricsCollector metrics(0, 3000, MetricsDir("mixed"));
    metrics.Start();
    Config config = MakeConfig("put/get", 1, 8);
    config.put_ratio = 0.5;
    BasicRunner runner(config, store, metrics);
    std::string error;
    ASSERT_TRUE(runner.Prepare(&error)) << error;
    ASSERT_TRUE(runner.Start(&error)) << error;

    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *put = FindSnapshot(snapshots, "put");
    const MetricSnapshot *get = FindSnapshot(snapshots, "get");
    ASSERT_NE(put, nullptr);
    ASSERT_NE(get, nullptr);
    EXPECT_EQ(put->total, 4u);
    EXPECT_EQ(get->total, 4u);
    EXPECT_EQ(get->success, 4u);
    EXPECT_EQ(store.data.size(), 4u);
    metrics.Stop();
}

TEST(BasicRunnerTest, MixedBatchInterfaces) {
    FakeStore store;
    MetricsCollector metrics(0, 3000, MetricsDir("mixed_batch"));
    metrics.Start();
    Config config = MakeConfig("put_batch/get_batch", 1, 8);
    config.put_ratio = 0.5;
    config.batch_keys_count = 2;
    BasicRunner runner(config, store, metrics);
    std::string error;
    ASSERT_TRUE(runner.Prepare(&error)) << error;
    ASSERT_TRUE(runner.Start(&error)) << error;

    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *put = FindSnapshot(snapshots, "put_batch");
    const MetricSnapshot *get = FindSnapshot(snapshots, "get_batch");
    ASSERT_NE(put, nullptr);
    ASSERT_NE(get, nullptr);
    EXPECT_EQ(put->total, 2u);
    EXPECT_EQ(get->total, 2u);
    EXPECT_EQ(get->success, 2u);
    metrics.Stop();
}

TEST(BasicRunnerTest, WarmupSamplesAreExcluded) {
    FakeStore store;
    MetricsCollector metrics(0, 3000, MetricsDir("warmup"));
    metrics.Start();
    Config config = MakeConfig("put", 1, 5);
    config.warmup_duration = 0.02;
    BasicRunner runner(config, store, metrics);
    std::string error;
    ASSERT_TRUE(runner.Prepare(&error)) << error;
    ASSERT_TRUE(runner.Start(&error)) << error;

    const auto stats = runner.GetStats();
    EXPECT_GT(stats.warmup_ops, 0u);
    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *put = FindSnapshot(snapshots, "put");
    ASSERT_NE(put, nullptr);
    EXPECT_EQ(put->total, 5u);
    EXPECT_EQ(put->success, 5u);
    EXPECT_EQ(store.data.size(), 5u);
    for (const auto &entry : store.data) {
        EXPECT_EQ(entry.first.rfind("mc_warmup", 0), std::string::npos)
            << entry.first;
    }
    metrics.Stop();
}

TEST(BasicRunnerTest, GetWarmupReusesMeasuredKeys) {
    FakeStore store;
    MetricsCollector metrics(0, 3000, MetricsDir("get_warmup_reuse"));
    metrics.Start();
    Config config = MakeConfig("get", 1, 6);
    config.warmup_duration = 0.02;
    Prefill(store, config, 6);
    BasicRunner runner(config, store, metrics);
    std::string error;
    ASSERT_TRUE(runner.Prepare(&error)) << error;
    ASSERT_TRUE(runner.Start(&error)) << error;

    const auto stats = runner.GetStats();
    EXPECT_GT(stats.warmup_ops, 0u);
    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *get = FindSnapshot(snapshots, "get");
    ASSERT_NE(get, nullptr);
    EXPECT_EQ(get->total, 6u);
    EXPECT_EQ(get->success, 6u);
    // The warmup must not allocate or delete a second key set.
    EXPECT_EQ(store.data.size(), 6u);
    for (const auto &entry : store.data) {
        EXPECT_EQ(entry.first.rfind("mc_warmup", 0), std::string::npos)
            << entry.first;
    }
    metrics.Stop();
}

TEST(BasicRunnerTest, PutFromUsesRegisteredBuffer) {
    FakeStore store;
    MetricsCollector metrics(0, 3000, MetricsDir("put_from"));
    metrics.Start();
    Config config = MakeConfig("put_from", 1, 6);
    config.batch_keys_count = 2;
    BasicRunner runner(config, store, metrics);
    std::string error;
    ASSERT_TRUE(runner.Prepare(&error)) << error;
    ASSERT_TRUE(runner.Start(&error)) << error;

    EXPECT_EQ(store.data.size(), 6u);
    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *put = FindSnapshot(snapshots, "put_from");
    ASSERT_NE(put, nullptr);
    EXPECT_EQ(put->total, 6u);
    EXPECT_EQ(put->success, 6u);
    metrics.Stop();
}

TEST(BasicRunnerTest, GetIntoUsesRegisteredBuffer) {
    FakeStore store;
    MetricsCollector metrics(0, 3000, MetricsDir("get_into"));
    metrics.Start();
    Config config = MakeConfig("get_into", 2, 6);
    config.batch_keys_count = 2;
    Prefill(store, config, 6);
    BasicRunner runner(config, store, metrics);
    std::string error;
    ASSERT_TRUE(runner.Prepare(&error)) << error;
    ASSERT_TRUE(runner.Start(&error)) << error;

    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *get = FindSnapshot(snapshots, "get_into");
    ASSERT_NE(get, nullptr);
    EXPECT_EQ(get->total, 6u);
    EXPECT_EQ(get->success, 6u);
    metrics.Stop();
}

TEST(BasicRunnerTest, ValidateOperation) {
    std::string error;
    Config config = MakeConfig("get_buffer", 1, 4);
    EXPECT_TRUE(ValidateBasicOperation(config, &error)) << error;

    config = MakeConfig("get/put", 1, 4);
    EXPECT_FALSE(ValidateBasicOperation(config, &error));

    config = MakeConfig("no_such_op", 1, 4);
    EXPECT_FALSE(ValidateBasicOperation(config, &error));

    config = MakeConfig("get", 1, 4);
    config.data_sizes = {64, 128};
    EXPECT_FALSE(ValidateBasicOperation(config, &error));
}

}  // namespace mooncake::kvtest
