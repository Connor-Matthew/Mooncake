#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "buffers.h"
#include "fake_store.h"
#include "metrics.h"
#include "notify_sender.h"
#include "pipeline.h"

namespace mooncake::kvtest {
namespace {

std::string MetricsDir(const std::string &name) {
    return (std::filesystem::path(::testing::TempDir()) /
            ("kvtest_pipeline_" + name))
        .string();
}

Config WriterConfig(const std::string &name) {
    Config config;
    config.mode = "pipeline";
    config.role = "writer";
    config.instance_id = 0;
    config.pipeline = {"put"};
    config.notify_pipeline = {"get_buffer"};
    config.num_threads = 1;
    config.num_total_threads = 2;
    config.target_qps = 0.0;
    config.batch_keys_count = 1;
    config.data_sizes = {64};
    config.duration = 0.02;
    config.warmup_duration = 0.0;
    config.notify_count = 1;
    config.notify_threads = 1;
    config.notify_queue_max = 64;
    config.notify_overflow = "block";
    config.notify_async = true;
    config.key_pool_size = 0;
    config.verify_level = "full";
    config.verify_sample_bytes = 64;
    config.verify_sample_step_bytes = 64;
    config.peers = {"http://peer1"};
    config.output_dir = MetricsDir(name);
    return config;
}

Config ReaderConfig(const std::string &name,
                    std::vector<std::string> notify_pipeline) {
    Config config;
    config.mode = "pipeline";
    config.role = "reader";
    config.instance_id = 1;
    config.notify_pipeline = std::move(notify_pipeline);
    config.num_threads = 1;
    config.num_total_threads = 2;
    config.data_sizes = {64};
    config.batch_keys_count = 1;
    config.verify_level = "full";
    config.verify_sample_bytes = 64;
    config.verify_sample_step_bytes = 64;
    config.output_dir = MetricsDir(name);
    return config;
}

const MetricSnapshot *FindSnapshot(const std::vector<MetricSnapshot> &snaps,
                                   const std::string &name) {
    for (const auto &snapshot : snaps) {
        if (snapshot.name == name) return &snapshot;
    }
    return nullptr;
}

void PrefillPattern(FakeStore &store, const std::vector<std::string> &keys,
                    int sender, uint64_t size = 64) {
    std::vector<char> value(static_cast<size_t>(size));
    FillPatternBytes(value.data(), size, static_cast<uint64_t>(sender));
    for (const auto &key : keys) {
        ASSERT_EQ(store.Put(key, std::span<const char>(value.data(), size)), 0);
    }
}

struct Capture {
    std::mutex mutex;
    std::vector<NotifyBatch> batches;
    std::atomic<int> warmup{0};
    std::atomic<int> measured{0};
};

NotifySender::SendFn CaptureSend(Capture *capture) {
    return [capture](const std::string &, const NotifyBatch &batch,
                     double *latency) {
        {
            std::lock_guard<std::mutex> lock(capture->mutex);
            capture->batches.push_back(batch);
        }
        if (batch.warmup) {
            capture->warmup.fetch_add(1);
        } else {
            capture->measured.fetch_add(1);
        }
        if (latency != nullptr) *latency = 0.05;
        return true;
    };
}

}  // namespace

TEST(PipelineWriterTest, MeasuredPhaseWritesAndNotifies) {
    FakeStore store;
    Config config = WriterConfig("writer");
    MetricsCollector metrics(0, 3000, config.output_dir);
    metrics.Start();
    Capture capture;
    NotifySender sender(2, 64, NotifySender::Overflow::kDrop, true,
                        CaptureSend(&capture), nullptr);
    PipelineWriter writer(config, store, metrics, &sender);
    std::string error;
    ASSERT_TRUE(writer.Prepare(&error)) << error;
    ASSERT_TRUE(writer.Start(&error)) << error;
    sender.Drain();

    const auto stats = writer.GetStats();
    EXPECT_GT(stats.measured_ops, 0u);
    EXPECT_EQ(stats.measured_failures, 0u);
    EXPECT_GT(stats.notifications, 0u);
    EXPECT_EQ(stats.phase, "stopped");
    EXPECT_FALSE(store.data.empty());

    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *put = FindSnapshot(snapshots, "put");
    ASSERT_NE(put, nullptr);
    EXPECT_GT(put->total, 0u);
    EXPECT_EQ(put->success, put->total);

    {
        std::lock_guard<std::mutex> lock(capture.mutex);
        ASSERT_FALSE(capture.batches.empty());
        for (const auto &batch : capture.batches) {
            EXPECT_EQ(batch.sender, 0);
            EXPECT_EQ(batch.size, 64u);
            EXPECT_FALSE(batch.warmup);
            EXPECT_FALSE(batch.keys.empty());
        }
    }
    metrics.Stop();
}

TEST(PipelineWriterTest, WarmupIsSeparatedFromMeasured) {
    FakeStore store;
    Config config = WriterConfig("warmup");
    config.warmup_duration = 0.01;
    config.duration = 0.01;
    MetricsCollector metrics(0, 3000, config.output_dir);
    metrics.Start();
    Capture capture;
    NotifySender sender(2, 64, NotifySender::Overflow::kDrop, true,
                        CaptureSend(&capture), nullptr);
    PipelineWriter writer(config, store, metrics, &sender);
    std::string error;
    ASSERT_TRUE(writer.Prepare(&error)) << error;
    ASSERT_TRUE(writer.Start(&error)) << error;
    sender.Drain();

    const auto stats = writer.GetStats();
    EXPECT_GT(stats.warmup_ops, 0u);
    EXPECT_GT(stats.measured_ops, 0u);
    EXPECT_GT(capture.warmup.load(), 0);
    EXPECT_GT(capture.measured.load(), 0);

    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *put = FindSnapshot(snapshots, "put");
    ASSERT_NE(put, nullptr);
    EXPECT_GT(put->total, 0u);
    EXPECT_EQ(put->success, put->total);
    metrics.Stop();
}

TEST(PipelineWriterTest, KeyWrapDeletesBeforePut) {
    FakeStore store;
    store.fail_existing_put = true;
    Config config = WriterConfig("wrap");
    config.key_pool_size = 1;  // num_threads * batch
    config.duration = 0.01;
    MetricsCollector metrics(0, 3000, config.output_dir);
    metrics.Start();
    Capture capture;
    NotifySender sender(1, 16, NotifySender::Overflow::kDrop, true,
                        CaptureSend(&capture), nullptr);
    PipelineWriter writer(config, store, metrics, &sender);
    std::string error;
    ASSERT_TRUE(writer.Prepare(&error)) << error;
    ASSERT_TRUE(writer.Start(&error)) << error;
    sender.Drain();

    const auto stats = writer.GetStats();
    EXPECT_EQ(stats.measured_failures, 0u);
    EXPECT_GT(stats.maintenance_ops, 0u);
    EXPECT_GT(store.put_count, 1u);
    EXPECT_EQ(store.data.size(), 1u);
    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *put = FindSnapshot(snapshots, "put");
    ASSERT_NE(put, nullptr);
    EXPECT_EQ(put->failure, 0u);
    EXPECT_GT(put->success, 1u);
    metrics.Stop();
}

TEST(PipelineReaderTest, GetBufferBatchAndMisses) {
    FakeStore store;
    Config config = ReaderConfig("reader", {"get_buffer"});
    MetricsCollector metrics(0, 3000, config.output_dir);
    metrics.Start();
    const std::vector<std::string> keys = {"k0", "k1"};
    PrefillPattern(store, keys, 7);
    PipelineReader reader(config, store, metrics);
    std::string error;
    ASSERT_TRUE(reader.Prepare(&error)) << error;

    NotifyBatch batch;
    batch.keys = keys;
    batch.sender = 7;
    batch.size = 64;
    reader.Execute(batch);

    NotifyBatch missing;
    missing.keys = {"missing"};
    missing.sender = 7;
    missing.size = 64;
    reader.Execute(missing);

    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *get = FindSnapshot(snapshots, "get_buffer");
    ASSERT_NE(get, nullptr);
    EXPECT_EQ(get->total, 3u);
    EXPECT_EQ(get->success, 2u);
    EXPECT_EQ(get->failure, 1u);
    EXPECT_EQ(get->miss, 1u);
    EXPECT_EQ(reader.GetStats().batches, 2u);
    metrics.Stop();
}

TEST(PipelineReaderTest, WarmupBatchesAreExcluded) {
    FakeStore store;
    Config config = ReaderConfig("reader_warmup", {"get_buffer"});
    MetricsCollector metrics(0, 3000, config.output_dir);
    metrics.Start();
    const std::vector<std::string> keys = {"k0"};
    PrefillPattern(store, keys, 8);
    PipelineReader reader(config, store, metrics);
    std::string error;
    ASSERT_TRUE(reader.Prepare(&error)) << error;

    NotifyBatch batch;
    batch.keys = keys;
    batch.sender = 8;
    batch.size = 64;
    batch.warmup = true;
    reader.Execute(batch);

    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *get = FindSnapshot(snapshots, "get_buffer");
    if (get != nullptr) {
        EXPECT_EQ(get->total, 0u);
    }
    EXPECT_EQ(reader.GetStats().batches, 1u);
    EXPECT_EQ(reader.GetStats().warmup_batches, 1u);
    metrics.Stop();
}

TEST(PipelineReaderTest, ExistPrecheckSkipsReadAndIsUntimed) {
    FakeStore store;
    Config config = ReaderConfig("reader_precheck", {"exist", "get_buffer"});
    MetricsCollector metrics(0, 3000, config.output_dir);
    metrics.Start();
    PipelineReader reader(config, store, metrics);
    std::string error;
    ASSERT_TRUE(reader.Prepare(&error)) << error;

    NotifyBatch batch;
    batch.keys = {"missing"};
    batch.sender = 9;
    batch.size = 64;
    reader.Execute(batch);

    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *get = FindSnapshot(snapshots, "get_buffer");
    ASSERT_NE(get, nullptr);
    EXPECT_EQ(get->total, 1u);
    EXPECT_EQ(get->miss, 1u);
    EXPECT_TRUE(FindSnapshot(snapshots, "exist") == nullptr);
    EXPECT_EQ(reader.GetStats().precheck_misses, 1u);
    metrics.Stop();
}

TEST(PipelineReaderTest, GetIntoUsesRegisteredBuffer) {
    FakeStore store;
    Config config = ReaderConfig("reader_into", {"get_into"});
    MetricsCollector metrics(0, 3000, config.output_dir);
    metrics.Start();
    const std::vector<std::string> keys = {"k0", "k1"};
    PrefillPattern(store, keys, 11);
    PipelineReader reader(config, store, metrics);
    std::string error;
    ASSERT_TRUE(reader.Prepare(&error)) << error;

    NotifyBatch batch;
    batch.keys = keys;
    batch.sender = 11;
    batch.size = 64;
    reader.Execute(batch);

    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *get = FindSnapshot(snapshots, "get_into");
    ASSERT_NE(get, nullptr);
    EXPECT_EQ(get->total, 2u);
    EXPECT_EQ(get->success, 2u);
    metrics.Stop();
}

TEST(PipelineDispatcherTest, ExecutesQueuedBatchAndRecordsQueueWait) {
    FakeStore store;
    Config config = ReaderConfig("dispatcher", {"get_buffer"});
    config.num_threads = 1;
    config.num_total_threads = 3;
    config.notify_queue_max = 16;
    MetricsCollector metrics(0, 3000, config.output_dir);
    metrics.Start();
    const std::vector<std::string> keys = {"k0", "k1"};
    PrefillPattern(store, keys, 12);
    PipelineReader reader(config, store, metrics);
    std::string error;
    ASSERT_TRUE(reader.Prepare(&error)) << error;
    PipelineDispatcher dispatcher(config, reader, metrics);

    NotifyBatch batch;
    batch.keys = keys;
    batch.sender = 12;
    batch.size = 64;
    ASSERT_TRUE(dispatcher.Dispatch(batch));

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snapshots = metrics.SnapshotCounts();
        const MetricSnapshot *get = FindSnapshot(snapshots, "get_buffer");
        if (get != nullptr && get->total >= 2) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto snapshots = metrics.SnapshotCounts();
    const MetricSnapshot *get = FindSnapshot(snapshots, "get_buffer");
    ASSERT_NE(get, nullptr);
    EXPECT_EQ(get->total, 2u);
    EXPECT_EQ(get->success, 2u);
    const MetricSnapshot *wait = FindSnapshot(snapshots, "notify_queue_wait");
    ASSERT_NE(wait, nullptr);
    EXPECT_GE(wait->total, 1u);
    dispatcher.Stop();
    metrics.Stop();
}




TEST(PipelineWriterTest, ReadBackPipelineKeepsWorkingAfterWrites) {
    FakeStore store;
    const std::string dir = MetricsDir("writer_readback");
    Config config = WriterConfig("writer_readback");
    config.pipeline = {"put", "get_buffer"};
    config.notify_count = 0;
    config.peers.clear();
    config.duration = 0.05;

    MetricsCollector metrics(0, 60000, dir);
    metrics.Start();
    PipelineWriter writer(config, store, metrics, nullptr);
    std::string error;
    ASSERT_TRUE(writer.Prepare(&error)) << error;
    ASSERT_TRUE(writer.Start(&error)) << error;

    const auto snaps = metrics.SnapshotCounts();
    const MetricSnapshot *put = FindSnapshot(snaps, "put");
    const MetricSnapshot *get = FindSnapshot(snaps, "get_buffer");
    ASSERT_NE(put, nullptr);
    ASSERT_NE(get, nullptr);
    EXPECT_GT(put->success, 0u);
    EXPECT_GT(get->success, 0u);
    EXPECT_EQ(get->mismatch, 0u);
    EXPECT_GT(store.data.size(), 0u);
    metrics.Stop();
}

}  // namespace mooncake::kvtest
