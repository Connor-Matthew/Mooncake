#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fake_store.h"
#include "operations.h"

namespace mooncake::kvtest {
namespace {

const MetricSnapshot *FindSnapshot(const std::vector<MetricSnapshot> &snaps,
                                   const std::string &name) {
    for (const auto &snapshot : snaps) {
        if (snapshot.name == name) return &snapshot;
    }
    return nullptr;
}

struct Harness {
    FakeStore store;
    MetricsCollector metrics;
    OpScratch scratch;
    ValueSource value;
    VerifyOptions verify;
    std::vector<std::string> keys;
    OpContext context;
    ExecuteOptions options;

    Harness(uint64_t value_size, std::vector<std::string> key_names)
        : metrics(0, 3000,
                  (std::filesystem::path(::testing::TempDir()) /
                   "kvtest_unit_metrics")
                      .string()),
          keys(std::move(key_names)) {
        metrics.Start();
        std::string error;
        EXPECT_TRUE(value.Load(7, "", {value_size}, &error)) << error;
        verify.level = "full";
        context.keys = &keys;
        context.value_size = value_size;
        context.value = &value;
        context.scratch = &scratch;
        context.verify = &verify;
        options.metrics = &metrics;
        options.fail_on_verify = true;
    }
    ~Harness() { metrics.Stop(); }

    OpOutcome Run(const std::string &name) {
        const OpSpec *spec = FindOp(name);
        if (spec == nullptr) {
            OpOutcome outcome;
            outcome.ok = false;
            return outcome;
        }
        return ExecuteOp(store, *spec, context, options);
    }
};

}  // namespace

TEST(OperationsTest, PutThenGetRoundTrip) {
    Harness harness(64, {"k0", "k1"});
    const OpOutcome put = harness.Run("put");
    EXPECT_TRUE(put.ok);
    EXPECT_EQ(put.bytes, 128u);
    EXPECT_EQ(harness.store.data.size(), 2u);

    const OpOutcome get = harness.Run("get");
    EXPECT_TRUE(get.ok);
    EXPECT_EQ(get.bytes, 128u);
}

TEST(OperationsTest, MissingKeyIsReportedAsMiss) {
    Harness harness(64, {"missing"});
    const OpOutcome get = harness.Run("get");
    EXPECT_FALSE(get.ok);
    EXPECT_EQ(get.status, RecordStatus::kMiss);

    const auto snapshots = harness.metrics.SnapshotCounts();
    const MetricSnapshot *op = FindSnapshot(snapshots, "get");
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(op->total, 1u);
    EXPECT_EQ(op->failure, 1u);
    EXPECT_EQ(op->miss, 1u);
}

TEST(OperationsTest, CorruptedValueIsMismatch) {
    Harness harness(64, {"k0"});
    ASSERT_TRUE(harness.Run("put").ok);
    harness.store.data["k0"][10] ^= 0xff;

    const OpOutcome get = harness.Run("get");
    EXPECT_FALSE(get.ok);
    EXPECT_EQ(get.status, RecordStatus::kMismatch);
    EXPECT_EQ(harness.metrics.VerifyFailures(), 1u);
}

TEST(OperationsTest, PutFromRequiresRegisteredBuffer) {
    Harness harness(64, {"k0"});
    const OpOutcome out = harness.Run("put_from");
    EXPECT_FALSE(out.ok);
    EXPECT_EQ(out.status, RecordStatus::kError);
}

TEST(OperationsTest, RegisteredBufferPutAndGetInto) {
    Harness harness(64, {"k0", "k1"});
    RegisteredBuffer write_buffer;
    ASSERT_TRUE(write_buffer.Create(harness.store, 64, 2));
    harness.context.write_buffer = &write_buffer;
    const OpOutcome put = harness.Run("put_from");
    EXPECT_TRUE(put.ok);

    RegisteredBuffer read_buffer;
    ASSERT_TRUE(read_buffer.Create(harness.store, 64, 2));
    harness.context.read_buffer = &read_buffer;
    const OpOutcome get_into = harness.Run("get_into");
    EXPECT_TRUE(get_into.ok);
}

TEST(OperationsTest, BatchOperationsRecordOneSample) {
    Harness harness(32, {"a", "b", "c"});
    const OpOutcome put = harness.Run("put_batch");
    EXPECT_TRUE(put.ok);
    EXPECT_EQ(put.bytes, 96u);

    auto snapshots = harness.metrics.SnapshotCounts();
    const MetricSnapshot *put_op = FindSnapshot(snapshots, "put_batch");
    ASSERT_NE(put_op, nullptr);
    EXPECT_EQ(put_op->total, 1u);
    EXPECT_EQ(put_op->success, 1u);

    const OpOutcome get = harness.Run("get_batch");
    EXPECT_TRUE(get.ok);
    EXPECT_EQ(get.bytes, 96u);
    snapshots = harness.metrics.SnapshotCounts();
    const MetricSnapshot *get_op = FindSnapshot(snapshots, "get_batch");
    ASSERT_NE(get_op, nullptr);
    EXPECT_EQ(get_op->total, 1u);
}

TEST(OperationsTest, ExistAndRemove) {
    Harness harness(16, {"k"});
    EXPECT_EQ(harness.Run("exist").status, RecordStatus::kMiss);
    ASSERT_TRUE(harness.Run("put").ok);
    EXPECT_TRUE(harness.Run("exist").ok);
    EXPECT_TRUE(harness.Run("remove").ok);
    EXPECT_EQ(harness.Run("exist").status, RecordStatus::kMiss);
}

TEST(OperationsTest, WarmupSamplesAreExcluded) {
    Harness harness(16, {"k"});
    harness.options.warmup = true;
    ASSERT_TRUE(harness.Run("put").ok);
    harness.options.warmup = false;
    ASSERT_TRUE(harness.Run("put").ok);

    const auto snapshots = harness.metrics.SnapshotCounts();
    const MetricSnapshot *op = FindSnapshot(snapshots, "put");
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(op->total, 1u);
    EXPECT_EQ(op->success, 1u);
}

TEST(OperationsTest, PutErrorIsClassified) {
    Harness harness(16, {"k"});
    harness.store.put_failures = 1;
    const OpOutcome put = harness.Run("put");
    EXPECT_FALSE(put.ok);
    EXPECT_EQ(put.status, RecordStatus::kError);
    EXPECT_EQ(put.code, -200);
}

TEST(OperationsTest, AliasesMapToCanonicalNames) {
    EXPECT_EQ(CanonicalName("setStringView"), "upsert");
    EXPECT_EQ(CanonicalName("upsertFrom"), "upsert_from");
    EXPECT_EQ(CanonicalName("mSet"), "batch_upsert_from");
    EXPECT_EQ(CanonicalName("getBuffer"), "get_buffer");
    EXPECT_EQ(CanonicalName("mGet"), "get_batch");
    EXPECT_EQ(CanonicalName("isExist"), "exist");
    EXPECT_EQ(CanonicalName("batchRemove"), "batch_remove");
    EXPECT_TRUE(FindOp("no_such_op") == nullptr);
}

TEST(OperationsTest, ValidateOpListEnforcesSideAndBuffers) {
    std::string error;
    const std::vector<std::string> reader{"exist", "get_buffer"};
    EXPECT_TRUE(ValidateOpList(reader, OpSide::kRead, false, &error)) << error;

    const std::vector<std::string> write_in_reader{"upsert_from"};
    EXPECT_FALSE(ValidateOpList(write_in_reader, OpSide::kRead, true, &error));

    const std::vector<std::string> read_in_writer{"get_buffer"};
    EXPECT_FALSE(ValidateOpList(read_in_writer, OpSide::kWrite, false, &error));

    const std::vector<std::string> needs_buffer{"put_from"};
    EXPECT_FALSE(ValidateOpList(needs_buffer, OpSide::kWrite, false, &error));
    EXPECT_TRUE(ValidateOpList(needs_buffer, OpSide::kWrite, true, &error))
        << error;
}

}  // namespace mooncake::kvtest
