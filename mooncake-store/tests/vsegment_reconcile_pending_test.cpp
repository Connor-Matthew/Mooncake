// aligned 模式下崩溃恢复的关键行为：重放得到的 pending reservation
// 必须能通过 ReconcileObjectReferences 的入参恢复，且不会被 reconcile 覆盖。
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "vsegment/vsegment_ha.h"
#include "vsegment/vsegment_manager.h"
#include "vsegment/vsegment_oplog_mode.h"
#include "vsegment/vsegment_runtime.h"

namespace mooncake::vsegment {
namespace {

class SyncCommitter : public VSegmentStateCommitter {
   public:
    ErrorCode Commit(const PartitionVSegmentSnapshot& state,
                     const std::string& mutation,
                     std::string* /*detail*/) override {
        last_state = state;
        mutations.push_back(mutation);
        return ErrorCode::OK;
    }
    std::vector<std::string> mutations;
    PartitionVSegmentSnapshot last_state;

    // aligned mode (default): O(1) reservation records instead of full state.
    ErrorCode CommitReservation(const std::string& /*partition_id*/,
                                const std::string& /*operation_id*/,
                                const std::string& /*vsegment_id*/,
                                const LogicalRange& /*range*/,
                                uint64_t /*route_epoch*/) override {
        return ErrorCode::OK;
    }

    ErrorCode CommitRelease(const std::string& /*partition_id*/,
                            const std::string& /*operation_id*/,
                            uint64_t /*route_epoch*/) override {
        return ErrorCode::OK;
    }
};

std::shared_ptr<SyncCommitter> Committer() {
    return std::make_shared<SyncCommitter>();
}

VSegmentProfile Profile() {
    return {.name = "default",
            .member_count = 2,
            .stripe_size = 64,
            .member_extent_size = 256,
            .io_alignment = 8,
            .required_medium = "DRAM"};
}

PartitionVSegmentConfig Config() {
    return {.partition_id = "partition-1",
            .profile_name = "default",
            .initial_vsegment_count = 1,
            .config_generation = 1,
            .quotas = {{"segment-a", 0, 512}, {"segment-b", 1024, 512}}};
}

TEST(VSegmentReconcilePendingTest, PendingReservationIsRestoredByReconcile) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    const std::string vsid = allocation.view.vsegment_id;

    // 崩溃前状态：一个已提交对象 + 一个在飞 reservation
    ASSERT_TRUE(manager.ReservePut(vsid, "committed-op", 64));
    LogicalRange committed_range;
    ASSERT_EQ(manager.CommitPut(vsid, "committed-op", "obj-1", &committed_range),
              ErrorCode::OK);
    auto pending = manager.ReservePut(vsid, "pending-op", 32);
    ASSERT_TRUE(pending) << pending.detail;

    // 重启后：对象元数据只能重建 committed 部分
    VSegmentDescriptor committed_desc{"partition-1", vsid,
                                      committed_range.offset,
                                      committed_range.length, "committed-op"};

    // ① 不传 pending：reservation 丢失（这就是必须补入参的原因）
    ASSERT_EQ(manager.ReconcileObjectReferences(
                  {{committed_desc, "obj-1", true}}),
              ErrorCode::OK);
    EXPECT_EQ(manager.Stats().committed_allocations, 1u);
    EXPECT_EQ(manager.Stats().reservations, 0u);

    // ② 传入重放得到的 pending：必须被恢复
    PendingReservation pending_record{.vsegment_id = vsid,
                                      .operation_id = "pending-op",
                                      .range = pending.range};
    ASSERT_EQ(manager.ReconcileObjectReferences(
                  {{committed_desc, "obj-1", true}}, nullptr,
                  {pending_record}),
              ErrorCode::OK);
    EXPECT_EQ(manager.Stats().committed_allocations, 1u);
    EXPECT_EQ(manager.Stats().reservations, 1u);

    // ③ 恢复出来的 reservation 必须真的占住那段区间、且 operation 映射正确
    const auto snapshot = manager.Snapshot();
    ASSERT_EQ(snapshot.vsegments.size(), 1u);
    ASSERT_EQ(snapshot.vsegments[0].logical_allocation.reservations.size(), 1u);
    EXPECT_EQ(snapshot.vsegments[0].logical_allocation.reservations[0].range,
              pending.range);
    EXPECT_EQ(snapshot.operation_vsegments.count("pending-op"), 1u);

    // ④ 该 reservation 仍然可以正常提交（说明区间确实被占住而不是空闲）
    LogicalRange reused;
    EXPECT_EQ(manager.CommitPut(vsid, "pending-op", "obj-2", &reused),
              ErrorCode::OK);
    EXPECT_EQ(reused, pending.range);
}

TEST(VSegmentReconcilePendingTest,
     PendingReservationOverlappingCommittedIsDropped) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    const std::string vsid = allocation.view.vsegment_id;

    ASSERT_TRUE(manager.ReservePut(vsid, "committed-op", 64));
    LogicalRange committed_range;
    ASSERT_EQ(manager.CommitPut(vsid, "committed-op", "obj-1", &committed_range),
              ErrorCode::OK);

    VSegmentDescriptor committed_desc{"partition-1", vsid,
                                      committed_range.offset,
                                      committed_range.length, "committed-op"};

    // 与已提交区间重叠的 pending 必须被丢弃，而不是造成重叠分配
    PendingReservation conflicting{
        .vsegment_id = vsid,
        .operation_id = "stale-pending-op",
        .range = {committed_range.offset, committed_range.length}};
    ASSERT_EQ(manager.ReconcileObjectReferences(
                  {{committed_desc, "obj-1", true}}, nullptr, {conflicting}),
              ErrorCode::OK);

    EXPECT_EQ(manager.Stats().committed_allocations, 1u);
    EXPECT_EQ(manager.Stats().reservations, 0u);
    const auto snapshot = manager.Snapshot();
    EXPECT_EQ(snapshot.operation_vsegments.count("stale-pending-op"), 0u);
}

TEST(VSegmentReconcilePendingTest, PendingForUnknownVSegmentIsIgnored) {
    VSegmentManager manager(Config(), {Profile()}, Committer());
    auto allocation = manager.Create("default");
    ASSERT_TRUE(allocation);
    const std::string vsid = allocation.view.vsegment_id;

    PendingReservation orphan{.vsegment_id = "vsegment-does-not-exist",
                               .operation_id = "orphan-op",
                               .range = {0, 64}};
    ASSERT_EQ(manager.ReconcileObjectReferences(
                  {}, nullptr, {orphan}),
              ErrorCode::OK);
    EXPECT_EQ(manager.Stats().reservations, 0u);
}

}  // namespace
}  // namespace mooncake::vsegment
