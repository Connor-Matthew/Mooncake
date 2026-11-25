#include <gtest/gtest.h>

#include "store_diagnostics.h"

namespace mooncake {
namespace {
using Clock = ScopedReadTransferTrace::Clock;
Clock::time_point At(int us) {
    return Clock::time_point(std::chrono::microseconds(us));
}

TEST(ReadTransferTrace, SplitsReadIntoBeforeTransferAndAfter) {
    ScopedReadTransferTrace trace(8, At(100));
    ScopedReadTransferTrace::record(At(120), At(220));
    ASSERT_TRUE(trace.valid(At(250)));
    EXPECT_DOUBLE_EQ(trace.beforeUs(At(250)), 20.0);
    EXPECT_DOUBLE_EQ(trace.transferUs(At(250)), 100.0);
    EXPECT_DOUBLE_EQ(trace.afterUs(At(250)), 30.0);
    EXPECT_DOUBLE_EQ(trace.beforeUs(At(250)) + trace.transferUs(At(250)) +
                         trace.afterUs(At(250)),
                     150.0);
}

TEST(ReadTransferTrace, RejectsMultipleOrUnobservedTransfers) {
    ScopedReadTransferTrace trace(8, At(100));
    EXPECT_FALSE(trace.valid(At(250)));
    EXPECT_DOUBLE_EQ(trace.afterUs(At(250)), -1.0);
    ScopedReadTransferTrace::record(At(120), At(220));
    EXPECT_FALSE(trace.valid(At(210)));
    ScopedReadTransferTrace::record(At(230), At(240));
    EXPECT_FALSE(trace.valid(At(250)));
    EXPECT_EQ(trace.transfers(), 2U);
}

TEST(ReadTransferTrace, NestedDisabledScopeDoesNotPolluteOuterRequest) {
    ScopedReadTransferTrace outer(8, At(100));
    {
        ScopedReadTransferTrace disabled(0, At(110));
        ScopedReadTransferTrace::record(At(120), At(130));
    }
    ScopedReadTransferTrace::record(At(140), At(150));
    EXPECT_TRUE(outer.valid(At(160)));
    EXPECT_EQ(outer.transfers(), 1U);
}
}  // namespace
}  // namespace mooncake
