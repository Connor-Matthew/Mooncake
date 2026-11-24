#include <gtest/gtest.h>

#include "mooncake_logging.h"

// CTest launches this executable with MC_HIFREQ_LOG_SAMPLE_RATE=0 before
// the process-wide sample rate is initialized.
TEST(LoggingSamplingZero, DoesNotEvaluateStreamArguments) {
    ASSERT_EQ(mooncake::logging::HiFreqLogSampleRate(), 0.0);
    int evaluated = 0;
    MC_LOG_SAMPLED(INFO) << ++evaluated;
    MC_VLOG(0) << ++evaluated;
    EXPECT_EQ(evaluated, 0);
}

TEST(LoggingSamplingZero, RejectsAllRequestIds) {
    EXPECT_FALSE(mooncake::logging::ShouldSampleHiFreqLog(0));
    EXPECT_FALSE(mooncake::logging::ShouldSampleHiFreqLog(1));
    EXPECT_FALSE(mooncake::logging::ShouldSampleHiFreqLog(UINT64_MAX));
    // Verify that the streaming macro is safe inside an unbraced if/else.
    bool reached_else = false;
    if (false)
        MC_LOG_SAMPLED(INFO) << "unreachable";
    else
        reached_else = true;
    EXPECT_TRUE(reached_else);
}
