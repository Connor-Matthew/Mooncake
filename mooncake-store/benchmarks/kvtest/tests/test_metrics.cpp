#include <gtest/gtest.h>

#include <json/json.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

#include "metrics.h"

namespace mooncake::kvtest {
namespace {

std::string MetricsDir(const std::string &name) {
    return (std::filesystem::path(::testing::TempDir()) /
            ("kvtest_metrics_" + name))
        .string();
}

uint64_t SumWindowCounts(const std::string &path) {
    std::ifstream input(path);
    std::string line;
    uint64_t total = 0;
    bool header = true;
    while (std::getline(input, line)) {
        if (header) {
            header = false;
            continue;
        }
        if (line.empty()) continue;
        std::istringstream row(line);
        std::string field;
        int column = 0;
        while (std::getline(row, field, ',')) {
            if (column == 2) {
                total += static_cast<uint64_t>(std::stoull(field));
                break;
            }
            ++column;
        }
    }
    return total;
}

}  // namespace

TEST(MetricsWindowTest, WritesPerIntervalRows) {
    const std::string dir = MetricsDir("window");
    std::filesystem::remove_all(dir);
    MetricsCollector metrics(7, 20, dir);
    metrics.Start();
    for (int i = 0; i < 5; ++i) {
        metrics.Record("op", 1.0 + i, RecordStatus::kOk, 100);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    metrics.Stop();

    const std::string path = dir + "/metrics_7.csv";
    ASSERT_TRUE(std::filesystem::exists(path));
    std::ifstream input(path);
    std::string header;
    ASSERT_TRUE(std::getline(input, header));
    EXPECT_NE(header.find("elapsed_ms,operation,count"), std::string::npos);
    EXPECT_EQ(SumWindowCounts(path), 5u);
}

TEST(MetricsWindowTest, ResetStartsAFreshWindow) {
    const std::string dir = MetricsDir("reset");
    std::filesystem::remove_all(dir);
    MetricsCollector metrics(8, 60000, dir);
    metrics.Start();
    for (int i = 0; i < 3; ++i) {
        metrics.Record("op", 1.0, RecordStatus::kOk, 10);
    }
    metrics.Reset();
    for (int i = 0; i < 2; ++i) {
        metrics.Record("op", 1.0, RecordStatus::kOk, 10);
    }
    metrics.Stop();
    EXPECT_EQ(SumWindowCounts(dir + "/metrics_8.csv"), 2u);
}




TEST(MetricsSamplesTest, DecimationCapsRetainedSamples) {
    const std::string dir = MetricsDir("decimation");
    std::filesystem::remove_all(dir);
    MetricsCollector metrics(9, 60000, dir, 10);
    metrics.Start();
    for (int i = 0; i < 200; ++i) {
        metrics.Record("op", 1.0 + i, RecordStatus::kOk, 10);
    }
    metrics.Stop();
    const auto snaps = metrics.SnapshotCounts();
    ASSERT_EQ(snaps.size(), 1u);
    EXPECT_EQ(snaps[0].total, 200u);
    EXPECT_EQ(snaps[0].success, 200u);

    std::ifstream input(dir + "/latency_samples.csv");
    std::string line;
    int lines = 0;
    while (std::getline(input, line)) ++lines;
    EXPECT_LE(lines - 1, 10);  // header + at most samples_max rows
    EXPECT_GT(lines - 1, 0);
}

TEST(MetricsSummaryTest, BusyThroughputUsesAllSuccessLatencies) {
    const std::string dir = MetricsDir("busy_throughput");
    std::filesystem::remove_all(dir);
    MetricsCollector metrics(11, 0, dir, 2);
    metrics.Start();
    for (int i = 1; i <= 10; ++i) {
        metrics.Record("op", static_cast<double>(i), RecordStatus::kOk, 100);
    }
    metrics.Stop();

    std::ifstream input(dir + "/summary.json");
    ASSERT_TRUE(input.good());
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    ASSERT_TRUE(Json::parseFromStream(builder, input, &root, &errors))
        << errors;

    const Json::Value &op = root["operations"]["op"];
    ASSERT_FALSE(op.isNull());
    // samples_max=2 only limits retained latency samples; busy_ms must cover
    // every successful request (1+2+...+10 = 55ms).
    EXPECT_DOUBLE_EQ(op["busy_ms"].asDouble(), 55.0);
    EXPECT_EQ(op["success"].asUInt64(), 10u);
    const double expected = 1000.0 / (55.0 / 1000.0) / (1024.0 * 1024.0);
    EXPECT_NEAR(op["throughput_mb_s"].asDouble(), expected, 1e-9);
    EXPECT_NEAR(op["throughput_gb_s"].asDouble(), expected / 1024.0, 1e-12);
}

TEST(MetricsWindowTest, DisabledWindowDoesNotCreateCsv) {
    const std::string dir = MetricsDir("window_disabled");
    std::filesystem::remove_all(dir);
    MetricsCollector metrics(12, 0, dir);
    metrics.Start();
    metrics.Record("op", 1.0, RecordStatus::kOk, 10);
    metrics.Stop();
    EXPECT_FALSE(std::filesystem::exists(dir + "/metrics_12.csv"));
    EXPECT_TRUE(std::filesystem::exists(dir + "/summary.json"));
}

TEST(MetricsMeasurementTest, ResetPausedStopsRecordingUntilArmed) {
    const std::string dir = MetricsDir("measurement_control");
    std::filesystem::remove_all(dir);
    MetricsCollector metrics(13, 0, dir);
    metrics.Start();
    metrics.Record("op", 1.0, RecordStatus::kOk, 10);
    metrics.Freeze();
    metrics.ResetPaused();
    metrics.Record("op", 2.0, RecordStatus::kOk, 10);
    auto snaps = metrics.SnapshotCounts();
    ASSERT_EQ(snaps.size(), 1u);
    EXPECT_EQ(snaps[0].total, 0u);

    metrics.BeginMeasurement();
    metrics.Record("op", 3.0, RecordStatus::kOk, 10);
    snaps = metrics.SnapshotCounts();
    EXPECT_EQ(snaps[0].total, 1u);
    EXPECT_EQ(snaps[0].success, 1u);
    metrics.Stop();
}

TEST(MetricsSummaryTest, PercentileUsesNearestRankRule) {
    const std::string dir = MetricsDir("nearest_rank");
    std::filesystem::remove_all(dir);
    MetricsCollector metrics(14, 0, dir);
    metrics.Start();
    for (int i = 1; i <= 10; ++i) {
        metrics.Record("op", static_cast<double>(i), RecordStatus::kOk, 1);
    }
    metrics.Stop();

    std::ifstream input(dir + "/summary.json");
    ASSERT_TRUE(input.good());
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    ASSERT_TRUE(Json::parseFromStream(builder, input, &root, &errors))
        << errors;
    const Json::Value &op = root["operations"]["op"];
    // round(0.9 * (10-1)) = 8 -> sorted value 9; linear interpolation would
    // have produced 9.1.
    EXPECT_DOUBLE_EQ(op["p90_ms"].asDouble(), 9.0);
    EXPECT_DOUBLE_EQ(op["max_ms"].asDouble(), 10.0);
}
}  // namespace mooncake::kvtest
