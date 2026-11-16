#include <gtest/gtest.h>

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

}  // namespace mooncake::kvtest
