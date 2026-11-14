#include "../benchmarks/stress_bench_validation.h"
#include "../benchmarks/latency_histogram.h"
#include "nof_gpu_direct_report.h"
#include "gpu_direct_read.h"
#include "../../mooncake-transfer-engine/include/ub_gpu_direct_stats.h"

#include <array>
#include <iostream>
#include <limits>
#include <stdexcept>

namespace {

void Require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void TestMergedLatencyHistogram() {
    mooncake::bench::LatencyHistogram all;
    Require(all.Count() == 0 && all.PercentileNs(0.99) == 0,
            "empty histogram must have no latency");
    mooncake::bench::LatencyHistogram slow_window;
    // An entire slow window is less than 1% of all requests. Taking the
    // maximum window P99 would incorrectly report 1 second globally.
    for (int i = 0; i < 10000; ++i) all.Record(1000);
    for (int i = 0; i < 10; ++i) slow_window.Record(1000000000);
    all.Merge(slow_window);
    Require(all.Count() == 10010 && all.PercentileNs(0.99) < 1020,
            "global P99 must be weighted by requests, not windows");
    Require(all.PercentileNs(0.9999) == 1000000000,
            "tail latency samples must survive histogram merging");
    mooncake::bench::LatencyHistogram final_partial_window;
    final_partial_window.Record(2000000000);
    all.Merge(final_partial_window);
    Require(all.Count() == 10011 && all.MaxNs() == 2000000000,
            "the final partial window must contribute to count and maximum");
    Require(all.MinNs() == 1000 && all.MeanNs() > 1000000,
            "minimum and mean must include every window");

    for (unsigned exponent = 0; exponent < 64; ++exponent) {
        const uint64_t boundary = uint64_t{1} << exponent;
        for (uint64_t value : {boundary - 1, boundary, boundary + 1}) {
            mooncake::bench::LatencyHistogram sample;
            sample.Record(value);
            sample.Record(std::numeric_limits<uint64_t>::max());
            const uint64_t upper = sample.PercentileNs(0.5);
            Require(upper >= value &&
                        static_cast<long double>(upper - value) <=
                            static_cast<long double>(value) / 64,
                    "bucket boundaries must preserve the error bound");
        }
    }
}

void TestGpuPaths() {
    mooncake::bench::GpuPathOptions path;
    path.mode = "gdr-peermem";
    path.protocol = "tcp";
    path.cuda_build = true;
    path.ub_build = true;
    path.nof_gpu_build = true;
    path.nof_gpu_available = true;
    path.require_direct = true;
    path.nof_replicas = true;
    Require(mooncake::bench::ValidateGpuPath(path).empty(),
            "pure NoF must allow TCP as the Store transport");
    path.memory_replicas = true;
    Require(!mooncake::bench::ValidateGpuPath(path).empty(),
            "TCP memory staging must not pass UB GDR validation");
    path.protocol = "ub";
    Require(mooncake::bench::ValidateGpuPath(path).empty(),
            "mixed UB memory and URMA NoF direct paths must be accepted");
    path.nof_gpu_build = false;
    Require(!mooncake::bench::ValidateGpuPath(path).empty(),
            "NoF GPU I/O must fail before submission without its build option");
    path.nof_gpu_build = true;
    path.mode = "staging";
    Require(!mooncake::bench::ValidateGpuPath(path).empty(),
            "memory staging cannot satisfy require_gpu_direct");
    path.memory_replicas = false;
    Require(mooncake::bench::ValidateGpuPath(path).empty(),
            "UB staging choice must not change the independent SPDK GPU path");
    path.mode = "host";
    Require(!mooncake::bench::ValidateGpuPath(path).empty(),
            "host buffers cannot satisfy require_gpu_direct");
    path.require_direct = false;
    path.cuda_build = false;
    path.ub_build = false;
    Require(mooncake::bench::ValidateGpuPath(path).empty(),
            "CPU-only builds must keep host benchmarks available");
}

void TestDirectReadSelection() {
    Require(mooncake::CanReadUbGpuDirect(true, "ub", "gdr-peermem", true,
                                       4096, 256, 1024),
            "registered GPU subranges must bypass the host read buffer");
    Require(!mooncake::CanReadUbGpuDirect(true, "ub", "gdr-peermem", false,
                                        4096, 256, 1024),
            "an unregistered GPU pointer must retain the staging path");
    Require(!mooncake::CanReadUbGpuDirect(true, "ub", "gdr-peermem", true,
                                        4096, 4097, 1),
            "offset overflow must not grant direct access");
    Require(!mooncake::CanReadUbGpuDirect(true, "ub", "gdr-peermem", true,
                                        4096, 4000, 1024),
            "direct reads must fit entirely in the registered range");
    Require(!mooncake::CanReadUbGpuDirect(true, "tcp", "gdr-peermem", true,
                                        4096, 0, 1024),
            "TCP reads must keep staging even when the UB mode is direct");
    Require(!mooncake::CanReadUbGpuDirect(true, "ub", "staging", true,
                                        4096, 0, 1024),
            "explicit staging must retain its host buffer");
    Require(!mooncake::CanReadUbGpuDirect(false, "ub", "gdr-peermem", true,
                                        4096, 0, 1024),
            "NoF/disk replicas must use their own read implementations");
}

void TestPlacement() {
    mooncake::bench::PlacementRegion region{1024, 4096, "endpoint-A", "ub"};
    Require(region.Contains(1024, 4096, "endpoint-A"), "full range is valid");
    Require(!region.Contains(1024, 4096, "endpoint-B"),
            "same address on a fallback endpoint is not the requested segment");
    Require(!region.Contains(1023, 8, "endpoint-A"), "lower bound must be checked");
    Require(!region.Contains(5120, 1, "endpoint-A"), "end address is exclusive");
    Require(!region.Contains(1024, std::numeric_limits<uint64_t>::max(), "endpoint-A"),
            "overflowing lengths must be rejected");
    region.base = std::numeric_limits<uint64_t>::max() - 4096;
    Require(region.Contains(region.base + 2048, 2048, "endpoint-A"),
            "range containment must avoid overflowing base+length arithmetic");
}

void TestPayloadAndExitStatus() {
    std::array<unsigned char, 19> data{};
    mooncake::bench::FillPattern(data.data(), data.size(), 7);
    Require(mooncake::bench::CheckPattern(data.data(), data.size(), data.size(), 7),
            "unaligned value sizes must round-trip");
    Require(!mooncake::bench::CheckPattern(data.data(), 8, data.size(), 7),
            "a successful short read must fail verification");
    Require(!mooncake::bench::CheckPattern(data.data(), data.size(), data.size(), 8),
            "the wrong segment key seed must fail verification");
    data.back() = 1;
    Require(!mooncake::bench::CheckPattern(data.data(), data.size(), data.size(), 7),
            "corruption in trailing bytes must be detected");
    Require(mooncake::bench::ExitCode(0) == 0, "successful operations must exit zero");
    Require(mooncake::bench::ExitCode(1) != 0, "read/remove failures must exit nonzero");
    Require(mooncake::bench::ExitCode(0, -1) != 0,
            "verification failure must propagate to the process exit status");
}

void TestMeasuredEvidence() {
    Require(mooncake::bench::KeySeed("nof_seg_endpoint_key_17") == 17,
            "segment keys must preserve the payload seed after shuffling");
    Require(mooncake::bench::KeySeed("bench_key_3") == 3,
            "ordinary keys must resolve their seed");
    Require(!mooncake::bench::KeySeed("bench_key_3garbage"),
            "malformed keys must never pass payload verification");
    Require(!mooncake::bench::NofGpuLayoutSupported(4096, 131072, 33554432, false),
            "4 KiB NoF GPU requests need full allocation registration");
    Require(mooncake::bench::NofGpuLayoutSupported(4096, 131072, 33554432, true),
            "a GPU page registered as a pool can serve subpage I/O");
    Require(!mooncake::bench::NofGpuLayoutSupported(65536, 4096, 33554432, false),
            "a subpage SPDK chunk must not enter peer-memory registration");
    mooncake::NofGpuDirectStats warmup;
    warmup.available = true;
    warmup.reads = {32, 32, 0, 4194304, 4194304};
    warmup.peer_memory_registrations = 1;
    auto delta = mooncake::NofGpuDirectDelta(warmup, warmup);
    Require(!mooncake::NofGpuDirectPassed(delta, true, false),
            "warmup/verification I/O cannot satisfy a measured phase");
    auto after = warmup;
    after.reads = {64, 64, 0, 8388608, 8388608};
    delta = mooncake::NofGpuDirectDelta(warmup, after);
    Require(mooncake::NofGpuDirectPassed(delta, true, false) &&
                delta.reads.success_bytes == 4194304,
            "cached registration must remain valid for measured GPU byte evidence");
    delta.host_io = 1;
    Require(!mooncake::NofGpuDirectPassed(delta, true, false),
            "a measured host I/O must fail strict GPU acceptance");
    mooncake::UbGpuDirectCounters counters;
    counters.Submit(false, 4096);
    counters.Complete(false, true, 4096);
    const auto before = counters.Snapshot();
    Require(!mooncake::UbGpuDirectPassed(
                mooncake::UbGpuDirectDelta(before, before), 4096, 0),
            "UB warmup cannot count as measured DMA");
    counters.Submit(false, 4096);
    counters.Complete(false, true, 4096);
    const auto measured = mooncake::UbGpuDirectDelta(before, counters.Snapshot());
    Require(mooncake::UbGpuDirectPassed(measured, 4096, 0),
            "all measured UB bytes must complete directly");
    Require(!mooncake::UbGpuDirectPassed(measured, 8192, 0),
            "partial direct transfer evidence cannot cover all measured bytes");
    counters.Stage(4096);
    Require(!mooncake::UbGpuDirectPassed(
                mooncake::UbGpuDirectDelta(before, counters.Snapshot()), 4096, 0),
            "UB staging must be visible to acceptance");
}

void TestGpuAcceptance() {
    mooncake::NofGpuDirectStats stats;
    stats.available = true;
    stats.writes = {128, 128, 0};
    stats.peer_memory_registrations = 1;
    Require(mooncake::NofGpuDirectPassed(stats, false, true),
            "one cached GPU registration may serve many successful writes");
    Require(!mooncake::NofGpuDirectPassed(stats, true, true),
            "a write-only run must not satisfy a read acceptance requirement");
    stats.writes.success = 127;
    Require(!mooncake::NofGpuDirectPassed(stats, false, true),
            "accepted submissions without all completions must fail");
    stats.writes.success = 128;
    stats.writes.failed = 1;
    Require(!mooncake::NofGpuDirectPassed(stats, false, true),
            "failed completions must be reported");
    stats.writes.failed = 0;
    stats.peer_memory_registrations = 0;
    stats.accelerator_attempts = 128;
    stats.dmabuf_exports = 128;
    Require(!mooncake::NofGpuDirectPassed(stats, false, true),
            "registration attempts and DMA-BUF exports do not prove registration");
    stats.dmabuf_registrations = 1;
    stats.staged_bytes = 4096;
    Require(!mooncake::NofGpuDirectPassed(stats, false, true),
            "host staging must fail direct acceptance");
    stats.staged_bytes = 0;
    stats.registration_failures = 1;
    Require(!mooncake::NofGpuDirectPassed(stats, false, true),
            "registration failures must fail acceptance");
}

}  // namespace

int main() {
    try {
        TestGpuPaths();
        TestDirectReadSelection();
        TestPlacement();
        TestPayloadAndExitStatus();
        TestGpuAcceptance();
        TestMeasuredEvidence();
        TestMergedLatencyHistogram();
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
    std::cout << "stress benchmark validation: PASS\n";
    return 0;
}
