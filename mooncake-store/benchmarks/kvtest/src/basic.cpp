#include "basic.h"

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "keys.h"
#include "traffic_gate.h"

namespace mooncake::kvtest {
namespace {

bool SplitOperation(const std::string &operation, std::string *put_name,
                    std::string *get_name, bool *mixed, std::string *error) {
    const auto slash = operation.find('/');
    if (slash == std::string::npos) {
        *mixed = false;
        *put_name = operation;
        get_name->clear();
        return true;
    }
    *mixed = true;
    *put_name = operation.substr(0, slash);
    *get_name = operation.substr(slash + 1);
    if (put_name->empty() || get_name->empty() ||
        get_name->find('/') != std::string::npos) {
        if (error) *error = "invalid mixed operation: " + operation;
        return false;
    }
    return true;
}

int64_t NowUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}  // namespace

bool ValidateBasicOperation(const Config &config, std::string *error) {
    if (config.data_sizes.size() != 1) {
        if (error) {
            *error = "basic mode requires exactly one data_sizes entry";
        }
        return false;
    }
    std::string put_name;
    std::string get_name;
    bool mixed = false;
    if (!SplitOperation(config.operation, &put_name, &get_name, &mixed,
                        error)) {
        return false;
    }
    if (mixed) {
        const OpSpec *put = FindOp(put_name);
        if (put == nullptr || put->side != OpSide::kWrite) {
            if (error) {
                *error = "mixed put side must be a write interface: " +
                         put_name;
            }
            return false;
        }
        const OpSpec *get = FindOp(get_name);
        if (get == nullptr || get->side != OpSide::kRead) {
            if (error) {
                *error = "mixed get side must be a read interface: " +
                         get_name;
            }
            return false;
        }
        return true;
    }
    const OpSpec *spec = FindOp(put_name);
    if (spec == nullptr) {
        if (error) *error = "unsupported basic operation: " + put_name;
        return false;
    }
    return true;
}

BasicRunner::BasicRunner(const Config &config, StoreAdapter &store,
                         MetricsCollector &metrics)
    : config_(config), store_(store), metrics_(metrics) {}

BasicRunner::~BasicRunner() { Stop(); }

bool BasicRunner::Prepare(std::string *error) {
    std::string put_name;
    std::string get_name;
    if (!SplitOperation(config_.operation, &put_name, &get_name, &mixed_,
                        error)) {
        return false;
    }
    if (mixed_) {
        put_spec_ = FindOp(put_name);
        get_spec_ = FindOp(get_name);
        if (put_spec_ == nullptr || put_spec_->side != OpSide::kWrite) {
            *error = "mixed put side must be a write interface: " + put_name;
            return false;
        }
        if (get_spec_ == nullptr || get_spec_->side != OpSide::kRead) {
            *error = "mixed get side must be a read interface: " + get_name;
            return false;
        }
        needs_prefill_ = false;
    } else {
        single_spec_ = FindOp(put_name);
        if (single_spec_ == nullptr) {
            *error = "unsupported basic operation: " + put_name;
            return false;
        }
        needs_prefill_ = single_spec_->side == OpSide::kRead;
    }
    if (config_.data_sizes.empty()) {
        *error = "basic mode requires a data size";
        return false;
    }
    data_size_ = config_.data_sizes.front();
    if (!value_.Load(config_.value_seed, config_.value_file, {data_size_},
                     error)) {
        return false;
    }
    verify_.level = config_.verify_level;
    verify_.sample_bytes = config_.verify_sample_bytes;
    verify_.sample_step_bytes = config_.verify_sample_step_bytes;
    if (!BuildKeys()) {
        *error = "empty key range (key_start/key_end)";
        return false;
    }
    if (!PrepareThreadBuffers(error)) return false;
    return true;
}

bool BasicRunner::BuildKeys() {
    const uint64_t begin = config_.key_start;
    const uint64_t end = std::min<uint64_t>(config_.EffectiveKeyEnd(),
                                            config_.key_count);
    if (end <= begin) return false;
    measured_keys_ = MakeKeyRange(config_.key_prefix, config_.key_scenario,
                                  begin, end);
    // Get-family warmup reuses the measured keys (YuanRong behavior): the
    // orchestration prefill already wrote them, so warmup must not allocate a
    // second key set. Write/mixed operations keep a dedicated warmup prefix.
    if (config_.warmup_duration > 0.0 && !needs_prefill_) {
        warmup_keys_ = MakeKeyRange(config_.warmup_key_prefix,
                                    config_.key_scenario, begin, end);
    }
    return true;
}

bool BasicRunner::PrepareThreadBuffers(std::string *error) {
    const bool need_write =
        mixed_ ? put_spec_->needs_registered_source
               : single_spec_->needs_registered_source;
    const bool need_read =
        mixed_ ? get_spec_->needs_registered_sink
               : single_spec_->needs_registered_sink;
    thread_states_.clear();
    thread_states_.resize(static_cast<size_t>(config_.num_threads));
    if (!need_write && !need_read) return true;

    const size_t slots = static_cast<size_t>(config_.BatchSize());
    for (int i = 0; i < config_.num_threads; ++i) {
        ThreadState &state = thread_states_[static_cast<size_t>(i)];
        if (need_write) {
            state.write_buffer = std::make_unique<RegisteredBuffer>();
            if (!state.write_buffer->Create(store_, data_size_, slots)) {
                *error = "failed to create registered writer buffer " +
                         std::to_string(i);
                return false;
            }
        }
        if (need_read) {
            state.read_buffer = std::make_unique<RegisteredBuffer>();
            if (!state.read_buffer->Create(store_, data_size_, slots)) {
                *error = "failed to create registered reader buffer " +
                         std::to_string(i);
                return false;
            }
        }
    }
    return true;
}

bool BasicRunner::Start(std::string *error) {
    (void)error;
    if (started_.exchange(true)) return true;
    running_.store(true, std::memory_order_release);
    if (config_.warmup_duration > 0.0) {
        if (!RunPhase(true)) {
            running_.store(false, std::memory_order_release);
            return false;
        }
        if (!running_.load(std::memory_order_acquire)) return true;
    }
    metrics_.Reset();
    if (!running_.load(std::memory_order_acquire)) {
        metrics_.Freeze();
        return true;
    }
    if (!RunPhase(false)) {
        running_.store(false, std::memory_order_release);
        return false;
    }
    metrics_.Freeze();
    running_.store(false, std::memory_order_release);
    return true;
}

void BasicRunner::RequestStop() {
    running_.store(false, std::memory_order_release);
}

void BasicRunner::Stop() {
    RequestStop();
    for (auto &thread : threads_) {
        if (thread.joinable()) thread.join();
    }
    threads_.clear();
}

BasicRunner::Stats BasicRunner::GetStats() const {
    Stats stats;
    stats.measured_ops = measured_ops_.load(std::memory_order_relaxed);
    stats.measured_failures =
        measured_failures_.load(std::memory_order_relaxed);
    stats.warmup_ops = warmup_ops_.load(std::memory_order_relaxed);
    stats.threads = config_.num_threads;
    return stats;
}

bool BasicRunner::RunPhase(bool warmup) {
    if (warmup) {
        if (needs_prefill_) {
            // Get-family warmup reads the measured keys prepared by the
            // orchestration prefill. No warmup key allocation, no extra
            // prefill write, and no RemoveKeys on the measured set.
            phase_.keys = &measured_keys_;
        } else {
            if (warmup_keys_.empty()) return true;
            phase_.keys = &warmup_keys_;
        }
        phase_.deadline_us =
            NowUs() + static_cast<int64_t>(config_.warmup_duration * 1e6);
    } else {
        phase_.keys = &measured_keys_;
        phase_.deadline_us =
            config_.duration > 0.0
                ? NowUs() + static_cast<int64_t>(config_.duration * 1e6)
                : 0;
    }
    phase_.warmup = warmup;

    threads_.clear();
    threads_.reserve(static_cast<size_t>(config_.num_threads));
    for (int i = 0; i < config_.num_threads; ++i) {
        threads_.emplace_back([this, i] { RunThread(i); });
    }
    for (auto &thread : threads_) {
        if (thread.joinable()) thread.join();
    }
    threads_.clear();
    if (warmup && !needs_prefill_) RemoveKeys(warmup_keys_);
    return true;
}

void BasicRunner::RemoveKeys(const std::vector<std::string> &keys) {
    if (keys.empty()) return;
    const OpSpec *remove = FindOp("batch_remove");
    if (remove == nullptr) return;
    OpScratch scratch;
    constexpr size_t kChunk = 256;
    for (size_t offset = 0; offset < keys.size(); offset += kChunk) {
        const size_t count = std::min(kChunk, keys.size() - offset);
        std::vector<std::string> group(keys.begin() + offset,
                                       keys.begin() + offset + count);
        OpContext context;
        context.keys = &group;
        context.value_size = data_size_;
        context.scratch = &scratch;
        ExecuteOptions options;
        options.metric_name = "batch_remove";
        ExecuteOp(store_, *remove, context, options);
    }
}

void BasicRunner::ExecuteGroup(const OpSpec &spec,
                               const std::vector<std::string> &keys,
                               ThreadState &state, bool warmup) {
    OpContext context;
    context.keys = &keys;
    context.value_size = data_size_;
    context.value = &value_;
    context.seed = config_.value_seed;
    context.write_buffer = state.write_buffer.get();
    context.read_buffer = state.read_buffer.get();
    context.scratch = &state.scratch;
    context.verify = &verify_;

    ExecuteOptions options;
    options.metrics = &metrics_;
    options.metric_name = std::string(spec.canonical);
    options.fail_on_verify = config_.verify_fail_op;
    options.warmup = warmup;

    const OpOutcome outcome = ExecuteOp(store_, spec, context, options);
    if (warmup) {
        warmup_ops_.fetch_add(1, std::memory_order_relaxed);
    } else {
        measured_ops_.fetch_add(1, std::memory_order_relaxed);
        if (!outcome.ok) {
            measured_failures_.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void BasicRunner::RunThread(int thread_id) {
    ThreadState &state = thread_states_[static_cast<size_t>(thread_id)];
    const std::vector<std::string> &keys = *phase_.keys;
    const size_t total = keys.size();
    const size_t begin =
        total * static_cast<size_t>(thread_id) /
        static_cast<size_t>(config_.num_threads);
    const size_t end =
        total * static_cast<size_t>(thread_id + 1) /
        static_cast<size_t>(config_.num_threads);
    if (begin >= end) return;

    const size_t batch = static_cast<size_t>(config_.BatchSize());
    const bool duration_mode = phase_.deadline_us > 0;
    const bool warmup = phase_.warmup;

    TrafficGate::Options gate_options;
    gate_options.mode = config_.traffic_mode == "pulse"
                            ? TrafficGate::Mode::kPulse
                            : TrafficGate::Mode::kBalanced;
    gate_options.qps = warmup ? 0.0 : config_.target_qps;
    gate_options.thread_id = thread_id;
    gate_options.thread_count = config_.num_threads;
    gate_options.window_ms = config_.pulse_window_ms;
    gate_options.jitter = config_.enable_jitter;
    gate_options.catch_up = config_.pacing_catch_up;
    gate_options.instance_id = config_.instance_id;
    SteadyClock clock;
    TrafficGate gate(gate_options, &clock);
    gate.Reset(NowUs());

    auto should_stop = [&]() {
        if (!running_.load(std::memory_order_acquire)) return true;
        return duration_mode && NowUs() >= phase_.deadline_us;
    };

    if (mixed_) {
        const size_t n = end - begin;
        const size_t put_count = static_cast<size_t>(
            static_cast<double>(n) * config_.put_ratio);
        const size_t put_end = begin + put_count;
        std::vector<std::string> put_keys(keys.begin() + begin,
                                          keys.begin() + put_end);
        std::vector<std::string> get_keys(keys.begin() + put_end,
                                          keys.begin() + end);
        const size_t n_put = put_keys.size();
        const size_t n_get = get_keys.size();
        const size_t n_put_units =
            n_put == 0 ? 0 : (n_put + batch - 1) / batch;
        const size_t n_get_units =
            n_get == 0 ? 0 : (n_get + batch - 1) / batch;
        const size_t units = std::max(n_put_units, n_get_units);
        if (units == 0) return;
        do {
            for (size_t i = 0; i < units; ++i) {
                if (should_stop()) return;
                if (i < n_put_units) {
                    gate.WaitTurn();
                    if (should_stop()) return;
                    const size_t offset = i * batch;
                    const size_t count = std::min(batch, n_put - offset);
                    std::vector<std::string> group(
                        put_keys.begin() + offset,
                        put_keys.begin() + offset + count);
                    ExecuteGroup(*put_spec_, group, state, warmup);
                }
                if (i < n_get_units) {
                    gate.WaitTurn();
                    if (should_stop()) return;
                    const size_t offset = i * batch;
                    const size_t count = std::min(batch, n_get - offset);
                    std::vector<std::string> group;
                    group.reserve(count);
                    for (size_t k = 0; k < count; ++k) {
                        const size_t index = offset + k;
                        group.push_back(n_put > 0 ? put_keys[index % n_put]
                                                  : get_keys[index]);
                    }
                    ExecuteGroup(*get_spec_, group, state, warmup);
                }
            }
            if (duration_mode && put_spec_->side == OpSide::kWrite &&
                std::string(put_spec_->canonical) != "upsert_from") {
                RemoveKeys(put_keys);
            }
        } while (duration_mode && !should_stop());
        return;
    }

    const OpSpec &spec = *single_spec_;
    const size_t step = spec.batch_call ? batch : 1;
    do {
        for (size_t offset = begin; offset < end; offset += step) {
            if (should_stop()) return;
            gate.WaitTurn();
            if (should_stop()) return;
            const size_t count = std::min(step, end - offset);
            std::vector<std::string> group(keys.begin() + offset,
                                           keys.begin() + offset + count);
            ExecuteGroup(spec, group, state, warmup);
        }
        if (duration_mode && spec.side == OpSide::kWrite &&
            std::string(spec.canonical) != "upsert_from") {
            std::vector<std::string> own(keys.begin() + begin,
                                         keys.begin() + end);
            RemoveKeys(own);
        }
    } while (duration_mode && !should_stop());
}

}  // namespace mooncake::kvtest
