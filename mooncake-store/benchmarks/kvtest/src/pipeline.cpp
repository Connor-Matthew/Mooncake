#include "pipeline.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <utility>

#include "traffic_gate.h"

namespace mooncake::kvtest {
namespace {

int64_t NowUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool IsPutFamily(const OpSpec *spec) {
    if (spec == nullptr) return false;
    return spec->canonical == "put" || spec->canonical == "put_batch" ||
           spec->canonical == "put_from" ||
           spec->canonical == "batch_put_from";
}

std::string JoinCanonical(const std::vector<const OpSpec *> &ops) {
    std::string out;
    for (const auto *op : ops) {
        if (!out.empty()) out += ',';
        out += op->canonical;
    }
    return out;
}

}  // namespace

PipelineWriter::PipelineWriter(const Config &config, StoreAdapter &store,
                               MetricsCollector &metrics, NotifySender *sender)
    : config_(config), store_(store), metrics_(metrics), sender_(sender) {}

PipelineWriter::~PipelineWriter() { Stop(); }

bool PipelineWriter::Prepare(std::string *error) {
    if (!PrepareOperations(error)) return false;
    verify_.level = config_.verify_level;
    verify_.sample_bytes = config_.verify_sample_bytes;
    verify_.sample_step_bytes = config_.verify_sample_step_bytes;
    max_size_ = 0;
    for (uint64_t size : config_.data_sizes) {
        max_size_ = std::max(max_size_, size);
    }
    if (max_size_ == 0) {
        *error = "data_sizes must not be empty";
        return false;
    }
    if (!value_.Load(config_.instance_id, config_.value_file,
                     config_.data_sizes, error)) {
        return false;
    }
    peers_ = config_.peers;
    return PrepareBuffers(error);
}

bool PipelineWriter::PrepareOperations(std::string *error) {
    ops_.clear();
    for (const auto &name : config_.pipeline) {
        const OpSpec *spec = FindOp(name);
        if (spec == nullptr) {
            *error = "unsupported pipeline operation: " + name;
            return false;
        }
        // Writer-side pipelines may read back data as well as write; the
        // notify pipeline is the read-only side and is validated separately.
        ops_.push_back(spec);
    }
    if (ops_.empty()) {
        *error = "pipeline write list is empty";
        return false;
    }
    return true;
}

bool PipelineWriter::PrepareBuffers(std::string *error) {
    bool need_write = false;
    bool need_read = false;
    for (const OpSpec *spec : ops_) {
        if (spec->needs_registered_source) need_write = true;
        if (spec->needs_registered_sink) need_read = true;
    }
    states_.clear();
    states_.resize(static_cast<size_t>(config_.num_threads));
    if (!need_write && !need_read) return true;
    const size_t slots = static_cast<size_t>(config_.BatchSize());
    for (int i = 0; i < config_.num_threads; ++i) {
        ThreadState &state = states_[static_cast<size_t>(i)];
        if (need_write) {
            state.buffer = std::make_unique<RegisteredBuffer>();
            if (!state.buffer->Create(store_, max_size_, slots)) {
                *error = "failed to create registered writer buffer " +
                         std::to_string(i);
                return false;
            }
        }
        if (need_read) {
            state.read_buffer = std::make_unique<RegisteredBuffer>();
            if (!state.read_buffer->Create(store_, max_size_, slots)) {
                *error = "failed to create registered read buffer " +
                         std::to_string(i);
                return false;
            }
        }
    }
    return true;
}

bool PipelineWriter::Start(std::string *error) {
    if (started_.exchange(true)) return true;
    running_.store(true, std::memory_order_release);
    if (config_.warmup_duration > 0.0) {
        phase_state_.store(static_cast<int>(PhaseState::kWarming),
                           std::memory_order_release);
        RunPhase(true, NowUs() + static_cast<int64_t>(
                                         config_.warmup_duration * 1e6));
        if (sender_ != nullptr) sender_->Drain();
        if (!running_.load(std::memory_order_acquire)) {
            phase_state_.store(static_cast<int>(PhaseState::kStopped),
                               std::memory_order_release);
            return true;
        }
    }
    metrics_.Reset();
    phase_state_.store(static_cast<int>(PhaseState::kMeasuring),
                       std::memory_order_release);
    RunPhase(false,
             config_.duration > 0.0
                 ? NowUs() +
                       static_cast<int64_t>(config_.duration * 1e6)
                 : 0);
    // Freeze the measured window before draining outbound notifications so
    // elapsed_seconds reflects the configured measurement window, not drain
    // time (matches the Python client).
    metrics_.Freeze();
    phase_state_.store(static_cast<int>(PhaseState::kDraining),
                       std::memory_order_release);
    if (sender_ != nullptr) sender_->Drain();
    phase_state_.store(static_cast<int>(PhaseState::kStopped),
                       std::memory_order_release);
    running_.store(false, std::memory_order_release);
    return true;
}

void PipelineWriter::RequestStop() {
    running_.store(false, std::memory_order_release);
}

void PipelineWriter::Stop() {
    RequestStop();
    for (auto &worker : workers_) {
        if (worker.joinable()) worker.join();
    }
    workers_.clear();
}

void PipelineWriter::RunPhase(bool warmup, int64_t deadline_us) {
    workers_.clear();
    workers_.reserve(static_cast<size_t>(config_.num_threads));
    for (int i = 0; i < config_.num_threads; ++i) {
        workers_.emplace_back(
            [this, i, warmup, deadline_us] { Worker(i, warmup, deadline_us); });
    }
    for (auto &worker : workers_) {
        if (worker.joinable()) worker.join();
    }
    workers_.clear();
}

uint64_t PipelineWriter::KeyIndex(uint64_t sequence, int thread_id,
                                  int key_index) const {
    if (config_.key_pool_size == 0) return sequence;
    const uint64_t batch = static_cast<uint64_t>(config_.BatchSize());
    const uint64_t threads = static_cast<uint64_t>(config_.num_threads);
    return (sequence * threads * batch +
            static_cast<uint64_t>(thread_id) * batch +
            static_cast<uint64_t>(key_index)) %
           config_.key_pool_size;
}

std::vector<std::string> PipelineWriter::KeysFor(uint64_t sequence,
                                                 int thread_id,
                                                 bool warmup) const {
    std::vector<std::string> keys;
    keys.reserve(static_cast<size_t>(config_.BatchSize()));
    const std::string prefix = warmup ? "mooncake_kvtest_warmup_" : "mooncake_kvtest_";
    for (int i = 0; i < config_.BatchSize(); ++i) {
        if (config_.key_pool_size == 0) {
            keys.push_back(prefix + std::to_string(config_.instance_id) + "_" +
                           std::to_string(thread_id) + "_" +
                           std::to_string(sequence) + "_" + std::to_string(i));
        } else {
            keys.push_back(prefix + std::to_string(config_.instance_id) + "_" +
                           std::to_string(KeyIndex(sequence, thread_id, i)));
        }
    }
    return keys;
}

uint64_t PipelineWriter::SizeFor(uint64_t sequence, int thread_id,
                                 std::mt19937 &rng) const {
    if (config_.key_pool_size == 0) {
        std::uniform_int_distribution<size_t> distribution(
            0, config_.data_sizes.size() - 1);
        return config_.data_sizes[distribution(rng)];
    }
    const uint64_t index = KeyIndex(sequence, thread_id, 0);
    return config_.data_sizes[index % config_.data_sizes.size()];
}

bool PipelineWriter::DeleteBeforeWrite(uint64_t sequence) const {
    if (config_.key_pool_size == 0) return false;
    const uint64_t cycle =
        config_.key_pool_size /
        (static_cast<uint64_t>(config_.num_threads) *
         static_cast<uint64_t>(config_.BatchSize()));
    if (sequence < cycle) return false;
    for (const OpSpec *spec : ops_) {
        if (IsPutFamily(spec)) return true;
    }
    return false;
}

void PipelineWriter::Notify(const NotifyBatch &batch, std::mt19937 &rng) {
    if (sender_ == nullptr || peers_.empty()) return;
    int targets = config_.notify_count < 0
                      ? static_cast<int>(peers_.size())
                      : config_.notify_count;
    if (targets <= 0) return;
    targets = std::min<int>(targets, static_cast<int>(peers_.size()));
    std::vector<size_t> indexes(peers_.size());
    for (size_t i = 0; i < indexes.size(); ++i) indexes[i] = i;
    std::shuffle(indexes.begin(), indexes.end(), rng);
    std::vector<std::string> selected;
    selected.reserve(static_cast<size_t>(targets));
    for (int i = 0; i < targets; ++i) selected.push_back(peers_[indexes[i]]);
    notifications_.fetch_add(selected.size(), std::memory_order_relaxed);
    if (!sender_->Dispatch(selected, batch)) {
        notify_failures_.fetch_add(1, std::memory_order_relaxed);
    }
}

void PipelineWriter::Worker(int thread_id, bool warmup,
                            int64_t deadline_us) {
    ThreadState &state = states_[static_cast<size_t>(thread_id)];
    std::mt19937 rng(static_cast<uint32_t>(config_.instance_id * 1000 +
                                           thread_id));

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
        return deadline_us > 0 && NowUs() >= deadline_us;
    };

    uint64_t sequence = 0;
    while (!should_stop()) {
        if (gate.Active()) {
            gate.WaitTurn();
            if (should_stop()) break;
        }
        std::vector<std::string> keys = KeysFor(sequence, thread_id, warmup);
        const uint64_t size = SizeFor(sequence, thread_id, rng);
        if (DeleteBeforeWrite(sequence)) {
            const OpSpec *remove = FindOp("batch_remove");
            if (remove != nullptr) {
                OpScratch scratch;
                OpContext context;
                context.keys = &keys;
                context.value_size = size;
                context.scratch = &scratch;
                ExecuteOptions options;
                options.metric_name = "batch_remove";
                ExecuteOp(store_, *remove, context, options);
                maintenance_ops_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        bool ok = true;
        for (const OpSpec *spec : ops_) {
            OpContext context;
            context.keys = &keys;
            context.value_size = size;
            context.value = &value_;
            context.seed = static_cast<uint64_t>(config_.instance_id);
            context.write_buffer = state.buffer.get();
            context.read_buffer = state.read_buffer.get();
            context.scratch = &state.scratch;
            context.verify = &verify_;
            ExecuteOptions options;
            options.metrics = &metrics_;
            options.fail_on_verify = config_.verify_fail_op;
            options.warmup = warmup;
            const OpOutcome outcome =
                ExecuteOp(store_, *spec, context, options);
            if (!outcome.ok) {
                ok = false;
                break;
            }
        }
        if (ok) {
            NotifyBatch batch;
            batch.keys = keys;
            batch.sender = config_.instance_id;
            batch.size = size;
            batch.warmup = warmup;
            Notify(batch, rng);
        }
        if (warmup) {
            warmup_ops_.fetch_add(1, std::memory_order_relaxed);
        } else {
            measured_ops_.fetch_add(1, std::memory_order_relaxed);
            if (!ok) {
                measured_failures_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        ++sequence;
    }
}

std::string PipelineWriter::PhaseName() const {
    switch (static_cast<PhaseState>(
        phase_state_.load(std::memory_order_acquire))) {
        case PhaseState::kIdle:
            return "idle";
        case PhaseState::kWarming:
            return "warming";
        case PhaseState::kMeasuring:
            return "measuring";
        case PhaseState::kDraining:
            return "draining";
        case PhaseState::kStopped:
            return "stopped";
    }
    return "idle";
}

PipelineWriter::Stats PipelineWriter::GetStats() const {
    Stats stats;
    stats.measured_ops = measured_ops_.load(std::memory_order_relaxed);
    stats.measured_failures =
        measured_failures_.load(std::memory_order_relaxed);
    stats.warmup_ops = warmup_ops_.load(std::memory_order_relaxed);
    stats.maintenance_ops =
        maintenance_ops_.load(std::memory_order_relaxed);
    stats.notifications = notifications_.load(std::memory_order_relaxed);
    stats.notify_failures =
        notify_failures_.load(std::memory_order_relaxed);
    stats.phase = PhaseName();
    return stats;
}

// ---------------------------------------------------------------------------
// PipelineReader
// ---------------------------------------------------------------------------

PipelineReader::PipelineReader(const Config &config, StoreAdapter &store,
                               MetricsCollector &metrics)
    : config_(config), store_(store), metrics_(metrics) {}

PipelineReader::~PipelineReader() = default;

bool PipelineReader::Prepare(std::string *error) {
    prechecks_.clear();
    data_op_ = nullptr;
    for (const auto &name : config_.notify_pipeline) {
        const OpSpec *spec = FindOp(name);
        if (spec == nullptr) {
            *error = "unsupported notify_pipeline operation: " + name;
            return false;
        }
        if (spec->side != OpSide::kRead) {
            *error = "notify_pipeline must contain read operations only: " +
                     name;
            return false;
        }
        if (spec->canonical == "exist" ||
            spec->canonical == "batch_exist") {
            prechecks_.push_back(spec);
            continue;
        }
        if (data_op_ != nullptr) {
            *error = "notify_pipeline has more than one data operation: " +
                     JoinCanonical(prechecks_) + " + " + name;
            return false;
        }
        data_op_ = spec;
    }
    if (data_op_ == nullptr && !prechecks_.empty()) {
        // Legacy behaviour: a lone exist step is a measured operation.
        data_op_ = prechecks_.front();
        prechecks_.clear();
    }
    if (data_op_ == nullptr) {
        *error = "notify_pipeline has no data operation";
        return false;
    }
    verify_.level = config_.verify_level;
    verify_.sample_bytes = config_.verify_sample_bytes;
    verify_.sample_step_bytes = config_.verify_sample_step_bytes;
    buffers_.clear();
    if (data_op_->needs_registered_sink) {
        const int workers = std::max(1, config_.NumReadThreads());
        for (int i = 0; i < workers; ++i) {
            BufferSlot slot;
            slot.buffer = std::make_unique<RegisteredBuffer>();
            if (!slot.buffer->Create(
                    store_, config_.data_sizes.front(),
                    static_cast<size_t>(config_.BatchSize()))) {
                *error = "failed to create reader buffer " +
                         std::to_string(i);
                return false;
            }
            buffers_.push_back(std::move(slot));
        }
    }
    return true;
}

bool PipelineReader::Precheck(const NotifyBatch &batch) {
    if (prechecks_.empty()) return true;
    for (const OpSpec *spec : prechecks_) {
        if (spec->batch_call) {
            const std::vector<int> results = store_.BatchIsExist(batch.keys);
            if (results.size() != batch.keys.size()) return false;
            for (int rc : results) {
                if (rc <= 0) return false;
            }
        } else {
            for (const auto &key : batch.keys) {
                if (store_.IsExist(key) <= 0) return false;
            }
        }
    }
    return true;
}

OpScratch &PipelineReader::Scratch() {
    thread_local OpScratch scratch;
    return scratch;
}

RegisteredBuffer *PipelineReader::AcquireBuffer() {
    std::unique_lock<std::mutex> lock(buffer_mutex_);
    buffer_cv_.wait(lock, [this] {
        for (const auto &slot : buffers_) {
            if (!slot.in_use) return true;
        }
        return buffers_.empty();
    });
    for (auto &slot : buffers_) {
        if (!slot.in_use) {
            slot.in_use = true;
            return slot.buffer.get();
        }
    }
    return nullptr;
}

void PipelineReader::ReleaseBuffer(RegisteredBuffer *buffer) {
    if (buffer == nullptr) return;
    {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        for (auto &slot : buffers_) {
            if (slot.buffer.get() == buffer) {
                slot.in_use = false;
                break;
            }
        }
    }
    buffer_cv_.notify_one();
}

void PipelineReader::Execute(const NotifyBatch &batch) {
    if (data_op_ == nullptr || batch.keys.empty()) return;
    batches_.fetch_add(1, std::memory_order_relaxed);
    if (batch.warmup) {
        warmup_batches_.fetch_add(1, std::memory_order_relaxed);
    }
    const std::string metric_name(data_op_->canonical);
    if (!Precheck(batch)) {
        precheck_misses_.fetch_add(1, std::memory_order_relaxed);
        metrics_.Record(metric_name, 0.0, RecordStatus::kMiss, 0, 0,
                        batch.warmup);
        return;
    }
    const uint64_t size =
        batch.size > 0 ? batch.size : config_.data_sizes.front();
    RegisteredBuffer *read_buffer = nullptr;
    if (data_op_->needs_registered_sink) {
        read_buffer = AcquireBuffer();
        if (read_buffer == nullptr) {
            metrics_.Record(metric_name, 0.0, RecordStatus::kError, 0, -2,
                            batch.warmup);
            return;
        }
    }
    OpContext context;
    context.keys = &batch.keys;
    context.value_size = size;
    context.value = nullptr;
    context.seed = static_cast<uint64_t>(batch.sender);
    context.read_buffer = read_buffer;
    context.scratch = &Scratch();
    context.verify = &verify_;
    ExecuteOptions options;
    options.metrics = &metrics_;
    options.metric_name = metric_name;
    options.fail_on_verify = config_.verify_fail_op;
    options.warmup = batch.warmup;
    ExecuteOp(store_, *data_op_, context, options);
    if (read_buffer != nullptr) ReleaseBuffer(read_buffer);
}

PipelineReader::Stats PipelineReader::GetStats() const {
    Stats stats;
    stats.batches = batches_.load(std::memory_order_relaxed);
    stats.warmup_batches = warmup_batches_.load(std::memory_order_relaxed);
    stats.precheck_misses = precheck_misses_.load(std::memory_order_relaxed);
    return stats;
}

// ---------------------------------------------------------------------------
// PipelineDispatcher
// ---------------------------------------------------------------------------

PipelineDispatcher::PipelineDispatcher(const Config &config,
                                       PipelineReader &reader,
                                       MetricsCollector &metrics)
    : config_(config),
      reader_(reader),
      metrics_(metrics),
      pool_(static_cast<size_t>(std::max(1, config.NumReadThreads())),
            config.notify_queue_max) {}

PipelineDispatcher::~PipelineDispatcher() { Stop(); }

bool PipelineDispatcher::Dispatch(NotifyBatch batch) {
    if (stopping_.load(std::memory_order_acquire)) return false;
    const int64_t arrival_us = NowUs();
    std::function<void()> job = [this, batch = std::move(batch),
                                 arrival_us]() mutable {
        metrics_.Record(
            "notify_queue_wait",
            static_cast<double>(NowUs() - arrival_us) / 1000.0,
            RecordStatus::kOk, 0);
        reader_.Execute(batch);
    };
    for (;;) {
        if (pool_.Submit(job)) return true;
        if (config_.notify_overflow != "block") {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        if (stopping_.load(std::memory_order_acquire)) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void PipelineDispatcher::StopNow() {
    stopping_.store(true, std::memory_order_release);
    pool_.StopNow();
}

bool PipelineDispatcher::Drain(std::chrono::milliseconds timeout) {
    return pool_.WaitIdleFor(timeout);
}

void PipelineDispatcher::Stop() {
    stopping_.store(true, std::memory_order_release);
    pool_.Stop();
}

}  // namespace mooncake::kvtest
