# TE / TENT transfer latency tracing

Enable with the existing switch (bytes, read once at process startup):

```sh
export MC_TRANSFER_TRACE_MIN_BYTES=8388608
```

Unset or `0` disables the new tracing. Both engines use the same store-side
fields. No additional switch is required. Existing `MC_STORE_TRANSFER_DIAG`
still controls the older `transfer_data_breakdown`; it is not required here.
These diagnostic logs cost time: compare traced runs with the same configuration.
Do not interpret measured elapsed time as CPU execution time.

## Records and boundaries

* `transfer_path_trace scope=get_buffer_read`: slice construction and replica
  filtering, `Client::Get`, existing get-breakdown log, then local teardown.
* `transfer_path_trace scope=client_get`: replica selection, cache/validation,
  transfer read, cache release/metrics, checksum, then admission/lease checks
  and local teardown. This instruments the QueryResult + slices Get overload
  used by get_buffer, not every batch/range overload.
* `transfer_path_trace scope=client_transfer_data`: planning/submission,
  future get (including its diagnostic logs), diagnostics, local teardown.
* `transfer_trace`: existing fields plus `prepare_requests_us` (classic request
  vector construction), `state_create_us` (post-submit state construction),
  `submit_unattributed_us` (unaccounted submission time, including trace setup
  and rounding), `path_total_us` (exact elapsed submission start to observed
  completion). `build_requests_us` retains its original meaning: conversion
  to TENT requests, not provider WQE construction.
  `submit_us` and `total_us` now include the newly measured request preparation
  and state construction phases; use `path_total_us` for the exact elapsed window.
* `ub_submit_trace engine=classic`: local slice preparation/device selection
  versus calls to `submitPostSend` (`enqueue_us`). This is enqueue time, not
  hardware post duration. Emitted on successful UB submission; submission
  failures retain the existing error logs and enclosing scope records.
* `transfer_wait_return_trace`: observed completion to future return boundary,
  separately reporting `completion_trace_log_us`. Its own log emission is
  included by the enclosing `future_get`, not by `observed_to_return_us`.
* `transfer_release_trace`: batch free and observed completion to release start.
  Release can happen much later with shared futures; this is a lifetime interval,
  not necessarily a request-critical delay. `status_observed=0` means the state
  was destroyed without observing completion; the observation interval is `-1`.
* `ub_task_trace` (TENT): existing prepare/post/device stages plus
  `prep_route_metadata_us` and `prep_local_location_us`, which split
  `prep_route_us`; `completion_finalize_us` covers last slice terminal to task
  terminal. Retried slices still describe their final attempt.

Each path record has consecutive phase intervals and `tail_us`; their sum is
`total_us`. Inner records are nested, not additive to their parent intervals.
The scope destructor records local teardown (including future/batch destruction
when locally owned), but excludes its own log formatting/emission. That emission
is visible in the enclosing scope. Early returns emit the completed checkpoints
plus the remaining tail; use existing error/status logs to identify failures.

## Correlation

Join store records by `trace_id`; join per-batch records by `batch` and process.
TENT UB tasks carry the same progress batch ID in `ub_task_trace batch`.
One request may create multiple batches/tasks; do not assume a one-to-one join.
Filter out warmup and aggregate the same request population before comparing.

`start_ns`, `end_ns`, `submit_start_ns`, `submit_end_ns`, `observed_done_ns`,
`return_ns`, and UB `created_ns`, `terminal_ns`, `first_post_ns`,
`last_complete_ns` use steady-clock nanoseconds. Compare only within the same
process/run, never across hosts or independent runs. For TENT, the interval
from the latest corresponding task `terminal_ns` to `observed_done_ns` locates
completion publication/observation delay; submit start to task creation locates
pre-task overhead. Batch IDs can be reused: also match timestamp windows.

`device_us` includes completion polling/handling. Classic `wait_us` begins at
the upper-layer wait boundary. Neither is a pure hardware DMA measurement.
These logs cover the current get_buffer transfer chain and its existing UB
task phases; they do not expose internal firmware or provider execution stages.

## Classic TE UB device and task timing

The same `MC_TRANSFER_TRACE_MIN_BYTES` threshold now enables per-request classic
UB task timing, emitted as `ub_task_trace engine=classic` when the task is
destroyed during batch recycling. It covers both READ and WRITE. The line has
`trace_id`, `batch`, and `task` for correlation; filter measurement windows by
`created_ns`, not the later log emission time (convert with a same-process
timestamp anchor, since these are monotonic timestamps, not wall time).

* `device_us[avg/p50/p99/max]`: provider post returned to successful slice
  terminal handling, including software completion discovery/handling. This
  matches the intended TENT device window, not pure DMA time.
* `post_us[avg/p50/p99/max]`: duration around the URMA post call. All slices
  submitted by one provider call share that call's timestamps.
* `total_us` / `ub_task_trace_total` (both microseconds): UB task construction
  start to task completion publication. `terminal_source=last_slice` indicates
  a fallback boundary (e.g. scheduled tasks), not the usual task publication.
* `created_ns`, `terminal_ns`, `first_post_ns`, `last_complete_ns`: monotonic
  timestamps for joining to the enclosing store transfer trace. First post and
  device timings describe final attempts for retried slices; `retries` counts
  additional provider post attempts, not all software route retries.
* `device_samples` / `device_samples_missing`: only successfully completed
  slices with ordered post-return/completion timestamps enter device statistics.
  Failed/unposted slices and completions preceding post return are excluded.
  No valid samples is reported as `-1`, not zero. Incomplete task total is `-1`.

For multiple slices, device statistics are per-slice, not the task critical
path: do not divide the entire task byte count by a per-slice mean. A single
8 MiB slice can be compared directly with the corresponding TENT slice.
Logging itself is outside task/device timing but inside batch release time.
Enabled tracing allocates diagnostic state and reads clocks; measure both
engines with consistent tracing, and retain an untraced throughput baseline.

## get_buffer A/B and high-frequency sampling

```sh
export MC_TRANSFER_TRACE_MIN_BYTES=8388608
export MC_HIFREQ_LOG_SAMPLE_RATE=1   # full diagnostic capture
# export MC_HIFREQ_LOG_SAMPLE_RATE=0 # disable request diagnostics
```

All new request diagnostic capture is gated before diagnostic clock reads,
trace-state allocation, snapshotting and formatting. The sample rate is cached
at startup; restart the process after changing it. With rate zero, operational
timestamps needed by the transport (timeouts, activity tracking), metrics,
SpDiag and request trace IDs still exist. Startup, warnings and errors remain.
This is low overhead for disabled logging, not literally zero CPU overhead.

`get_breakdown` adds the following fields in microseconds:

* `read_a_us`: `t_alloc` (allocation finished) to the selected transfer's
  `submit_start_ns` (openSegment path start).
* `read_transfer_window_us`: that submit start to `observed_done_ns`.
* `read_b_us`: observed completion to the `get_breakdown` cutoff, including
  completion diagnostic output, future return, batch release when locally
  owned, checksums/cache/lease checks, and intervening diagnostic output.
* `read_ab_valid`: true only for exactly one observed transfer with ordered
  boundaries; otherwise A/B/window are `-1`, not zero.
* `read_transfer_count`: number of observed transfer waits recorded in scope.

For valid single-transfer get_buffer requests, A + transfer_window + B equals
the elapsed read window (within the existing integer read_us rounding). The
transfer window is exact elapsed time; `transfer_trace total_us` remains a sum
of rounded phases and can differ by `submit_unattributed_us`. The synchronous
wait caller records boundaries, so no pointer to a stack context is handed to
the transport workers. Multiple-transfer, cache-only, local memcpy and other
non-TE paths are deliberately reported as invalid instead of misattributed.

Audit of the current get_buffer/upsert_from + UB hot paths:

| Diagnostics | Sampling and output |
| --- | --- |
| get_breakdown, upsert_from_breakdown | sample by request trace ID before capture; MC_LOG |
| query_rpc_breakdown / master_rpc_client_breakdown | sample before log-only timing/formatting; MC_LOG |
| transfer_trace / transfer_path_trace / wait-return / release | byte threshold + request sample before capture; MC_LOG |
| classic ub_task_trace / ub_submit_trace | byte threshold + request sample; MC_LOG |
| TENT ub_task_trace | sampled on submit; no reporter thread at rate zero; MC_LOG on reporter thread with original trace ID |
| urma_queue_depth / slice_chip_bindings | request sample, plus chip diagnostic opt-in; MC_LOG |
| URMA setup/import diagnostics | sampled asynchronous MC_LOG_SAMPLED; log-only clock reads skipped at zero |
| verbose request logs / ScopedVLogTimer | verbose level + sampling; asynchronous MC_LOG backend |

Plain `MC_LOG` is asynchronous, but does not automatically sample and is not
itself a lazy stream gate. Hot diagnostics use an outer sampled guard before
capture or `MC_LOG_SAMPLED`, which checks sampling and severity before creating
the stream or evaluating its arguments. `MC_VLOG` now uses the same lazy rule
in addition to its verbose-level check. No errors are sampled away.

The URMA INFO/DEBUG redirect is sampled before constructing the local log
string; formatting already performed inside liburma is outside this control.
Keep the provider's own debug settings off for throughput baselines.
