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
