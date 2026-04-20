# Julia Implementation — Profile & Optimization Notes

This document captures the profiling of the original Julia `WCAStats.jl`
implementation, the hotspots it revealed, and the optimizations that were
applied on top.

The profile was collected with the built-in `--profile` flag (Julia's sampling
profiler, 10 ms interval) and corroborated with coarse `time()` wrappers around
each phase. Source data: `WCA_export_v2_110_20260420T000013Z.tsv.zip` (~500 MB
`WCA_export_results.tsv` inside); 24 years × 2 categories = 48 output CSVs.

Hardware / env: single-thread Julia 1.x, DataFrames.jl + CSV.jl + StatsBase +
RollingFunctions, Linux.

## 1. Baseline — where the time was going

End-to-end wall time: **~128 s** (`julia --project=. WCAStats.jl -- <zip>`), or
~147 s when profiling overhead is included.

### Phase breakdown (instrumented run)

| Phase                                    |  Time  |  % total |
| ---------------------------------------- | -----: | -------: |
| `load_wca` (unzip + 4× `CSV.read`)       | 37.5 s |     28 % |
| `get_event_years`                        |  2.4 s |      2 % |
| `calc()` × 48 iters (24 yrs × `in`/`to`) | 80.0 s |     60 % |
| `CSV.write` × 48                         | 12.3 s |      9 % |
| Measured work total                      |  133 s |    100 % |
| Julia startup + JIT (derived)            |  ~13 s |        — |
| **Wall total**                           | ~147 s |          |

### Inside a single `calc()` call (largest iter, `<= 2026`, 3.92 s)

| Step                                                          |   Time | Share |
| ------------------------------------------------------------- | -----: | ----: |
| `get_event_result` (2× filter: comps-by-year, results-by-ids) | 3.16 s |  81 % |
| `get_single_res_df`                                           | 0.12 s |   3 % |
| `stats_round_result` + `stats_single_result` + non_best       | 0.60 s |  15 % |
| 2× `leftjoin` on `personId`                                   | 0.01 s |   0 % |
| Ranks + country-group ranks + right-join with persons         | ~0.8 s |  20 % |

### Sampling-profile groupings (`Profile.print(:flat, :count)`)

From `/tmp/julia_profile.txt` (24 037 samples), the hottest clusters were:

- **`DataFrames.filter` / broadcast helpers** — 2.3–2.9 k samples each. The
  per-year re-filter of the 74 k-row results table was the single biggest
  hotspot, executed 48×.
- **CSV parsing** (`CSV.File` / `Parsers.xparse` / `parsefilechunk!`) —
  2.1–2.4 k samples; all in `load_wca`.
- **Split-apply-combine** (`DataFrames._combine*`) in the `stats_*` helpers —
  1.8–2.1 k samples.
- **JIT compilation** (`libLLVM`, `jl_compile_method_internal`,
  `jl_type_infer`) — 1.2–4.3 k samples; a first-run-only cost roughly matching
  the ~13 s "invisible" portion of wall time.
- **GC** (`ijl_gc_collect`) — 1.4 k samples (~14 s); meaningful but not
  dominant.

### Takeaways from the baseline

1. **~60 % of wall is the `calc()` loop, and within each iteration ~80 % was
   just re-filtering the global `results` table** by event then by per-year
   competition IDs. This was the single biggest optimization target — a
   one-time index of `results` restricted to the event of interest, with `year`
   pre-attached, would eliminate most of it.
2. **~28 % is cold-start data loading**, dominated by `CSV.read` on the
   ~500 MB `results.tsv`. Caching a parsed Arrow/Feather file would cut this
   materially but requires an extra format on disk.
3. **CSV write is ~9 %** — not a primary target.
4. **JIT + startup ≈ 10 %** — unavoidable single-cost; `PackageCompiler.jl`
   would trade compile time for cleaner repeat runs.

## 2. Applied optimizations

Two localized changes in `WCAStats.jl`, no behavioral impact (all 48 output
CSVs are byte-identical to the baseline):

1. **`prepare_event_results(wca_dict, event_id)`** — computed once in
   `process_data`. Filters `results` to the event of interest and left-joins
   `competitions[:, [:id, :year]]` so each row carries its competition year.
   `calc` now takes this prepared frame and does a cheap per-iter
   `filter(:year => year_filter, event_all)` instead of two filters and an
   `∈`-membership scan. This replaces the `get_event_result` work on every
   call.
2. **`prepare_persons(wca_dict)`** — the `persons[sub_id==1]` projection +
   rename used by `calc`'s right-join is hoisted out of the per-year loop.

The single-argument `calc(wca_data, year_filter)` is kept as a thin wrapper
that prepares inputs on demand, preserving backward compatibility.

## 3. Results after optimization

End-to-end wall time (same command, same input):

|                            |   Before |  After |    Δ    |
| -------------------------- | -------: | -----: | :-----: |
| `julia WCAStats.jl … <zip>` |   ~128 s | ~104 s | −19 %   |
| `calc()` loop (48 iters)   |   80.0 s | 34.5 s | **−57 %** |
| Largest single `calc()`    |   3.92 s | 1.69 s | −57 %   |
| Output parity              |        — | 48/48 byte-identical | |

Updated phase breakdown:

| Phase                             |  Time |
| --------------------------------- | ----: |
| `load_wca`                        | 32.5 s |
| `prepare_event_results` (new)     |  4.6 s |
| `prepare_persons` (new)           |  0.1 s |
| `get_event_years`                 |  1.3 s |
| `calc()` × 48                     | 34.5 s |
| `CSV.write` × 48                  |  8.3 s |
| Measured work total               | 82.4 s |

The optimization eliminates most of the redundant 48× filter cost. What
remains is essentially irreducible given the current stack:

- CSV parsing of the ~500 MB TSV (`load_wca` ≈ 32 s).
- Legitimate per-year split-apply-combine inside the `stats_*` helpers.
- Julia startup + JIT (~13 s, first-run only).

## 4. Reference

- Rust port achieves ~17 s on the same input (~6× faster than the optimized
  Julia, ~7.5× vs. the baseline). See
  [`rust-wca-stats/README.md`](rust-wca-stats/README.md) for details and
  per-CSV parity notes.
- Reproduce the profile with:
  ```
  julia --project=. WCAStats.jl -- --profile \
      --profile-out julia_profile.txt <zip>
  ```
  Output is `Profile.print(format=:flat, sortedby=:count)` — hottest frames
  are at the tail of the file.
