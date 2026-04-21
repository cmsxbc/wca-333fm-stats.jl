# wca-stats (Rust port of WCAStats.jl)

A Rust re-implementation of [`WCAStats.jl`](../WCAStats.jl) that computes per-year
3x3 Fewest Moves statistics from a WCA results export and emits matching CSV
files.

## Build

```bash
cargo build --release
```

## Run

```bash
# default: generate results.in<Y>.csv / results.to<Y>.csv for every year into ./results
./target/release/wca-stats <WCA_export_...tsv.zip>

# top-K by a column in the latest "to-year" result
./target/release/wca-stats <zip> topk average --k 10
./target/release/wca-stats <zip> topk best --k 20 --country China

# print specific persons from the latest "to-year" result
./target/release/wca-stats <zip> person 2011TRON02 2012PARK03

# per-person summary across all years (also writes ./results/<ID>.csv)
./target/release/wca-stats <zip> summary 2011TRON02
```

The input is the official WCA export zip (e.g. `WCA_export_…_tsv.zip`)
containing `WCA_export_Persons.tsv`, `WCA_export_Competitions.tsv`,
`WCA_export_Results.tsv` and `WCA_export_Scrambles.tsv`.

## Source layout

| File | Responsibility |
| --- | --- |
| `src/main.rs`     | CLI, orchestration, summary bookkeeping |
| `src/loader.rs`   | zip + TSV parsing; `WcaData` structs |
| `src/stats.rs`    | `trim_avg`, `std`, `median`, `mode_count`, `calc_consecutive`, `rolling_mean`, `rolling_trim_avg` |
| `src/calc.rs`     | 69-column schema, `Row`/`Frame`, per-year `calc()`, `competerank_col` |
| `src/output.rs`   | CSV writer, `print_topk`, `print_some_persons`, `write_summary_csv` |

## Parity with the Julia reference

The Rust port reproduces `WCAStats.jl` semantics, including a number of
Julia/DataFrames.jl-specific quirks:

* `competerank` treats each `NaN` as distinct (no tie-grouping for `NaN`).
* `std` of a 1-element vector returns `NaN`, ranked accordingly.
* Rank-column emission order: asc columns first (in `COLS` order), then 20 desc
  columns in Julia source order (see `DESC_ORDER` in `calc.rs`).
* `rolling_mean` uses `sum / n` (not `sum * (1/n)`) to match Julia's float bits.
* `trim_avg` sorts then sums the interior window, matching Julia's
  `mytrimmean` reduction shape.
* Summary preserves the original typo `"detla"`; the current-year row is pushed
  before the delta row; meta fields (name / country / gender) are carried across
  years.
* Medal counts are incremented only on finals/combined rounds with `best > 0`.
* `average_real = average / 100`; rolling windows run on the solved subset
  (`value > 0`) in `(result_id, attempt_number)` order.
* Right-join against `persons` is honoured: unmatched persons appear at the
  file tail.

## Verified parity

Running both on the same export (48 CSVs produced: 24 years × {`in`,`to`}):

* **Value parity: 48 / 48** — when numeric fields are compared at 10 significant
  figures (modulo row order).
* **Byte-identical: 13 / 48.** The remaining 35 files differ only in:
  1. Last-ULP float printing of `average_avg` (Julia's `@simd`-backed `sum`
     uses a slightly different reduction order than Rust's scalar sum,
     producing ±1 ULP differences such as `31.667500000000004` vs `31.6675`).
  2. Rank columns shifting by ±1 for rows whose `*_std` is `NaN`, caused by
     `DataFrames.leftjoin`'s non-deterministic `order=:undefined` hash-join
     placing a handful of persons (e.g. `2009ANON01`) at the tail instead of
     in alphabetical position.

## Performance

Full-export run on the same machine (input ≈ 353 MB zipped, 48 output CSVs):

| Command | Julia | Rust | Speed-up |
| --- | --- | --- | --- |
| `default` (all years) | 128.4 s | 10.6 s | **~12.1×** |
| `summary <id>` | ~148 s | ~20 s | ~7.4× |

The Rust port now runs the full export in ~10.6 s; the C++23 port is faster
still at ~7.9 s thanks to `libdeflate` and more cache-friendly allocation
patterns (see [`cpp-wca-stats/README.md`](../cpp-wca-stats/README.md)).

The loader reads each zipped TSV into a single buffer (zlib-ng decompression)
and parses it with a hand-rolled `memchr`-based splitter and a custom
`FromDecimal` integer parser (no UTF-8 validation on the numeric hot path).
Load time dropped from ≈ 9.8 s (with `csv` + `BufReader`) to ≈ 6.9 s. Per-year
compute + CSV write is 0.01–0.2 s each.

### Calc-path micro-optimisations

Guided by the TopDown L3 profile in [`PERF_ANALYSIS.md`](../PERF_ANALYSIS.md)
(Rust was ~32 % DRAM-bound in the per-person compute), two calc-only changes
trimmed wall time from **11.24 s → 10.60 s** (best-of-5) without touching the
loader or changing any output byte:

* **Precomputed column indices** (`ColIdx`): the old `set(row, "name", …)`
  helper did a linear scan of the 69-entry `COLS` slice for every write. That
  scan ran ~500 k × 48 × ~70 times and dominated L1-bound stalls. `ColIdx`
  resolves every column name once and `compute_row` now writes
  `row.vals[ci.field]` directly. The rolling-mean column lookups
  (`solved_mo{n}_last`/`_best`, `solved_ao{n}_last`/`_best`) that previously
  built strings via `format!` in a hot loop are now indexed from small
  `[(n, last_idx, best_idx); …]` arrays on `ColIdx`.
* **Scratch-buffer reuse**: the per-person Vec allocations (`bests`, `avgs_i`,
  `avgs_real`, `avgs_sorted`, `uniq`, `solved`, `worsts`, `medians`,
  `att_vs`, `single_values`) were re-allocated roughly 500 k × 48 times. They
  are now fields on a `Scratch` struct owned by `calc()`, `clear()`-ed before
  each person so the capacity is retained. As part of this change the
  `SingleRow` struct shrank to a plain `Vec<Option<i32>>` — only the attempt
  `value` field was ever read downstream, so the other fields were dead code.

Net effect on the TopDown profile: L1-bound dropped from ~7 % → 4 %, total
instructions retired fell from 57.8 G → 55.5 G, and wall time dropped 0.6 s.
DRAM-bound % is unchanged (the working-set footprint is the same), so the
remaining ~2.7 s gap vs C++ is still dominated by DRAM latency — further
wins would need either a tighter layout for `Result333`/`Attempt` or a
columnar recomputation that streams over attempts once per event filter.
