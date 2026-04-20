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
| `default` (all years) | 128.4 s | 11.0 s | **~11.7×** |
| `summary <id>` | ~148 s | ~20 s | ~7.4× |

The loader reads each zipped TSV into a single buffer (zlib-ng decompression)
and parses it with a hand-rolled `memchr`-based splitter and a custom
`FromDecimal` integer parser (no UTF-8 validation on the numeric hot path).
Load time dropped from ≈ 9.8 s (with `csv` + `BufReader`) to ≈ 6.9 s. Per-year
compute + CSV write is 0.01–0.2 s each.
