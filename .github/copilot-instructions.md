# Copilot Instructions

## Repository overview

Three parallel implementations of the same thing: per-year WCA 3x3 Fewest Moves
(`333fm`) statistics computed from a WCA results export zip, emitting a matching
set of CSVs under `./results/` (24 years × {`in`,`to`} = 48 files by default).

- `WCAStats.jl` — the original Julia reference implementation (single file,
  `module WCAStats`). **This is the semantic source of truth.**
- `rust-wca-stats/` — Rust port. Value-equivalent to Julia; byte-identical to
  C++.
- `cpp-wca-stats/` — C++23 port. Byte-identical to the Rust port and to the
  reference Julia run (modulo documented float-printing quirks).

When modifying any port, the other ports and `WCAStats.jl` must be treated as
the cross-check. Output parity (ideally byte-identical, at minimum
value-equivalent at 10 sig figs) is the acceptance criterion, not "looks right".

## Build / run commands

Input is always the official WCA export zip (e.g.
`WCA_export_v2_…_tsv.zip`). `download_wca_results.sh <dir>` fetches today's
export.

Julia:
```sh
julia --project=. -e 'include("WCAStats.jl")' -- <zip>
julia --project=. -e 'include("WCAStats.jl")' -- <zip> --year 2024 topk best --k 10
./profile.sh <zip>   # --profile --pprof wrapper
```

Rust (from `rust-wca-stats/`):
```sh
cargo build --release
./target/release/wca-stats <zip>
./target/release/wca-stats <zip> topk average --k 10 --country China
./target/release/wca-stats <zip> person 2011TRON02 2012PARK03
./target/release/wca-stats <zip> summary 2011TRON02
cargo test          # run all tests
cargo test <name>   # run a single test by name substring
```

C++ (from `cpp-wca-stats/`, needs C++23, `libzip`, `libdeflate`):
```sh
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/wca-stats <zip>
./build/wca-stats <zip> --year 2024 topk best 10
```

Perf profiling (Linux `perf`, pins to P-cores):
```sh
./perf_topdown.sh ./rust-wca-stats/target/release/wca-stats <zip>
./perf_topdown.sh ./cpp-wca-stats/build/wca-stats <zip>
```

There is no repository-wide test suite; parity is checked by running each
binary on the same zip and diffing `./results/results.{in,to}<year>.csv`.

## Architecture (all three ports)

The pipeline is identical across languages; layout mirrors the Julia reference:

1. **Load** the zip and parse four TSVs: `Persons`, `Competitions`, `Results`,
   `Result_attempts` (Julia also reads the lowercase names). → `loader.{rs,cpp}`.
2. **Prepare once**: filter results to event `333fm`, left-join with
   `competitions.year`, rename columns to camelCase
   (`competitionId`, `personId`, `roundTypeId`); select `persons[sub_id==1]`.
3. **Per year, per category** (`in` = `==year`, `to` = `<=year`):
   - `stats_round_result` — per-`personId` aggregation over rounds
     (`best`, `average`, `pos` for medals on round types `f`/`c`).
   - `stats_single_result` — per-`personId` aggregation over single attempts
     (`value > 0`), including `solved_mo{3,5,12,50,100}` rolling means and
     `solved_ao{5,12,50,100}` rolling trim-averages.
   - `stats_round_non_best_result` — `avg_item_2nd`/`_3rd` min/max over
     attempts that participated in valid averages.
   - Right-join against persons so unmatched persons appear at the tail.
   - Emit two rank columns for every stat column: `<col>_rank` (global) and
     `<col>_nr` (per-`countryId`), using `competerank`. 20 columns are ranked
     descending (see `DESC_ORDER` in `calc.rs`); everything else ascending.
4. **CLI subcommands** (all ports): default (write 48 CSVs), `topk`, `person`,
   `summary <id>` (also writes `results/<id>.csv` with `in-year`/`to-year`
   rows plus `*-detla` rows — typo preserved intentionally).

## Conventions & parity gotchas

These are load-bearing. Changing them breaks cross-port parity — do not
"clean them up" without updating every port and verifying CSVs still match.

- **`trim_avg`** ("WCA average" of N): drops min+max, averages the rest;
  returns `missing`/`NaN` for `len ≤ 2`. Julia uses `sum(xs) - sum(extrema(xs))`
  divided by `l - 2`. Rust/C++ implement pairwise summation (threshold `n ≤ 16`)
  to match Julia's `@simd` reduction bit-for-bit.
- **`rolling_mean`** uses `sum / n`, not `sum * (1/n)` (matters for ULPs).
- **`average_real = average / 100`**: the integer `average` field is cs; real
  averages are in seconds. Rolling windows run on the solved subset (`value > 0`)
  ordered by `(result_id, attempt_number)`.
- **`competerank` with NaN**: each NaN is its own group; rank follows input
  order. C++ uses `std::ranges::stable_sort` to preserve this; Rust replicates
  Julia's `isless` ordering.
- **Medal counts** (`gold`/`silver`/`bronze`) are only incremented on round
  types `"f"` (final) and `"c"` (combined-final) with `best > 0`.
- **Rank-column emission order**: asc columns in `COLS` order first, then the
  20 desc columns in Julia source order (`DESC_ORDER` in `calc.rs`).
- **Summary quirks**: the misspelling `"detla"` (for "delta") is intentional
  and must stay; the current-year row is pushed before the delta row; meta
  fields (`personName`, `countryId`, `gender`) are carried across years.
- **Float printing**: ports use Ryu-style shortest round-trip and append `.0`
  for integral doubles to match Julia's `CSV.write` output. ±1 ULP differences
  in `average_avg` vs Julia are known-accepted (documented in
  `rust-wca-stats/README.md`).
- **Rust `Result333::round_type_id` is `u8`**, not `String` — one-char values
  like `"f"`, `"c"`, `"1"` would otherwise each heap-allocate (no SSO in Rust
  `String`). Don't widen this back to `String`.
- **Rust `data.results` is sorted by `(person_key, id)` at load time** so
  per-person slices are contiguous. Several downstream simplifications depend
  on this invariant (no per-person `HashMap`, no per-person re-sort).

## Performance context

`PERF_ANALYSIS.md` and `PROFILE.md` hold the current TopDown L1/L2/L3 numbers
and the optimisation history. Before proposing calc-path changes in Rust/C++,
re-read the "Calc-path micro-optimisations" and "Structural memory-layout
optimisations" sections of `rust-wca-stats/README.md` — several obvious
"simplifications" (linear column-name lookups, per-row `format!`, per-person
`Vec` allocation, `AHashMap`-indexed grouping) have already been removed and
should not be re-introduced.
