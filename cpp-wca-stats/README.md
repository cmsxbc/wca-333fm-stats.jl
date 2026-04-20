# cpp-wca-stats

C++23 port of [`WCAStats.jl`](../WCAStats.jl), produced as a follow-up to the
Rust port in [`rust-wca-stats/`](../rust-wca-stats). It computes the same 333fm
year-by-year statistics and emits byte-identical CSVs.

## Build

Requires a C++23 compiler (tested with `g++ 15.2.1`) and `libzip`.

```sh
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

The resulting binary is `build/wca-stats`.

## Run

```sh
./build/wca-stats ../WCA_export_v2_110_20260420T000013Z.tsv.zip
# optional subcommands (mirror Rust):
./build/wca-stats <zip> --year 2024 topk best 10
./build/wca-stats <zip> person 2003HARR01 2010BENT01
```

48 CSVs are written to `./results/` (24 years × `in`/`to` categories).

## Modern C++ features used

* `std::format` / `std::print` / `std::println` (C++20/23)
* `std::ranges` algorithms, `std::ranges::stable_sort`
* `std::span`, `std::string_view`, `std::optional`, `std::variant`
* `std::from_chars` / `std::to_chars` — locale-free, shortest-round-trip
* Designated initializers, structured bindings in range-`for`
* `std::filesystem`, `std::expected` (C++23)
* `libzip` wrapped in `unique_ptr` with custom deleters

## Output parity

All 48 output CSVs are **byte-identical** to `results-rust/` (and to the
reference Julia run). To match Rust's floating-point behavior exactly, the
port replicates:

* Pairwise summation in `trim_avg_f` (threshold `n ≤ 16`, matches `stats.rs`).
* Competition-rank ordering with **stable** sort (each NaN is its own group
  whose rank follows input order, like Julia's `isless`).
* Ryu-style float formatting (appends `.0` for integral doubles).

## Benchmark

Input: `WCA_export_v2_110_20260420T000013Z.tsv.zip` (~110 MB zip, 333fm event).
Hardware: local workstation, `-O3 -flto`, best of 3 runs.

| Implementation | Time   | Relative |
|----------------|--------|----------|
| Julia (original)  | ~128 s | 12.7×    |
| Julia (optimized) | ~104 s | 10.3×    |
| Rust (release)    | ~11.0 s | 1.09×   |
| **C++23 (release)** | **~10.1 s** | **1.00×** |

The C++ build is roughly **8 % faster than the optimized Rust port** on this
workload. The remaining gap is dominated by:

* Plain `memcpy`-based buffered writes vs Rust's `BufWriter` + `write!` macros.
* Fewer allocations in the per-person calc path (std::vector reuse vs fresh
  `Vec`/`AHashSet` allocation per row).
* Aggressive LTO across 5 translation units.

Both ports are single-threaded; load+parse is ~6.9 s of the total on the Rust
side (after switching the `zip` crate to `zlib-ng` and replacing the `csv`
crate with a hand-rolled `memchr` TSV parser) and ~7 s on the C++ side.
