# cpp-wca-stats

C++23 port of [`WCAStats.jl`](../WCAStats.jl), produced as a follow-up to the
Rust port in [`rust-wca-stats/`](../rust-wca-stats). It computes the same 333fm
year-by-year statistics and emits byte-identical CSVs.

## Build

Requires a C++23 compiler (tested with `g++ 15.2.1`), `libzip`, and
`libdeflate` (used for faster inflate; falls back is not required).

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
| Julia (original)  | ~128 s | 16.2×    |
| Julia (optimized) | ~104 s | 13.2×    |
| Rust (release)    | ~10.9 s | 1.38×   |
| **C++23 (release)** | **~7.9 s** | **1.00×** |

The C++ build is roughly **28 % faster than the optimized Rust port** on this
workload. The gap is dominated by:

* **libdeflate** for inflate (vs zlib in libzip's default build and vs
  zlib-ng used by the Rust port). libdeflate is ~2× faster than zlib and
  ~1.3–1.5× faster than zlib-ng on this export.
* Transparent (heterogeneous) hash lookup on `std::string` maps keyed by
  `std::string_view`, which avoids the `std::string` temporary allocation
  the results loop used to pay for each of its millions of rows.
* Plain `memcpy`-based buffered writes vs Rust's `BufWriter` + `write!` macros.
* Fewer allocations in the per-person calc path (std::vector reuse vs fresh
  `Vec`/`AHashSet` allocation per row).
* Aggressive LTO across 5 translation units.

Both ports are single-threaded. After switching to libdeflate, C++ load+parse
is ~4.5 s of the total; Rust load+parse (with zlib-ng + a hand-rolled
`memchr` TSV parser) is ~6.9 s.
