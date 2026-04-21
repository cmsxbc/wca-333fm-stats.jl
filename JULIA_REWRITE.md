# Julia WCAStats Rewrite Notes

## Summary

The original `WCAStats.jl` used DataFrames.jl, CSV.jl, RollingFunctions, and StatsBase for all data processing. This incurred heavy framework overhead (~104 s for a full export) and made byte-identical parity with the Rust/C++ ports difficult.

This rewrite eliminates DataFrames.jl entirely, replacing it with:

- Manual TSV parsing over raw `Vector{UInt8}` (no CSV.jl)
- Custom structs (`WcaData`, `Result333`, `Person`, `Competition`, `Attempt`, `Row`, `Frame`)
- In-place statistical helpers with pre-allocated scratch buffers
- Manual buffered CSV writing with `ryu`-style float formatting

The rewrite achieves **byte-identical CSV parity** with the Rust reference (`results-rust-opt/`) across all 48 output files, while cutting runtime from ~104 s to ~48 s.

## Architecture Changes

### 1. Loader (`load_wca`)

**Before:** `CSV.File` + DataFrames for `WCA_export_Persons.tsv`, `Competitions.tsv`, `Results.tsv`, `Scrambles.tsv`.

**After:** Custom ZIP extraction and TSV parser:
- Reads each TSV into a `Vector{UInt8}` via `ZipFile`.
- Scans for newlines and tabs using `findnext` / `findfirst` on bytes.
- Parses integers with `parse_int` (manual digit accumulation).
- Builds lookup maps (`person_idx`, `comp_idx`, `event_idx`) for foreign keys.
- Sorts `results` by `(person_key, id)` so per-person slices are contiguous (matches Rust).

### 2. Data Structures

```julia
struct Result333
    id::Int64
    pos::Int32
    best::Int32
    average::Int32
    comp_key::UInt32
    round_type_id::UInt8
    person_key::UInt32
    event_id::UInt16
end

struct Row          # one output row
    person_key::UInt32
    country_id::String
    name::String
    gender::Char
    vals::Vector{Cell}    # 69 value columns (Union{Missing,Int64,Float64,String})
    ranks::Vector{Cell}   # 69 rank columns
    nrs::Vector{Cell}     # 69 national-rank columns
end

struct Frame
    rows::Vector{Row}
end
```

### 3. Stats (`stats.jl` logic inlined)

All statistical helpers operate on caller-provided buffers to avoid allocations:

- `mean_i(xs)` — sums via `Int128`, divides to `Float64`
- `mean_f(xs)` — **sequential summation** (matches Rust `Iterator::sum`)
- `std_f(xs)` — sequential squared-deviation sum (matches Rust)
- `trim_avg_f!(buf)` — sorts buffer, then calls `pairwise_sum_f` on the trimmed slice
- `pairwise_sum_f(xs)` — recursive pairwise split at `n/2`, sequential leaf for `n ≤ 16` (matches Rust exactly)
- `median_f_from_i!` / `median_f!` — sorts caller buffer, picks middle
- `mode_count_i!` — sorts + `unique!` on caller buffer
- `calc_consecutive!` — longest run with steps in allowed set
- `rolling_mean` / `rolling_trim_avg` — `O(n·W)` window scan (windows ≤ 100)

### 4. Calc (`calc!`)

- Filters `results` by event/year into per-person contiguous slices.
- `compute_row` receives a reusable `Scratch` struct with pre-allocated `Vector`s (`bests`, `avgs_i`, `avgs_real`, `solved`, `tmp_i64`, `tmp_f64`, etc.).
- At the start of each row, `clear_all!(sc)` resets lengths to zero (no `empty!` reallocation).

### 5. Ranking (`competerank_col`)

Matches Julia `StatsBase.competerank` semantics exactly:
- Skips `missing` values.
- Sorts with NaN as greatest (Julia `isless` behavior).
- Assigns **minimum rank** on ties (e.g., three people tied at 31.5 all get rank 225).
- Each NaN is its own group (no tie-grouping with other NaNs).

Column ordering for rank/nr emission follows `RANK_COL_ORDER`:
1. All `:Asc` columns in schema order.
2. Then `:Desc` columns in the explicit `DESC_ORDER` list.

### 6. Output (`write_csv`)

- Manual `IOBuffer` writing.
- Floats use `Base.show(io, v)` which produces Ryu-style shortest decimal (appends `.0` for integral doubles, matching Rust).
- Strings containing `,`, `"`, or `\n` are quoted and escaped.
- Missing values emitted as empty fields.

## Key Parity Fixes

| Issue | Cause | Fix |
|-------|-------|-----|
| Column ordering wrong | Ranks/nrs emitted in `COLS` schema order instead of `RANK_COL_ORDER` | `write_csv` now loops over `RANK_COL_ORDER` for both rank and nr blocks |
| Float precision in `average_avg` | Julia `sum` uses full pairwise summation; Rust `mean_f` uses sequential, `trim_avg_f` uses threshold-16 pairwise | Implement Rust's exact `pairwise_sum_f` algorithm; use sequential for `mean_f` |
| Rank off-by-one | Sub-ULP float differences changed sort order in `competerank_col` | Matched summation algorithms so derived floats are bit-identical |
| Person name newlines | Some WCA names contain `\n` | `write_str_csv` quotes strings containing `\n` |
| Tie handling | Needed min-rank for ties, NaN after all reals | Custom `nan_lt_asc` / `nan_lt_desc` comparators + `prev == v` tie check |

## Performance

| Implementation | Full export runtime |
|----------------|---------------------|
| Original Julia (DataFrames.jl) | ~104 s |
| Rewrite Julia (zero DataFrames) | ~48 s |
| Rust optimized | ~9 s |
| C++23 | ~7.7 s |

The remaining ~5× gap vs Rust is primarily due to:
1. Julia's GC and allocation overhead in `compute_row` (hash lookups, string construction).
2. Lack of inlining / loop unrolling guarantees compared to Rust/LLVM.
3. Julia's `sort!` overhead on small vectors inside tight loops.

Further gains would require aggressive manual loop fusion and possibly `@inbounds @simd` annotations on the hot paths.

## Files Changed

- `WCAStats.jl` — complete rewrite (1,449 insertions, 417 deletions)

## Patch

`wca-stats-rewrite.patch` is generated from `git diff WCAStats.jl`.
