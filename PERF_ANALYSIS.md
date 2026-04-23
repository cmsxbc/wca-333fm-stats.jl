# Pipeline / Memory Analysis: C++ vs Rust WCAStats Ports

This note captures the `perf`-based top-down pipeline analysis that compares
the C++23 and Rust ports of `WCAStats.jl` on a full WCA export run.

* Input: `WCA_export_v2_110_20260420T000013Z.tsv.zip` (~110 MB zipped)
* Binaries:
  * `cpp-wca-stats/build/wca-stats` (g++ 15, `-O3 -flto`, `libzip` + `libdeflate`)
  * `rust-wca-stats/target/release/wca-stats` (rustc stable, `zip` crate with
    `deflate-zlib-ng` + hand-rolled `memchr` TSV parser)
* Hardware: 13th Gen Intel i7-13700H (Raptor Lake-H), Linux 6.19, `perf` 6.19.
  All measurements are pinned to the P-cores via `taskset -c 0-11` so that
  `cpu_core/` events do not mix with `cpu_atom/`.

> For the same comparison on **AMD Ryzen 9 7950X (Zen 4)**, extended to
> include the Julia reference implementation, see
> [`PERF_ANALYSIS_ZEN4.md`](./PERF_ANALYSIS_ZEN4.md).

To reproduce (or re-run on a different machine) use the
[`perf_topdown.sh`](./perf_topdown.sh) script in the repo root. It auto-picks
P-cores, runs three top-down levels plus a raw counter block, and emits a
`summary.txt` under `perf-reports/<binary>-<stamp>/`.

```sh
./perf_topdown.sh ./cpp-wca-stats/build/wca-stats  <zip>
./perf_topdown.sh ./rust-wca-stats/target/release/wca-stats <zip>
```

---

## 1. TopDown L1 — where pipeline slots go

| Bucket            | **C++ (~7.9 s)** | **Rust (~10.6 s)** |
|-------------------|------------------|---------------------|
| Frontend bound    | 13.4 %           | 9.2 %               |
| Bad speculation   | 15.5 %           | 15.8 %              |
| **Retiring**      | **31.5 %**       | 24.4 %              |
| **Backend bound** | 39.6 %           | **50.7 %**          |

The Rust port spends **11 more points of slots** in the backend than the C++
port — that is the bulk of its extra wall-time.

## 2. TopDown L2 — backend split

| Bucket        | C++     | Rust       |
|---------------|---------|------------|
| Memory bound  | 20.5 %  | **33.1 %** |
| Core bound    | 12.9 %  | 12.9 %     |

`Core bound` is identical, so the extra backend stalls are *entirely* in the
memory-bound bucket.

## 3. TopDown L3 — memory split (the headline)

| Bucket         | C++     | Rust       |
|----------------|---------|------------|
| L1 bound       | 11.3 %  | 7.0 %      |
| L2 bound       | 5.3 %   | 3.6 %      |
| L3 bound       | 2.9 %   | 1.9 %      |
| **DRAM bound** | 1.2 %   | **31.7 %** |
| Store bound    | 0.9 %   | 0.6 %      |

Rust's single biggest TopDown bucket in the entire program is **DRAM-bound at
31.7 %**, vs. 1.2 % for C++ — a **~26×** difference. The Rust port is
**waiting on main memory for almost a third of its pipeline slots**.

## 4. Raw counters

| Counter                        | C++           | Rust          |
|--------------------------------|---------------|---------------|
| Cycles                         | 26.4 G        | 33.6 G        |
| Instructions                   | 61.5 G        | 57.8 G        |
| **IPC**                        | **2.33**      | 1.72          |
| L1 d-cache loads               | 12.0 G        | 10.5 G        |
| L1 d-cache load-miss rate      | **1.56 %**    | **3.78 %**    |
| LLC loads                      | 250 M         | 317 M         |
| LLC load-miss rate             | 84 %          | 72 %          |
| **LLC misses per 1k inst**     | **3.42**      | 3.97          |
| dTLB loads                     | 12.1 G        | 10.5 G        |
| **dTLB load-miss rate**        | **0.073 %**   | **0.41 %**    |
| Branch miss rate               | 0.30 %        | 0.32 %        |

Rust executes ~6 % fewer instructions than C++, but each instruction on
average takes ~35 % more cycles (IPC 2.33 vs 1.72) — consistent with "waiting
for cache lines" being the limiting factor rather than raw compute.

The TLB miss rate for Rust is **5.6 × higher**, which also points to its hot
path touching a much wider working-set footprint than C++'s.

---

## Verdict

**C++ (~7.9 s): not memory/cache bound.**

Retiring 31.5 % of slots with IPC 2.33 is healthy. Memory stalls exist but
are almost entirely hits on short-latency caches (L1 11.3 %, L2 5.3 %);
DRAM accounts for just 1.2 % of slots. The remaining budget is spread across
bad speculation (15.5 %), frontend/decode (13.4 %, dominated by `mite`), and
core-execution. No single bottleneck dominates — this is a reasonably
well-balanced program.

**Rust (~10.6 s): yes, memory/DRAM bound.**

Backend stalls consume 50.7 % of slots; of those, 33.1 % are memory-bound
and **31.7 % of the total are specifically DRAM-bound**. The counters
corroborate the story:

* L1 d-cache misses: **2.4 × more frequent** than C++.
* dTLB misses: **5.6 × more frequent** than C++.
* IPC collapses from 2.33 → 1.72.

The parser side is already at parity with C++ (load-time numbers are within
~2 s; see the port READMEs). The remaining gap lives in `calc::compute_row`,
which is called once per `(year, filter)` × person. Likely contributors:

* Per-person `AHashMap` lookups into the multi-GB persons/competitions/
  results tables (random access, cold lines).
* Fresh `Vec<SingleRow>`, `Vec<&Result333>`, `sorted_rs.clone()` allocations
  per row — defeats both temporal locality and allocator re-use.
* Fine-grained `Vec::push` loops instead of pre-sized writes.

**Highest-leverage next optimisations** (if further Rust work is pursued):

1. Reuse scratch `Vec`s across `compute_row` calls instead of reallocating.
2. Replace the cloned `Vec<&Result333>` per person with an index range into
   a once-per-event sort, so the hot loop walks contiguous memory.
3. Group result/attempt data by person once up front so per-person calls
   become dense linear scans over a cache-friendly slab.

None of these are required for correctness — all 48 output CSVs are
byte-identical across the two ports — but they directly target the 31.7 %
DRAM-bound slot budget that currently separates Rust from C++.

---

## Follow-up — calc-path micro-optimisations (Rust, post-analysis)

Acting on items (1) and a partial (2) from the list above, two calc-only
changes landed without touching the loader:

* **Precomputed column indices** replace the linear scan in `set(row, name,
  cell)` — the 69-entry `COLS` slice was otherwise scanned ~500 k × 48 × ~70
  times. `ColIdx` stores a `usize` for every referenced column plus two small
  `[(n, last_idx, best_idx); …]` arrays that remove the `format!` allocations
  from the rolling-mean hot loops.
* **`Scratch` struct reuses per-person Vec buffers** (`bests`, `avgs_i`,
  `avgs_real`, `avgs_sorted`, `uniq`, `solved`, `worsts`, `medians`,
  `att_vs`, `single_values`). Capacity is retained across the ~500 k × 48
  person × event-filter calls instead of re-allocating each time. As part of
  this refactor `SingleRow` collapses to a plain `Vec<Option<i32>>`; the
  other struct fields were never read.

Before → after (best-of-5, full export, same machine):

| Metric                 | Before (4341538) | After   | Delta  |
|------------------------|------------------|---------|--------|
| Wall time              | 11.24 s          | 10.60 s | −0.64 s |
| Instructions retired   | 57.8 G           | 55.5 G  | −2.3 G |
| Cycles                 | 33.6 G           | 33.6 G  | ≈0     |
| IPC                    | 1.72             | 1.65    | −0.07  |
| L1 d-cache miss rate   | 3.78 %           | 4.02 %  | +0.24  |
| **TopDown L1 bound**   | **7.0 %**        | **4.0 %** | **−3.0** |
| TopDown DRAM bound     | 31.7 %           | 33.5 %  | +1.8   |
| TopDown backend bound  | 46.6 %           | 43.9 %  | −2.7   |

Interpretation: the wins came from **doing less work**, not from becoming
less memory-bound. Instructions retired dropped by 2.3 G (the eliminated
linear scans + `format!` + per-person Vec allocations) and the L1-bound slot
share fell 3 points (fewer small re-allocations hitting hot cache lines).
DRAM-bound % actually nudged up because the same ~235 M LLC misses are now
spread over fewer cycles — the working-set footprint is unchanged.

To cut the remaining ~2.7 s gap vs the C++ port, the next steps would need
to target that working-set itself: pack `Result333` / `Attempt` more tightly
(remove padding, split cold string fields off the hot path), or recompute
columnar summaries once per event filter rather than per person. Both are
structural changes and were deferred because earlier attempts to restructure
the loader regressed load time by 1–3 s.

The output remains byte-identical to the committed `results-rust/` goldens
(0/48 diffs via `cmp -s`).

---

## Follow-up 2 — structural memory-layout optimisations (Rust, 2026-04-21)

Acting on the structural suggestions deferred above, a second round of changes
targeted the working-set footprint and memory-access patterns:

1. **`Result333::round_type_id` shrunk from `String` → `u8`.**
   Rust's `String` has no small-string optimisation (SSO); every one-char
   round-type string ("f", "c", "1" …) was a separate 24-byte heap allocation.
   C++ `std::string` stores these inline via SSO. Replacing with a raw byte
   eliminated millions of tiny allocations and reduced the struct size from
   ~56 bytes to ~32 bytes.

2. **Scratch-based in-place statistical helpers.**
   `median_f_from_i`, `trim_avg_f`, `mode_count_i`, and `calc_consecutive` were
   rewritten as `_in_place` variants that sort/dedup on caller-provided scratch
   buffers instead of allocating fresh `Vec`s internally. This removed ~15
   small allocations per person × ~20 k persons × 48 filters.

3. **`rank_col_order()` cached in `OnceLock`.**
   The 69-element ordering vector was previously rebuilt on every CSV-header
   and every rank-column write.

4. **`event_years` / `person_event_years` — `AHashSet` → boolean scan.**
   Replaced hash-set deduplication with a `Vec<bool>` seen-array (C++ already
   did this).

5. **Sorted `data.results` by `(person_key, id)` during loading.**
   This makes each person's results contiguous in memory. It enabled:
   * Replacing the `by_person: AHashMap<u32, Vec<usize>>` in `calc()` with a
     single linear scan of `kept`.
   * Eliminating the per-person `rs: Vec<&Result333>` allocation.
   * Removing the per-person `sorted_rs.sort_by_key(|r| r.id)` (now free).

Before → after (best-of-3 clean pinned runs, full export, same machine):

| Metric                 | Before (stable) | After (optimised) | Delta   |
|------------------------|-----------------|-------------------|---------|
| Wall time (clean)      | **10.67 s**     | **8.99 s**        | **−1.68 s** |
| Cycles                 | 33.6 G          | 31.2 G            | −2.4 G  |
| Instructions           | 57.8 G          | 56.1 G            | −1.7 G  |
| **IPC**                | 1.72            | **1.80**          | **+0.08** |
| L1 d-cache miss rate   | 3.78 %          | 3.00 %            | −0.78   |
| LLC misses / 1k inst   | 3.97            | 3.26              | −0.71   |
| dTLB load-miss rate    | 0.41 %          | 0.39 %            | −0.02   |
| TopDown DRAM bound     | 31.7 %          | 28.3 %            | −3.4    |
| TopDown backend bound  | 50.7 %          | 45.0 %            | −5.7    |

Interpretation: the program is **still DRAM-bound**, but the bound dropped
from 31.7 % to 28.3 % of slots. The `round_type_id` SSO fix and the sorted
contiguous layout reduced LLC misses per kilo-instruction by **18 %** and
pushed IPC up from 1.72 → 1.80. The remaining ~1.3 s gap vs C++ (~7.7 s)
likely lives in `libzip`+`libdeflate` decompression (loader) and the
`attempts_by_result: AHashMap<i64, Vec<Attempt>>` hash lookups in the hot
`compute_row` path, both of which are harder to close without re-architecting
the data model.

The 48 output CSVs remain byte-identical to the C++ port (`diff -rq` clean).
