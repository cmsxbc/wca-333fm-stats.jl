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
