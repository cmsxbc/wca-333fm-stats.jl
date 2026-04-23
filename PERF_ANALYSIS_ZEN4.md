# TopDown Pipeline Analysis on Ryzen 9 7950X (Zen 4)

Companion to [`PERF_ANALYSIS.md`](./PERF_ANALYSIS.md) (which covers the Intel
Raptor Lake-H port comparison). This note extends the analysis to:

* a different µarch — **AMD Zen 4** — where the TopDown sub-buckets differ
  from Intel's (`PipelineL1`/`PipelineL2` metric groups)
* a **three-way** comparison including the Julia reference implementation

All numbers cover the **whole program** (load + per-year calc + CSV save).

## Setup

* Host: `homelab2`, **AMD Ryzen 9 7950X** (Zen 4, 16 cores / 32 threads)
* Kernel: 6.19.12-arch1-1, `perf` from the same kernel
* Governor: `powersave` with boost enabled (not `performance` — no passwordless
  sudo on the box), so absolute frequency may vary run-to-run. Cycle-count
  based metrics below are frequency-independent and were stable to <1 %
  across 5 warm, interleaved runs.
* Pinning: `taskset -c 0` (SMT sibling `16` idle during the run)
* Input: `WCA_export_v2_110_20260420T000013Z.tsv.zip` (337 MB on disk),
  warmed into the page cache before every measurement.
* Toolchain
  * julia 1.12.6
  * rustc 1.95.0, `--release` (thin-LTO, `codegen-units = 1`)
  * g++ 15.2.1, `-O3 -flto`, `libzip` + `libdeflate`
* Output parity: all three binaries produce **byte-identical** CSVs on this
  input (`diff -rq` clean, 48 files each).

Raw `perf stat` logs are kept under
[`perf-reports/7950x-20260423/`](./perf-reports/7950x-20260423/)
(one directory per port, containing `td1.txt`, `td2.txt`, `counters.txt`).

Each TopDown measurement is a separate `perf stat` run; the 50 % /  25 % numbers
shown for individual events in the raw logs are the per-group multiplexing
fractions (AMD's 6-counter PMU has to multiplex the full TopDown set).

## 1. Wall-time summary (median of 5 interleaved warm runs)

| lang  | total (ms) |   cycles         | instructions   |  IPC  | vs Julia |
|-------|-----------:|-----------------:|---------------:|------:|---------:|
| julia |     18 043 |   96.32 B        |  150.31 B      | 1.56  |   1.00 × |
| rust  |      6 814 |   35.97 B        |   56.28 B      | 1.56  |   2.65 × |
| cpp   |  **4 940** |   24.30 B        |   61.04 B      |**2.51**| **3.65 ×** |

Two things jump out vs. the Intel numbers in `PERF_ANALYSIS.md`:

1. **The C++/Rust gap is wider on Zen 4** than on Raptor Lake (1.38× here vs.
   ~1.35× on the Intel box), despite being run on the same source trees and
   the same input.
2. **Rust and Julia have essentially identical IPC (1.56)** but Rust executes
   **2.7× fewer instructions**, which is where its entire 2.65× speedup over
   Julia comes from. Rust is not "faster because of the CPU"; it is faster
   because it does strictly less work.

## 2. TopDown L1 — dispatch-slot breakdown (AMD PipelineL1)

| Bucket           | julia      | rust       | cpp        |
|------------------|-----------:|-----------:|-----------:|
| Retiring         |   24.9 %   |   25.1 %   | **39.1 %** |
| Frontend bound   |   19.1 %   |    9.5 %   |   15.7 %   |
| Bad speculation  |    7.4 %   |    8.9 %   |    8.5 %   |
| Backend bound    |   48.5 %   | **56.5 %** |   36.8 %   |
| SMT contention   |   ~0.0 %   |    0.1 %   |    0.1 %   |

**C++ retires 39 % of slots; Rust and Julia only ~25 %.** That 14-point
retiring deficit on Rust is the entire source of its per-cycle disadvantage.
The slack is picked up almost entirely by the **backend**, not by frontend or
bad-speculation.

## 3. TopDown L2 — backend split (AMD PipelineL2)

| Bucket                    | julia      | rust       | cpp        |
|---------------------------|-----------:|-----------:|-----------:|
| **backend_bound_memory**  |   41.9 %   | **48.3 %** |   31.3 %   |
| backend_bound_cpu         |    5.3 %   |    8.3 %   |    5.4 %   |
| frontend_bound_latency    |   12.7 %   |    5.2 %   |    8.5 %   |
| frontend_bound_bandwidth  |    7.1 %   |    4.3 %   |    7.3 %   |
| bad_spec_mispredicts      |    7.3 %   |    8.8 %   |    8.3 %   |
| bad_spec_pipeline_restarts|    0.2 %   |    0.2 %   |    0.1 %   |
| retiring_fastpath         |   25.1 %   |   24.7 %   |   38.7 %   |
| retiring_microcode        |    0.2 %   |    0.5 %   |    0.4 %   |

**Rust loses the most cycles to `backend_bound_memory`** (48.3 %, vs C++'s
31.3 % — a 17-point gap). `backend_bound_cpu` (execution-unit contention,
dependency chains) is only 3 points higher; this is almost all a memory-stall
story, matching what the Intel TopDown L3 split showed.

Julia's frontend story is different — 12.7 % is `frontend_bound_latency`
(iTLB / icache misses, long decode flushes) vs. only 5.2 % for Rust. That's
consistent with JIT-compiled code pathologically churning the I-cache.

## 4. Raw cache / TLB counters

From the 10-event raw block:

| counter            |  julia       |  rust       |  cpp        |  rust / cpp |
|--------------------|-------------:|------------:|------------:|------------:|
| cycles             |   98.83 B    |  35.88 B    |  24.20 B    |   1.48 ×    |
| instructions       |  151.15 B    |  56.19 B    |  60.97 B    |   0.92 ×    |
| **IPC**            |   **1.53**   |  **1.57**   |  **2.52**   |             |
| branches           |   25.22 B    |  10.77 B    |  12.93 B    |   0.83 ×    |
| branch-misses      |    0.67 B    |   0.19 B    |   0.17 B    |   1.11 ×    |
| branch-miss rate   |    2.65 %    |   1.76 %    |   1.32 %    |             |
| L1-dcache-loads    |   56.92 B    |  15.91 B    |  16.92 B    |   0.94 ×    |
| L1-dcache-misses   |    2.07 B    |   0.76 B    |   0.80 B    |   0.95 ×    |
| L1 miss rate       |    3.64 %    |   4.76 %    |   4.72 %    |             |
| LLC cache-misses   |  843.9 M     | 218.5 M     |  90.9 M     | **2.40 ×**  |
| dTLB-loads         |  349.9 M     |  98.98 M    |  43.78 M    |             |
| dTLB-load-misses   |   35.51 M    |  37.25 M    |  13.66 M    | **2.73 ×**  |
| dTLB miss rate     |   10.15 %    |  37.63 %    |  31.21 %    |             |

The Rust↔C++ gap is **not** in L1 (within 1 %) and **not** in the amount of
branching code. It is concentrated in:

* **LLC misses: 2.4 ×** more than C++
* **dTLB misses: 2.7 ×** more than C++

i.e. Rust's working set touches more 4 KB pages and misses the last-level
cache more often, even though the hot loops themselves fit fine in L1. That
points at **memory layout / allocation patterns**, not at the compute code:
likely candidates are the `ahash`-backed `HashMap` node storage, per-person
`Vec` allocations, and the `String` fields in `Row` / `Person`. The Intel
port analysis (`PERF_ANALYSIS.md` § 3) reached the same conclusion from the
TopDown L3 "DRAM bound" split; Zen 4 confirms it at the TLB level as well.

## 5. Takeaways

1. **Measured on Zen 4, the ranking and the qualitative story are unchanged
   from Raptor Lake**: C++ > Rust > Julia, with the C++↔Rust gap being
   memory-stall dominated and *not* an IPC-of-compute-code problem.
2. **Rust has headroom.** It retires at the same IPC as the Julia JIT (~1.56)
   on a CPU whose physical limit is ~6. 48 % of its cycles are spent waiting
   on memory; those are the cycles to chase.
3. **Julia's extra cost over Rust is a pure instruction-count story** (2.7 ×
   more instructions at the same IPC). Zen 4 also shows a real, but small,
   frontend-latency tax that Rust/C++ don't pay — the JIT does not place code
   as friendly to the I-cache as AOT binaries do.
4. **C++ leads on Zen 4 more than on Raptor Lake** (3.65 × julia vs.
   Rust's 2.65 ×). The AMD core's better memory subsystem disproportionately
   rewards the port that stalls least on memory — i.e. the one that already
   had the best dTLB and LLC numbers on Intel.
