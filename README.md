# wca-333fm-stats.jl

WCA 3x3 Fewest Moves statistics, originally implemented in Julia
([`WCAStats.jl`](WCAStats.jl)).

A Rust port lives under [`rust-wca-stats/`](rust-wca-stats/) and a C++23 port
under [`cpp-wca-stats/`](cpp-wca-stats/); both produce value-equivalent CSV
outputs. The C++23 port runs a full WCA export in about **7.9 s**
(vs. ~10.9 s for Rust and ~128 s for Julia). See the port READMEs for usage,
source layout and parity notes.

Pipeline / cache / memory analysis of the two ports is in
[`PERF_ANALYSIS.md`](PERF_ANALYSIS.md) and can be regenerated on any machine
with [`perf_topdown.sh`](perf_topdown.sh).
