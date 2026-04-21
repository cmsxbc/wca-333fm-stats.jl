# wca-333fm-stats.jl

WCA 3x3 Fewest Moves statistics with the Julia reference implementation in
[`WCAStats.jl`](WCAStats.jl).

A Rust port lives under [`rust-wca-stats/`](rust-wca-stats/) and a C++23 port
under [`cpp-wca-stats/`](cpp-wca-stats/); both produce value-equivalent CSV
outputs. The current Julia rewrite runs a full WCA export in about **48 s**
(vs. **~9.0 s** for Rust and **~7.7 s** for C++23). See the port READMEs for
usage, source layout and parity notes.

The Julia rewrite and its parity/performance notes are documented in
[`JULIA_REWRITE.md`](JULIA_REWRITE.md). Historical profiling notes for the
pre-rewrite DataFrames-based Julia implementation are kept in
[`PROFILE.md`](PROFILE.md).

Pipeline / cache / memory analysis of the two ports is in
[`PERF_ANALYSIS.md`](PERF_ANALYSIS.md) and can be regenerated on any machine
with [`perf_topdown.sh`](perf_topdown.sh).
