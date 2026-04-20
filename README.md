# wca-333fm-stats.jl

WCA 3x3 Fewest Moves statistics, originally implemented in Julia
([`WCAStats.jl`](WCAStats.jl)).

A Rust port lives under [`rust-wca-stats/`](rust-wca-stats/) and produces
value-equivalent CSV outputs roughly **7× faster** than the Julia version on a
full WCA export. See [`rust-wca-stats/README.md`](rust-wca-stats/README.md) for
details on usage, source layout, parity notes and benchmark numbers.
