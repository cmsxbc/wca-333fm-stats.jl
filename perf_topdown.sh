#!/usr/bin/env bash
# perf_topdown.sh - recall profiling runner for the C++ and Rust ports.
#
# Runs a binary under `perf stat` and collects the data used in
# PERF_ANALYSIS.md:
#   * TopDown L1 (frontend / bad-spec / retiring / backend)
#   * TopDown L2 (backend -> memory-bound / core-bound)
#   * TopDown L3 (memory-bound -> L1 / L2 / L3 / DRAM / store)
#   * A raw-counter block (IPC, L1/LLC/dTLB miss rates, branches)
#
# The script pins execution to P-cores via taskset so hybrid CPUs
# (e.g. 13th-gen Intel) don't mix cpu_core + cpu_atom counters.
#
# Usage:
#   ./perf_topdown.sh <binary> [args...]
#
# Examples:
#   ./perf_topdown.sh ./cpp-wca-stats/build/wca-stats \
#       WCA_export_v2_110_20260420T000013Z.tsv.zip
#   ./perf_topdown.sh ./rust-wca-stats/target/release/wca-stats \
#       WCA_export_v2_110_20260420T000013Z.tsv.zip
#
# Outputs are written to perf-reports/<basename>-<timestamp>/ as four
# text files (td1.txt, td2.txt, td3.txt, counters.txt) plus a short
# summary.txt that extracts the headline numbers.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <binary> [args...]" >&2
    exit 2
fi

if ! command -v perf >/dev/null 2>&1; then
    echo "error: perf not found in PATH" >&2
    exit 1
fi

BIN=$1
shift
ARGS=("$@")

# Pick the P-core CPU list on hybrid Intel; fall back to all cpus.
PCORES=$(lscpu --all --parse=CPU,CORE,MAXMHZ 2>/dev/null \
    | awk -F, '!/^#/ { print $1 "," $3 }' \
    | sort -t, -k2 -n -r \
    | awk -F, 'NR==1{max=$2} $2==max{print $1}' \
    | paste -sd,)
if [[ -z "${PCORES:-}" ]]; then
    PCORES=$(nproc --all | awk '{print "0-"$1-1}')
fi

OUT_ROOT=${PERF_OUT:-perf-reports}
stamp=$(date +%Y%m%d-%H%M%S)
name=$(basename "$BIN")
OUT_DIR="$OUT_ROOT/${name}-${stamp}"
mkdir -p "$OUT_DIR"

echo "binary : $BIN ${ARGS[*]}"
echo "cpus   : $PCORES (P-cores)"
echo "output : $OUT_DIR"
echo

run_perf() {
    local label=$1
    local out=$2
    shift 2
    echo "--- $label ---"
    # Drop stdout from the binary, keep perf's stderr summary.
    taskset -c "$PCORES" perf stat "$@" -- "$BIN" "${ARGS[@]}" \
        >/dev/null 2>"$out" || true
    # Strip the binary's own logs (everything before the perf block),
    # keeping just the "Performance counter stats" section.
    awk '/Performance counter stats/{found=1} found' "$out" > "$out.trim" \
        && mv "$out.trim" "$out"
    cat "$out"
    echo
}

run_perf "TopDown L1" "$OUT_DIR/td1.txt"            --topdown --td-level 1
run_perf "TopDown L2" "$OUT_DIR/td2.txt"            --topdown --td-level 2
run_perf "TopDown L3" "$OUT_DIR/td3.txt"            --topdown --td-level 3
run_perf "Raw counters" "$OUT_DIR/counters.txt" \
    -e cpu_core/cycles/ \
    -e cpu_core/instructions/ \
    -e cpu_core/cache-references/ \
    -e cpu_core/cache-misses/ \
    -e cpu_core/L1-dcache-loads/ \
    -e cpu_core/L1-dcache-load-misses/ \
    -e cpu_core/LLC-loads/ \
    -e cpu_core/LLC-load-misses/ \
    -e cpu_core/dTLB-loads/ \
    -e cpu_core/dTLB-load-misses/ \
    -e cpu_core/branch-misses/

# Extract the headline numbers into a one-page summary.
python3 - "$OUT_DIR" <<'PY' > "$OUT_DIR/summary.txt"
import os, re, sys
d = sys.argv[1]

def read(p):
    with open(os.path.join(d, p)) as f: return f.read()

def nums(text):
    # Numbers in the topdown header line appear whitespace-separated under
    # the column headers.  We extract them by finding the header row and
    # the row that follows.
    rows = [l for l in text.splitlines() if l.strip()]
    # last row of floats is our data row
    for l in reversed(rows):
        vals = re.findall(r'[-+]?\d*\.\d+|\d+', l)
        if len(vals) >= 2 and all('.' in v or v.isdigit() for v in vals):
            return vals
    return []

def kv_from_counters(text):
    out = {}
    for line in text.splitlines():
        m = re.match(r'\s*([\d,]+)\s+([A-Za-z0-9_\-./]+)', line)
        if m:
            out[m.group(2)] = int(m.group(1).replace(',', ''))
    return out

td1 = read('td1.txt')
td2 = read('td2.txt')
td3 = read('td3.txt')
cc = kv_from_counters(read('counters.txt'))

def pct(k, total):
    if k not in cc or total not in cc or cc[total]==0: return 'n/a'
    return f"{100*cc[k]/cc[total]:.2f}%"

ipc = cc.get('cpu_core/instructions/u',0) / max(cc.get('cpu_core/cycles/u',1),1)
lkp = lambda k: f"{cc[k]:,}" if k in cc else 'n/a'

print(f"== summary : {d} ==\n")
print("# TopDown L1\n" + td1)
print("# TopDown L2\n" + td2)
print("# TopDown L3\n" + td3)
print("# Raw counters\n" + read('counters.txt'))
print("# Derived")
print(f"IPC                    : {ipc:.2f}")
print(f"L1 d-cache miss rate   : "
      + pct('cpu_core/L1-dcache-load-misses/u','cpu_core/L1-dcache-loads/u'))
print(f"LLC load miss rate     : "
      + pct('cpu_core/LLC-load-misses/u','cpu_core/LLC-loads/u'))
print(f"dTLB load miss rate    : "
      + pct('cpu_core/dTLB-load-misses/u','cpu_core/dTLB-loads/u'))
if 'cpu_core/instructions/u' in cc and 'cpu_core/LLC-load-misses/u' in cc:
    mpki = 1000 * cc['cpu_core/LLC-load-misses/u'] / cc['cpu_core/instructions/u']
    print(f"LLC misses / kilo-inst : {mpki:.2f}")
PY

echo "summary written to: $OUT_DIR/summary.txt"
