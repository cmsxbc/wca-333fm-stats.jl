// Low-level statistical helpers matching the Julia semantics.

/// "Trim extremes" average used throughout: if n > 2, drop one min and one
/// max then take the mean of the rest, else None.
pub fn trim_avg_i(xs: &[i64]) -> Option<f64> {
    if xs.len() <= 2 { return None; }
    let mut sum: i128 = 0;
    let mut mn = i64::MAX;
    let mut mx = i64::MIN;
    for &v in xs {
        sum += v as i128;
        if v < mn { mn = v; }
        if v > mx { mx = v; }
    }
    let n = (xs.len() - 2) as f64;
    Some(((sum - mn as i128 - mx as i128) as f64) / n)
}

fn pairwise_sum(xs: &[f64]) -> f64 {
    let n = xs.len();
    if n <= 16 {
        let mut s = 0.0;
        for &v in xs { s += v; }
        return s;
    }
    let m = n / 2;
    pairwise_sum(&xs[..m]) + pairwise_sum(&xs[m..])
}

pub fn trim_avg_f(xs: &[f64]) -> Option<f64> {
    if xs.len() <= 2 { return None; }
    let mut v: Vec<f64> = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    let sum = pairwise_sum(&v[1..n-1]);
    Some(sum / ((n - 2) as f64))
}

/// In-place variant: sorts `buf` (which must be a copy of `xs`) and returns trim avg.
pub fn trim_avg_f_in_place(buf: &mut [f64]) -> Option<f64> {
    if buf.len() <= 2 { return None; }
    buf.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = buf.len();
    let sum = pairwise_sum(&buf[1..n-1]);
    Some(sum / ((n - 2) as f64))
}

pub fn mean_i(xs: &[i64]) -> f64 {
    let s: i128 = xs.iter().map(|&v| v as i128).sum();
    s as f64 / xs.len() as f64
}

pub fn mean_f(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

/// StatsBase.std = corrected sample std (divide by n-1). Returns NaN for n=1.
pub fn std_i(xs: &[i64]) -> f64 {
    let n = xs.len();
    if n < 2 { return f64::NAN; }
    let m = mean_i(xs);
    let mut s = 0.0;
    for &v in xs {
        let d = v as f64 - m;
        s += d * d;
    }
    (s / (n as f64 - 1.0)).sqrt()
}

pub fn std_f(xs: &[f64]) -> f64 {
    let n = xs.len();
    if n < 2 { return f64::NAN; }
    let m = mean_f(xs);
    let mut s = 0.0;
    for &v in xs {
        let d = v - m;
        s += d * d;
    }
    (s / (n as f64 - 1.0)).sqrt()
}

/// Statistics.median. For integer input the Julia code either keeps Float
/// (promotes) or wraps with Int(...) where intended.
pub fn median_f_from_i(xs: &[i64]) -> f64 {
    let mut v: Vec<i64> = xs.to_vec();
    v.sort_unstable();
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2] as f64
    } else {
        (v[n / 2 - 1] + v[n / 2]) as f64 / 2.0
    }
}

/// In-place variant: `buf` must contain a copy of `xs`; it will be sorted.
pub fn median_f_from_i_in_place(buf: &mut [i64]) -> f64 {
    buf.sort_unstable();
    let n = buf.len();
    if n % 2 == 1 {
        buf[n / 2] as f64
    } else {
        (buf[n / 2 - 1] + buf[n / 2]) as f64 / 2.0
    }
}

/// Median as i64 (used for median_in_average), matching Int(median(...)).
/// Julia's Int() on a Float64 requires no fractional part; in practice we
/// call this on odd-sized arrays so it's safe.
pub fn median_i(xs: &[i64]) -> i64 {
    let mut v = xs.to_vec();
    v.sort_unstable();
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2
    }
}

/// In-place variant: `buf` must contain a copy of `xs`; it will be sorted.
pub fn median_i_in_place(buf: &mut [i64]) -> i64 {
    buf.sort_unstable();
    let n = buf.len();
    if n % 2 == 1 {
        buf[n / 2]
    } else {
        (buf[n / 2 - 1] + buf[n / 2]) / 2
    }
}

/// mode_count: pick the smallest element among the modes (most-frequent
/// values), return (that element, count-of-that-element).
pub fn mode_count_i(xs: &[i64]) -> (i64, i64) {
    use ahash::AHashMap;
    let mut counts: AHashMap<i64, i64> = AHashMap::new();
    for &v in xs { *counts.entry(v).or_insert(0) += 1; }
    let max = counts.values().copied().max().unwrap();
    let mn = counts
        .iter()
        .filter(|(_, c)| **c == max)
        .map(|(k, _)| *k)
        .min()
        .unwrap();
    (mn, max)
}

/// Sort-based mode_count that needs no hashmap. `buf` must be a mutable copy of xs.
/// Returns (smallest mode, count).
pub fn mode_count_i_in_place(buf: &mut [i64]) -> (i64, i64) {
    buf.sort_unstable();
    let mut best_val = buf[0];
    let mut best_cnt = 1i64;
    let mut cur_val = buf[0];
    let mut cur_cnt = 1i64;
    for i in 1..buf.len() {
        if buf[i] == cur_val {
            cur_cnt += 1;
        } else {
            if cur_cnt > best_cnt {
                best_cnt = cur_cnt;
                best_val = cur_val;
            }
            cur_val = buf[i];
            cur_cnt = 1;
        }
    }
    if cur_cnt > best_cnt {
        best_cnt = cur_cnt;
        best_val = cur_val;
    }
    (best_val, best_cnt)
}

/// calc_consecutive: sort unique; walk left-to-right; longest run where
/// every step's difference is in `diffs`. Return (length, start, end).
pub fn calc_consecutive(xs: &[i64], diffs: &[i64]) -> (i64, i64, i64) {
    let mut v: Vec<i64> = xs.to_vec();
    v.sort_unstable();
    v.dedup();
    let mut ccount = 1i64;
    let mut cstart = v[0];
    let mut cend = v[0];
    let mut cur_count = 1i64;
    let mut cur_start = v[0];
    for i in 1..v.len() {
        let d = v[i] - v[i - 1];
        if diffs.contains(&d) {
            cur_count += 1;
        } else {
            if cur_count > ccount {
                ccount = cur_count;
                cstart = cur_start;
                cend = v[i - 1];
            }
            cur_count = 1;
            cur_start = v[i];
        }
    }
    if cur_count > ccount {
        ccount = cur_count;
        cstart = cur_start;
        cend = *v.last().unwrap();
    }
    (ccount, cstart, cend)
}

/// In-place variant: `buf` must contain a copy of `xs`; it will be sorted/deduped.
pub fn calc_consecutive_in_place(buf: &mut Vec<i64>, diffs: &[i64]) -> (i64, i64, i64) {
    buf.sort_unstable();
    buf.dedup();
    let mut ccount = 1i64;
    let mut cstart = buf[0];
    let mut cend = buf[0];
    let mut cur_count = 1i64;
    let mut cur_start = buf[0];
    for i in 1..buf.len() {
        let d = buf[i] - buf[i - 1];
        if diffs.contains(&d) {
            cur_count += 1;
        } else {
            if cur_count > ccount {
                ccount = cur_count;
                cstart = cur_start;
                cend = buf[i - 1];
            }
            cur_count = 1;
            cur_start = buf[i];
        }
    }
    if cur_count > ccount {
        ccount = cur_count;
        cstart = cur_start;
        cend = *buf.last().unwrap();
    }
    (ccount, cstart, cend)
}

/// rolling mean of window n. Returns (last, min) as Option; None if len<n.
pub fn rolling_mean(xs: &[i64], n: usize) -> Option<(f64, f64)> {
    if xs.len() < n { return None; }
    let mut sum: i128 = xs[..n].iter().map(|&v| v as i128).sum();
    let denom = n as f64;
    let first = sum as f64 / denom;
    let mut min = first;
    let mut last = first;
    for i in n..xs.len() {
        sum += xs[i] as i128 - xs[i - n] as i128;
        last = sum as f64 / denom;
        if last < min { min = last; }
    }
    Some((last, min))
}

/// rolling "trimmed average" (extremes removed) of window n.
/// Returns (last, min). None if len<n. We keep a sorted window using a
/// sliding ordered structure (small n up to 100 => linear scan is fine).
pub fn rolling_trim_avg(xs: &[i64], n: usize) -> Option<(f64, f64)> {
    if xs.len() < n || n <= 2 { return None; }
    let denom = (n - 2) as f64;
    // Maintain window sum, and rely on window-wide min/max scan (O(n*W)).
    // Good enough: window sizes <= 100, total attempts small.
    let mut last = 0.0;
    let mut min = f64::INFINITY;
    for i in (n - 1)..xs.len() {
        let w = &xs[i + 1 - n..=i];
        let mut sum: i128 = 0;
        let mut mn = i64::MAX;
        let mut mx = i64::MIN;
        for &v in w {
            sum += v as i128;
            if v < mn { mn = v; }
            if v > mx { mx = v; }
        }
        let avg = (sum - mn as i128 - mx as i128) as f64 / denom;
        last = avg;
        if avg < min { min = avg; }
    }
    Some((last, min))
}
