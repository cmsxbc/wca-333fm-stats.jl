#pragma once
// Small statistical helpers that mirror WCAStats.jl semantics.  Header-only
// for inlining; they're called in tight per-person loops.

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

namespace wca::stats {

using i64 = std::int64_t;
using i128 = __int128_t;

// "Trim extremes" average: if n > 2, drop one min + one max and take mean.
inline std::optional<double> trim_avg_i(std::span<const i64> xs) noexcept {
    if (xs.size() <= 2) return std::nullopt;
    i128 sum = 0;
    i64 mn = std::numeric_limits<i64>::max();
    i64 mx = std::numeric_limits<i64>::min();
    for (auto v : xs) {
        sum += v;
        mn = std::min(mn, v);
        mx = std::max(mx, v);
    }
    const double n = static_cast<double>(xs.size() - 2);
    return static_cast<double>(sum - static_cast<i128>(mn) - static_cast<i128>(mx)) / n;
}

// Mean helpers.
inline double mean_i(std::span<const i64> xs) noexcept {
    i128 s = 0;
    for (auto v : xs) s += v;
    return static_cast<double>(s) / static_cast<double>(xs.size());
}

inline double mean_f(std::span<const double> xs) noexcept {
    double s = 0.0;
    for (auto v : xs) s += v;
    return s / static_cast<double>(xs.size());
}

// StatsBase.std: corrected sample std (divide by n-1); NaN for n<2.
inline double std_i(std::span<const i64> xs) noexcept {
    const auto n = xs.size();
    if (n < 2) return std::numeric_limits<double>::quiet_NaN();
    const double m = mean_i(xs);
    double s = 0.0;
    for (auto v : xs) {
        const double d = static_cast<double>(v) - m;
        s += d * d;
    }
    return std::sqrt(s / static_cast<double>(n - 1));
}

inline double std_f(std::span<const double> xs) noexcept {
    const auto n = xs.size();
    if (n < 2) return std::numeric_limits<double>::quiet_NaN();
    const double m = mean_f(xs);
    double s = 0.0;
    for (auto v : xs) {
        const double d = v - m;
        s += d * d;
    }
    return std::sqrt(s / static_cast<double>(n - 1));
}

// Trimmed average over a float slice (used for average_avg).
inline double pairwise_sum(std::span<const double> xs) {
    const auto n = xs.size();
    if (n <= 16) {
        double s = 0.0;
        for (double v : xs) s += v;
        return s;
    }
    const auto m = n / 2;
    return pairwise_sum(xs.subspan(0, m)) + pairwise_sum(xs.subspan(m));
}

inline std::optional<double> trim_avg_f(std::span<const double> xs) {
    if (xs.size() <= 2) return std::nullopt;
    std::vector<double> v(xs.begin(), xs.end());
    std::ranges::sort(v);
    const double sum = pairwise_sum(std::span<const double>(v).subspan(1, v.size() - 2));
    return sum / static_cast<double>(v.size() - 2);
}

inline double median_f_from_i(std::span<const i64> xs) {
    std::vector<i64> v(xs.begin(), xs.end());
    std::ranges::sort(v);
    const auto n = v.size();
    return (n % 2 == 1) ? static_cast<double>(v[n / 2])
                        : (static_cast<double>(v[n / 2 - 1]) +
                           static_cast<double>(v[n / 2])) / 2.0;
}

inline i64 median_i(std::span<const i64> xs) {
    std::vector<i64> v(xs.begin(), xs.end());
    std::ranges::sort(v);
    const auto n = v.size();
    return (n % 2 == 1) ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2;
}

// Pick smallest value among most-frequent; return (value, count).
inline std::pair<i64, i64> mode_count_i(std::span<const i64> xs) {
    std::unordered_map<i64, i64> counts;
    counts.reserve(xs.size());
    for (auto v : xs) ++counts[v];
    i64 max_count = 0;
    for (auto [_, c] : counts) max_count = std::max(max_count, c);
    i64 chosen = std::numeric_limits<i64>::max();
    for (auto [k, c] : counts) {
        if (c == max_count) chosen = std::min(chosen, k);
    }
    return {chosen, max_count};
}

// Longest "consecutive" run over sorted-unique xs where step diff ∈ diffs.
// Returns (length, start, end).
inline std::tuple<i64, i64, i64>
calc_consecutive(std::span<const i64> xs, std::span<const i64> diffs) {
    std::vector<i64> v(xs.begin(), xs.end());
    std::ranges::sort(v);
    v.erase(std::ranges::unique(v).begin(), v.end());

    i64 ccount = 1, cstart = v.front(), cend = v.front();
    i64 cur_count = 1, cur_start = v.front();
    for (std::size_t i = 1; i < v.size(); ++i) {
        const i64 d = v[i] - v[i - 1];
        if (std::ranges::find(diffs, d) != diffs.end()) {
            ++cur_count;
        } else {
            if (cur_count > ccount) {
                ccount = cur_count;
                cstart = cur_start;
                cend   = v[i - 1];
            }
            cur_count = 1;
            cur_start = v[i];
        }
    }
    if (cur_count > ccount) {
        ccount = cur_count;
        cstart = cur_start;
        cend   = v.back();
    }
    return {ccount, cstart, cend};
}

// Rolling arithmetic mean over window n; returns (last, min).
inline std::optional<std::pair<double, double>>
rolling_mean(std::span<const i64> xs, std::size_t n) {
    if (xs.size() < n) return std::nullopt;
    i128 sum = 0;
    for (std::size_t i = 0; i < n; ++i) sum += xs[i];
    const double denom = static_cast<double>(n);
    double last = static_cast<double>(sum) / denom;
    double mn = last;
    for (std::size_t i = n; i < xs.size(); ++i) {
        sum += static_cast<i128>(xs[i]) - static_cast<i128>(xs[i - n]);
        last = static_cast<double>(sum) / denom;
        mn = std::min(mn, last);
    }
    return std::pair{last, mn};
}

// Rolling trimmed average over window n (O(|xs|*n)); fine for n <= 100.
inline std::optional<std::pair<double, double>>
rolling_trim_avg(std::span<const i64> xs, std::size_t n) {
    if (xs.size() < n || n <= 2) return std::nullopt;
    const double denom = static_cast<double>(n - 2);
    double last = 0.0;
    double mn = std::numeric_limits<double>::infinity();
    for (std::size_t i = n - 1; i < xs.size(); ++i) {
        i128 sum = 0;
        i64 lo = std::numeric_limits<i64>::max();
        i64 hi = std::numeric_limits<i64>::min();
        for (std::size_t j = i + 1 - n; j <= i; ++j) {
            sum += xs[j];
            lo = std::min(lo, xs[j]);
            hi = std::max(hi, xs[j]);
        }
        const double a =
            static_cast<double>(sum - static_cast<i128>(lo) - static_cast<i128>(hi)) / denom;
        last = a;
        mn = std::min(mn, a);
    }
    return std::pair{last, mn};
}

}  // namespace wca::stats
