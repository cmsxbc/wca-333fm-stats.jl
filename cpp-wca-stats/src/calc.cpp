// Per-year computation mirroring Julia's calc(): filter results to event+year,
// group by person, compute round- and attempt-level statistics, then assign
// world and national competition ranks.

#include "calc.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <format>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "stats.hpp"

namespace wca {

// clang-format off
const std::array<ColumnSpec, 69> COLS = {{
    {"competitions",              ColKind::Int,   ColDir::Desc},
    {"rounds",                    ColKind::Int,   ColDir::Desc},
    {"best",                      ColKind::Int,   ColDir::Asc},
    {"best_max",                  ColKind::Int,   ColDir::Asc},
    {"best_count",                ColKind::Int,   ColDir::Desc},
    {"best_nunique",              ColKind::Int,   ColDir::Desc},
    {"best_mean",                 ColKind::Float, ColDir::Asc},
    {"best_std",                  ColKind::Float, ColDir::Asc},
    {"best_avg",                  ColKind::Float, ColDir::Asc},
    {"best_median",               ColKind::Float, ColDir::Asc},
    {"best_mode",                 ColKind::Int,   ColDir::Asc},
    {"best_mode_count",           ColKind::Int,   ColDir::Desc},
    {"best_consecutive",          ColKind::Int,   ColDir::Desc},
    {"best_consecutive_start",    ColKind::Int,   ColDir::Asc},
    {"best_consecutive_end",      ColKind::Int,   ColDir::Asc},
    {"average_attempts",          ColKind::Int,   ColDir::Desc},
    {"average",                   ColKind::Float, ColDir::Asc},
    {"average_max",               ColKind::Float, ColDir::Asc},
    {"average_count",             ColKind::Int,   ColDir::Desc},
    {"average_nunique",           ColKind::Int,   ColDir::Desc},
    {"average_mean",              ColKind::Float, ColDir::Asc},
    {"average_std",               ColKind::Float, ColDir::Asc},
    {"average_avg",               ColKind::Float, ColDir::Asc},
    {"average_median",            ColKind::Float, ColDir::Asc},
    {"average_mode",              ColKind::Float, ColDir::Asc},
    {"average_mode_count",        ColKind::Int,   ColDir::Desc},
    {"average_consecutive",       ColKind::Int,   ColDir::Desc},
    {"average_consecutive_start", ColKind::Float, ColDir::Asc},
    {"average_consecutive_end",   ColKind::Float, ColDir::Asc},
    {"gold",                      ColKind::Int,   ColDir::Desc},
    {"silver",                    ColKind::Int,   ColDir::Desc},
    {"bronze",                    ColKind::Int,   ColDir::Desc},
    {"chances",                   ColKind::Int,   ColDir::Desc},
    {"attempts",                  ColKind::Int,   ColDir::Desc},
    {"solved_count",              ColKind::Int,   ColDir::Desc},
    {"solved_nunique",            ColKind::Int,   ColDir::Desc},
    {"solved_mean",               ColKind::Float, ColDir::Asc},
    {"solved_std",                ColKind::Float, ColDir::Asc},
    {"solved_avg",                ColKind::Float, ColDir::Asc},
    {"solved_median",             ColKind::Float, ColDir::Asc},
    {"solved_mode",               ColKind::Int,   ColDir::Asc},
    {"solved_mode_count",         ColKind::Int,   ColDir::Desc},
    {"solved_min",                ColKind::Int,   ColDir::Asc},
    {"solved_max",                ColKind::Int,   ColDir::Asc},
    {"solved_consecutive",        ColKind::Int,   ColDir::Desc},
    {"solved_consecutive_start",  ColKind::Int,   ColDir::Asc},
    {"solved_consecutive_end",    ColKind::Int,   ColDir::Asc},
    {"solved_mo3_last",           ColKind::Float, ColDir::Asc},
    {"solved_mo3_best",           ColKind::Float, ColDir::Asc},
    {"solved_mo5_last",           ColKind::Float, ColDir::Asc},
    {"solved_mo5_best",           ColKind::Float, ColDir::Asc},
    {"solved_mo12_last",          ColKind::Float, ColDir::Asc},
    {"solved_mo12_best",          ColKind::Float, ColDir::Asc},
    {"solved_mo50_last",          ColKind::Float, ColDir::Asc},
    {"solved_mo50_best",          ColKind::Float, ColDir::Asc},
    {"solved_mo100_last",         ColKind::Float, ColDir::Asc},
    {"solved_mo100_best",         ColKind::Float, ColDir::Asc},
    {"solved_ao5_last",           ColKind::Float, ColDir::Asc},
    {"solved_ao5_best",           ColKind::Float, ColDir::Asc},
    {"solved_ao12_last",          ColKind::Float, ColDir::Asc},
    {"solved_ao12_best",          ColKind::Float, ColDir::Asc},
    {"solved_ao50_last",          ColKind::Float, ColDir::Asc},
    {"solved_ao50_best",          ColKind::Float, ColDir::Asc},
    {"solved_ao100_last",         ColKind::Float, ColDir::Asc},
    {"solved_ao100_best",         ColKind::Float, ColDir::Asc},
    {"avg_item_3rd_min",          ColKind::Int,   ColDir::Asc},
    {"avg_item_3rd_max",          ColKind::Int,   ColDir::Asc},
    {"avg_item_2nd_min",          ColKind::Int,   ColDir::Asc},
    {"avg_item_2nd_max",          ColKind::Int,   ColDir::Asc},
}};
// clang-format on

std::size_t col_idx(std::string_view name) {
    for (std::size_t i = 0; i < COLS.size(); ++i) {
        if (COLS[i].name == name) return i;
    }
    throw std::runtime_error(std::format("unknown column: {}", name));
}

namespace {

// The order of _rank / _nr columns that Julia emits.  Stored in a function-
// local static so it's built once and reused.
constexpr std::array DESC_ORDER = std::to_array<std::string_view>({
    "competitions", "rounds", "chances", "attempts",
    "solved_count", "solved_nunique", "solved_mode_count", "solved_consecutive",
    "best_count", "best_nunique", "best_mode_count", "best_consecutive",
    "average_attempts", "average_count", "average_nunique", "average_mode_count", "average_consecutive",
    "gold", "silver", "bronze",
});

}  // namespace

const std::vector<std::size_t>& rank_col_order() {
    static const std::vector<std::size_t> order = [] {
        std::vector<std::size_t> out;
        out.reserve(COLS.size());
        for (std::size_t i = 0; i < COLS.size(); ++i) {
            if (COLS[i].dir == ColDir::Asc) out.push_back(i);
        }
        for (auto n : DESC_ORDER) out.push_back(col_idx(n));
        return out;
    }();
    return order;
}

namespace {

Row make_row(std::uint32_t pk, const Person& p) {
    return Row{
        .person_key  = pk,
        .person_id   = p.wca_id,
        .person_name = p.name,
        .country_id  = p.country_id,
        .gender      = p.gender,
        .vals        = std::vector<Cell>(COLS.size()),
        .ranks       = std::vector<Cell>(COLS.size()),
        .nrs         = std::vector<Cell>(COLS.size()),
    };
}

inline void set(Row& r, std::string_view name, Cell c) {
    r.vals[col_idx(name)] = std::move(c);
}

void compute_row(Row& row,
                 const WcaData& data,
                 std::span<const std::size_t> result_idxs) {
    using i64 = std::int64_t;

    // Round-level collections
    std::vector<const Result333*> rs;
    rs.reserve(result_idxs.size());
    for (auto i : result_idxs) rs.push_back(&data.results[i]);

    // competitions (unique) and rounds
    std::unordered_set<std::uint32_t> comp_set;
    comp_set.reserve(rs.size());
    for (auto* r : rs) comp_set.insert(r->comp_key);
    set(row, "competitions", Cell{static_cast<i64>(comp_set.size())});
    set(row, "rounds",       Cell{static_cast<i64>(rs.size())});

    // best stats (best > 0)
    std::vector<i64> bests;
    bests.reserve(rs.size());
    for (auto* r : rs) if (r->best > 0) bests.push_back(r->best);
    if (!bests.empty()) {
        auto [mn, mx] = std::ranges::minmax(bests);
        set(row, "best",         Cell{mn});
        set(row, "best_max",     Cell{mx});
        set(row, "best_count",   Cell{static_cast<i64>(bests.size())});
        auto uniq = bests;
        std::ranges::sort(uniq);
        uniq.erase(std::ranges::unique(uniq).begin(), uniq.end());
        set(row, "best_nunique", Cell{static_cast<i64>(uniq.size())});
        set(row, "best_mean",    Cell{stats::mean_i(bests)});
        set(row, "best_std",     Cell{stats::std_i(bests)});
        if (auto v = stats::trim_avg_i(bests)) set(row, "best_avg", Cell{*v});
        set(row, "best_median",  Cell{stats::median_f_from_i(bests)});
        auto [mode, mc] = stats::mode_count_i(bests);
        set(row, "best_mode",       Cell{mode});
        set(row, "best_mode_count", Cell{mc});
        static constexpr i64 d1[] = {1};
        auto [cc, cs, ce] = stats::calc_consecutive(bests, d1);
        set(row, "best_consecutive",       Cell{cc});
        set(row, "best_consecutive_start", Cell{cs});
        set(row, "best_consecutive_end",   Cell{ce});
    }

    // average_attempts (average != 0)
    i64 avg_attempts = 0;
    for (auto* r : rs) if (r->average != 0) ++avg_attempts;
    if (avg_attempts > 0) set(row, "average_attempts", Cell{avg_attempts});

    // average stats (average > 0)
    std::vector<i64> avgs_i;
    for (auto* r : rs) if (r->average > 0) avgs_i.push_back(r->average);
    if (!avgs_i.empty()) {
        std::vector<double> avgs_real;
        avgs_real.reserve(avgs_i.size());
        for (auto v : avgs_i) avgs_real.push_back(static_cast<double>(v) / 100.0);
        auto [mn, mx] = std::ranges::minmax(avgs_real);
        set(row, "average",       Cell{mn});
        set(row, "average_max",   Cell{mx});
        set(row, "average_count", Cell{static_cast<i64>(avgs_real.size())});
        auto uniq = avgs_i;
        std::ranges::sort(uniq);
        uniq.erase(std::ranges::unique(uniq).begin(), uniq.end());
        set(row, "average_nunique", Cell{static_cast<i64>(uniq.size())});
        set(row, "average_mean",    Cell{stats::mean_f(avgs_real)});
        set(row, "average_std",     Cell{stats::std_f(avgs_real)});
        if (auto v = stats::trim_avg_f(avgs_real))
            set(row, "average_avg", Cell{*v});
        auto sorted = avgs_real;
        std::ranges::sort(sorted);
        const auto n = sorted.size();
        const double med = (n % 2 == 1)
            ? sorted[n / 2]
            : (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
        set(row, "average_median", Cell{med});
        auto [mode_i, mc] = stats::mode_count_i(avgs_i);
        set(row, "average_mode",       Cell{static_cast<double>(mode_i) / 100.0});
        set(row, "average_mode_count", Cell{mc});
        static constexpr std::int64_t dd[] = {33, 34};
        auto [cc, cs, ce] = stats::calc_consecutive(avgs_i, dd);
        set(row, "average_consecutive",       Cell{cc});
        set(row, "average_consecutive_start", Cell{static_cast<double>(cs) / 100.0});
        set(row, "average_consecutive_end",   Cell{static_cast<double>(ce) / 100.0});
    }

    // Medals: final rounds ("f"/"c") with best > 0; count pos 1/2/3.
    i64 g = 0, s = 0, b = 0;
    bool any_final_best = false;
    for (auto* r : rs) {
        if (r->best > 0 && (r->round_type_id == "f" || r->round_type_id == "c")) {
            any_final_best = true;
            switch (r->pos) { case 1: ++g; break; case 2: ++s; break; case 3: ++b; break; }
        }
    }
    if (any_final_best) {
        set(row, "gold",   Cell{g});
        set(row, "silver", Cell{s});
        set(row, "bronze", Cell{b});
    }

    // Single-attempt level: iterate results in id order; the attempts for
    // each result were already sorted by attempt_number during loading.
    std::vector<const Result333*> sorted_rs = rs;
    std::ranges::sort(sorted_rs, {}, &Result333::id);

    struct SingleRow {
        std::int64_t   result_id;
        std::optional<std::int32_t> value;   // nullopt for a result with no attempts
        const Result333* result;
    };
    std::vector<SingleRow> single;
    single.reserve(sorted_rs.size() * 5);
    for (auto* r : sorted_rs) {
        auto it = data.attempts_by_result.find(r->id);
        if (it != data.attempts_by_result.end() && !it->second.empty()) {
            for (const auto& a : it->second) {
                single.push_back({r->id, a.value, r});
            }
        } else {
            single.push_back({r->id, std::nullopt, r});
        }
    }

    set(row, "chances", Cell{static_cast<i64>(single.size())});
    i64 attempts_count = 0;
    for (const auto& sr : single) if (sr.value && *sr.value > -2) ++attempts_count;
    set(row, "attempts", Cell{attempts_count});

    std::vector<i64> solved;
    solved.reserve(single.size());
    for (const auto& sr : single) {
        if (sr.value && *sr.value > 0) solved.push_back(*sr.value);
    }
    if (!solved.empty()) {
        set(row, "solved_count", Cell{static_cast<i64>(solved.size())});
        auto uniq = solved;
        std::ranges::sort(uniq);
        uniq.erase(std::ranges::unique(uniq).begin(), uniq.end());
        set(row, "solved_nunique", Cell{static_cast<i64>(uniq.size())});
        set(row, "solved_mean",    Cell{stats::mean_i(solved)});
        set(row, "solved_std",     Cell{stats::std_i(solved)});
        if (auto v = stats::trim_avg_i(solved)) set(row, "solved_avg", Cell{*v});
        set(row, "solved_median",  Cell{stats::median_f_from_i(solved)});
        auto [mode, mc] = stats::mode_count_i(solved);
        set(row, "solved_mode",       Cell{mode});
        set(row, "solved_mode_count", Cell{mc});
        auto [mn, mx] = std::ranges::minmax(solved);
        set(row, "solved_min", Cell{mn});
        set(row, "solved_max", Cell{mx});
        static constexpr i64 d1[] = {1};
        auto [cc, cs, ce] = stats::calc_consecutive(solved, d1);
        set(row, "solved_consecutive",       Cell{cc});
        set(row, "solved_consecutive_start", Cell{cs});
        set(row, "solved_consecutive_end",   Cell{ce});

        for (std::size_t n : {3uz, 5uz, 12uz, 50uz, 100uz}) {
            if (auto r = stats::rolling_mean(solved, n)) {
                set(row, std::format("solved_mo{}_last", n), Cell{r->first});
                set(row, std::format("solved_mo{}_best", n), Cell{r->second});
            }
        }
        for (std::size_t n : {5uz, 12uz, 50uz, 100uz}) {
            if (auto r = stats::rolling_trim_avg(solved, n)) {
                set(row, std::format("solved_ao{}_last", n), Cell{r->first});
                set(row, std::format("solved_ao{}_best", n), Cell{r->second});
            }
        }
    }

    // avg_item 3rd/2nd: per-result max and median of attempt values over
    // results with average > 0, then per-person extrema.
    std::vector<i64> worsts;
    std::vector<i64> medians;
    for (auto* r : sorted_rs) {
        if (r->average <= 0) continue;
        auto it = data.attempts_by_result.find(r->id);
        if (it == data.attempts_by_result.end() || it->second.empty()) continue;
        std::vector<i64> vs;
        vs.reserve(it->second.size());
        for (const auto& a : it->second) vs.push_back(a.value);
        worsts.push_back(*std::ranges::max_element(vs));
        medians.push_back(stats::median_i(vs));
    }
    if (!worsts.empty()) {
        auto [wmn, wmx] = std::ranges::minmax(worsts);
        set(row, "avg_item_3rd_min", Cell{wmn});
        set(row, "avg_item_3rd_max", Cell{wmx});
        auto [mmn, mmx] = std::ranges::minmax(medians);
        set(row, "avg_item_2nd_min", Cell{mmn});
        set(row, "avg_item_2nd_max", Cell{mmx});
    }
}

// Competition ranking ("1224"): equal values share the smallest rank; the
// next distinct value takes rank = position + 1.  NaN sorts after all numbers
// and each NaN is distinct (matching Julia's isless on floats).
std::vector<Cell> competerank_col(std::span<const Row> rows,
                                  std::size_t ci,
                                  ColDir dir,
                                  std::span<const std::size_t> subset = {}) {
    const bool has_subset = !subset.empty();
    const auto n = has_subset ? subset.size() : rows.size();

    std::vector<Cell> out(n);
    struct KV { std::size_t k; double v; };
    std::vector<KV> present;
    present.reserve(n);
    for (std::size_t k = 0; k < n; ++k) {
        const auto& r = has_subset ? rows[subset[k]] : rows[k];
        if (auto f = as_f64(r.vals[ci])) present.push_back({k, *f});
    }
    if (present.empty()) return out;

    auto cmp = [](double a, double b) {
        const bool an = std::isnan(a), bn = std::isnan(b);
        if (an && bn) return std::strong_ordering::equal;
        if (an) return std::strong_ordering::greater;
        if (bn) return std::strong_ordering::less;
        return a < b ? std::strong_ordering::less
             : a > b ? std::strong_ordering::greater
                     : std::strong_ordering::equal;
    };
    std::ranges::stable_sort(present, [&](const KV& x, const KV& y) {
        auto o = cmp(x.v, y.v);
        return dir == ColDir::Desc ? o > 0 : o < 0;
    });

    std::int64_t rank = 1;
    bool have_prev = false;
    double prev = 0.0;
    for (std::size_t pos = 0; pos < present.size(); ++pos) {
        const auto [k, v] = present[pos];
        // Julia: NaN != NaN so each NaN is its own group.
        const bool eq = have_prev && !std::isnan(v) && !std::isnan(prev) && prev == v;
        if (!eq) rank = static_cast<std::int64_t>(pos) + 1;
        out[k] = Cell{rank};
        have_prev = true;
        prev = v;
    }
    return out;
}

void compute_ranks(std::vector<Row>& rows) {
    for (std::size_t ci = 0; ci < COLS.size(); ++ci) {
        auto dir = COLS[ci].dir;
        auto ranks = competerank_col(rows, ci, dir);
        for (std::size_t i = 0; i < rows.size(); ++i) rows[i].ranks[ci] = std::move(ranks[i]);
    }
    // National ranks.
    std::unordered_map<std::string, std::vector<std::size_t>> by_country;
    for (std::size_t i = 0; i < rows.size(); ++i) {
        by_country[rows[i].country_id].push_back(i);
    }
    for (std::size_t ci = 0; ci < COLS.size(); ++ci) {
        auto dir = COLS[ci].dir;
        for (auto& [_, idxs] : by_country) {
            auto ranks = competerank_col(rows, ci, dir, idxs);
            for (std::size_t j = 0; j < idxs.size(); ++j)
                rows[idxs[j]].nrs[ci] = std::move(ranks[j]);
        }
    }
}

}  // namespace

Frame calc(const WcaData& data, std::uint16_t event_id, YearFilter yf) {
    std::vector<std::size_t> kept;
    kept.reserve(data.results.size() / 16);
    for (std::size_t i = 0; i < data.results.size(); ++i) {
        const auto& r = data.results[i];
        if (r.event_id != event_id) continue;
        if (!yf.matches(data.competitions[r.comp_key].year)) continue;
        kept.push_back(i);
    }

    std::unordered_map<std::uint32_t, std::vector<std::size_t>> by_person;
    by_person.reserve(kept.size() / 4);
    for (auto i : kept) by_person[data.results[i].person_key].push_back(i);

    // Stable deterministic order: by wca_id (all our person_keys point at
    // sub_id==1 entries, so they're all "matched" persons).
    std::vector<std::uint32_t> person_order;
    person_order.reserve(by_person.size());
    for (const auto& [pk, _] : by_person) person_order.push_back(pk);
    std::ranges::sort(person_order, [&](auto a, auto b) {
        return data.persons[a].wca_id < data.persons[b].wca_id;
    });

    std::vector<Row> rows;
    rows.reserve(person_order.size());
    for (auto pk : person_order) {
        Row r = make_row(pk, data.persons[pk]);
        compute_row(r, data, by_person[pk]);
        rows.push_back(std::move(r));
    }

    compute_ranks(rows);
    return Frame{.rows = std::move(rows), .year_filter = yf};
}

Row row_delta(const Row& self, const Row& other) {
    auto sub = [](const Cell& a, const Cell& b, ColKind kind) -> Cell {
        if (is_missing(a) || is_missing(b)) return {};
        auto x = as_f64(a), y = as_f64(b);
        if (!x || !y) return {};
        const double d = *x - *y;
        return kind == ColKind::Int
                   ? Cell{static_cast<std::int64_t>(d)}
                   : Cell{d};
    };
    Row r = self;
    for (std::size_t i = 0; i < COLS.size(); ++i) {
        r.vals[i]  = sub(self.vals[i],  other.vals[i],  COLS[i].kind);
        r.ranks[i] = sub(self.ranks[i], other.ranks[i], ColKind::Int);
        r.nrs[i]   = sub(self.nrs[i],   other.nrs[i],   ColKind::Int);
    }
    if (self.category) {
        std::string_view c = *self.category;
        if (c.ends_with("-year")) {
            r.category = std::format("{}-year-detla", c.substr(0, c.size() - 5));
        } else {
            r.category = std::format("{}-detla", c);
        }
    }
    return r;
}

}  // namespace wca
