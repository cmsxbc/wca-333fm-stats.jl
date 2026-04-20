// CSV writing and terminal output.  We use std::to_chars for fast, locale-
// independent, shortest-round-trip formatting of floats and ints, so there is
// no std::ostream overhead.

#include "output.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <format>
#include <fstream>
#include <iterator>
#include <print>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace wca {

namespace {

class BufWriter {
public:
    explicit BufWriter(const std::filesystem::path& path) : f_(std::fopen(path.string().c_str(), "wb")) {
        if (!f_) throw std::runtime_error(std::format("open file: {}", path.string()));
        buf_.resize(1 << 20);
    }
    ~BufWriter() { flush(); if (f_) std::fclose(f_); }
    BufWriter(const BufWriter&) = delete;
    BufWriter& operator=(const BufWriter&) = delete;

    void write(std::string_view s) {
        if (s.size() > buf_.size()) { flush(); std::fwrite(s.data(), 1, s.size(), f_); return; }
        if (pos_ + s.size() > buf_.size()) flush();
        std::memcpy(buf_.data() + pos_, s.data(), s.size());
        pos_ += s.size();
    }
    void put(char c) {
        if (pos_ == buf_.size()) flush();
        buf_[pos_++] = c;
    }
    void flush() {
        if (pos_) { std::fwrite(buf_.data(), 1, pos_, f_); pos_ = 0; }
    }
private:
    std::FILE* f_{nullptr};
    std::vector<char> buf_;
    std::size_t pos_{0};
};

void write_i64(BufWriter& w, std::int64_t v) {
    std::array<char, 32> tmp;
    auto [p, _] = std::to_chars(tmp.data(), tmp.data() + tmp.size(), v);
    w.write({tmp.data(), static_cast<std::size_t>(p - tmp.data())});
}

void write_double_like_ryu(BufWriter& w, double v) {
    if (std::isnan(v))      { w.write("NaN"); return; }
    if (std::isinf(v))      { w.write(v > 0 ? "Inf" : "-Inf"); return; }
    std::array<char, 64> tmp;
    auto [p, ec] = std::to_chars(tmp.data(), tmp.data() + tmp.size(), v);
    std::string_view s{tmp.data(), static_cast<std::size_t>(p - tmp.data())};
    // std::to_chars with no format arg prints the shortest round-trip form.
    // For whole numbers like 1.0 it emits "1"; ryu emits "1.0".  Mirror ryu
    // so the CSV values are recognisable as floats.
    const bool has_dot = s.find('.') != std::string_view::npos;
    const bool has_exp = s.find('e') != std::string_view::npos
                      || s.find('E') != std::string_view::npos;
    w.write(s);
    if (!has_dot && !has_exp) w.write(".0");
}

void write_cell(BufWriter& w, const Cell& c, ColKind kind) {
    if (is_missing(c)) return;
    if (auto* p = std::get_if<std::int64_t>(&c)) { write_i64(w, *p); return; }
    if (auto* p = std::get_if<double>(&c)) {
        if (kind == ColKind::Int) write_i64(w, static_cast<std::int64_t>(*p));
        else                      write_double_like_ryu(w, *p);
    }
}

void write_csv_str(BufWriter& w, std::string_view s) {
    const bool needs_quote = s.find_first_of(",\"\n") != std::string_view::npos;
    if (!needs_quote) { w.write(s); return; }
    w.put('"');
    for (char ch : s) {
        if (ch == '"') w.write("\"\"");
        else           w.put(ch);
    }
    w.put('"');
}

void write_header(BufWriter& w, bool summary_tail) {
    w.write("personId,personName,countryId,gender");
    for (const auto& c : COLS) { w.put(','); w.write(c.name); }
    for (auto i : rank_col_order()) { w.put(','); w.write(COLS[i].name); w.write("_rank"); }
    for (auto i : rank_col_order()) { w.put(','); w.write(COLS[i].name); w.write("_nr"); }
    if (summary_tail) w.write(",year,category");
    w.put('\n');
}

void write_body_row(BufWriter& w, const Row& r) {
    write_csv_str(w, r.person_id);   w.put(',');
    write_csv_str(w, r.person_name); w.put(',');
    write_csv_str(w, r.country_id);  w.put(',');
    write_csv_str(w, r.gender);
    for (std::size_t i = 0; i < COLS.size(); ++i) {
        w.put(',');
        write_cell(w, r.vals[i], COLS[i].kind);
    }
    for (auto i : rank_col_order()) { w.put(','); write_cell(w, r.ranks[i], ColKind::Int); }
    for (auto i : rank_col_order()) { w.put(','); write_cell(w, r.nrs[i],   ColKind::Int); }
}

}  // namespace

void write_csv(const std::filesystem::path& path, const Frame& frame) {
    BufWriter w{path};
    write_header(w, /*summary_tail=*/false);
    for (const auto& r : frame.rows) {
        write_body_row(w, r);
        w.put('\n');
    }
}

void write_summary_csv(const std::filesystem::path& path,
                       std::span<const Row> rows) {
    BufWriter w{path};
    write_header(w, /*summary_tail=*/true);
    for (const auto& r : rows) {
        write_body_row(w, r);
        w.put(',');
        if (r.year) {
            std::array<char, 32> tmp;
            auto [p, _] = std::to_chars(tmp.data(), tmp.data() + tmp.size(), *r.year);
            w.write({tmp.data(), static_cast<std::size_t>(p - tmp.data())});
        }
        w.put(',');
        if (r.category) w.write(*r.category);
        w.put('\n');
    }
}

void print_topk(const Frame& frame, std::string_view col, std::size_t k,
                std::optional<std::string_view> country) {
    const auto ci = col_idx(col);
    std::vector<const Row*> filtered;
    filtered.reserve(frame.rows.size());
    for (const auto& r : frame.rows) {
        if (country && r.country_id != *country) continue;
        filtered.push_back(&r);
    }
    const bool use_nr = country.has_value();
    std::vector<std::pair<std::int64_t, const Row*>> with_rank;
    for (const Row* r : filtered) {
        const Cell& c = use_nr ? r->nrs[ci] : r->ranks[ci];
        if (auto p = std::get_if<std::int64_t>(&c); p && static_cast<std::size_t>(*p) <= k) {
            with_rank.emplace_back(*p, r);
        }
    }
    std::ranges::sort(with_rank, {}, &std::pair<std::int64_t, const Row*>::first);

    std::println("{:>20} {:>20} {:>20} {:>10} {:>10}",
                 "personName", "countryId", col,
                 std::format("{}_nr", col), std::format("{}_rank", col));
    for (const auto& [_, r] : with_rank) {
        auto fmt_cell = [](const Cell& c) -> std::string {
            if (is_missing(c)) return "";
            if (auto p = std::get_if<std::int64_t>(&c)) return std::format("{}", *p);
            if (auto p = std::get_if<double>(&c))       return std::format("{}", *p);
            return {};
        };
        std::println("{:>20} {:>20} {:>20} {:>10} {:>10}",
                     r->person_name, r->country_id,
                     fmt_cell(r->vals[ci]),
                     fmt_cell(r->nrs[ci]),
                     fmt_cell(r->ranks[ci]));
    }
}

void print_some_persons(const Frame& frame, std::span<const std::string> ids) {
    std::unordered_set<std::string_view> want;
    for (const auto& s : ids) want.insert(s);
    std::vector<const Row*> rows;
    for (const auto& r : frame.rows) if (want.contains(r.person_id)) rows.push_back(&r);
    if (rows.empty()) return;

    std::vector<std::size_t> name_lens;
    name_lens.reserve(rows.size());
    for (auto* r : rows) name_lens.push_back(std::max<std::size_t>(1, r->person_name.size()));

    std::vector<std::string> col_names{"personId", "personName", "countryId", "gender"};
    for (const auto& c : COLS) col_names.emplace_back(c.name);
    for (const auto& c : COLS) col_names.push_back(std::format("{}_rank", c.name));
    for (const auto& c : COLS) col_names.push_back(std::format("{}_nr",   c.name));
    std::size_t col_name_len = 0;
    for (const auto& n : col_names) col_name_len = std::max(col_name_len, n.size());

    auto print_meta = [&](const char* label, auto proj) {
        std::print("{:>{}}", label, col_name_len);
        for (std::size_t i = 0; i < rows.size(); ++i) {
            std::print("    {:>{}}", proj(*rows[i]), name_lens[i]);
        }
        std::println("");
    };
    print_meta("personId",   [](const Row& r) -> std::string_view { return r.person_id; });
    print_meta("personName", [](const Row& r) -> std::string_view { return r.person_name; });
    print_meta("countryId",  [](const Row& r) -> std::string_view { return r.country_id; });
    print_meta("gender",     [](const Row& r) -> std::string_view { return r.gender; });

    auto print_cells = [&](std::string_view label, auto select) {
        std::print("{:>{}}", label, col_name_len);
        for (std::size_t i = 0; i < rows.size(); ++i) {
            const Cell& c = select(*rows[i]);
            if (is_missing(c)) {
                std::print("    {:>{}}", "", name_lens[i]);
            } else if (auto p = std::get_if<std::int64_t>(&c)) {
                std::print("    {:>{}}", *p, name_lens[i]);
            } else if (auto p = std::get_if<double>(&c)) {
                std::print("    {:>{}.2f}", *p, name_lens[i]);
            }
        }
        std::println("");
    };
    for (std::size_t i = 0; i < COLS.size(); ++i) {
        print_cells(COLS[i].name, [i](const Row& r) -> const Cell& { return r.vals[i]; });
    }
    for (std::size_t i = 0; i < COLS.size(); ++i) {
        print_cells(std::format("{}_rank", COLS[i].name),
                    [i](const Row& r) -> const Cell& { return r.ranks[i]; });
    }
    for (std::size_t i = 0; i < COLS.size(); ++i) {
        print_cells(std::format("{}_nr", COLS[i].name),
                    [i](const Row& r) -> const Cell& { return r.nrs[i]; });
    }
}

}  // namespace wca
