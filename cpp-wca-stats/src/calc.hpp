#pragma once
// Per-year 3x3 FM stats computation.  Mirrors WCAStats.jl:calc().

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "loader.hpp"

namespace wca {

enum class ColKind : std::uint8_t { Int, Float };
enum class ColDir  : std::uint8_t { Asc, Desc };

struct ColumnSpec {
    std::string_view name;
    ColKind          kind;
    ColDir           dir;
};

// Ordered value-column schema; order matches the Julia CSV output.
extern const std::array<ColumnSpec, 69> COLS;
std::size_t col_idx(std::string_view name);
// Order in which Julia emits _rank/_nr columns: all asc cols (in COLS
// order), then the desc cols in the specific order from the Julia source.
const std::vector<std::size_t>& rank_col_order();

// Cell = missing | int | double.  std::monostate stands in for Julia's missing.
using Cell = std::variant<std::monostate, std::int64_t, double>;

inline bool is_missing(const Cell& c) noexcept {
    return std::holds_alternative<std::monostate>(c);
}

inline std::optional<double> as_f64(const Cell& c) noexcept {
    if (auto p = std::get_if<std::int64_t>(&c)) return static_cast<double>(*p);
    if (auto p = std::get_if<double>(&c))       return *p;
    return std::nullopt;
}

struct Row {
    std::uint32_t person_key{};
    std::string   person_id;
    std::string   person_name;
    std::string   country_id;
    std::string   gender;
    std::vector<Cell> vals;
    std::vector<Cell> ranks;
    std::vector<Cell> nrs;
    std::optional<std::int64_t> year;
    std::optional<std::string>  category;
};

enum class YearOp : std::uint8_t { Eq, Le };
struct YearFilter {
    YearOp op;
    std::int32_t y;
    constexpr bool matches(std::int32_t year) const noexcept {
        return op == YearOp::Eq ? year == y : year <= y;
    }
};

struct Frame {
    std::vector<Row> rows;
    YearFilter       year_filter{};
};

Frame calc(const WcaData& data, std::uint16_t event_id, YearFilter yf);

// Element-wise subtraction (self - other) across val/rank/nr cells.
// Category suffix follows the Julia typo "{cat}-year-detla".
Row row_delta(const Row& self, const Row& other);

}  // namespace wca
