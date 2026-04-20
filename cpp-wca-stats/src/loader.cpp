// Read a WCA export zip with libzip and fill WcaData.

#include "loader.hpp"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <format>
#include <memory>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <zip.h>

namespace wca {

namespace {

using std::string_view;

// Parse a signed integer; returns def on failure.  C++17+ std::from_chars
// avoids locale overhead that std::stoi has.
template <class T>
constexpr T parse_int(string_view s, T def = 0) noexcept {
    T v{};
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    return ec == std::errc{} ? v : def;
}

// Split a TSV line into fields.  The caller must keep the line alive.
struct TsvLine {
    std::vector<string_view> fields;
    void split(string_view line) {
        fields.clear();
        std::size_t start = 0;
        for (std::size_t i = 0; i < line.size(); ++i) {
            if (line[i] == '\t') {
                fields.emplace_back(line.data() + start, i - start);
                start = i + 1;
            }
        }
        fields.emplace_back(line.data() + start, line.size() - start);
    }
    string_view at(std::size_t i) const {
        return i < fields.size() ? fields[i] : string_view{};
    }
};

// Read the full contents of a named zip entry into a string.
std::string read_entry(zip_t* z, const char* name) {
    zip_stat_t st;
    zip_stat_init(&st);
    if (zip_stat(z, name, 0, &st) != 0) {
        throw std::runtime_error(std::format("missing zip entry: {}", name));
    }
    auto size = st.size;
    std::string buf;
    buf.resize(size);
    zip_file_t* f = zip_fopen(z, name, 0);
    if (!f) throw std::runtime_error(std::format("open entry: {}", name));
    std::size_t off = 0;
    while (off < size) {
        auto n = zip_fread(f, buf.data() + off, size - off);
        if (n <= 0) break;
        off += static_cast<std::size_t>(n);
    }
    zip_fclose(f);
    buf.resize(off);
    return buf;
}

// Iterate lines of a TSV (no embedded quoted newlines), skipping the header.
template <class Fn>
void for_each_data_line(string_view text, Fn fn) {
    std::size_t pos = 0;
    bool header_skipped = false;
    while (pos < text.size()) {
        auto nl = text.find('\n', pos);
        auto end = (nl == string_view::npos) ? text.size() : nl;
        auto line = text.substr(pos, end - pos);
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
        if (!header_skipped) {
            header_skipped = true;
        } else if (!line.empty()) {
            fn(line);
        }
        pos = end + 1;
    }
}

// Parse the header row into a name->index map.
std::vector<string_view> header_fields(string_view text) {
    auto nl = text.find('\n');
    auto line = text.substr(0, nl == string_view::npos ? text.size() : nl);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    TsvLine t; t.split(line);
    return t.fields;
}

}  // namespace

std::optional<std::uint16_t> WcaData::event_id(string_view name) const {
    if (auto it = event_idx.find(std::string{name}); it != event_idx.end()) return it->second;
    return std::nullopt;
}

std::optional<std::uint32_t> WcaData::person_key(string_view wca_id) const {
    if (auto it = person_idx_by_wca_id.find(std::string{wca_id});
        it != person_idx_by_wca_id.end()) return it->second;
    return std::nullopt;
}

std::vector<std::int32_t> WcaData::event_years(std::uint16_t eid) const {
    std::vector<std::uint8_t> seen(competitions.size(), 0);
    std::int32_t lo = INT32_MAX, hi = INT32_MIN;
    for (const auto& r : results) {
        if (r.event_id != eid) continue;
        if (!seen[r.comp_key]) {
            seen[r.comp_key] = 1;
            auto y = competitions[r.comp_key].year;
            lo = std::min(lo, y);
            hi = std::max(hi, y);
        }
    }
    std::vector<std::int32_t> out;
    if (lo <= hi) {
        out.reserve(static_cast<std::size_t>(hi - lo + 1));
        for (std::int32_t y = lo; y <= hi; ++y) out.push_back(y);
    }
    return out;
}

std::vector<std::int32_t> WcaData::person_event_years(std::uint32_t pk,
                                                      std::uint16_t eid) const {
    std::int32_t lo = INT32_MAX, hi = INT32_MIN;
    std::int32_t plo = INT32_MAX;
    for (const auto& r : results) {
        if (r.event_id != eid) continue;
        auto y = competitions[r.comp_key].year;
        lo = std::min(lo, y);
        hi = std::max(hi, y);
        if (r.person_key == pk) plo = std::min(plo, y);
    }
    std::vector<std::int32_t> out;
    if (plo != INT32_MAX && lo <= hi) {
        for (std::int32_t y = plo; y <= hi; ++y) out.push_back(y);
    }
    return out;
}

WcaData load_wca(const std::filesystem::path& zip_path) {
    auto fname = zip_path.filename().string();
    if (fname.find("WCA_export_v2_") == std::string::npos) {
        throw std::runtime_error("not a WCA v2 export zip");
    }

    int err = 0;
    std::unique_ptr<zip_t, decltype(&zip_close)> z{
        zip_open(zip_path.string().c_str(), ZIP_RDONLY, &err), &zip_close};
    if (!z) throw std::runtime_error(std::format("zip_open failed: {}", err));

    WcaData d;

    // ---- persons ----  (cols: name, gender, wca_id, sub_id, country_id)
    {
        auto body = read_entry(z.get(), "WCA_export_persons.tsv");
        TsvLine t;
        for_each_data_line(body, [&](string_view line) {
            t.split(line);
            auto wca_id = std::string{t.at(2)};
            auto sub_id = parse_int<std::uint16_t>(t.at(3), 1);
            if (sub_id == 1) {
                d.person_idx_by_wca_id.emplace(wca_id, static_cast<std::uint32_t>(d.persons.size()));
            }
            d.persons.push_back(Person{
                .wca_id     = std::move(wca_id),
                .sub_id     = sub_id,
                .name       = std::string{t.at(0)},
                .country_id = std::string{t.at(4)},
                .gender     = std::string{t.at(1)},
            });
        });
    }

    // ---- competitions ----
    {
        auto body = read_entry(z.get(), "WCA_export_competitions.tsv");
        auto hdr  = header_fields(body);
        auto col  = [&](string_view n) -> std::size_t {
            auto it = std::ranges::find(hdr, n);
            return static_cast<std::size_t>(it - hdr.begin());
        };
        const auto c_id   = col("id");
        const auto c_year = col("year");

        TsvLine t;
        for_each_data_line(body, [&](string_view line) {
            t.split(line);
            if (t.fields.size() <= c_year) return;
            auto id = std::string{t.at(c_id)};
            auto year = parse_int<std::int32_t>(t.at(c_year), 0);
            d.comp_idx_by_id.emplace(id, static_cast<std::uint32_t>(d.competitions.size()));
            d.competitions.push_back(Competition{.id = std::move(id), .year = year});
        });
    }

    // ---- results ---- (columns at fixed positions matching the Rust port)
    {
        auto body = read_entry(z.get(), "WCA_export_results.tsv");
        TsvLine t;
        for_each_data_line(body, [&](string_view line) {
            t.split(line);
            auto id      = parse_int<std::int64_t>(t.at(0), -1);
            if (id < 0) return;
            auto pos     = parse_int<std::int32_t>(t.at(1), 0);
            auto best    = parse_int<std::int32_t>(t.at(2), 0);
            auto average = parse_int<std::int32_t>(t.at(3), 0);
            auto comp_id = t.at(4);
            auto round   = t.at(5);
            auto ev      = t.at(6);
            auto person  = t.at(8);

            auto comp_it = d.comp_idx_by_id.find(std::string{comp_id});
            if (comp_it == d.comp_idx_by_id.end()) return;
            auto person_it = d.person_idx_by_wca_id.find(std::string{person});
            if (person_it == d.person_idx_by_wca_id.end()) return;

            std::uint16_t eid;
            if (auto eit = d.event_idx.find(std::string{ev}); eit != d.event_idx.end()) {
                eid = eit->second;
            } else {
                eid = static_cast<std::uint16_t>(d.events.size());
                d.events.emplace_back(ev);
                d.event_idx.emplace(std::string{ev}, eid);
            }

            d.results.push_back(Result333{
                .id            = id,
                .pos           = pos,
                .best          = best,
                .average       = average,
                .comp_key      = comp_it->second,
                .round_type_id = std::string{round},
                .person_key    = person_it->second,
                .event_id      = eid,
            });
        });
    }

    // ---- result_attempts ---- (keep only those referencing a kept result)
    {
        std::unordered_map<std::int64_t, std::uint8_t> wanted;
        wanted.reserve(d.results.size());
        for (const auto& r : d.results) wanted.emplace(r.id, 1);

        auto body = read_entry(z.get(), "WCA_export_result_attempts.tsv");
        TsvLine t;
        for_each_data_line(body, [&](string_view line) {
            t.split(line);
            auto value  = parse_int<std::int32_t>(t.at(0), INT32_MIN);
            if (value == INT32_MIN) return;
            auto ano    = parse_int<std::int32_t>(t.at(1), -1);
            if (ano < 0) return;
            auto rid    = parse_int<std::int64_t>(t.at(2), -1);
            if (rid < 0) return;
            if (!wanted.contains(rid)) return;
            d.attempts_by_result[rid].push_back(Attempt{
                .result_id      = rid,
                .attempt_number = static_cast<std::uint8_t>(ano),
                .value          = value,
            });
        });
        for (auto& [_, v] : d.attempts_by_result) {
            std::ranges::sort(v, {}, &Attempt::attempt_number);
        }
    }

    return d;
}

}  // namespace wca
