// CLI entry point.  Mirrors the Rust port's surface (source positional,
// --year, summary/topk/person subcommands).

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <expected>
#include <filesystem>
#include <format>
#include <print>
#include <string>
#include <string_view>
#include <vector>

#include "calc.hpp"
#include "loader.hpp"
#include "output.hpp"

namespace {

struct Cli {
    std::filesystem::path        source;
    std::optional<std::int32_t>  year;
    enum class Sub { None, Summary, Topk, Person } sub = Sub::None;
    std::string                  summary_id;
    std::string                  topk_col;
    std::size_t                  topk_k = 10;
    std::optional<std::string>   topk_country;
    std::vector<std::string>     person_ids;
};

[[noreturn]] void die(std::string_view msg) {
    std::println(stderr, "error: {}", msg);
    std::exit(2);
}

Cli parse_args(int argc, char** argv) {
    if (argc < 2) die("usage: wca-stats <zip> [--year Y] [summary ID | topk COL [--k N] [--country C] | person ID...]");
    Cli cli;
    cli.source = argv[1];
    int i = 2;
    while (i < argc) {
        std::string_view a = argv[i];
        if (a == "--year" && i + 1 < argc) {
            cli.year = std::stoi(argv[++i]);
        } else if (a == "summary" && i + 1 < argc) {
            cli.sub = Cli::Sub::Summary;
            cli.summary_id = argv[++i];
        } else if ((a == "topk" || a == "K") && i + 1 < argc) {
            cli.sub = Cli::Sub::Topk;
            cli.topk_col = argv[++i];
            while (i + 1 < argc) {
                std::string_view b = argv[i + 1];
                if (b == "--k" && i + 2 < argc) { cli.topk_k = std::stoul(argv[i + 2]); i += 2; }
                else if (b == "--country" && i + 2 < argc) { cli.topk_country = argv[i + 2]; i += 2; }
                else break;
            }
        } else if ((a == "person" || a == "P") && i + 1 < argc) {
            cli.sub = Cli::Sub::Person;
            ++i;
            while (i < argc) cli.person_ids.emplace_back(argv[i++]);
            return cli;
        } else {
            die(std::format("unexpected arg: {}", a));
        }
        ++i;
    }
    return cli;
}

}  // namespace

int main(int argc, char** argv) try {
    using clk = std::chrono::steady_clock;
    const Cli cli = parse_args(argc, argv);

    auto t0 = clk::now();
    auto data = wca::load_wca(cli.source);
    auto dt = std::chrono::duration<double>(clk::now() - t0).count();
    std::println(stderr, "Load Data done ({:.2f}s)", dt);

    std::filesystem::create_directories("results");

    auto event_id_opt = data.event_id("333fm");
    if (!event_id_opt) die("333fm event missing in data");
    const auto event_id = *event_id_opt;

    const bool is_summary = cli.sub == Cli::Sub::Summary;
    std::optional<std::uint32_t> summary_person_key;
    if (is_summary) {
        summary_person_key = data.person_key(cli.summary_id);
        if (!summary_person_key) die(std::format("unknown person id {}", cli.summary_id));
    }

    std::vector<std::int32_t> years = is_summary
        ? data.person_event_years(*summary_person_key, event_id)
        : data.event_years(event_id);
    std::ranges::sort(years);
    if (!is_summary && cli.year) {
        std::erase_if(years, [y = *cli.year](auto x) { return x != y; });
    }

    std::vector<wca::Row> summary_rows;
    std::unordered_map<std::string, wca::Row> last_rows;

    const std::size_t n_years = years.size();
    for (std::size_t yi = 0; yi < n_years; ++yi) {
        const auto year = years[yi];
        std::println(stderr, "dealing {} ...", year);
        for (const auto& [cat, filter] : std::array<std::pair<std::string_view, wca::YearFilter>, 2>{{
                 {"in", {wca::YearOp::Eq, year}},
                 {"to", {wca::YearOp::Le, year}},
             }}) {
            auto df = wca::calc(data, event_id, filter);
            auto fname = std::filesystem::path{"results"} /
                         std::format("results.{}{}.csv", cat, year);
            auto tio = clk::now();
            wca::write_csv(fname, df);
            std::println(stderr, "saved: {} ({:.2f}s)", fname.string(),
                         std::chrono::duration<double>(clk::now() - tio).count());

            if (is_summary) {
                const auto pk = *summary_person_key;
                auto it = std::ranges::find(df.rows, pk, &wca::Row::person_key);
                wca::Row row = (it != df.rows.end())
                    ? *it
                    : wca::Row{
                          .person_key  = pk,
                          .vals  = std::vector<wca::Cell>(wca::COLS.size()),
                          .ranks = std::vector<wca::Cell>(wca::COLS.size()),
                          .nrs   = std::vector<wca::Cell>(wca::COLS.size()),
                      };
                const auto& p = data.persons[pk];
                row.person_id   = p.wca_id;
                row.person_name = p.name;
                row.country_id  = p.country_id;
                row.gender      = p.gender;
                row.year        = year;
                row.category    = std::format("{}-year", cat);
                summary_rows.push_back(row);
                if (auto lit = last_rows.find(std::string{cat}); lit != last_rows.end()) {
                    auto delta = wca::row_delta(row, lit->second);
                    delta.year = year;
                    summary_rows.push_back(std::move(delta));
                }
                last_rows[std::string{cat}] = std::move(row);
            }

            const bool is_last_year = yi + 1 == n_years;
            if (is_last_year && cat == "to") {
                switch (cli.sub) {
                case Cli::Sub::Topk:
                    wca::print_topk(
                        df, cli.topk_col, cli.topk_k,
                        cli.topk_country
                            ? std::optional<std::string_view>{*cli.topk_country}
                            : std::nullopt);
                    break;
                case Cli::Sub::Person:
                    wca::print_some_persons(df, cli.person_ids);
                    break;
                default: break;
                }
            }
        }
    }

    if (is_summary) {
        auto path = std::filesystem::path{"results"} /
                    std::format("{}.csv", cli.summary_id);
        wca::write_summary_csv(path, summary_rows);
    }
    return 0;
} catch (const std::exception& e) {
    std::println(stderr, "fatal: {}", e.what());
    return 1;
}
