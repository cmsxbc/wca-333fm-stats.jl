#pragma once
// Load WCA export zip: persons, competitions, results (kept whole), attempts
// per result.  Same data shape as the Rust port, using C++23 containers.

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace wca {

struct Person {
    std::string wca_id;
    std::uint16_t sub_id{};
    std::string name;
    std::string country_id;
    std::string gender;
};

struct Competition {
    std::string id;
    std::int32_t year{};
};

struct Result333 {
    std::int64_t  id{};
    std::int32_t  pos{};
    std::int32_t  best{};
    std::int32_t  average{};
    std::uint32_t comp_key{};
    std::string   round_type_id;
    std::uint32_t person_key{};  // index of the sub_id==1 person entry
    std::uint16_t event_id{};
};

struct Attempt {
    std::int64_t result_id{};
    std::uint8_t attempt_number{};
    std::int32_t value{};
};

struct WcaData {
    std::vector<Person> persons;
    std::unordered_map<std::string, std::uint32_t> person_idx_by_wca_id;

    std::vector<Competition> competitions;
    std::unordered_map<std::string, std::uint32_t> comp_idx_by_id;

    std::vector<std::string> events;
    std::unordered_map<std::string, std::uint16_t> event_idx;

    std::vector<Result333> results;
    // For each result id, a vector of attempts sorted by attempt_number.
    std::unordered_map<std::int64_t, std::vector<Attempt>> attempts_by_result;

    std::optional<std::uint16_t> event_id(std::string_view name) const;
    std::optional<std::uint32_t> person_key(std::string_view wca_id) const;

    std::vector<std::int32_t> event_years(std::uint16_t event_id) const;
    std::vector<std::int32_t> person_event_years(std::uint32_t person_key,
                                                 std::uint16_t event_id) const;
};

WcaData load_wca(const std::filesystem::path& zip_path);

}  // namespace wca
