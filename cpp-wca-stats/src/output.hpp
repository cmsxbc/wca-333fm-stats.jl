#pragma once
// CSV writing and human-readable print helpers.

#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "calc.hpp"

namespace wca {

void write_csv(const std::filesystem::path& path, const Frame& frame);
void write_summary_csv(const std::filesystem::path& path,
                       std::span<const Row> rows);
void print_some_persons(const Frame& frame, std::span<const std::string> ids);
void print_topk(const Frame& frame, std::string_view col, std::size_t k,
                std::optional<std::string_view> country);

}  // namespace wca
