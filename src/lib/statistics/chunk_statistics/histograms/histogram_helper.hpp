#pragma once

#include <cmath>

#include <string>

namespace opossum {

template <typename T>
std::enable_if_t<std::is_integral_v<T>, T> previous_value(const T value) {
  return value - 1;
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, T> previous_value(const T value) {
  return std::nextafter(value, value - 1);
}

template <typename T>
std::enable_if_t<std::is_integral_v<T>, T> next_value(const T value) {
  return value + 1;
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, T> next_value(const T value) {
  return std::nextafter(value, value + 1);
}

std::string previous_value(const std::string& value, const std::string& supported_characters,
                           const uint64_t string_prefix_length, const bool pad_and_trim = true);

std::string next_value(const std::string& value, const std::string& supported_characters,
                       const uint64_t string_prefix_length, const bool pad_and_trim = true);

uint64_t ipow(uint64_t base, uint64_t exp);

int64_t convert_string_to_number_representation(const std::string& value, const std::string& supported_characters,
                                                const uint64_t string_prefix_length);

std::string convert_number_representation_to_string(const int64_t value, const std::string& supported_characters,
                                                    const uint64_t string_prefix_length);

}  // namespace opossum
