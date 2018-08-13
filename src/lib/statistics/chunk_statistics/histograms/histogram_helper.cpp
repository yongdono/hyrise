#include <string>

namespace opossum {

std::string previous_value(const std::string& value, const std::string& supported_characters,
                           const uint64_t string_prefix_length, const bool pad_and_trim) {
  if (value.empty() || value.find_first_not_of(supported_characters.front()) == std::string::npos) {
    return "";
  }

  std::string cleaned_value;
  if (pad_and_trim) {
    cleaned_value = value.length() >= string_prefix_length
                        ? value.substr(0, string_prefix_length)
                        : value + std::string(string_prefix_length - value.length(), supported_characters.front());
  } else {
    cleaned_value = value;
  }

  const auto last_char = cleaned_value.back();
  const auto substring = cleaned_value.substr(0, cleaned_value.length() - 1);

  if (last_char == supported_characters.front()) {
    return previous_value(substring, supported_characters, string_prefix_length, false) + supported_characters.back();
  }

  return substring + static_cast<char>(last_char - 1);
}

std::string next_value(const std::string& value, const std::string& supported_characters,
                       const uint64_t string_prefix_length, const bool pad_and_trim) {
  if (value.empty()) {
    return std::string{supported_characters.front()};
  }

  if (value == std::string(string_prefix_length, supported_characters.back())) {
    return value + supported_characters.front();
  }

  std::string cleaned_value;
  if (pad_and_trim) {
    cleaned_value = value.length() >= string_prefix_length
                        ? value.substr(0, string_prefix_length)
                        : value + std::string(string_prefix_length - value.length(), supported_characters.front());
  } else {
    cleaned_value = value;
  }

  const auto last_char = cleaned_value.back();
  const auto substring = cleaned_value.substr(0, cleaned_value.length() - 1);

  if (last_char != supported_characters.back()) {
    return substring + static_cast<char>(last_char + 1);
  }

  return next_value(substring, supported_characters, string_prefix_length, false) + supported_characters.front();
}

uint64_t ipow(uint64_t base, uint64_t exp) {
  uint64_t result = 1;

  for (;;) {
    if (exp & 1) {
      result *= base;
    }

    exp >>= 1;

    if (!exp) {
      break;
    }

    base *= base;
  }

  return result;
}

int64_t convert_string_to_number_representation(const std::string& value, const std::string& supported_characters,
                                                const uint64_t string_prefix_length) {
  if (value.empty()) {
    return -1;
  }

  const auto trimmed = value.substr(0, string_prefix_length);

  uint64_t result = 0;
  for (auto it = trimmed.cbegin(); it < trimmed.cend(); it++) {
    const auto power = string_prefix_length - std::distance(trimmed.cbegin(), it) - 1;
    result += (*it - supported_characters.front()) * ipow(supported_characters.length(), power);
  }

  return result;
}

std::string convert_number_representation_to_string(const int64_t value, const std::string& supported_characters,
                                                    const uint64_t string_prefix_length) {
  if (value < 0) {
    return "";
  }

  std::string result_string;

  auto remainder = value;
  for (auto str_pos = string_prefix_length; str_pos > 0; str_pos--) {
    const auto pow_result = ipow(supported_characters.length(), str_pos - 1);
    const auto div = remainder / pow_result;
    remainder -= div * pow_result;
    result_string += supported_characters.at(div);
  }

  return result_string;
}

}  // namespace opossum
