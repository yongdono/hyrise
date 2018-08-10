#include "abstract_histogram.hpp"

#include <cmath>

#include <algorithm>
#include <memory>
#include <vector>

#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/aggregate.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"

namespace opossum {

template <typename T>
AbstractHistogram<T>::AbstractHistogram(const std::shared_ptr<const Table>& table)
    : _table(table), _supported_characters(""), _string_prefix_length(0ul) {}

template <>
AbstractHistogram<std::string>::AbstractHistogram(const std::shared_ptr<const Table>& table)
    : _table(table),
      _supported_characters(
          " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~") {
  _string_prefix_length = static_cast<uint64_t>(std::log(std::pow(2, 63)) / std::log(_supported_characters.length()));
}

template <>
AbstractHistogram<std::string>::AbstractHistogram(const std::shared_ptr<const Table>& table,
                                                  const std::string& supported_characters)
    : _table(table) {
  Assert(supported_characters.length() > 1, "String range must consist of more than one character.");

  _supported_characters = supported_characters;
  std::sort(_supported_characters.begin(), _supported_characters.end());

  for (auto it = _supported_characters.begin(); it < _supported_characters.end(); it++) {
    Assert(std::distance(_supported_characters.begin(), it) == *it - _supported_characters.front(),
           "Non-consecutive string ranges are not supported.");
  }

  _string_prefix_length = static_cast<uint64_t>(std::log(std::pow(2, 63)) / std::log(_supported_characters.length()));
}

template <>
AbstractHistogram<std::string>::AbstractHistogram(const std::shared_ptr<const Table>& table,
                                                  const std::string& supported_characters,
                                                  const uint64_t string_prefix_length)
    : _table(table), _string_prefix_length(string_prefix_length) {
  Assert(string_prefix_length > 0, "Invalid prefix length.");
  Assert(supported_characters.length() > 1, "String range must consist of more than one character.");
  Assert(std::pow(supported_characters.length(), string_prefix_length) < std::pow(2, 63), "Prefix too long.");

  _supported_characters = supported_characters;
  std::sort(_supported_characters.begin(), _supported_characters.end());

  for (auto it = _supported_characters.begin(); it < _supported_characters.end(); it++) {
    Assert(std::distance(_supported_characters.begin(), it) == *it - _supported_characters.front(),
           "Non-consecutive string ranges are not supported.");
  }
}

template <typename T>
std::string AbstractHistogram<T>::description() const {
  std::stringstream stream;
  stream << histogram_type_to_string.at(histogram_type()) << std::endl;
  stream << "  distinct    " << total_count_distinct() << std::endl;
  stream << "  min         " << min() << std::endl;
  stream << "  max         " << max() << std::endl;
  // TODO(tim): consider non-null ration in histograms
  // stream << "  non-null " << non_null_value_ratio() << std::endl;
  stream << "  buckets     " << num_buckets() << std::endl;

  stream << "  boundaries / counts " << std::endl;
  for (auto bucket = 0u; bucket < num_buckets(); bucket++) {
    stream << "              [" << _bucket_min(bucket) << ", " << _bucket_max(bucket) << "]: ";
    stream << _bucket_count(bucket) << std::endl;
  }

  return stream.str();
}

template <>
const std::string& AbstractHistogram<std::string>::supported_characters() const {
  return _supported_characters;
}

template <typename T>
const std::shared_ptr<const Table> AbstractHistogram<T>::_get_value_counts(const ColumnID column_id) const {
  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of histogram is deleted.");

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto aggregate_args = std::vector<AggregateColumnDefinition>{{std::nullopt, AggregateFunction::Count}};
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, aggregate_args, std::vector<ColumnID>{column_id});
  aggregate->execute();

  auto sort = std::make_shared<Sort>(aggregate, ColumnID{0});
  sort->execute();

  return sort->get_output();
}

template <typename T>
std::vector<std::pair<T, uint64_t>> AbstractHistogram<T>::_sort_value_counts(
    const std::unordered_map<T, uint64_t>& value_counts) {
  std::vector<std::pair<T, uint64_t>> result(value_counts.cbegin(), value_counts.cend());

  std::sort(result.begin(), result.end(),
            [](std::pair<T, uint64_t> lhs, std::pair<T, uint64_t> rhs) { return lhs.first < rhs.first; });

  return result;
}

template <typename T>
// typename std::enable_if_t<std::is_arithmetic_v<T>, std::vector<std::pair<T, uint64_t>>>
std::vector<std::pair<T, uint64_t>> AbstractHistogram<T>::_calculate_value_counts(
    const std::shared_ptr<const BaseColumn>& column) {
  std::unordered_map<T, uint64_t> value_counts;
  uint64_t nulls = 0;

  resolve_column_type<T>(*column, [&](auto& typed_column) {
    auto iterable = create_iterable_from_column<T>(typed_column);
    iterable.for_each([&](const auto& value) {
      if (value.is_null()) {
        nulls++;
      } else {
        value_counts[value.value()]++;
      }
    });
  });

  return AbstractHistogram<T>::_sort_value_counts(value_counts);
}

template <>
std::vector<std::pair<std::string, uint64_t>> AbstractHistogram<std::string>::_calculate_value_counts(
    const std::shared_ptr<const BaseColumn>& column, const std::string& supported_characters,
    const uint64_t string_prefix_length) {
  std::unordered_map<std::string, uint64_t> value_counts;
  uint64_t nulls = 0;

  resolve_column_type<std::string>(*column, [&](auto& typed_column) {
    auto iterable = create_iterable_from_column<std::string>(typed_column);
    iterable.for_each([&](const auto& value) {
      if (value.is_null()) {
        nulls++;
      } else {
        Assert(value.value().find_first_not_of(supported_characters) == std::string::npos, "Unsupported characters.");
        value_counts[value.value().substr(0, string_prefix_length)]++;
      }
    });
  });

  return AbstractHistogram<std::string>::_sort_value_counts(value_counts);
}

template <>
const std::shared_ptr<const Table> AbstractHistogram<std::string>::_get_value_counts(const ColumnID column_id) const {
  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of histogram is deleted.");

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto col_expression =
      std::make_shared<PQPColumnExpression>(column_id, table->column_data_type(column_id),
                                            table->column_is_nullable(column_id), table->column_name(column_id));
  const auto substr_expression =
      opossum::expression_functional::substr_(col_expression, 1, static_cast<int>(_string_prefix_length));
  auto projection =
      std::make_shared<Projection>(table_wrapper, std::vector<std::shared_ptr<AbstractExpression>>{substr_expression});
  projection->execute();

  const auto aggregate_args = std::vector<AggregateColumnDefinition>{{std::nullopt, AggregateFunction::Count}};
  auto aggregate = std::make_shared<Aggregate>(projection, aggregate_args, std::vector<ColumnID>{ColumnID{0}});
  aggregate->execute();

  auto sort = std::make_shared<Sort>(aggregate, ColumnID{0});
  sort->execute();

  return sort->get_output();
}

template <typename T>
void AbstractHistogram<T>::generate(const ColumnID column_id, const size_t max_num_buckets) {
  DebugAssert(max_num_buckets > 0u, "Cannot generate histogram with less than one bucket.");

  const auto result = _get_value_counts(column_id);
  if (result->row_count() == 0u) {
    return;
  }

  // TODO(tim): fix
  DebugAssert(result->chunk_count() == 1u, "Multiple chunks are currently not supported.");

  const auto distinct_column =
      std::static_pointer_cast<const ValueColumn<T>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{0}));
  const auto count_column =
      std::static_pointer_cast<const ValueColumn<int64_t>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{1}));

  _generate(distinct_column, count_column, max_num_buckets);
}

template <typename T>
T AbstractHistogram<T>::min() const {
  DebugAssert(num_buckets() > 0u, "Called method on histogram before initialization.");
  return _bucket_min(0u);
}

template <typename T>
T AbstractHistogram<T>::max() const {
  DebugAssert(num_buckets() > 0u, "Called method on histogram before initialization.");
  return _bucket_max(num_buckets() - 1u);
}

template <>
std::string AbstractHistogram<std::string>::_bucket_width(const BucketID /*index*/) const {
  Fail("Method not supported for string histograms.");
}

template <typename T>
T AbstractHistogram<T>::_bucket_width(const BucketID index) const {
  DebugAssert(index < num_buckets(), "Index is not a valid bucket.");
  return next_value(_bucket_max(index) - _bucket_min(index));
}

template <typename T>
T AbstractHistogram<T>::previous_value(const T value, const bool /*pad_and_trim*/) {
  if constexpr (std::is_floating_point_v<T>) {
    return std::nextafter(value, value - 1);
  }

  return value - 1;
}

template <>
std::string AbstractHistogram<std::string>::previous_value(const std::string value, const bool /*pad_and_trim*/) {
  // TODO(tim): deactivate
  Fail("Not supported.");
}

template <>
std::string AbstractHistogram<std::string>::previous_value(const std::string& value,
                                                           const std::string& supported_characters,
                                                           const uint64_t string_prefix_length,
                                                           const bool pad_and_trim) {
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
    return AbstractHistogram<std::string>::previous_value(substring, supported_characters, string_prefix_length,
                                                          false) +
           supported_characters.back();
  }

  return substring + static_cast<char>(last_char - 1);
}

template <>
std::string AbstractHistogram<std::string>::_previous_value(const std::string value, const bool pad_and_trim) const {
  return AbstractHistogram<std::string>::previous_value(value, _supported_characters, _string_prefix_length,
                                                        pad_and_trim);
}

template <typename T>
T AbstractHistogram<T>::_previous_value(const T value, const bool /*pad_and_trim*/) const {
  return AbstractHistogram<T>::previous_value(value);
}

template <typename T>
T AbstractHistogram<T>::next_value(const T value, const bool /*pad_and_trim*/) {
  if constexpr (std::is_floating_point_v<T>) {
    return std::nextafter(value, value + 1);
  }

  return value + 1;
}

template <>
std::string AbstractHistogram<std::string>::next_value(const std::string value, const bool /*pad_and_trim*/) {
  // TODO(tim): deactivate
  Fail("Not supported.");
}

template <>
std::string AbstractHistogram<std::string>::next_value(const std::string& value,
                                                       const std::string& supported_characters,
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

template <>
std::string AbstractHistogram<std::string>::_next_value(const std::string value, const bool pad_and_trim) const {
  return AbstractHistogram<std::string>::next_value(value, _supported_characters, _string_prefix_length, pad_and_trim);
}

template <typename T>
T AbstractHistogram<T>::_next_value(const T value, const bool /*pad_and_trim*/) const {
  return AbstractHistogram<T>::next_value(value);
}

template <>
uint64_t AbstractHistogram<std::string>::_ipow(uint64_t base, uint64_t exp) {
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

template <>
int64_t AbstractHistogram<std::string>::convert_string_to_number_representation(const std::string& value,
                                                                                const std::string& supported_characters,
                                                                                const uint64_t string_prefix_length) {
  if (value.empty()) {
    return -1;
  }

  const auto trimmed = value.substr(0, string_prefix_length);

  uint64_t result = 0;
  for (auto it = trimmed.cbegin(); it < trimmed.cend(); it++) {
    const auto power = string_prefix_length - std::distance(trimmed.cbegin(), it) - 1;
    result += (*it - supported_characters.front()) *
              AbstractHistogram<std::string>::_ipow(supported_characters.length(), power);
  }

  return result;
}

template <>
int64_t AbstractHistogram<std::string>::_convert_string_to_number_representation(const std::string& value) const {
  return AbstractHistogram<std::string>::convert_string_to_number_representation(value, _supported_characters,
                                                                                 _string_prefix_length);
}

template <>
std::string AbstractHistogram<std::string>::convert_number_representation_to_string(
    const int64_t value, const std::string& supported_characters, const uint64_t string_prefix_length) {
  if (value < 0) {
    return "";
  }

  std::string result_string;

  auto remainder = value;
  for (auto str_pos = string_prefix_length; str_pos > 0; str_pos--) {
    const auto pow_result = static_cast<uint64_t>(std::pow(supported_characters.length(), str_pos - 1));
    const auto div = remainder / pow_result;
    remainder -= div * pow_result;
    result_string += supported_characters.at(div);
  }

  return result_string;
}

template <>
std::string AbstractHistogram<std::string>::_convert_number_representation_to_string(const int64_t value) const {
  return AbstractHistogram<std::string>::convert_number_representation_to_string(value, _supported_characters,
                                                                                 _string_prefix_length);
}

template <typename T>
float AbstractHistogram<T>::_bucket_share(const BucketID bucket_id, const T value) const {
  return static_cast<float>(value - _bucket_min(bucket_id)) / _bucket_width(bucket_id);
}

template <>
float AbstractHistogram<std::string>::_bucket_share(const BucketID bucket_id, const std::string value) const {
  /**
   * Calculate range between two strings.
   * This is based on the following assumptions:
   *    - a consecutive byte range, e.g. lower case letters in ASCII
   *    - fixed-length strings
   *
   * Treat the string range similar to the decimal system (base 26 for lower case letters).
   * Characters in the beginning of the string have a higher value than ones at the end.
   * Assign each letter the value of the index in the alphabet (zero-based).
   * Then convert it to a number.
   *
   * Example with fixed-length 4 (possible range: [aaaa, zzzz]):
   *
   *  Number of possible strings: 26**4 = 456,976
   *
   * 1. aaaa - zzzz
   *
   *  repr(aaaa) = 0 * 26**3 + 0 * 26**2 + 0 * 26**1 + 0 * 26**0 = 0
   *  repr(zzzz) = 25 * 26**3 + 25 * 26**2 + 25 * 26**1 + 25 * 26**0 = 456,975
   *  Size of range: repr(zzzz) - repr(aaaa) + 1 = 456,976
   *  Share of the range: 456,976 / 456,976 = 1
   *
   * 2. bhja - mmmm
   *
   *  repr(bhja): 1 * 26**3 + 7 * 26**2 + 9 * 26**1 + 0 * 26**0 = 22,542
   *  repr(mmmm): 12 * 26**3 + 12 * 26**2 + 12 * 26**1 + 12 * 26**0 = 219,348
   *  Size of range: repr(mmmm) - repr(bhja) + 1 = 196,807
   *  Share of the range: 196,807 / 456,976 ~= 0.43
   *
   *  Note that strings shorter than the fixed-length will induce a small error,
   *  because the missing characters will be treated as 'a'.
   *  Since we are dealing with approximations this is acceptable.
   */
  const auto value_repr = _convert_string_to_number_representation(value);
  const auto min_repr = _convert_string_to_number_representation(_bucket_min(bucket_id));
  const auto max_repr = _convert_string_to_number_representation(_bucket_max(bucket_id));
  return static_cast<float>(value_repr - min_repr) / (max_repr - min_repr + 1);
}

template <typename T>
float AbstractHistogram<T>::estimate_cardinality(const PredicateCondition predicate_type, const T value,
                                                 const std::optional<T>& value2) const {
  DebugAssert(num_buckets() > 0u, "Called method on histogram before initialization.");

  T cleaned_value = value;
  if constexpr (std::is_same_v<T, std::string>) {
    cleaned_value = value.substr(0, _string_prefix_length);
  }

  if constexpr (std::is_same_v<T, std::string>) {
    Assert(cleaned_value.find_first_not_of(_supported_characters) == std::string::npos, "Unsupported characters.");
  }

  if (can_prune(predicate_type, AllTypeVariant{cleaned_value}, std::optional<AllTypeVariant>(*value2))) {
    return 0.f;
  }

  switch (predicate_type) {
    case PredicateCondition::Equals: {
      const auto index = _bucket_for_value(cleaned_value);

      if (index == INVALID_BUCKET_ID) {
        return 0.f;
      }

      return static_cast<float>(_bucket_count(index)) / static_cast<float>(_bucket_count_distinct(index));
    }
    case PredicateCondition::NotEquals: {
      const auto index = _bucket_for_value(cleaned_value);

      if (index == INVALID_BUCKET_ID) {
        return total_count();
      }

      return total_count() -
             static_cast<float>(_bucket_count(index)) / static_cast<float>(_bucket_count_distinct(index));
    }
    case PredicateCondition::LessThan: {
      if (cleaned_value > max()) {
        return total_count();
      }

      if (cleaned_value <= min()) {
        return 0.f;
      }

      auto index = _bucket_for_value(cleaned_value);
      auto cardinality = 0.f;

      if (index == INVALID_BUCKET_ID) {
        // The value is within the range of the histogram, but does not belong to a bucket.
        // Therefore, we need to sum up the counts of all buckets with a max < value.
        index = _upper_bound_for_value(cleaned_value);
      } else {
        cardinality += _bucket_share(index, cleaned_value) * _bucket_count(index);
      }

      // Sum up all buckets before the bucket (or gap) containing the value.
      for (BucketID bucket = 0u; bucket < index; bucket++) {
        cardinality += _bucket_count(bucket);
      }

      /**
       * The cardinality is capped at total_count().
       * It is possible for a value that is smaller than or equal to the max of the EqualHeightHistogram
       * to yield a calculated cardinality higher than total_count.
       * This is due to the way EqualHeightHistograms store the count for a bucket,
       * which is in a single value (count_per_bucket) for all buckets rather than a vector (one value for each bucket).
       * Consequently, this value is the desired count for all buckets.
       * In practice, _bucket_count(n) >= _count_per_bucket for n < num_buckets() - 1,
       * because buckets are filled up until the count is at least _count_per_bucket.
       * The last bucket typically has a count lower than _count_per_bucket.
       * Therefore, if we calculate the share of the last bucket based on _count_per_bucket
       * we might end up with an estimate higher than total_count(), which is then capped.
       */
      return std::min(cardinality, static_cast<float>(total_count()));
    }
    case PredicateCondition::LessThanEquals:
      return estimate_cardinality(PredicateCondition::LessThan, next_value(cleaned_value));
    case PredicateCondition::GreaterThanEquals:
      return total_count() - estimate_cardinality(PredicateCondition::LessThan, cleaned_value);
    case PredicateCondition::GreaterThan:
      return total_count() - estimate_cardinality(PredicateCondition::LessThanEquals, cleaned_value);
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(value2), "Between operator needs two values.");
      return estimate_cardinality(PredicateCondition::LessThanEquals, *value2) -
             estimate_cardinality(PredicateCondition::LessThan, cleaned_value);
    }
    // TODO(tim): implement more meaningful things here
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      return total_count();
    default:
      Fail("Predicate condition not yet supported.");
  }
}

template <typename T>
float AbstractHistogram<T>::estimate_selectivity(const PredicateCondition predicate_type, const T value,
                                                 const std::optional<T>& value2) const {
  return estimate_cardinality(predicate_type, value, value2) / total_count();
}

template <typename T>
float AbstractHistogram<T>::estimate_distinct_count(const PredicateCondition predicate_type, const T value,
                                                    const std::optional<T>& value2) const {
  switch (predicate_type) {
    case PredicateCondition::Equals: {
      if (can_prune(predicate_type, value)) {
        return 0.f;
      }

      return 1.f;
    }
    case PredicateCondition::NotEquals: {
      if (can_prune(predicate_type, value)) {
        return 0.f;
      }

      if (_bucket_for_value(value) == INVALID_BUCKET_ID) {
        return total_count_distinct();
      }

      return total_count_distinct() - 1.f;
    }
    case PredicateCondition::LessThan: {
      if (value <= min()) {
        return 0.f;
      }

      if (value > max()) {
        return total_count_distinct();
      }

      auto distinct_count = 0.f;
      auto bucket_id = _bucket_for_value(value);
      if (bucket_id == INVALID_BUCKET_ID) {
        // The value is within the range of the histogram, but does not belong to a bucket.
        // Therefore, we need to sum up the distinct counts of all buckets with a max < value.
        bucket_id = _upper_bound_for_value(value);
      } else {
        distinct_count += _bucket_share(bucket_id, value) * _bucket_count_distinct(bucket_id);
      }

      // Sum up all buckets before the bucket (or gap) containing the value.
      for (BucketID bucket = 0u; bucket < bucket_id; bucket++) {
        distinct_count += _bucket_count_distinct(bucket);
      }

      return distinct_count;
    }
    case PredicateCondition::LessThanEquals:
      return estimate_distinct_count(PredicateCondition::LessThan, next_value(value));
    case PredicateCondition::GreaterThanEquals:
      return total_count_distinct() - estimate_distinct_count(PredicateCondition::LessThan, value);
    case PredicateCondition::GreaterThan:
      return total_count_distinct() - estimate_distinct_count(PredicateCondition::LessThanEquals, value);
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(value2), "Between operator needs two values.");
      return estimate_distinct_count(PredicateCondition::LessThanEquals, *value2) -
             estimate_distinct_count(PredicateCondition::LessThan, value);
    }
    // TODO(tim): implement more meaningful things here
    default:
      return total_count_distinct();
  }
}

template <typename T>
bool AbstractHistogram<T>::can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                     const std::optional<AllTypeVariant>& variant_value2) const {
  DebugAssert(num_buckets() > 0, "Called method on histogram before initialization.");

  T value = type_cast<T>(variant_value);
  if constexpr (std::is_same_v<T, std::string>) {
    value = value.substr(0, _string_prefix_length);
  }

  switch (predicate_type) {
    case PredicateCondition::Equals:
      return _bucket_for_value(value) == INVALID_BUCKET_ID;
    case PredicateCondition::NotEquals:
      return num_buckets() == 1 && _bucket_min(0) == value && _bucket_max(0) == value;
    case PredicateCondition::LessThan:
      return value <= min();
    case PredicateCondition::LessThanEquals:
      return value < min();
    case PredicateCondition::GreaterThanEquals:
      return value > max();
    case PredicateCondition::GreaterThan:
      return value >= max();
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");
      const auto value2 = type_cast<T>(*variant_value2);
      return value > max() || value2 < min() ||
             (_bucket_for_value(value) == INVALID_BUCKET_ID && _bucket_for_value(value2) == INVALID_BUCKET_ID &&
              _upper_bound_for_value(value) == _upper_bound_for_value(value2));
    }
    default:
      // Rather than failing we simply do not prune for things we cannot (yet) handle.
      // TODO(tim): think about like and not like
      return false;
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(AbstractHistogram);

}  // namespace opossum
