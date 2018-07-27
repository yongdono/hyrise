#include "pos_list.hpp"

namespace opossum {

PosList::PosList() noexcept(noexcept(underlying::allocator_type()))
    : underlying(), _chunk_cardinality(ChunkCardinality::MultipleChunks) {}

PosList::PosList(const underlying::allocator_type& alloc) noexcept
    : underlying(alloc), _chunk_cardinality(ChunkCardinality::MultipleChunks) {}

PosList::PosList(PosList::underlying::size_type count, const RowID value, const underlying::allocator_type& alloc)
    : underlying(count, value, alloc), _chunk_cardinality(ChunkCardinality::MultipleChunks) {}

PosList::PosList(PosList::underlying::size_type count, const underlying::allocator_type& alloc)
    : underlying(count, alloc), _chunk_cardinality(ChunkCardinality::MultipleChunks) {}

PosList::PosList(PosList&& other) noexcept
    : underlying(std::move(other)), _chunk_cardinality(std::move(other._chunk_cardinality)) {}

PosList::PosList(PosList&& other, const underlying::allocator_type& alloc)
    : underlying(std::move(other), alloc), _chunk_cardinality(std::move(other._chunk_cardinality)) {}

PosList::PosList(std::initializer_list<RowID> init, const underlying::allocator_type& alloc)
    : underlying(init, alloc), _chunk_cardinality(ChunkCardinality::MultipleChunks) {}

PosList& PosList::operator=(std::initializer_list<RowID> ilist) {
  underlying::operator=(ilist);
  _chunk_cardinality = ChunkCardinality::MultipleChunks;
  return *this;
}

bool PosList::is_equal_helper(const PosList& rhs) const { return as_vector() == rhs.as_vector(); }

PosList::ChunkCardinality PosList::chunk_cardinality() const { return _chunk_cardinality; }

void PosList::set_chunk_cardinality(PosList::ChunkCardinality cardinality) { _chunk_cardinality = cardinality; }

const PosList::underlying& PosList::as_vector() const { return *static_cast<const underlying*>(this); }

PosList& PosList::operator=(PosList&& other) {
  underlying::operator=(std::move(other));
  _chunk_cardinality = std::move(other._chunk_cardinality);
  return *this;
}

bool operator==(const PosList& lhs, const PosList& rhs) { return lhs.is_equal_helper(rhs); }
bool operator!=(const PosList& lhs, const PosList& rhs) { return !(lhs == rhs); }

}  // namespace opossum
