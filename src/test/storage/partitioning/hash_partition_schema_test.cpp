#include "../../base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class StorageHashPartitionSchemaTest : public BaseTest {
 protected:
  void SetUp() override {
    HashFunction hf;
    t0.create_hash_partitioning(ColumnID{0}, hf, 3u);
    t0.add_column("int_column", opossum::DataType::Int, false);
    t0.add_column("string_column", opossum::DataType::String, false);
  }

  Table t0{2};
};

TEST_F(StorageHashPartitionSchemaTest, CreateHashPartitioning) {
  EXPECT_EQ(t0.row_count(), 0u);
  EXPECT_EQ(t0.chunk_count(), 3u);
  EXPECT_TRUE(t0.is_partitioned());
}

TEST_F(StorageHashPartitionSchemaTest, AppendViaTable) {
  // three in one partition --> two chunks in this partition
  t0.append({1, "Foo"});
  t0.append({1, "Bar"});
  t0.append({1, "Baz"});

  EXPECT_EQ(t0.row_count(), 3u);
  EXPECT_EQ(t0.chunk_count(), 4u);
}

TEST_F(StorageHashPartitionSchemaTest, AppendDirectly) {
  t0.get_modifiable_partition_schema()->append({1, "Foo"});
  t0.get_modifiable_partition_schema()->append({2, "Bar"});

  EXPECT_EQ(t0.row_count(), 2u);
  EXPECT_EQ(t0.chunk_count(), 3u);
}

TEST_F(StorageHashPartitionSchemaTest, AppendDirectlyCanExceedMaxChunkSize) {
  t0.get_modifiable_partition_schema()->append({1, "Foo"});
  t0.get_modifiable_partition_schema()->append({1, "Bar"});
  t0.get_modifiable_partition_schema()->append({1, "Baz"});

  EXPECT_EQ(t0.row_count(), 3u);
  EXPECT_EQ(t0.chunk_count(), 3u);
}

TEST_F(StorageHashPartitionSchemaTest, Name) { EXPECT_EQ(t0.get_partition_schema()->name(), "HashPartition"); }

}  // namespace opossum
