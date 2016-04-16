// Test CQL script
//
// transition-before = B
// transition-after = A
// transition-destructive = true

USE test_cql_migration;

ALTER TABLE a_table DROP field2;
