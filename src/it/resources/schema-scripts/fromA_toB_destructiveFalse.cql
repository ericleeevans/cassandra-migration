-- Test CQL script

ALTER TABLE a_table ADD field2 boolean;

UPDATE a_table SET field2 = false WHERE id = 1;
UPDATE a_table SET field2 = true WHERE id = 2;

-- transition-before = A
-- transition-after = B
-- transition-destructive = false
