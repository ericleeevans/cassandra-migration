------------------
-- Test CQL script
--
-- transition-before = None
  -- transition-after = A
    -- transition-destructive = false

CREATE KEYSPACE test_cql_migration WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

-- Comment on its own line
USE test_cql_migration;

// Comment on its own line
CREATE TABLE a_table ( -- comment at end of line, within a statement
  id int PRIMARY KEY,  // comment with 'quoted' characters
  field1 text          /* block comment
                          within statement
                          with 'quoted' characters
   */
);

/*
  Comment block
*/INSERT INTO a_table (id, field1) VALUES ( 1, ' /* a */ ' );  -- comment at end of line ; after statement  /*
  INSERT INTO a_table (id, field1) VALUES ( 2, '-- b ' );  // ditto
   /* comment */INSERT INTO a_table (id, field1) VALUES ( 3, '// ''//c'' ' );  /* -- ditto */

-- Comment on its own line, after all statements

// ditto
/* ditto */

------------------

