SET max_threads = 1;
CREATE TABLE a (id UInt32) ENGINE = MergeTree() ORDER BY id;
