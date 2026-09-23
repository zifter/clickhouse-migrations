CREATE TABLE b (id UInt32) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO b SELECT 1 SETTINGS max_threads = 1;
SELECT 'SET x = 1';
/* SET y = 2; */ SELECT 2;
