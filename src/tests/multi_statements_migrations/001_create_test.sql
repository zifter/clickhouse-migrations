CREATE TABLE sample(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple()
ORDER BY tuple();

CREATE TABLE sample2(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple()
ORDER BY tuple();