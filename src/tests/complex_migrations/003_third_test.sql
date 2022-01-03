CREATE TABLE sample31(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple()
ORDER BY tuple();

CREATE TABLE sample32(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple()
ORDER BY tuple();

CREATE TABLE sample33(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple()
ORDER BY tuple()