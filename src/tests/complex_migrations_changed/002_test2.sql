-- Add some comments for because wtf

CREATE TABLE sample21(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple()
ORDER BY tuple();

CREATE TABLE sample22(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple()
ORDER BY tuple();