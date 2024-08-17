-- Add some comments for because wtf
CREATE TABLE sample11(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple()
ORDER BY tuple();
