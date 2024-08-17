-- Add some comments for because wtf

CREATE TABLE sample101(id UInt32, name String) ENGINE MergeTree PARTITION BY tuple()
ORDER BY tuple()