-- Create the events table; the rollback is DROP TABLE events.
CREATE TABLE events
(
    `id;key` UInt32,
    note String DEFAULT 'DROP TABLE x; TRUNCATE y; SET a = 1',
    "quoted;name" String DEFAULT 'it''s; fine'
) ENGINE = MergeTree() ORDER BY `id;key`;

/* block; comment: ALTER TABLE events DELETE WHERE 1; ON CLUSTER c */
INSERT INTO events (`id;key`, note) VALUES (1, 'a\'b; ON CLUSTER c'), (2, 'SET x = 1');
