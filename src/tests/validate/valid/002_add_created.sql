ALTER TABLE events ADD COLUMN created DateTime DEFAULT now() COMMENT 'DROP COLUMN later';
-- A TTL rule's DELETE is not a mutation.
ALTER TABLE events MODIFY TTL created + INTERVAL 1 YEAR DELETE;
SELECT truncate(1.5)
