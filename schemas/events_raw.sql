CREATE TABLE events_raw (
  id mediumint(9) DEFAULT NULL,
  timestamp datetime NOT NULL,
  event longtext NOT NULL,
  metadata text DEFAULT NULL,
  source text NOT NULL,
  KEY idx_timestamp (timestamp),
  KEY idx_source (source(3072))
) ENGINE=INNODB;
