CREATE TABLE events_raw (
  id int AUTO_INCREMENT PRIMARY KEY,
  timestamp datetime NOT NULL,
  event longtext NOT NULL,
  metadata text DEFAULT NULL,
  source text NOT NULL,
  KEY idx_timestamp (timestamp),
  KEY idx_source (source(3072))
) ENGINE=INNODB;
