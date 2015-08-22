CREATE TABLE `%s`(
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(1024) NOT NULL,
  collection VARCHAR(255) NOT NULL,
  metric VARCHAR(255) NOT NULL,
  delta DOUBLE NOT NULL,
  aggregate_unit CHAR(255) DEFAULT "HOURS",
  aggregate_size INT DEFAULT 1,
  baseline_unit CHAR(255) DEFAULT "DAYS",
  baseline_size INT DEFAULT 7,
  consecutive_buckets INT DEFAULT 1,
  cron_definition VARCHAR(255) DEFAULT NULL,
  delta_table VARCHAR(255) DEFAULT NULL,
  is_active BOOLEAN DEFAULT TRUE
);