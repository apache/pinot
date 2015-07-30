CREATE TABLE IF NOT EXISTS %s(
  id INT PRIMARY KEY AUTO_INCREMENT,
  function_id INT NOT NULL,
  function_name VARCHAR(255) NOT NULL,
  function_description VARCHAR(1024) NOT NULL,
  collection VARCHAR(255) NOT NULL,
  time_window DATETIME NOT NULL,
  non_star_count INT NOT NULL,
  dimensions VARCHAR(900) NOT NULL,
  dimensions_contribution DOUBLE NOT NULL,
  metrics VARCHAR(255) NOT NULL,
  anomaly_score DOUBLE NOT NULL,
  anomaly_volume DOUBLE NOT NULL,
  properties VARCHAR(60000),
  FOREIGN KEY (function_id) REFERENCES %s(id),
  CONSTRAINT unique_anomaly UNIQUE (function_id, time_window, dimensions, anomaly_score)
);