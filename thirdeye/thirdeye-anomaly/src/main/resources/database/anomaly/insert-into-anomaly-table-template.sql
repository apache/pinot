INSERT IGNORE INTO `%s`(
  function_table,
  function_id,
  function_description,
  function_name,
  collection,
  time_window,
  non_star_count,
  dimensions,
  dimensions_contribution,
  metrics,
  anomaly_score,
  anomaly_volume,
  properties
) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);