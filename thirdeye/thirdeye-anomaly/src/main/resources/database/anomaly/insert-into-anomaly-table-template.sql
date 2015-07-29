INSERT IGNORE INTO %s(
  function_id,
  function_description,
  function_name,
  collection,
  time_window,
  non_star_count,
  dimension,
  metrics,
  anomaly_score,
  anomaly_volume,
  properties
) VALUES(?,?,?,?,?,?,?,?,?,?,?);