INSERT INTO `%s`(
  name,
  description,
  collection,
  metric,
  delta,
  aggregate_unit,
  aggregate_size,
  baseline_unit,
  baseline_size,
  consecutive_buckets,
  cron_definition,
  delta_table
) values (?,?,?,?,?,?,?,?,?,?,?,?);