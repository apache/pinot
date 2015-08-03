package com.linkedin.thirdeye.anomaly.rulebased;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;

/**
 *
 */
public final class RuleBasedFunctionTableRow extends FunctionTableRow {

  /** Name of the metric to monitor */
  private String metricName;

  /** Default threshold to apply */
  private double delta;

  /** The granularity at which data is to be provided to the function*/
  private String aggregateUnit;
  private int aggregateSize;

  /** The number of consecutive violations before alerting */
  private int consecutiveBuckets;

  /** Expression to restrict application of the function */
  private String cronDefinition;

  /** The period with which to compare as a baseline, e.g., '1-week' */
  private String baselineUnit;
  private int baselineSize;

  /** Table with dimension specific deltas */
  private String deltaTableName;

  public String getMetricName() {
    return metricName;
  }

  public double getDelta() {
    return delta;
  }

  public String getAggregateUnit() {
    return aggregateUnit;
  }

  public int getAggregateSize() {
    return aggregateSize;
  }

  public int getConsecutiveBuckets() {
    return consecutiveBuckets;
  }

  public String getCronDefinition() {
    return cronDefinition;
  }

  public String getBaselineUnit() {
    return baselineUnit;
  }

  public int getBaselineSize() {
    return baselineSize;
  }

  public String getDeltaTableName() {
    return deltaTableName;
  }

  public void subclassInit(ResultSet rs) throws SQLException {
    metricName = rs.getString("metric");
    delta = rs.getDouble("delta");

    consecutiveBuckets = rs.getInt("consecutive_buckets");
    cronDefinition = rs.getString("cron_definition");

    aggregateUnit = rs.getString("aggregate_unit");
    aggregateSize = rs.getInt("aggregate_size");

    baselineUnit = rs.getString("baseline_unit");
    baselineSize = rs.getInt("baseline_size");

    deltaTableName = rs.getString("delta_table");
  }
}
