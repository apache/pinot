package com.linkedin.thirdeye.anomaly.rulebased;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;

/**
 *
 */
public final class RuleBasedFunctionTableRow extends FunctionTableRow {

  private String metricName;
  private double delta;

  private String aggregateUnit;
  private int aggregateSize;

  private int consecutiveBuckets;
  private String cronDefinition;

  private String baselineUnit;
  private int baselineSize;

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
