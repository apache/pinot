package com.linkedin.thirdeye.anomaly.rulebased;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;
import com.linkedin.thirdeye.anomaly.generic.GenericFunctionTableRow;
import com.linkedin.thirdeye.anomaly.util.ResourceUtils;

/**
 *
 */
public final class RuleBasedFunctionTableRow extends FunctionTableRow {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenericFunctionTableRow.class);

  /** Name of the metric to monitor */
  private String metricName;

  /** Default threshold to apply */
  private double delta;

  /** The granularity at which data is to be provided to the function*/
  private TimeUnit aggregateUnit;
  private int aggregateSize;

  /** The number of consecutive violations before alerting */
  private int consecutiveBuckets;

  /** Expression to restrict application of the function */
  private String cronDefinition;

  /** The period with which to compare as a baseline, e.g., '1-week' */
  private TimeUnit baselineUnit;
  private int baselineSize;

  /** Table with dimension specific deltas */
  private String deltaTableName;

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public double getDelta() {
    return delta;
  }

  public void setDelta(double delta) {
    this.delta = delta;
  }

  public TimeUnit getAggregateUnit() {
    return aggregateUnit;
  }

  public void setAggregateUnit(TimeUnit aggregateUnit) {
    this.aggregateUnit = aggregateUnit;
  }

  public int getAggregateSize() {
    return aggregateSize;
  }

  public void setAggregateSize(int aggregateSize) {
    this.aggregateSize = aggregateSize;
  }

  public int getConsecutiveBuckets() {
    return consecutiveBuckets;
  }

  public void setConsecutiveBuckets(int consecutiveBuckets) {
    this.consecutiveBuckets = consecutiveBuckets;
  }

  public String getCronDefinition() {
    return cronDefinition;
  }

  public void setCronDefinition(String cronDefinition) {
    this.cronDefinition = cronDefinition;
  }

  public TimeUnit getBaselineUnit() {
    return baselineUnit;
  }

  public void setBaselineUnit(TimeUnit baselineUnit) {
    this.baselineUnit = baselineUnit;
  }

  public int getBaselineSize() {
    return baselineSize;
  }

  public void setBaselineSize(int baselineSize) {
    this.baselineSize = baselineSize;
  }

  public String getDeltaTableName() {
    return deltaTableName;
  }

  public void setDeltaTableName(String deltaTableName) {
    this.deltaTableName = deltaTableName;
  }

  /**
   * {@inheritDoc}
   * @see com.linkedin.thirdeye.anomaly.database.FunctionTableRow#insert(com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig)
   */
  @Override
  public void insert(AnomalyDatabaseConfig dbConfig) throws Exception {
    Connection conn = null;
    PreparedStatement preparedStmt = null;

    try {
      conn = dbConfig.getConnection();
      preparedStmt = conn.prepareStatement(String.format(
          ResourceUtils.getResourceAsString("database/rulebased/insert-rule-template.sql"),
          dbConfig.getFunctionTableName()));
      preparedStmt.setString(1, getFunctionName());
      preparedStmt.setString(2, getFunctionDescription());
      preparedStmt.setString(3, getCollectionName());
      preparedStmt.setString(4, getMetricName());
      preparedStmt.setDouble(5, getDelta());
      preparedStmt.setString(6, getAggregateUnit().toString());
      preparedStmt.setInt(7, getAggregateSize());
      preparedStmt.setString(8, getBaselineUnit().toString());
      preparedStmt.setInt(9, getBaselineSize());
      preparedStmt.setInt(10, getConsecutiveBuckets());
      preparedStmt.setString(11, getCronDefinition());
      preparedStmt.setString(12, getDeltaTableName());
      preparedStmt.executeUpdate();
    } catch (SQLException e) {
      throw e;
    } finally {
      try {
        if (conn != null) {
          conn.close();
        }
        if (preparedStmt != null) {
          preparedStmt.close();
        }
      } catch (SQLException e) {
        LOGGER.error("close exception", e);
      }
    }
  }

  public void subclassInit(ResultSet rs) throws SQLException {
    metricName = rs.getString("metric");
    delta = rs.getDouble("delta");

    consecutiveBuckets = rs.getInt("consecutive_buckets");
    cronDefinition = rs.getString("cron_definition");

    aggregateUnit = TimeUnit.valueOf(rs.getString("aggregate_unit"));
    aggregateSize = rs.getInt("aggregate_size");

    baselineUnit = TimeUnit.valueOf(rs.getString("baseline_unit"));
    baselineSize = rs.getInt("baseline_size");

    deltaTableName = rs.getString("delta_table");
  }
}
