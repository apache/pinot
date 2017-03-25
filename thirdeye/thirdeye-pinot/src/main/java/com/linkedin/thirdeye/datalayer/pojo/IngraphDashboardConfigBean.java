package com.linkedin.thirdeye.datalayer.pojo;

import java.util.concurrent.TimeUnit;

public class IngraphDashboardConfigBean extends AbstractBean {

  public static final int DEFAULT_GRANULARITY_SIZE = 5;
  public static final TimeUnit DEFAULT_GRANULARITY_UNIT = TimeUnit.MINUTES;
  public static final Long DEFAULT_FETCH_INTERVAL_PERIOD = 3600_000L;
  public static final Integer DEFAULT_MERGE_NUM_AVRO_RECORDS = 100;

  /**
   * Name of ingraph dashboard
   */
  private String name;

  private String fabricGroup;

  private boolean active = true;

  /**
   * flag to indicate if this dashboard needs to be setup
   */
  private boolean bootstrap = false;

  private boolean fromIngraphDashboard = true;

  /**
   * window to bootstrap in milliseconds
   */
  private long bootstrapStartTime; // Inclusive

  private long bootstrapEndTime; // Inclusive

  /**
   * interval to fetch at one time from ingraph in milliseconds
   */
  private long fetchIntervalPeriod = DEFAULT_FETCH_INTERVAL_PERIOD;

  /**
   * number of avro records to merge
   */
  private long mergeNumAvroRecords = DEFAULT_MERGE_NUM_AVRO_RECORDS;

  private int granularitySize = DEFAULT_GRANULARITY_SIZE;

  private TimeUnit granularityUnit = DEFAULT_GRANULARITY_UNIT;

  public IngraphDashboardConfigBean() {
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getFabricGroup() {
    return fabricGroup;
  }

  public void setFabricGroup(String fabricGroup) {
    this.fabricGroup = fabricGroup;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public boolean isBootstrap() {
    return bootstrap;
  }

  public void setBootstrap(boolean bootstrap) {
    this.bootstrap = bootstrap;
  }

  public boolean isFromIngraphDashboard() {
    return fromIngraphDashboard;
  }

  public void setFromIngraphDashboard(boolean fromIngraphDashboard) {
    this.fromIngraphDashboard = fromIngraphDashboard;
  }

  public long getBootstrapStartTime() {
    return bootstrapStartTime;
  }

  public void setBootstrapStartTime(long bootstrapStartTime) {
    this.bootstrapStartTime = bootstrapStartTime;
  }

  public long getBootstrapEndTime() {
    return bootstrapEndTime;
  }

  public void setBootstrapEndTime(long bootstrapEndTime) {
    this.bootstrapEndTime = bootstrapEndTime;
  }

  public long getFetchIntervalPeriod() {
    return fetchIntervalPeriod;
  }

  public void setFetchIntervalPeriod(long fetchIntervalPeriod) {
    this.fetchIntervalPeriod = fetchIntervalPeriod;
  }

  public long getMergeNumAvroRecords() {
    return mergeNumAvroRecords;
  }

  public void setMergeNumAvroRecords(long mergeNumAvroRecords) {
    this.mergeNumAvroRecords = mergeNumAvroRecords;
  }

  public int getGranularitySize() {
    return granularitySize;
  }

  public void setGranularitySize(int granularitySize) {
    this.granularitySize = granularitySize;
  }

  public TimeUnit getGranularityUnit() {
    return granularityUnit;
  }

  public void setGranularityUnit(TimeUnit granularityUnit) {
    this.granularityUnit = granularityUnit;
  }
}
