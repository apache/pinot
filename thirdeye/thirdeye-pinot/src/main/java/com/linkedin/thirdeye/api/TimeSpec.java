package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.TimeUnit;

public class TimeSpec {
  private static final TimeGranularity DEFAULT_TIME_INPUT = new TimeGranularity(1, TimeUnit.HOURS);
  private static final TimeGranularity DEFAULT_TIME_BUCKET = new TimeGranularity(1, TimeUnit.HOURS);
  private static final TimeGranularity DEFAULT_TIME_RETENTION =
      new TimeGranularity(30, TimeUnit.DAYS);

  private String columnName;
  private TimeGranularity input = DEFAULT_TIME_INPUT;
  private TimeGranularity bucket = DEFAULT_TIME_BUCKET;
  private TimeGranularity retention = DEFAULT_TIME_RETENTION;
  private String format;

  public TimeSpec() {
  }

  public TimeSpec(String columnName, TimeGranularity input, TimeGranularity bucket,
      TimeGranularity retention, String format) {
    this.columnName = columnName;
    this.input = input;
    this.bucket = bucket;
    this.retention = retention;
    this.format = format;
  }

  @JsonProperty
  public String getColumnName() {
    return columnName;
  }

  @JsonProperty
  public TimeGranularity getInput() {
    return input;
  }

  @JsonProperty
  public TimeGranularity getBucket() {
    return bucket;
  }

  @JsonProperty
  public TimeGranularity getRetention() {
    return retention;
  }

  @JsonProperty
  public String getFormat() {
    return format;
  }
}
