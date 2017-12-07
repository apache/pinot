package com.linkedin.thirdeye.api;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;

public class TimeSpec {
  private static final TimeGranularity DEFAULT_TIME_GRANULARITY= new TimeGranularity(1, TimeUnit.DAYS);
  private String columnName;
  private TimeGranularity dataGranularity = DEFAULT_TIME_GRANULARITY;
  private String format = SINCE_EPOCH_FORMAT; //sinceEpoch or yyyyMMdd
  public static String SINCE_EPOCH_FORMAT  = TimeFormat.EPOCH.toString();
  public static String DEFAULT_TIMEZONE = "UTC";

  public TimeSpec() {
  }

  public TimeSpec(String columnName, TimeGranularity dataGranularity, String format) {
    this.columnName = columnName;
    this.dataGranularity = dataGranularity;
    this.format = format;
  }

  @JsonProperty
  public String getColumnName() {
    return columnName;
  }

  @JsonProperty
  public TimeGranularity getDataGranularity() {
    return dataGranularity;
  }

  @JsonProperty
  public String getFormat() {
    return format;
  }
}
