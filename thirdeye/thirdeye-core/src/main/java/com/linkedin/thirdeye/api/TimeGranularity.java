package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.TimeUnit;

public class TimeGranularity {
  private int size;
  private TimeUnit unit;

  public TimeGranularity() {
  }

  public TimeGranularity(int size, TimeUnit unit) {
    this.size = size;
    this.unit = unit;
  }

  @JsonProperty
  public int getSize() {
    return size;
  }

  @JsonProperty
  public TimeUnit getUnit() {
    return unit;
  }

  @Override
  public String toString() {
    return size + "-" + unit;
  }
}
