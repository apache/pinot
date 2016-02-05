package com.linkedin.thirdeye.api;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;

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

  @Override
  public int hashCode() {
    return Objects.hash(size, unit);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TimeGranularity)) {
      return false;
    }
    TimeGranularity other = (TimeGranularity) obj;
    return Objects.equals(other.size, this.size) && Objects.equals(other.unit, this.unit);
  }
}
