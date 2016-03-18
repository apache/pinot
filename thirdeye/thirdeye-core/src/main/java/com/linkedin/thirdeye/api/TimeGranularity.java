package com.linkedin.thirdeye.api;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TimeGranularity {
  private static int DEFAULT_TIME_SIZE = 1;

  private int size = DEFAULT_TIME_SIZE;
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

  public long toMillis() {
    return toMillis(1);
  }

  public long toMillis(long time) {
    return unit.toMillis(time * size);
  }

  public long convertToUnit(long millis) {
    return unit.convert(millis, TimeUnit.MILLISECONDS) / size;
  }

  public long toMillis(long time) {
    return unit.toMillis(time * size);
  }

  public long convertToUnit(long millis) {
    return unit.convert(millis, TimeUnit.MILLISECONDS) / size;
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
