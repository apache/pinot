package com.linkedin.thirdeye.api;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.joda.time.Period;

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

  public long toMillis() {
    return toMillis(1);
  }

  public Period toPeriod() {
    Period period = null;
    switch (unit) {
    case DAYS:
      period = new Period(0, 0, 0, size, 0, 0, 0, 0);
      break;
    case HOURS:
      period = new Period(0, 0, 0, 0, size, 0, 0, 0);
      break;
    case MINUTES:
      period = new Period(0, 0, 0, 0, 0, size, 0, 0);
      break;
    case SECONDS:
      period = new Period(0, 0, 0, 0, 0, 0, size, 0);
      break;
    case MILLISECONDS:
      period = new Period(0, 0, 0, 0, 0, 0, 0, size);
      break;
    default:
      period = new Period(0, 0, 0, 0, size, 0, 0, 0);
      break;
    }
    return period;
  }

  /**
   * Converts time in bucketed unit to millis
   * @param time
   * @return
   */
  public long toMillis(long time) {
    return unit.toMillis(time * size);
  }

  /**
   * Converts millis to time unit
   * e.g. If TimeGranularity is defined as 1 HOURS,
   * and we invoke convertToUnit(1458284400000) (i.e. 2016-03-18 00:00:00)
   * this method will return HOURS.convert(1458284400000, MILLISECONDS)/1 = 405079 hoursSinceEpoch
   * If TimeGranularity is defined as 10 MINUTES,
   * and we invoke convertToUnit(1458284400000) (i.e. 2016-03-18 00:00:00)
   * this method will return MINUTES.convert(1458284400000, MILLISECONDS)/10 = 2430474
   * tenMinutesSinceEpoch
   * @param millis
   * @return
   */
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
