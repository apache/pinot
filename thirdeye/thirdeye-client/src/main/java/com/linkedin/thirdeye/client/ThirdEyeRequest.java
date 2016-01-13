package com.linkedin.thirdeye.client;

import java.util.Objects;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.MoreObjects;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.client.util.SqlUtils;

public class ThirdEyeRequest {
  private String collection;
  private String metricFunction;
  private DateTime startTime;
  private DateTime endTime;
  private Multimap<String, String> dimensionValues = LinkedListMultimap.create();

  public ThirdEyeRequest() {
  }

  public ThirdEyeRequest(ThirdEyeRequest other) {
    this.collection = other.getCollection();
    this.metricFunction = other.getMetricFunction();
    this.startTime = other.getStartTime();
    this.endTime = other.getEndTime();
    this.dimensionValues = LinkedListMultimap.create(other.getDimensionValues());
  }

  public ThirdEyeRequest setCollection(String collection) {
    this.collection = collection;
    return this;
  }

  public ThirdEyeRequest setMetricFunction(String metricFunction) {
    this.metricFunction = metricFunction;
    return this;
  }

  public ThirdEyeRequest setStartTime(long startTimeMillis) {
    this.startTime = new DateTime(startTimeMillis, DateTimeZone.UTC);
    return this;
  }

  public ThirdEyeRequest setStartTime(DateTime startTime) {
    this.startTime = startTime;
    return this;
  }

  public ThirdEyeRequest setEndTime(long endTimeMillis) {
    this.endTime = new DateTime(endTimeMillis, DateTimeZone.UTC);
    return this;
  }

  public ThirdEyeRequest setEndTime(DateTime endTime) {
    this.endTime = endTime;
    return this;
  }

  public ThirdEyeRequest addDimensionValue(String name, String value) {
    this.dimensionValues.put(name, value);
    return this;
  }

  public ThirdEyeRequest setDimensionValues(Multimap<String, String> dimensionValues) {
    this.dimensionValues = dimensionValues;
    return this;
  }

  public ThirdEyeRequest setGroupBy(String name) {
    this.dimensionValues.put(name, "!");
    return this;
  }

  public String getCollection() {
    return collection;
  }

  public String getMetricFunction() {
    return metricFunction;
  }

  public DateTime getStartTime() {
    return startTime;
  }

  public DateTime getEndTime() {
    return endTime;
  }

  public Multimap<String, String> getDimensionValues() {
    return dimensionValues;
  }

  public String toSql() {
    if (metricFunction == null) {
      throw new IllegalStateException("Must provide metric function, e.g. `AGGREGATE_1_HOURS(m1)`");
    }
    if (collection == null) {
      throw new IllegalStateException("Must provide collection name");
    }
    if (startTime == null || endTime == null) {
      throw new IllegalStateException("Must provide start and end time");
    }
    return SqlUtils.getSql(metricFunction, collection, startTime, endTime, dimensionValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(collection, metricFunction, startTime, endTime, dimensionValues);
  };

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ThirdEyeRequest)) {
      return false;
    }
    ThirdEyeRequest other = (ThirdEyeRequest) o;
    return Objects.equals(getCollection(), other.getCollection())
        && Objects.equals(getMetricFunction(), other.getMetricFunction())
        && Objects.equals(getStartTime(), other.getStartTime())
        && Objects.equals(getEndTime(), other.getEndTime())
        && Objects.equals(getDimensionValues(), other.getDimensionValues());

  };

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("collection", collection)
        .add("metricFunction", metricFunction).add("startTime", startTime).add("endTime", endTime)
        .add("dimensionValues", dimensionValues).toString();
  }
}
