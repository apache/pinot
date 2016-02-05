package com.linkedin.thirdeye.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.MoreObjects;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;

/**
 * Request object containing all information for a {@link ThirdEyeClient} to retrieve data. Request
 * objects can be constructed via {@link ThirdEyeRequestBuilder}.
 */
public class ThirdEyeRequest {
  public static final String GROUP_BY_VALUE = "!";

  private final String collection;
  private final ThirdEyeMetricFunction metricFunction;
  private final DateTime startTime;
  private final DateTime endTime;
  private final Multimap<String, String> dimensionValues;
  private final Set<String> groupBy;

  private ThirdEyeRequest(ThirdEyeRequestBuilder builder) {
    this.collection = builder.collection;
    this.metricFunction = builder.metricFunction;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
    this.dimensionValues = builder.dimensionValues;
    this.groupBy = builder.groupBy;
  }

  public static ThirdEyeRequestBuilder newBuilder() {
    return new ThirdEyeRequestBuilder();
  }

  public static ThirdEyeRequestBuilder newBuilder(ThirdEyeRequest request) {
    return new ThirdEyeRequestBuilder(request);
  }

  public String getCollection() {
    return collection;
  }

  public ThirdEyeMetricFunction getMetricFunction() {
    return metricFunction;
  }

  @JsonIgnore
  public List<String> getMetricNames() {
    return metricFunction.getMetricNames();
  }

  @JsonIgnore
  public List<String> getRawMetricNames() {
    return metricFunction.getRawMetricNames();
  }

  @JsonIgnore
  public TimeGranularity getTimeGranularity() {
    return metricFunction.getTimeGranularity();
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

  public Set<String> getGroupBy() {
    return groupBy;
  }

  @Override
  public int hashCode() {
    return Objects.hash(collection, metricFunction, startTime, endTime, dimensionValues, groupBy);
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
        && Objects.equals(getDimensionValues(), other.getDimensionValues())
        && Objects.equals(getGroupBy(), other.getGroupBy());

  };

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("collection", collection)
        .add("metricFunction", metricFunction).add("startTime", startTime).add("endTime", endTime)
        .add("dimensionValues", dimensionValues).add("groupBy", groupBy).toString();
  }

  public static class ThirdEyeRequestBuilder {
    private String collection;
    private ThirdEyeMetricFunction metricFunction;
    private DateTime startTime;
    private DateTime endTime;
    private final Multimap<String, String> dimensionValues;
    private final Set<String> groupBy;

    public ThirdEyeRequestBuilder() {
      this.dimensionValues = LinkedListMultimap.create();
      this.groupBy = new LinkedHashSet<String>();
    }

    public ThirdEyeRequestBuilder(ThirdEyeRequest request) {
      this.collection = request.getCollection();
      this.metricFunction = request.getMetricFunction();
      this.startTime = request.getStartTime();
      this.endTime = request.getEndTime();
      this.dimensionValues = LinkedListMultimap.create(request.getDimensionValues());
      this.groupBy = new LinkedHashSet<String>(request.getGroupBy());
    }

    public ThirdEyeRequestBuilder setCollection(String collection) {
      this.collection = collection;
      return this;
    }

    public ThirdEyeRequestBuilder setMetricFunction(String metricFunction) {
      return setMetricFunction(ThirdEyeMetricFunction.fromStr(metricFunction));
    }

    public ThirdEyeRequestBuilder setMetricFunction(ThirdEyeMetricFunction metricFunction) {
      this.metricFunction = metricFunction;
      return this;
    }

    public ThirdEyeRequestBuilder setStartTime(long startTimeMillis) {
      this.startTime = new DateTime(startTimeMillis, DateTimeZone.UTC);
      return this;
    }

    public ThirdEyeRequestBuilder setStartTime(DateTime startTime) {
      this.startTime = startTime;
      return this;
    }

    public ThirdEyeRequestBuilder setEndTime(long endTimeMillis) {
      this.endTime = new DateTime(endTimeMillis, DateTimeZone.UTC);
      return this;
    }

    public ThirdEyeRequestBuilder setEndTime(DateTime endTime) {
      this.endTime = endTime;
      return this;
    }

    public ThirdEyeRequestBuilder addDimensionValue(String name, String value) {
      this.dimensionValues.put(name, value);
      return this;
    }

    public ThirdEyeRequestBuilder setDimensionValues(Map<String, String> dimensionValues) {
      return setDimensionValues(ThirdEyeRequestUtils.toMultimap(dimensionValues));
    }

    public ThirdEyeRequestBuilder setDimensionValues(Multimap<String, String> dimensionValues) {
      this.dimensionValues.clear();
      if (dimensionValues != null) {
        this.dimensionValues.putAll(dimensionValues);
      }
      return this;
    }

    /** Removes any existing groupings and adds the provided names. */
    public ThirdEyeRequestBuilder setGroupBy(Collection<String> names) {
      this.groupBy.clear();
      addGroupBy(names);
      return this;
    }

    /** See {@link #setGroupBy(List)} */
    public ThirdEyeRequestBuilder setGroupBy(String... names) {
      return setGroupBy(Arrays.asList(names));
    }

    /** Adds the provided names to the existing groupings. */
    public ThirdEyeRequestBuilder addGroupBy(Collection<String> names) {
      if (names != null) {
        for (String name : names) {
          if (name != null) {
            this.groupBy.add(name);
          }
        }
      }
      return this;
    }

    /** See {@link ThirdEyeRequestBuilder#addGroupBy(List)} */
    public ThirdEyeRequestBuilder addGroupBy(String... names) {
      return addGroupBy(Arrays.asList(names));
    }

    public ThirdEyeRequest build() {
      // clean up any potential legacy dependencies on "!" for group by value.
      List<String> removedKeys = new ArrayList<String>();
      for (String key : dimensionValues.keySet()) {
        Collection<String> values = dimensionValues.get(key);
        if (values != null && values.size() == 1
            && GROUP_BY_VALUE.equals(values.iterator().next())) {
          this.groupBy.add(key);
          removedKeys.add(key);
        }
      }
      for (String keyToRemove : removedKeys) {
        this.dimensionValues.removeAll(keyToRemove);
      }

      // Validate no remaining groupBy + dimension value overlaps
      for (String groupName : groupBy) {
        if (dimensionValues.containsKey(groupName)) {
          throw new IllegalArgumentException("Cannot group by fixed dimension " + groupName);
        }
      }
      return new ThirdEyeRequest(this);
    }
  }
}
