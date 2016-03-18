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
 * objects can be constructed via {@link ThirdEyeRequestBuilder}. <br/>
 * By default all requests are structured so that time series data will be returned. If the results
 * should be aggregated over the entire time window rather than grouped by each time bucket, see
 * {@link ThirdEyeRequestBuilder#setShouldGroupByTime(boolean)}.
 * <br/>
 * Date ranges are specified as [start, end), ie inclusive start and exclusive end.
 */
public class ThirdEyeRequest {
  public static final String GROUP_BY_VALUE = "!";

  private final String collection;
  private final ThirdEyeMetricFunction metricFunction;
  private final DateTime startTimeInclusive;
  private final DateTime endTimeExclusive;
  private final Multimap<String, String> dimensionValues;
  private final Set<String> groupBy;
  private final boolean shouldGroupByTime;
  private final Integer topCount;

  private ThirdEyeRequest(ThirdEyeRequestBuilder builder) {
    this.collection = builder.collection;
    this.metricFunction = builder.metricFunction;
    this.startTimeInclusive = builder.startTimeInclusive;
    this.endTimeExclusive = builder.endTimeExclusive;
    this.dimensionValues = builder.dimensionValues;
    this.groupBy = builder.groupBy;
    this.shouldGroupByTime = builder.shouldGroupByTime;
    this.topCount = builder.topCount;
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

  /** Start of request window, inclusive. */
  public DateTime getStartTimeInclusive() {
    return startTimeInclusive;
  }

  /** End of request window, exclusive. */
  public DateTime getEndTimeExclusive() {
    return endTimeExclusive;
  }

  public Multimap<String, String> getDimensionValues() {
    return dimensionValues;
  }

  public Set<String> getGroupBy() {
    return groupBy;
  }

  public boolean shouldGroupByTime() {
    return shouldGroupByTime;
  }

  /** Number of dimension groups to return per time bucket when group by is provided */
  public Integer getTopCount() {
    return topCount;
  }

  @Override
  public int hashCode() {
    return Objects.hash(collection, metricFunction, startTimeInclusive, endTimeExclusive,
        dimensionValues, groupBy);
  };

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ThirdEyeRequest)) {
      return false;
    }
    ThirdEyeRequest other = (ThirdEyeRequest) o;
    return Objects.equals(getCollection(), other.getCollection())
        && Objects.equals(getMetricFunction(), other.getMetricFunction())
        && Objects.equals(getStartTimeInclusive(), other.getStartTimeInclusive())
        && Objects.equals(getEndTimeExclusive(), other.getEndTimeExclusive())
        && Objects.equals(getDimensionValues(), other.getDimensionValues())
        && Objects.equals(getGroupBy(), other.getGroupBy())
        && Objects.equals(getTopCount(), other.getTopCount());

  };

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("collection", collection)
        .add("metricFunction", metricFunction).add("startTime", startTimeInclusive)
        .add("endTime", endTimeExclusive).add("dimensionValues", dimensionValues)
        .add("groupBy", groupBy).add("shouldGroupByTime", shouldGroupByTime)
        .add("topCount", topCount).toString();
  }

  public static class ThirdEyeRequestBuilder {
    private String collection;
    private ThirdEyeMetricFunction metricFunction;
    private DateTime startTimeInclusive;
    private DateTime endTimeExclusive;
    private final Multimap<String, String> dimensionValues;
    private final Set<String> groupBy;
    private boolean shouldGroupByTime = true;
    private Integer topCount = null;

    public ThirdEyeRequestBuilder() {
      this.dimensionValues = LinkedListMultimap.create();
      this.groupBy = new LinkedHashSet<String>();
    }

    public ThirdEyeRequestBuilder(ThirdEyeRequest request) {
      this.collection = request.getCollection();
      this.metricFunction = request.getMetricFunction();
      this.startTimeInclusive = request.getStartTimeInclusive();
      this.endTimeExclusive = request.getEndTimeExclusive();
      this.dimensionValues = LinkedListMultimap.create(request.getDimensionValues());
      this.groupBy = new LinkedHashSet<String>(request.getGroupBy());
      this.shouldGroupByTime = request.shouldGroupByTime();
      this.topCount = request.getTopCount();
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

    public ThirdEyeRequestBuilder setStartTime(long startTimeMillisInclusive) {
      this.startTimeInclusive = new DateTime(startTimeMillisInclusive, DateTimeZone.UTC);
      return this;
    }

    public ThirdEyeRequestBuilder setStartTimeInclusive(DateTime startTimeInclusive) {
      this.startTimeInclusive = startTimeInclusive;
      return this;
    }

    public ThirdEyeRequestBuilder setEndTimeExclusive(long endTimeMillisExclusive) {
      this.endTimeExclusive = new DateTime(endTimeMillisExclusive, DateTimeZone.UTC);
      return this;
    }

    public ThirdEyeRequestBuilder setEndTime(DateTime endTimeExclusive) {
      this.endTimeExclusive = endTimeExclusive;
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

    public ThirdEyeRequestBuilder setShouldGroupByTime(boolean shouldGroupByTime) {
      this.shouldGroupByTime = shouldGroupByTime;
      return this;
    }

    public ThirdEyeRequestBuilder setTopCount(Integer topCount) {
      this.topCount = topCount;
      return this;
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
