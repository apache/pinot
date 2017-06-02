package com.linkedin.thirdeye.datasource;

import com.linkedin.thirdeye.datasource.pinot.PinotThirdEyeDataSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.MoreObjects;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Request object containing all information for a {@link ThirdEyeDataSource} to retrieve data. Request
 * objects can be constructed via {@link ThirdEyeRequestBuilder}.
 */
public class ThirdEyeRequest {
  private final List<MetricFunction> metricFunctions;
  private final DateTime startTime;
  private final DateTime endTime;
  private final Multimap<String, String> filterSet;
  // TODO - what kind of advanced expressions do we want here? This could potentially force code to
  // depend on a specific client implementation
  private final String filterClause;
  private final List<String> groupByDimensions;
  private final TimeGranularity groupByTimeGranularity;
  private final List<String> metricNames;
  private final String dataSource;
  private final String requestReference;

  private ThirdEyeRequest(String requestReference, ThirdEyeRequestBuilder builder) {
    this.requestReference = requestReference;
    this.metricFunctions = builder.metricFunctions;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
    this.filterSet = builder.filterSet;
    this.filterClause = builder.filterClause;
    this.groupByDimensions = builder.groupBy;
    this.groupByTimeGranularity = builder.groupByTimeGranularity;
    this.dataSource = builder.dataSource;
    metricNames = new ArrayList<>();
    for (MetricFunction metric : metricFunctions) {
      metricNames.add(metric.toString());
    }
  }

  public static ThirdEyeRequestBuilder newBuilder() {
    return new ThirdEyeRequestBuilder();
  }

  public String getRequestReference() {
    return requestReference;
  }

  public List<MetricFunction> getMetricFunctions() {
    return metricFunctions;
  }

  public List<String> getMetricNames() {
    return metricNames;
  }

  @JsonIgnore
  public TimeGranularity getGroupByTimeGranularity() {
    return groupByTimeGranularity;
  }

  public DateTime getStartTimeInclusive() {
    return startTime;
  }

  public DateTime getEndTimeExclusive() {
    return endTime;
  }

  public Multimap<String, String> getFilterSet() {
    return filterSet;
  }

  public String getFilterClause() {
    // TODO check if this is being used?
    return filterClause;
  }

  public List<String> getGroupBy() {
    return groupByDimensions;
  }


  public String getDataSource() {
    return dataSource;
  }

  @Override
  public int hashCode() {
    // TODO do we intentionally omit request reference here?
    return Objects.hash(metricFunctions, startTime, endTime, filterSet, filterClause,
        groupByDimensions, groupByTimeGranularity);
  };

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ThirdEyeRequest)) {
      return false;
    }
    ThirdEyeRequest other = (ThirdEyeRequest) o;
    // TODO do we intentionally omit request reference here?
    return Objects.equals(getMetricFunctions(), other.getMetricFunctions())
        && Objects.equals(getStartTimeInclusive(), other.getStartTimeInclusive())
        && Objects.equals(getEndTimeExclusive(), other.getEndTimeExclusive())
        && Objects.equals(getFilterSet(), other.getFilterSet())
        && Objects.equals(getFilterClause(), other.getFilterClause())
        && Objects.equals(getGroupBy(), other.getGroupBy())
        && Objects.equals(getGroupByTimeGranularity(), other.getGroupByTimeGranularity());

  };

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("requestReference", requestReference)
        .add("metricFunctions", metricFunctions)
        .add("startTime", startTime).add("endTime", endTime).add("filterSet", filterSet)
        .add("filterClause", filterClause).add("groupBy", groupByDimensions)
        .add("groupByTimeGranularity", groupByTimeGranularity)
        .add("dataSource", dataSource).toString();
  }

  public static class ThirdEyeRequestBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeRequestBuilder.class);

    private List<MetricFunction> metricFunctions;
    private DateTime startTime;
    private DateTime endTime;
    private final Multimap<String, String> filterSet;
    private String filterClause;
    private final List<String> groupBy;
    private TimeGranularity groupByTimeGranularity;
    private String dataSource = PinotThirdEyeDataSource.DATA_SOURCE_NAME;

    public ThirdEyeRequestBuilder() {
      this.filterSet = LinkedListMultimap.create();
      this.groupBy = new ArrayList<String>();
      metricFunctions = new ArrayList<>();
    }

    public ThirdEyeRequestBuilder setDatasets(List<String> datasets) {
      return this;
    }

    public ThirdEyeRequestBuilder addMetricFunction(MetricFunction metricFunction) {
      metricFunctions.add(metricFunction);
      return this;
    }

    public ThirdEyeRequestBuilder setStartTimeInclusive(long startTimeMillis) {
      this.startTime = new DateTime(startTimeMillis, DateTimeZone.UTC);
      return this;
    }

    public ThirdEyeRequestBuilder setStartTimeInclusive(DateTime startTime) {
      this.startTime = startTime;
      return this;
    }

    public ThirdEyeRequestBuilder setEndTimeExclusive(long endTimeMillis) {
      this.endTime = new DateTime(endTimeMillis, DateTimeZone.UTC);
      return this;
    }

    public ThirdEyeRequestBuilder setEndTimeExclusive(DateTime endTime) {
      this.endTime = endTime;
      return this;
    }

    public ThirdEyeRequestBuilder addFilterValue(String column, String... values) {
      for (String value : values) {
        this.filterSet.put(column, value);
      }
      return this;
    }

    public ThirdEyeRequestBuilder setFilterClause(String filterClause) {
      this.filterClause = filterClause;
      return this;
    }

    public ThirdEyeRequestBuilder setFilterSet(Multimap<String, String> filterSet) {
      if (filterSet != null) {
        this.filterSet.clear();
        this.filterSet.putAll(filterSet);
      }
      return this;
    }

    /** Removes any existing groupings and adds the provided names. */
    public ThirdEyeRequestBuilder setGroupBy(Collection<String> names) {
      this.groupBy.clear();
      addGroupBy(names);
      return this;
    }

    /** See {@link #setGroupBy(Collection)} */
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

    /** See {@link ThirdEyeRequestBuilder#addGroupBy(Collection)} */
    public ThirdEyeRequestBuilder addGroupBy(String... names) {
      return addGroupBy(Arrays.asList(names));
    }

    public ThirdEyeRequestBuilder setGroupByTimeGranularity(TimeGranularity timeGranularity) {
      groupByTimeGranularity = timeGranularity;
      return this;
    }

    public ThirdEyeRequestBuilder setMetricFunctions(List<MetricFunction> metricFunctions) {
      this.metricFunctions = metricFunctions;
      return this;
    }


    public ThirdEyeRequestBuilder setDataSource(String dataSource) {
      this.dataSource = dataSource;
      return this;
    }

    public ThirdEyeRequest build(String requestReference) {
      return new ThirdEyeRequest(requestReference, this);
    }
  }
}
