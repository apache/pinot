/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.datasource;

import com.google.common.collect.ArrayListMultimap;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;

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
  private final int limit;

  private ThirdEyeRequest(String requestReference, ThirdEyeRequestBuilder builder) {
    this.requestReference = requestReference;
    this.metricFunctions = new ArrayList<>(builder.metricFunctions);
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
    this.filterSet = ArrayListMultimap.create(builder.filterSet);
    this.filterClause = builder.filterClause;
    this.groupByDimensions = new ArrayList<>(builder.groupBy);
    this.groupByTimeGranularity = builder.groupByTimeGranularity;
    this.dataSource = builder.dataSource;
    metricNames = new ArrayList<>();
    for (MetricFunction metric : metricFunctions) {
      metricNames.add(metric.toString());
    }
    this.limit = builder.limit;
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

  public int getLimit() {
    return limit;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ThirdEyeRequest)) {
      return false;
    }
    ThirdEyeRequest that = (ThirdEyeRequest) o;
    return Objects.equals(metricFunctions, that.metricFunctions) && Objects.equals(startTime, that.startTime) && Objects
        .equals(endTime, that.endTime) && Objects.equals(filterSet, that.filterSet) && Objects.equals(filterClause,
        that.filterClause) && Objects.equals(groupByDimensions, that.groupByDimensions) && Objects.equals(
        groupByTimeGranularity, that.groupByTimeGranularity) && Objects.equals(metricNames, that.metricNames) && Objects
        .equals(dataSource, that.dataSource) && Objects.equals(requestReference, that.requestReference) &&
        Objects.equals(requestReference, that.requestReference);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricFunctions, startTime, endTime, filterSet, filterClause, groupByDimensions,
        groupByTimeGranularity, metricNames, dataSource, requestReference, limit);
  }

  @Override
  public String toString() {
    return "ThirdEyeRequest{" + "metricFunctions=" + metricFunctions + ", startTime=" + startTime + ", endTime="
        + endTime + ", filterSet=" + filterSet + ", filterClause='" + filterClause + '\'' + ", groupByDimensions="
        + groupByDimensions + ", groupByTimeGranularity=" + groupByTimeGranularity + ", metricNames=" + metricNames
        + ", dataSource='" + dataSource + '\'' + ", requestReference='" + requestReference + '\'' +
        ", limit='" + limit + '\'' + '}';
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
    private String dataSource;
    private int limit;

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

    public ThirdEyeRequestBuilder setLimit(int limit) {
      this.limit = limit;
      return this;
    }

    public ThirdEyeRequest build(String requestReference) {
      return new ThirdEyeRequest(requestReference, this);
    }
  }
}
