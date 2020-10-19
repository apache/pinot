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
 *
 */

package org.apache.pinot.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@JsonIgnoreProperties(ignoreUnknown=true)
public class DatasetConfigBean extends AbstractBean {
  public static String DEFAULT_PREAGGREGATED_DIMENSION_VALUE = "all";
  // This is the expected delay for the hourly/daily data source.
  // 1 hour delay means we always expect to have 1 hour's before's data.
  public static TimeGranularity DEFAULT_HOURLY_EXPECTED_DELAY = new TimeGranularity(1, TimeUnit.HOURS);
  public static TimeGranularity DEFAULT_DAILY_EXPECTED_DELAY = new TimeGranularity(24, TimeUnit.HOURS);

  private String dataset;

  private String displayName;

  private List<String> dimensions;

  private String timeColumn;

  private TimeUnit timeUnit;

  private Integer timeDuration;

  private String timeFormat = TimeSpec.SINCE_EPOCH_FORMAT;

  private String timezone = TimeSpec.DEFAULT_TIMEZONE;

  private String dataSource;

  private Set<String> owners;

  private boolean active = true;

  /** Configuration for non-additive dataset **/
  // By default, every dataset is additive and this section of configurations should be ignored.
  private boolean additive = true;

  // We assume that non-additive dataset has a default TOP dimension value, which is specified via preAggregatedKeyword,
  // for each dimension. When ThirdEye constructs query string to backend database, it automatically appends the keyword
  // for ALL dimensions except the dimension that has been specified by filter the one specified in
  // dimensionsHaveNoPreAggregation (because some dimension may not have such pre-aggregated dimension value).
  // For example, assume that we have three dimensions: D1, D2, D3. The preAggregatedKeyword is "all" and D2 does not
  // has any pre-aggregated dimension value, i.e., dimensionsHaveNoPreAggregation=[D2]. The filter given by user is
  // {D3=V3}. Then the query should append the WHERE condition {D1=all, D2=V3} in order to get the correct results from
  // this non-additive dataset.
  private List<String> dimensionsHaveNoPreAggregation = Collections.emptyList();

  // The pre-aggregated keyword
  private String preAggregatedKeyword = DEFAULT_PREAGGREGATED_DIMENSION_VALUE;

  // the actual time duration for non-additive dataset
  private Integer nonAdditiveBucketSize;

  // the actual time unit for non-additive dataset
  private TimeUnit nonAdditiveBucketUnit;
  /** End of Configuration for non-additive dataset **/

  private boolean realtime = false;

  // delay expected for a dataset for data to arrive
  private TimeGranularity expectedDelay = DEFAULT_DAILY_EXPECTED_DELAY;
  // latest timestamp of the dataset updated by external events
  private long lastRefreshTime;
  // timestamp of receiving the last update event
  private long lastRefreshEventTime = 0;


  private Map<String, String> properties = new HashMap<>();


  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<String> dimensions) {
    this.dimensions = dimensions;
  }

  public String getTimeColumn() {
    return timeColumn;
  }

  public void setTimeColumn(String timeColumn) {
    this.timeColumn = timeColumn;
  }

  /**
   * Use DatasetConfigDTO.bucketTimeGranularity instead of this method for considering the additives of the dataset.
   *
   * This method is preserved for reading object from database via object mapping (i.e., Java reflection)
   *
   * @return the time unit of the granularity of the timestamp of each data point.
   */
  @Deprecated
  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
  }

  /**
   * Use DatasetConfigDTO.bucketTimeGranularity instead of this method for considering the additives of the dataset.
   *
   * This method is preserved for reading object from database via object mapping (i.e., Java reflection)
   *
   * @return the duration of the granularity of the timestamp of each data point.
   */
  @Deprecated
  public Integer getTimeDuration() {
    return timeDuration;
  }

  public void setTimeDuration(Integer timeDuration) {
    this.timeDuration = timeDuration;
  }

  public String getTimeFormat() {
    return timeFormat;
  }

  public void setTimeFormat(String timeFormat) {
    this.timeFormat = timeFormat;
  }

  public String getTimezone() {
    return timezone;
  }

  public void setTimezone(String timezone) {
    this.timezone = timezone;
  }

  public String getDataSource() {
    return dataSource;
  }

  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  public Set<String> getOwners() {
    return owners;
  }

  public void setOwners(Set<String> owners) {
    this.owners = owners;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public boolean isAdditive() {
    return additive;
  }

  public void setAdditive(boolean additive) {
    this.additive = additive;
  }

  public List<String> getDimensionsHaveNoPreAggregation() {
    return dimensionsHaveNoPreAggregation;
  }

  public void setDimensionsHaveNoPreAggregation(List<String> dimensionsHaveNoPreAggregation) {
    this.dimensionsHaveNoPreAggregation = dimensionsHaveNoPreAggregation;
  }

  public String getPreAggregatedKeyword() {
    return preAggregatedKeyword;
  }

  public void setPreAggregatedKeyword(String preAggregatedKeyword) {
    this.preAggregatedKeyword = preAggregatedKeyword;
  }

  public Integer getNonAdditiveBucketSize() {
    return nonAdditiveBucketSize;
  }

  public void setNonAdditiveBucketSize(Integer nonAdditiveBucketSize) {
    this.nonAdditiveBucketSize = nonAdditiveBucketSize;
  }

  public TimeUnit getNonAdditiveBucketUnit() {
    return nonAdditiveBucketUnit;
  }

  public void setNonAdditiveBucketUnit(TimeUnit nonAdditiveBucketUnit) {
    this.nonAdditiveBucketUnit = nonAdditiveBucketUnit;
  }

  public boolean isRealtime() {
    return realtime;
  }

  public void setRealtime(boolean realtime) {
    this.realtime = realtime;
  }

  public TimeGranularity getExpectedDelay() {
    return expectedDelay;
  }

  public void setExpectedDelay(TimeGranularity expectedDelay) {
    this.expectedDelay = expectedDelay;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public long getLastRefreshTime() {
    return lastRefreshTime;
  }

  public void setLastRefreshTime(long lastRefreshTime) {
    this.lastRefreshTime = lastRefreshTime;
  }

  public long getLastRefreshEventTime() {
    return lastRefreshEventTime;
  }

  public void setLastRefreshEventTime(long lastRefreshEventTime) {
    this.lastRefreshEventTime = lastRefreshEventTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DatasetConfigBean)) {
      return false;
    }
    DatasetConfigBean that = (DatasetConfigBean) o;
    return active == that.active && additive == that.additive && realtime == that.realtime &&
        Objects.equals(dataset, that.dataset) && Objects.equals(displayName, that.displayName)
        && Objects.equals(dimensions, that.dimensions) && Objects.equals(timeColumn, that.timeColumn)
        && timeUnit == that.timeUnit && Objects.equals(timeDuration, that.timeDuration)
        && Objects.equals(timeFormat, that.timeFormat) && Objects.equals(timezone, that.timezone)
        && Objects.equals(dataSource, that.dataSource) && Objects.equals(owners, that.owners) && Objects.equals(
        dimensionsHaveNoPreAggregation, that.dimensionsHaveNoPreAggregation) && Objects.equals(preAggregatedKeyword,
        that.preAggregatedKeyword) && Objects.equals(nonAdditiveBucketSize, that.nonAdditiveBucketSize)
        && nonAdditiveBucketUnit == that.nonAdditiveBucketUnit && Objects.equals(expectedDelay, that.expectedDelay)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataset, displayName, dimensions, timeColumn, timeUnit, timeDuration, timeFormat, timezone,
        dataSource, owners, active, additive, dimensionsHaveNoPreAggregation, preAggregatedKeyword,
        nonAdditiveBucketSize, nonAdditiveBucketUnit, realtime, expectedDelay, properties);
  }
}
