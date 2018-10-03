/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.datasource.comparison;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datasource.MetricExpression;

public class TimeOnTimeComparisonRequest {

  private String collectionName;

  private List<MetricExpression> metricExpressions;

  private List<String> groupByDimensions;

  /**
   * easy way to represent AND of multiple dimensions
   * for example
   * filterSet = new HashMap
   * filterSet.put("country", "us")
   * filterSet.put("device", "android")
   * is equivalent to setting filterClause country='us' and device='android'
   */
  private Multimap<String, String> filterSet;

  /**
   * Allows one to specify complex boolean expressions, advanced usage
   */
  private String filterClause;

  // TIME RELATED PARAMETERs
  private DateTime baselineStart;

  private DateTime baselineEnd;

  private DateTime currentStart;

  private DateTime currentEnd;

  private TimeGranularity aggregationTimeGranularity;
  private boolean isEndDateInclusive = false;

  public TimeOnTimeComparisonRequest() {
  }

  public TimeOnTimeComparisonRequest(TimeOnTimeComparisonRequest that) {
    this.collectionName = that.collectionName;
    this.filterClause = that.filterClause;
    this.baselineStart = that.baselineStart;
    this.baselineEnd = that.baselineEnd;
    this.currentStart = that.currentStart;
    this.currentEnd = that.currentEnd;
    if (that.groupByDimensions != null) {
      this.groupByDimensions = new ArrayList<>(that.groupByDimensions);
    }
    if (that.filterSet != null) {
      this.filterSet = ArrayListMultimap.create(that.filterSet);
    }
    if (that.aggregationTimeGranularity != null) {
      this.aggregationTimeGranularity =
          new TimeGranularity(that.aggregationTimeGranularity.getSize(),
              that.aggregationTimeGranularity.getUnit());
    }
    this.metricExpressions = new ArrayList<>(that.metricExpressions);
    this.isEndDateInclusive = that.isEndDateInclusive;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

  public List<MetricExpression> getMetricExpressions() {
    return metricExpressions;
  }

  public void setMetricExpressions(List<MetricExpression> metricExpressions) {
    this.metricExpressions = metricExpressions;
  }

  public List<String> getGroupByDimensions() {
    return groupByDimensions;
  }

  public void setGroupByDimensions(List<String> groupByDimensions) {
    this.groupByDimensions = groupByDimensions;
  }

  public Multimap<String, String> getFilterSet() {
    return filterSet;
  }

  public void setFilterSet(Multimap<String, String> filterSet) {
    this.filterSet = filterSet;
  }

  public String getFilterClause() {
    return filterClause;
  }

  public void setFilterClause(String filterClause) {
    this.filterClause = filterClause;
  }

  public DateTime getBaselineStart() {
    return baselineStart;
  }

  public void setBaselineStart(DateTime baselineStart) {
    this.baselineStart = baselineStart;
  }

  public DateTime getBaselineEnd() {
    return baselineEnd;
  }

  public void setBaselineEnd(DateTime baselineEnd) {
    this.baselineEnd = baselineEnd;
  }

  public DateTime getCurrentStart() {
    return currentStart;
  }

  public void setCurrentStart(DateTime currentStart) {
    this.currentStart = currentStart;
  }

  public DateTime getCurrentEnd() {
    return currentEnd;
  }

  public void setCurrentEnd(DateTime currentEnd) {
    this.currentEnd = currentEnd;
  }

  public TimeGranularity getAggregationTimeGranularity() {
    return aggregationTimeGranularity;
  }

  public void setAggregationTimeGranularity(TimeGranularity aggregationTimeGranularity) {
    this.aggregationTimeGranularity = aggregationTimeGranularity;
  }

  public boolean isEndDateInclusive() {
    return isEndDateInclusive;
  }

  public void setEndDateInclusive(boolean isEndDateInclusive) {
    this.isEndDateInclusive = isEndDateInclusive;
  }
}
