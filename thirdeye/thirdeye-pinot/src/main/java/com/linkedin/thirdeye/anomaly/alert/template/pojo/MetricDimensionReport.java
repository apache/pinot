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

package com.linkedin.thirdeye.anomaly.alert.template.pojo;

import java.util.Map;

public class MetricDimensionReport {
  long metricId;
  String dataset;
  String metricName;
  String dimensionName;
  long currentStartTime;
  long currentEndTime;
  long baselineStartTime;
  long baselineEndTime;
  Map<String, Map<String, String>> subDimensionValueMap;
  Map<String, String> subDimensionTotalValueMap;
  Map<String, String> subDimensionShareValueMap;

  String compareMode = "WoW";

  public long getMetricId() {
    return metricId;
  }

  public void setMetricId(long metricId) {
    this.metricId = metricId;
  }

  public String getCompareMode() {
    return compareMode;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public void setCompareMode(String compareMode) {
    this.compareMode = compareMode;
  }

  public Map<String, Map<String, String>> getSubDimensionValueMap() {
    return subDimensionValueMap;
  }

  public void setSubDimensionValueMap(Map<String, Map<String, String>> subDimensionValueMap) {
    this.subDimensionValueMap = subDimensionValueMap;
  }

  public long getBaselineEndTime() {
    return baselineEndTime;
  }

  public void setBaselineEndTime(long baselineEndTime) {
    this.baselineEndTime = baselineEndTime;
  }

  public long getBaselineStartTime() {
    return baselineStartTime;
  }

  public void setBaselineStartTime(long baselineStartTime) {
    this.baselineStartTime = baselineStartTime;
  }

  public long getCurrentEndTime() {
    return currentEndTime;
  }

  public void setCurrentEndTime(long currentEndTime) {
    this.currentEndTime = currentEndTime;
  }

  public long getCurrentStartTime() {
    return currentStartTime;
  }

  public void setCurrentStartTime(long currentStartTime) {
    this.currentStartTime = currentStartTime;
  }

  public String getDimensionName() {
    return dimensionName;
  }

  public void setDimensionName(String dimensionName) {
    this.dimensionName = dimensionName;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public Map<String, String> getSubDimensionShareValueMap() {
    return subDimensionShareValueMap;
  }

  public void setSubDimensionShareValueMap(Map<String, String> subDimensionShareValueMap) {
    this.subDimensionShareValueMap = subDimensionShareValueMap;
  }

  public Map<String, String> getSubDimensionTotalValueMap() {
    return subDimensionTotalValueMap;
  }

  public void setSubDimensionTotalValueMap(Map<String, String> subDimensionTotalValueMap) {
    this.subDimensionTotalValueMap = subDimensionTotalValueMap;
  }
}
