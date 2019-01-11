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

package org.apache.pinot.thirdeye.dashboard.resources.v2.pojo;

public class MetricSummary {
  Long metricId;
  String metricName;
  String metricAlias;

  long baselineStart;
  long baselineEnd;
  long currentStart;
  long currentEnd;

  double baselineValue;
  double currentValue;
  double wowPercentageChange;

  AnomaliesSummary anomaliesSummary;

  public Long getMetricId() {
    return metricId;
  }
  public void setMetricId(Long metricId) {
    this.metricId = metricId;
  }
  public String getMetricName() {
    return metricName;
  }
  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }
  public String getMetricAlias() {
    return metricAlias;
  }
  public void setMetricAlias(String metricAlias) {
    this.metricAlias = metricAlias;
  }
  public long getBaselineStart() {
    return baselineStart;
  }
  public void setBaselineStart(long baselineStart) {
    this.baselineStart = baselineStart;
  }
  public long getBaselineEnd() {
    return baselineEnd;
  }
  public void setBaselineEnd(long baselineEnd) {
    this.baselineEnd = baselineEnd;
  }
  public long getCurrentStart() {
    return currentStart;
  }
  public void setCurrentStart(long currentStart) {
    this.currentStart = currentStart;
  }
  public long getCurrentEnd() {
    return currentEnd;
  }
  public void setCurrentEnd(long currentEnd) {
    this.currentEnd = currentEnd;
  }
  public double getBaselineValue() {
    return baselineValue;
  }
  public void setBaselineValue(double baselineValue) {
    this.baselineValue = baselineValue;
  }
  public double getCurrentValue() {
    return currentValue;
  }
  public void setCurrentValue(double currentValue) {
    this.currentValue = currentValue;
  }
  public double getWowPercentageChange() {
    return wowPercentageChange;
  }
  public void setWowPercentageChange(double wowPercentageChange) {
    this.wowPercentageChange = wowPercentageChange;
  }
  public AnomaliesSummary getAnomaliesSummary() {
    return anomaliesSummary;
  }
  public void setAnomaliesSummary(AnomaliesSummary anomaliesSummary) {
    this.anomaliesSummary = anomaliesSummary;
  }




}
