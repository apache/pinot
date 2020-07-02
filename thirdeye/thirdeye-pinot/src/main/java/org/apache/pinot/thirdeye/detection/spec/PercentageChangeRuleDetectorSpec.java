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

package org.apache.pinot.thirdeye.detection.spec;

import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;


public class PercentageChangeRuleDetectorSpec extends AbstractSpec {
  private double percentageChange = Double.NaN;
  private String offset = "wo1w";
  private String timezone = DEFAULT_TIMEZONE;
  private String pattern = "UP_OR_DOWN";
  private String weekStart = "WEDNESDAY";
  private String monitoringGranularity = MetricSlice.NATIVE_GRANULARITY.toAggregationGranularityString(); // use native granularity by default

  public String getMonitoringGranularity() {
    return monitoringGranularity;
  }

  public void setMonitoringGranularity(String monitoringGranularity) {
    this.monitoringGranularity = monitoringGranularity;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  public String getTimezone() {
    return timezone;
  }

  public void setTimezone(String timezone) {
    this.timezone = timezone;
  }

  public String getOffset() {
    return offset;
  }

  public void setOffset(String offset) {
    this.offset = offset;
  }

  public double getPercentageChange() {
    return percentageChange;
  }

  public void setPercentageChange(double percentageChange) {
    this.percentageChange = percentageChange;
  }

  public String getWeekStart() {
    return weekStart;
  }

  public void setWeekStart(String weekStart) {
    this.weekStart = weekStart;
  }
}
