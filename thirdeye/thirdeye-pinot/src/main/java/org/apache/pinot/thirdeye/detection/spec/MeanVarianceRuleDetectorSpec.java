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
import org.apache.pinot.thirdeye.detection.Pattern;


public class MeanVarianceRuleDetectorSpec extends AbstractSpec {
  private String monitoringGranularity = MetricSlice.NATIVE_GRANULARITY.toAggregationGranularityString(); // use native granularity by default
  private int lookback = 52; //default look back of 52 units
  private double sensitivity = 5; //default sensitivity of 5, equals +/- 1 sigma
  private Pattern pattern = Pattern.UP_OR_DOWN;

  public String getMonitoringGranularity() {
    return monitoringGranularity;
  }

  public void setMonitoringGranularity(String monitoringGranularity) {
    this.monitoringGranularity = monitoringGranularity;
  }

  public Pattern getPattern() {
    return pattern;
  }

  public void setPattern(Pattern pattern) {
    this.pattern = pattern;
  }

  public int getLookback() { return lookback;}

  public void setLookback(int lookback) {this.lookback = lookback;}

  public double getSensitivity() {
    return sensitivity;
  }

  public void setSensitivity(double sensitivity) {
    this.sensitivity = sensitivity;
  }

}
