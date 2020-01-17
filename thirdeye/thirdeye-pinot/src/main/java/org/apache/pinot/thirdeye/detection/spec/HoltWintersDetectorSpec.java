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


public class HoltWintersDetectorSpec  extends AbstractSpec  {

  private double alpha = -1;
  private double beta = -1;
  private double gamma = -1;
  private int period = 7;
  private double sensitivity = 5;
  private Pattern pattern = Pattern.UP_OR_DOWN;
  private boolean smoothing = true;
  private String monitoringGranularity = MetricSlice.NATIVE_GRANULARITY.toAggregationGranularityString(); // use native granularity by default
  private String weekStart = "WEDNESDAY";

  public boolean getSmoothing() {
    return smoothing;
  }

  public Pattern getPattern() {
    return pattern;
  }

  public double getSensitivity() {
    return sensitivity;
  }

  public double getAlpha() {
    return alpha;
  }

  public double getBeta() {
    return beta;
  }

  public double getGamma() {
    return gamma;
  }

  public int getPeriod() {
    return period;
  }

  public String getMonitoringGranularity() {
    return monitoringGranularity;
  }

  public void setAlpha(double alpha) {
    this.alpha = alpha;
  }

  public void setBeta(double beta) {
    this.beta = beta;
  }

  public void setGamma(double gamma) {
    this.gamma = gamma;
  }

  public void setPeriod(int period) {
    this.period = period;
  }

  public void setPattern(Pattern pattern) {
    this.pattern = pattern;
  }

  public void setSensitivity(double sensitivity) {
    this.sensitivity = sensitivity;
  }

  public void setSmoothing(boolean smoothing) {
    this.smoothing = smoothing;
  }

  public void setMonitoringGranularity(String monitoringGranularity) {
    this.monitoringGranularity = monitoringGranularity;
  }

  public String getWeekStart() {
    return weekStart;
  }

  public void setWeekStart(String weekStart) {
    this.weekStart = weekStart;
  }
}
