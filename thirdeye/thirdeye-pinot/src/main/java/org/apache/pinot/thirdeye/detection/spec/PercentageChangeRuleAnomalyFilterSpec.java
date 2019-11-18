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

public class PercentageChangeRuleAnomalyFilterSpec extends AbstractSpec {
  private String timezone = DEFAULT_TIMEZONE;
  private String offset;
  private String pattern= "UP_OR_DOWN";
  private double threshold = 0.0; // by default set threshold to 0 to pass all anomalies
  private double upThreshold = Double.NaN;
  private double downThreshold = Double.NaN;

  public String getTimezone() {
    return timezone;
  }

  public void setTimezone(String timezone) {
    this.timezone = timezone;
  }

  public double getThreshold() {
    return threshold;
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }

  public String getOffset() {
    return offset;
  }

  public void setOffset(String offset) {
    this.offset = offset;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  public double getUpThreshold() {
    return upThreshold;
  }

  public void setUpThreshold(double upThreshold) {
    this.upThreshold = upThreshold;
  }

  public double getDownThreshold() {
    return downThreshold;
  }

  public void setDownThreshold(double downThreshold) {
    this.downThreshold = downThreshold;
  }
}
