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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class SitewideImpactRuleAnomalyFilterSpec extends AbstractSpec {
  private String timezone = "UTC";
  private double threshold = Double.NaN;
  private String offset;
  private String pattern = "UP_OR_DOWN";
  private String sitewideMetricName;
  private String sitewideCollection;
  private Map<String, Collection<String>> filters = new HashMap<>();

  public String getSitewideMetricName() {
    return sitewideMetricName;
  }

  public void setSitewideMetricName(String sitewideMetricName) {
    this.sitewideMetricName = sitewideMetricName;
  }

  public String getSitewideCollection() {
    return sitewideCollection;
  }

  public void setSitewideCollection(String sitewideCollection) {
    this.sitewideCollection = sitewideCollection;
  }

  public Map<String, Collection<String>> getFilters() {
    return filters;
  }

  public void setFilters(Map<String, Collection<String>> filters) {
    this.filters = filters;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  public String getOffset() {
    return offset;
  }

  public void setOffset(String offset) {
    this.offset = offset;
  }

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
}
