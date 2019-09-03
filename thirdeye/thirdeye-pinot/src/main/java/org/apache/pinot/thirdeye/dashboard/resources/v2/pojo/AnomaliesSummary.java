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

public class AnomaliesSummary {
  Long metricId;
  String metricName;
  long startTime;
  long endTime;
  int numAnomalies;
  int numAnomaliesResolved;
  int numAnomaliesUnresolved;

  public int getNumAnomalies() {
    return numAnomalies;
  }
  public void setNumAnomalies(int numAnomalies) {
    this.numAnomalies = numAnomalies;
  }
  public int getNumAnomaliesResolved() {
    return numAnomaliesResolved;
  }
  public void setNumAnomaliesResolved(int numAnomaliesResolved) {
    this.numAnomaliesResolved = numAnomaliesResolved;
  }
  public int getNumAnomaliesUnresolved() {
    return numAnomaliesUnresolved;
  }
  public void setNumAnomaliesUnresolved(int numAnomaliesUnresolved) {
    this.numAnomaliesUnresolved = numAnomaliesUnresolved;
  }
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
  public long getStartTime() {
    return startTime;
  }
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
  public long getEndTime() {
    return endTime;
  }
  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }


}
