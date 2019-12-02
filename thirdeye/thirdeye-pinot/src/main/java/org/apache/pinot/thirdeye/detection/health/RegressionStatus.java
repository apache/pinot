/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.pinot.thirdeye.detection.health;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * The regression status for a detection config
 */
public class RegressionStatus {
  // the average mape for each detector
  @JsonProperty
  private final Map<String, Double> detectorMapes;

  // the health status for each detector
  @JsonProperty
  private final Map<String, HealthStatus> detectorHealthStatus;

  // the overall regression health for the detection config
  @JsonProperty
  private final HealthStatus healthStatus;

  // default constructor for deserialization
  public RegressionStatus() {
    this.detectorMapes = Collections.emptyMap();
    this.detectorHealthStatus = Collections.emptyMap();
    this.healthStatus = HealthStatus.UNKNOWN;
  }

  public RegressionStatus(Map<String, Double> detectorMapes, Map<String, HealthStatus> detectorHealthStatus,
      HealthStatus healthStatus) {
    this.detectorMapes = detectorMapes;
    this.detectorHealthStatus = detectorHealthStatus;
    this.healthStatus = healthStatus;
  }

  public static RegressionStatus fromDetectorMapes(Map<String, Double> detectorMapes) {
    Map<String, HealthStatus> detectorHealthStatus = detectorMapes.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> classifyMapeHealth(e.getValue())));
    return new RegressionStatus(detectorMapes, detectorHealthStatus,
        classifyOverallRegressionStatus(detectorHealthStatus));
  }

  public Map<String, Double> getDetectorMapes() {
    return detectorMapes;
  }

  public Map<String, HealthStatus> getDetectorHealthStatus() {
    return detectorHealthStatus;
  }

  public HealthStatus getHealthStatus() {
    return healthStatus;
  }

  private static HealthStatus classifyMapeHealth(double mape) {
    if (Double.isNaN(mape)) {
      return HealthStatus.UNKNOWN;
    }
    if (mape < 0.2) {
      return HealthStatus.GOOD;
    }
    if (mape < 0.5) {
      return HealthStatus.MODERATE;
    }
    return HealthStatus.BAD;
  }

  /**
   * Classify the regression status of the detection config based on the health status for each detector
   * @param detectorHealthStatus the health status for each detector
   * @return the overall regression status
   */
  private static HealthStatus classifyOverallRegressionStatus(Map<String, HealthStatus> detectorHealthStatus) {
    if (detectorHealthStatus.isEmpty()) {
      return HealthStatus.UNKNOWN;
    }
    if (detectorHealthStatus.values().contains(HealthStatus.GOOD)) {
      return HealthStatus.GOOD;
    }
    if (detectorHealthStatus.values().contains(HealthStatus.MODERATE)) {
      return HealthStatus.MODERATE;
    }
    return HealthStatus.BAD;
  }
}
