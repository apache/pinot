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


/**
 * The anomaly coverage status for a detection config
 */
public class AnomalyCoverageStatus {
  // the anomaly coverage ratio. the percentage of anomalous duration in the duration of the whole window
  @JsonProperty
  private final double anomalyCoverageRatio;

  // the health status of the anomaly coverage ratio
  @JsonProperty
  private final HealthStatus healthStatus;

  private static final double COVERAGE_RATIO_BAD_UPPER_LIMIT = 0.85;
  private static final double COVERAGE_RATIO_BAD_LOWER_LIMIT = 0.01;
  private static final double COVERAGE_RATIO_MODERATE_LIMIT = 0.5;

  // default constructor for deserialization
  public AnomalyCoverageStatus() {
    this.anomalyCoverageRatio = Double.NaN;
    this.healthStatus = HealthStatus.UNKNOWN;
  }

  public AnomalyCoverageStatus(double anomalyCoverageRatio, HealthStatus healthStatus) {
    this.anomalyCoverageRatio = anomalyCoverageRatio;
    this.healthStatus = healthStatus;
  }

  public static AnomalyCoverageStatus fromCoverageRatio(double anomalyCoverageRatio) {
    return new AnomalyCoverageStatus(anomalyCoverageRatio, classifyCoverageStatus(anomalyCoverageRatio));
  }

  private static HealthStatus classifyCoverageStatus(double anomalyCoverageRatio) {
    if (Double.isNaN(anomalyCoverageRatio)) {
      return HealthStatus.UNKNOWN;
    }
    if (anomalyCoverageRatio > COVERAGE_RATIO_BAD_UPPER_LIMIT
        || anomalyCoverageRatio < COVERAGE_RATIO_BAD_LOWER_LIMIT) {
      return HealthStatus.BAD;
    }
    if (anomalyCoverageRatio > COVERAGE_RATIO_MODERATE_LIMIT) {
      return HealthStatus.MODERATE;
    }
    return HealthStatus.GOOD;
  }

  public double getAnomalyCoverageRatio() {
    return anomalyCoverageRatio;
  }

  public HealthStatus getHealthStatus() {
    return healthStatus;
  }
}
