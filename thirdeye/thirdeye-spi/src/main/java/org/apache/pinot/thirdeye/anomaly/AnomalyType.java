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
package org.apache.pinot.thirdeye.anomaly;

/**
 * The type of anomaly.
 */
public enum AnomalyType {
  // Metric deviates from normal behavior. This is the default type.
  DEVIATION ("Deviation"),
  // There is a trend change for underline metric.
  TREND_CHANGE ("Trend Change"),
  // The metric is not available within specified time.
  DATA_SLA ("SLA Violation");

  private String label;

  AnomalyType(String label) {
    this.label = label;
  }

  public String getLabel() {
    return label;
  }
}