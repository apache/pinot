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
 *
 */

package org.apache.pinot.thirdeye.detection;

import org.apache.pinot.thirdeye.dataframe.DataFrame;


/**
 * The time series prediction result
 */
public class PredictionResult {
  private final String detectorName;
  private final String metricUrn;
  private final DataFrame predictedTimeSeries;

  /**
   * Construct a prediction result
   * @param detectorName the name for the detector, for example "detection_rule_1:PERCENTAGE_RULE"
   * @param metricUrn the metric urn
   * @param predictedTimeSeries the predicted time series
   */
  public PredictionResult(String detectorName, String metricUrn, DataFrame predictedTimeSeries) {
    this.detectorName = detectorName;
    this.metricUrn = metricUrn;
    this.predictedTimeSeries = predictedTimeSeries;
  }

  public String getDetectorName() {
    return detectorName;
  }

  public String getMetricUrn() {
    return metricUrn;
  }

  public DataFrame getPredictedTimeSeries() {
    return predictedTimeSeries;
  }

  @Override
  public String toString() {
    return "PredictionResult{" + "detectorName='" + detectorName + '\'' + ", metricUrn='" + metricUrn + '\''
        + ", predictedTimeSeries=" + predictedTimeSeries + '}';
  }
}
