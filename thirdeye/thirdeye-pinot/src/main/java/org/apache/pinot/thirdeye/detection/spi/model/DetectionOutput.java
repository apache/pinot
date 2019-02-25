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

package org.apache.pinot.thirdeye.detection.spi.model;

import java.util.List;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

/**
 * Result for anomaly detector which contains anomalies and TimeSeries, which contains baseline/upper/lower bounds.
 *
 * @see TimeSeries
 *
 */
public class DetectionOutput {

  public DetectionOutput(List<MergedAnomalyResultDTO> anomalies, TimeSeries predictions) {
    this.anomalies = anomalies;
    this.predictions = predictions;
  }

  public List<MergedAnomalyResultDTO> getAnomalies() {
    return anomalies;
  }

  public void setAnomalies(List<MergedAnomalyResultDTO> anomalies) {
    this.anomalies = anomalies;
  }

  public TimeSeries getPredictions() {
    return predictions;
  }

  public void setPredictions(TimeSeries predictions) {
    this.predictions = predictions;
  }

  List<MergedAnomalyResultDTO> anomalies;
  TimeSeries predictions;
}
