package org.apache.pinot.thirdeye.dashboard.resources.v2.pojo;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;


public class DetectionPipelineResultView {
  public List<MergedAnomalyResultDTO> getAnomalies() {
    return anomalies;
  }

  public long getLastTimestamp() {
    return lastTimestamp;
  }

  public List<PredictionView> getPredictions() {
    return predictions;
  }

  public Map<String, Object> getDiagnostics() {
    return diagnostics;
  }

  Map<String, Object> diagnostics;
  List<MergedAnomalyResultDTO> anomalies;
  long lastTimestamp;
  List<PredictionView> predictions;

  public DetectionPipelineResultView(DetectionPipelineResult result) {
    this.lastTimestamp = result.getLastTimestamp();
    this.anomalies = result.getAnomalies();
    this.diagnostics = result.getDiagnostics();
    predictions = new ArrayList<>();
    for (TimeSeries timeSeries : result.getPredictions()) {
      PredictionView view = new PredictionView(timeSeries);
      predictions.add(view);
    }
  }
}
