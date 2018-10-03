/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DetectionPipelineResult {
  public static String DIAGNOSTICS_DATA = "data";
  public static String DIAGNOSTICS_CHANGE_POINTS = "changepoints";

  Map<String, Object> diagnostics;
  List<MergedAnomalyResultDTO> anomalies;
  long lastTimestamp;

  public DetectionPipelineResult(List<MergedAnomalyResultDTO> anomalies) {
    this.anomalies = anomalies;
    this.lastTimestamp = getMaxTime(anomalies);
    this.diagnostics = new HashMap<>();
  }

  public DetectionPipelineResult(List<MergedAnomalyResultDTO> anomalies, long lastTimestamp) {
    this.anomalies = anomalies;
    this.lastTimestamp = lastTimestamp;
  }

  public List<MergedAnomalyResultDTO> getAnomalies() {
    return anomalies;
  }

  public DetectionPipelineResult setAnomalies(List<MergedAnomalyResultDTO> anomalies) {
    this.anomalies = anomalies;
    return this;
  }

  public long getLastTimestamp() {
    return lastTimestamp;
  }

  public DetectionPipelineResult setLastTimestamp(long lastTimestamp) {
    this.lastTimestamp = lastTimestamp;
    return this;
  }

  public Map<String, Object> getDiagnostics() {
    return diagnostics;
  }

  public DetectionPipelineResult setDiagnostics(Map<String, Object> diagnostics) {
    this.diagnostics = diagnostics;
    return this;
  }

  private static long getMaxTime(Iterable<MergedAnomalyResultDTO> anomalies) {
    long maxTime = -1;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      maxTime = Math.max(maxTime, anomaly.getEndTime());
    }
    return maxTime;
  }
}
