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
