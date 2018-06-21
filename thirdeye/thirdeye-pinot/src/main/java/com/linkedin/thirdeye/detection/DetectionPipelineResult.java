package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;


public class DetectionPipelineResult {
  List<MergedAnomalyResultDTO> anomalies;
  long lastTimestamp;

  public DetectionPipelineResult(List<MergedAnomalyResultDTO> anomalies) {
    this.anomalies = anomalies;
    this.lastTimestamp = getMaxTime(anomalies);
  }

  public DetectionPipelineResult(List<MergedAnomalyResultDTO> anomalies, long lastTimestamp) {
    this.anomalies = anomalies;
    this.lastTimestamp = lastTimestamp;
  }

  public List<MergedAnomalyResultDTO> getAnomalies() {
    return anomalies;
  }

  public void setAnomalies(List<MergedAnomalyResultDTO> anomalies) {
    this.anomalies = anomalies;
  }

  public long getLastTimestamp() {
    return lastTimestamp;
  }

  public void setLastTimestamp(long lastTimestamp) {
    this.lastTimestamp = lastTimestamp;
  }

  private static long getMaxTime(Iterable<MergedAnomalyResultDTO> anomalies) {
    long maxTime = -1;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      maxTime = Math.max(maxTime, anomaly.getEndTime());
    }
    return maxTime;
  }
}
