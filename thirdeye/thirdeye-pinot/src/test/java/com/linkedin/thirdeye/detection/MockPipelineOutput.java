package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import java.util.Objects;


public class MockPipelineOutput {
  final List<MergedAnomalyResultDTO> anomalies;
  final long lastTimestamp;

  public MockPipelineOutput(List<MergedAnomalyResultDTO> anomalies, long lastTimestamp) {
    this.anomalies = anomalies;
    this.lastTimestamp = lastTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MockPipelineOutput that = (MockPipelineOutput) o;
    return lastTimestamp == that.lastTimestamp && Objects.equals(anomalies, that.anomalies);
  }

  @Override
  public int hashCode() {

    return Objects.hash(anomalies, lastTimestamp);
  }
}
