package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import java.util.Objects;


public class MockPipeline extends DetectionPipeline {
  private final MockPipelineOutput output;

  public MockPipeline(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime, MockPipelineOutput output) {
    super(provider, config, startTime, endTime);
    this.output = output;
  }

  @Override
  public DetectionPipelineResult run() {
    return new DetectionPipelineResult(this.output.anomalies, this.output.lastTimestamp);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MockPipeline that = (MockPipeline) o;
    return startTime == that.startTime && endTime == that.endTime && Objects.equals(provider, that.provider)
        && Objects.equals(config, that.config) && Objects.equals(output, that.output);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, config, startTime, endTime, output);
  }
}
