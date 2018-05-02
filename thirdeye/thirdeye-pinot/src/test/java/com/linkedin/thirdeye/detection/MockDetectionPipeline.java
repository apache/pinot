package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.Objects;


public class MockDetectionPipeline extends DetectionPipeline {
  public MockDetectionPipeline(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);
  }

  @Override
  public DetectionPipelineResult run() {
    return new DetectionPipelineResult(Collections.<MergedAnomalyResultDTO>emptyList(), -1);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DetectionPipeline that = (DetectionPipeline) o;
    return startTime == that.startTime && endTime == that.endTime && Objects.equals(provider, that.provider)
        && Objects.equals(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, config, startTime, endTime);
  }
}
