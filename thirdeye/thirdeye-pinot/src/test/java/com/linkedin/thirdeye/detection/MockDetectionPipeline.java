package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class MockDetectionPipeline extends DetectionPipeline {

  List<MergedAnomalyResultDTO> mockAnomalies;
  Integer mockLastTimeStamp;

  public MockDetectionPipeline(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);
  }

  public MockDetectionPipeline(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime, List<MergedAnomalyResultDTO> mockAnomalies,
       Integer mockLastTimeStamp){
    super(provider, config, startTime, endTime);
    this.mockAnomalies = mockAnomalies;
    this.mockLastTimeStamp = mockLastTimeStamp;
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    if (this.mockAnomalies != null){
      if (this.mockLastTimeStamp != null) {
        return new DetectionPipelineResult(this.mockAnomalies, this.mockLastTimeStamp);
      } else {
        return new DetectionPipelineResult(this.mockAnomalies, -1);
      }
    }
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
