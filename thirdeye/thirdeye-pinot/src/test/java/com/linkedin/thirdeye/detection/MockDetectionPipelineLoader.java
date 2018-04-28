package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;


public class MockDetectionPipelineLoader extends DetectionPipelineLoader {
  final List<MockDetectionPipeline> runs;
  private List<MergedAnomalyResultDTO> mockMergedAnomalies;
  private Integer mockLastTimeStamp;

  public MockDetectionPipelineLoader(List<MockDetectionPipeline> runs) {
    this.runs = runs;
  }

  @Override
  public DetectionPipeline from(DataProvider provider, DetectionConfigDTO config, long start, long end) {
    MockDetectionPipeline p = new MockDetectionPipeline(provider, config, start, end, mockMergedAnomalies, mockLastTimeStamp);
    runs.add(p);
    return p;
  }

  public List<MergedAnomalyResultDTO> getMockMergedAnomalies() {
    return mockMergedAnomalies;
  }

  public void setMockMergedAnomalies(List<MergedAnomalyResultDTO> mockMergedAnomalies) {
    this.mockMergedAnomalies = mockMergedAnomalies;
  }

  public Integer getMockLastTimeStamp() {
    return mockLastTimeStamp;
  }

  public void setMockLastTimeStamp(Integer mockLastTimeStamp) {
    this.mockLastTimeStamp = mockLastTimeStamp;
  }
}
