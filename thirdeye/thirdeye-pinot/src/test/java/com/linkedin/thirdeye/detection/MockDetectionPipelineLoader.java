package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import java.util.List;


public class MockDetectionPipelineLoader extends DetectionPipelineLoader {
  private final List<MockDetectionPipeline> runs;

  public MockDetectionPipelineLoader(List<MockDetectionPipeline> runs) {
    this.runs = runs;
  }

  @Override
  public DetectionPipeline from(DataProvider provider, DetectionConfigDTO config, long start, long end) {
    MockDetectionPipeline p = new MockDetectionPipeline(provider, config, start, end);
    runs.add(p);
    return p;
  }
}
