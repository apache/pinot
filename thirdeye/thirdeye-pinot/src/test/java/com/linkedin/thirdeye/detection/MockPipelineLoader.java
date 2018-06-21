package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.List;


public class MockPipelineLoader extends DetectionPipelineLoader {
  private final List<MockPipeline> runs;
  private final List<MockPipelineOutput> outputs;
  private int offset = 0;

  public MockPipelineLoader(List<MockPipeline> runs, List<MockPipelineOutput> outputs) {
    this.outputs = outputs;
    this.runs = runs;
  }

  @Override
  public DetectionPipeline from(DataProvider provider, DetectionConfigDTO config, long start, long end) {
    MockPipelineOutput output = this.outputs.isEmpty() ?
        new MockPipelineOutput(Collections.<MergedAnomalyResultDTO>emptyList(), -1) :
        this.outputs.get(this.offset++);
    MockPipeline p = new MockPipeline(provider, config, start, end, output);
    this.runs.add(p);
    return p;
  }
}
