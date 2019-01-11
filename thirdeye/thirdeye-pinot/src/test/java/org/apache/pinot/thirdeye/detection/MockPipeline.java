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

package org.apache.pinot.thirdeye.detection;

import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
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
