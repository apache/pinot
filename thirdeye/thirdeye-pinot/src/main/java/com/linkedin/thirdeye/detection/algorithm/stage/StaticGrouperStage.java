/*
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

package com.linkedin.thirdeye.detection.algorithm.stage;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import java.util.List;

import static com.linkedin.thirdeye.detection.wrapper.DetectionUtils.*;

/**
 * Static Grouper stage. High level interface for grouper stage.
 */
public abstract class StaticGrouperStage implements GrouperStage {
  /**
   * Returns a data spec describing all required data(time series, aggregates, existing anomalies) to perform a stage.
   * Data is retrieved in one pass and cached between executions if possible.
   * @return input data spec
   */
  abstract InputDataSpec getInputDataSpec();

  /**
   * group anomalies.
   *
   * @param anomalies list of anomalies
   * @return list of anomalies, with grouped dimensions
   */
  abstract List<MergedAnomalyResultDTO> group(List<MergedAnomalyResultDTO> anomalies, InputData data);

  @Override
  public final List<MergedAnomalyResultDTO> group(List<MergedAnomalyResultDTO> anomalies, DataProvider provider) {
    return this.group(anomalies, getDataForSpec(provider, this.getInputDataSpec()));
  }
}
