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
import com.linkedin.thirdeye.detection.DefaultInputDataFetcher;
import com.linkedin.thirdeye.detection.InputDataFetcher;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import java.util.Map;


/**
 * Static anomaly filter stage. High level interface for anomaly filter.
 */
public abstract class StaticAnomalyFilterStage implements AnomalyFilterStage {
  private long configId;

  @Override
  public void init(Map<String, Object> specs, Long configId, long startTime, long endTime) {
    this.configId = configId;
  }

  /**
   * Returns a data spec describing all required data(time series, aggregates, existing anomalies) to perform a stage.
   * Data is retrieved in one pass and cached between executions if possible.
   * @return input data spec
   */
  abstract InputDataSpec getInputDataSpec();

  /**
   * Check if an anomaly is qualified to pass the filter
   * @param anomaly the anomaly
   * @param data data(time series, anomalies, etc.) as described by data spec
   * @return a boolean value to suggest if the anomaly should be filtered
   */
  abstract boolean isQualified(MergedAnomalyResultDTO anomaly, InputData data);

  @Override
  public final boolean isQualified(MergedAnomalyResultDTO anomaly, DataProvider provider) {
    InputDataFetcher dataFetcher = new DefaultInputDataFetcher(provider, -1);
    return isQualified(anomaly, dataFetcher.fetchData(this.getInputDataSpec()));
  }
}
