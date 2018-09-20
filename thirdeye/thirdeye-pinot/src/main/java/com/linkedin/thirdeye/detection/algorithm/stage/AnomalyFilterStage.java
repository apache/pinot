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


/**
 * The anomaly filter stage. Low level interface of anomaly filter stage.
 * User uses data provider to fetch the data they need to filter anomalies.
 */
public interface AnomalyFilterStage extends BaseDetectionStage {
  /**
   * Check if an anomaly is qualified to pass the filter
   * @param anomaly the anomaly
   * @param provider centralized data source for time series, anomalies, events, etc.
   * @return a boolean value to suggest if the anomaly should be filtered
   */
  boolean isQualified(MergedAnomalyResultDTO anomaly, DataProvider provider);
}
