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
import java.util.List;

/**
 * The anomaly grouping stage. Low level interface of grouper stage.
 * User use data provider to fetch the data they need to group anomalies.
 */
public interface GrouperStage extends BaseDetectionStage {
  /**
   * group anomalies.
   *
   * @param anomalies list of anomalies
   * @return list of anomalies, with grouped dimensions
   */
  List<MergedAnomalyResultDTO> group(List<MergedAnomalyResultDTO> anomalies, DataProvider provider);
}
