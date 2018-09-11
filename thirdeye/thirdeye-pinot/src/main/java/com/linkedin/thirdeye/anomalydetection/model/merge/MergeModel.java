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

package com.linkedin.thirdeye.anomalydetection.model.merge;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Properties;

public interface MergeModel {
  /**
   * Initializes this model with the given properties.
   * @param properties the given properties.
   */
  void init(Properties properties);

  /**
   * Returns the properties of this model.
   */
  Properties getProperties();

  /**
   * Computes the information, e.g., weight, score, average current values, average baseline values,
   * etc., of the given anomaly and update the information to the anomaly.
   *
   * @param anomalyDetectionContext the context that provides the trained prediction model for
   *                                update the information of the given (merged) anomaly.
   *
   * @param anomalyToUpdated the anomaly of which the information is updated.
   */
  void update(AnomalyDetectionContext anomalyDetectionContext, MergedAnomalyResultDTO anomalyToUpdated);
}
