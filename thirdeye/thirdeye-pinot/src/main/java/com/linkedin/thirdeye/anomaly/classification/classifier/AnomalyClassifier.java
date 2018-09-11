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

package com.linkedin.thirdeye.anomaly.classification.classifier;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import java.util.Map;

public interface AnomalyClassifier {
  /**
   * Sets the properties of this classifier.
   *
   * @param props the properties for this classifier.
   */
  void setParameters(Map<String, String> props);

  /**
   * Given a main anomaly and lists of auxiliary anomalies, which could be retrieve through its corresponding metric
   * name, this method returns the issue type for the main anomaly. The issue type will be stored in the property field
   * of anomalies.
   *
   * @param mainAnomaly the main anomaly for which the issue typed is determined.
   * @param auxAnomalies   the auxiliary anomalies for determining the issue type of the main anomaly.
   *
   * @return the issue type to be stored in the property field of the anomaly. Return null to do no-op.
   */
  String classify(MergedAnomalyResultDTO mainAnomaly, Map<String, List<MergedAnomalyResultDTO>> auxAnomalies);
}
