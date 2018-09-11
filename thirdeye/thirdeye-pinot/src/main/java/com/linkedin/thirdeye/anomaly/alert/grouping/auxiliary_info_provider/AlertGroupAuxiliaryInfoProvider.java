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

package com.linkedin.thirdeye.anomaly.alert.grouping.auxiliary_info_provider;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import java.util.Map;

/**
 * Given a dimension map, a provider returns a list of email recipients (separated by commas) for that dimension.
 */
public interface AlertGroupAuxiliaryInfoProvider {

  /**
   * Sets the properties of this grouper.
   *
   * @param props the properties for this grouper.
   */
  void setParameters(Map<String, String> props);

  /**
   * Returns a list of email recipients (separated by commas) for the given dimension.
   *
   * @param dimensions the dimension of the group, which is used to look for the recipients.
   * @param anomalyResultList the list of anomalies of this group, whose information could be used to determine the
   *                          recipients.
   *
   * @return auxiliary alert group info, including a list of auxiliary email recipients (separated by commas).
   */
  AuxiliaryAlertGroupInfo getAlertGroupAuxiliaryInfo(DimensionMap dimensions, List<MergedAnomalyResultDTO> anomalyResultList);
}
