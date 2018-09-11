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

package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;

public interface GroupedAnomalyResultsManager extends AbstractManager<GroupedAnomalyResultsDTO> {
  /**
   * Returns the GroupedAnomalyResults, which has the largest end time, in the specified time window.
   *
   * @param alertConfigId the alert config id of the grouped anomaly results.
   * @param dimensions the dimension map of the grouped anomaly results.
   * @param windowStart the start time in milliseconds of the time range.
   * @param windowEnd the end time in milliseconds of the time range.
   *
   * @return the GroupedAnomalyResults, which has the largest end time, in the specified time window.
   */
  GroupedAnomalyResultsDTO findMostRecentInTimeWindow(long alertConfigId, String dimensions, long windowStart,
      long windowEnd);
}
