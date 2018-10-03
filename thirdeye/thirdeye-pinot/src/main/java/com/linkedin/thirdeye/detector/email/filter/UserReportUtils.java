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

package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class UserReportUtils {
  /**
   * Evaluate user report anomaly is qualified given alert filter, user report anomaly, as well as total anomaly set
   * Runs through total anomaly set, find out if total qualified region for system anomalies can reach more than 50% of user report region,
   * return user report anomaly as qualified, otherwise return false
   * @param alertFilter alert filter to evaluate system detected anoamlies isQualified
   * @param userReportAnomaly
   * @return
   */
  public static Boolean isUserReportAnomalyIsQualified(AlertFilter alertFilter,
      MergedAnomalyResultDTO userReportAnomaly) {
    MergedAnomalyResultManager mergedAnomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    List<MergedAnomalyResultDTO> systemAnomalies = mergedAnomalyDAO.findByFunctionId(userReportAnomaly.getFunction().getId());
    long startTime = userReportAnomaly.getStartTime();
    long endTime = userReportAnomaly.getEndTime();
    long qualifiedRegion = 0;
    Collections.sort(systemAnomalies, new Comparator<MergedAnomalyResultDTO>() {
          @Override
          public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
            return Long.compare(o1.getStartTime(), o2.getStartTime());
          }
        });
    for (MergedAnomalyResultDTO anomalyResult : systemAnomalies) {
      if (anomalyResult.getAnomalyResultSource().equals(AnomalyResultSource.DEFAULT_ANOMALY_DETECTION)
          && anomalyResult.getEndTime() >= startTime && anomalyResult.getStartTime() <= endTime &&
          anomalyResult.getDimensions().equals(userReportAnomaly.getDimensions())) {
        if (alertFilter.isQualified(anomalyResult)) {
          qualifiedRegion += anomalyResult.getEndTime() - anomalyResult.getStartTime();
        }
      }
    }
    return qualifiedRegion >= (endTime - startTime) * 0.5;
  }
}
