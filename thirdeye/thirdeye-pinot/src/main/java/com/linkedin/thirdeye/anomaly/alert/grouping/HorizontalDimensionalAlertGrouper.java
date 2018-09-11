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

package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A grouper that groups anomalies by each of the specified dimensions individually.
 *
 * An usage example:
 * Assume that we have four anomalies, whose dimensions are enclosed in brackets: a1={D1=G1, D2=H1}, a2={D1=G1, D2=H2},
 * a3={D1=G2, D2=H1}, and a4={D1=G2, D2=H2}. If we group by these dimensions: "D1,D2", then this grouper returns these
 * groups:
 *
 * groupKey={D1=G1} : a1, a2
 * groupKey={D1=G2} : a3, a4
 * groupKey={D2=H1} : a1, a3
 * groupKey={D2=H2} : a2, a4
 */
public class HorizontalDimensionalAlertGrouper extends BaseAlertGrouper {
  private static final Logger LOG = LoggerFactory.getLogger(HorizontalDimensionalAlertGrouper.class);
  // Used when the user does not specify any dimensions to group by
  private static final DummyAlertGrouper DUMMY_ALERT_GROUPER = new DummyAlertGrouper();

  public static final String GROUP_BY_KEY = "dimensions";
  public static final String GROUP_BY_SEPARATOR = ",";

  // The dimension names to group the anomalies (e.g., country, page_name)
  private List<String> groupByDimensions = new ArrayList<>();

  @Override
  public void setParameters(Map<String, String> props) {
    super.setParameters(props);
    // Initialize dimension names to be grouped by
    if (this.props.containsKey(GROUP_BY_KEY)) {
      String[] dimensions = this.props.get(GROUP_BY_KEY).split(GROUP_BY_SEPARATOR);
      for (String dimension : dimensions) {
        groupByDimensions.add(dimension.trim());
      }
    }
  }

  @Override
  public Map<DimensionMap, GroupedAnomalyResultsDTO> group(List<MergedAnomalyResultDTO> anomalyResults) {
    if (CollectionUtils.isEmpty(groupByDimensions)) {
      return DUMMY_ALERT_GROUPER.group(anomalyResults);
    } else {
      Map<DimensionMap, GroupedAnomalyResultsDTO> groupedAnomaliesMap = new HashMap<>();
      for (String groupByDimension : groupByDimensions) {
        for (MergedAnomalyResultDTO anomalyResult : anomalyResults) {
          DimensionMap anomalyDimensionMap = anomalyResult.getDimensions();
          DimensionMap alertGroupKey = this.constructGroupKey(anomalyDimensionMap, groupByDimension);
          if (groupedAnomaliesMap.containsKey(alertGroupKey)) {
            GroupedAnomalyResultsDTO groupedAnomalyResults = groupedAnomaliesMap.get(alertGroupKey);
            groupedAnomalyResults.getAnomalyResults().add(anomalyResult);
          } else {
            GroupedAnomalyResultsDTO groupedAnomalyResults = new GroupedAnomalyResultsDTO();
            groupedAnomalyResults.getAnomalyResults().add(anomalyResult);
            groupedAnomaliesMap.put(alertGroupKey, groupedAnomalyResults);
          }
        }
      }
     return groupedAnomaliesMap;
    }
  }

  private DimensionMap constructGroupKey(DimensionMap rawKey, String dimensionName) {
    DimensionMap alertGroupKey = new DimensionMap();
    if (rawKey.containsKey(dimensionName)) {
      alertGroupKey.put(dimensionName, rawKey.get(dimensionName));
    }
    return alertGroupKey;
  }
}
