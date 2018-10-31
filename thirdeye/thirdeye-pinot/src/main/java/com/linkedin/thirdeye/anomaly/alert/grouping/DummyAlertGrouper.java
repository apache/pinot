package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Returns a group that contains all input anomalies. Additionally, this class always returns empty auxiliary email
 * recipients.
 */
public class DummyAlertGrouper extends BaseAlertGrouper {

  @Override
  public Map<DimensionMap, GroupedAnomalyResultsDTO> group(List<MergedAnomalyResultDTO> anomalyResults) {
    Map<DimensionMap, GroupedAnomalyResultsDTO> groupMap = new HashMap<>();
    GroupedAnomalyResultsDTO groupedAnomalyResults = new GroupedAnomalyResultsDTO();
    groupedAnomalyResults.setAnomalyResults(anomalyResults);
    groupMap.put(new DimensionMap(), groupedAnomalyResults);
    return groupMap;
  }
}
