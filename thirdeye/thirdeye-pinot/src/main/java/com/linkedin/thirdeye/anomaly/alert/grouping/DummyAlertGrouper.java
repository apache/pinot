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
public class DummyAlertGrouper extends BaseAlertGrouper<DimensionMap> {

  @Override
  public Map<AlertGroupKey<DimensionMap>, GroupedAnomalyResultsDTO> group(List<MergedAnomalyResultDTO> anomalyResults) {
    Map<AlertGroupKey<DimensionMap>, GroupedAnomalyResultsDTO> groupMap = new HashMap<>();
    AlertGroupKey<DimensionMap> alertGroupKey = this.constructGroupKey(null);
    GroupedAnomalyResultsDTO groupedAnomalyResults = new GroupedAnomalyResultsDTO();
    groupedAnomalyResults.setAnomalyResults(anomalyResults);
    groupMap.put(alertGroupKey, groupedAnomalyResults);
    return groupMap;
  }

  @Override
  public AlertGroupKey<DimensionMap> constructGroupKey(DimensionMap rawKey) {
    return AlertGroupKey.emptyKey();
  }

  @Override
  public String groupEmailRecipients(AlertGroupKey alertGroupKey) {
    return BaseAlertGrouper.EMPTY_RECIPIENTS;
  }
}
