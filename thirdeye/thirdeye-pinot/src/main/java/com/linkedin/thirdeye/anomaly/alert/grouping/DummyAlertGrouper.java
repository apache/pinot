package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DummyAlertGrouper extends BaseAlertGrouper<DimensionMap> {

  @Override
  public Map<GroupKey<DimensionMap>, GroupedAnomalyResults> group(List<MergedAnomalyResultDTO> anomalyResults) {
    Map<GroupKey<DimensionMap>, GroupedAnomalyResults> groupMap = new HashMap<>();
    GroupKey<DimensionMap> groupKey = this.constructGroupKey(null);
    GroupedAnomalyResults groupedAnomalyResults = new GroupedAnomalyResults();
    groupedAnomalyResults.setAnomalyResults(anomalyResults);
    groupMap.put(groupKey, groupedAnomalyResults);
    return groupMap;
  }

  @Override
  public GroupKey<DimensionMap> constructGroupKey(DimensionMap rawKey) {
    return new GroupKey<>(new DimensionMap());
  }

  @Override
  public String groupEmailRecipients(GroupKey groupKey) {
    return "";
  }
}
