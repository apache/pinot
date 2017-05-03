package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;

public class GroupedAnomalySizeSeverityFilter {
  private int threshold = 3;
  private Map<Set<String>, Integer> overrideThreshold = new HashMap<>();

  public GroupedAnomalySizeSeverityFilter() {
    // TODO: Read the override config from DB
    Set<String> country = new HashSet<>();
    country.add("country");
    overrideThreshold.put(country, 4);
    Set<String> pageName = new HashSet<>();
    pageName.add("page_name");
    overrideThreshold.put(pageName, 3);
  }

  public boolean isQualified(DimensionMap dimensionGroupKey, GroupedAnomalyResultsDTO groupedAnomaly) {
    Set<String> dimensionNames = new HashSet<>();
    dimensionNames.addAll(dimensionGroupKey.keySet());
    int threshold = this.threshold;
    if (overrideThreshold.containsKey(dimensionNames)) {
      threshold = overrideThreshold.get(dimensionNames);
    }
    return CollectionUtils.size(groupedAnomaly.getAnomalyResults()) > threshold;
  }
}
