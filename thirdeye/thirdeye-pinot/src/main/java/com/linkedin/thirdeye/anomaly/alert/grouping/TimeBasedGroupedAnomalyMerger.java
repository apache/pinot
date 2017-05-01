package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import java.util.Map;

public class TimeBasedGroupedAnomalyMerger {
  public static Map<AlertGroupKey, GroupedAnomalyResultsDTO> mergeAnomalies(
      Map<AlertGroupKey, GroupedAnomalyResultsDTO> groupedAnomalyResults, long maxMergedDurationMillis,
      long sequentialAllowedGap) {

      return null;
  }
}
