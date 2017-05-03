package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;

public class GroupedAnomalyTimeBasedMerger {
  public static GroupedAnomalyResultsDTO mergeGroupedAnomalies(GroupedAnomalyResultsDTO recentGroupedAnomaly,
      GroupedAnomalyResultsDTO newGroupedAnomaly, long sequentialAllowedGap) {

    long approximateCreateTime = System.currentTimeMillis();
    // The grouped anomaly to be updated
    GroupedAnomalyResultsDTO groupedAnomalyResultsToUpdate;

    // Determine if the new grouped anomaly should be merged to the most recent grouped anomaly
    // TODO: Don't merge group anomaly if recentGroupedAnomaly lasts too long
    if (recentGroupedAnomaly != null) {
      // If the new grouped anomaly is not close to the end of the most recent one, don't merge them.
      // From the empirical observations, anomalies that occur closely are usually induced by the same issue. Thus,
      // we don't use the start time of new grouped anomaly because it could contain an anomaly result, whose start
      // time is earlier than the end time of recentGroupedAnomaly and just passes alert filter in the current
      // anomaly detection.
      long gap = approximateCreateTime - recentGroupedAnomaly.getEndTime();
      if (gap <= 0 || (sequentialAllowedGap > 0 && gap < sequentialAllowedGap)) {
        List<MergedAnomalyResultDTO> mergedAnomalyResults = recentGroupedAnomaly.getAnomalyResults();
        mergedAnomalyResults.addAll(newGroupedAnomaly.getAnomalyResults());
        groupedAnomalyResultsToUpdate = recentGroupedAnomaly;
      } else {
        groupedAnomalyResultsToUpdate = newGroupedAnomaly;
      }
    } else {
      groupedAnomalyResultsToUpdate = newGroupedAnomaly;
    }

    return groupedAnomalyResultsToUpdate;
  }
}
