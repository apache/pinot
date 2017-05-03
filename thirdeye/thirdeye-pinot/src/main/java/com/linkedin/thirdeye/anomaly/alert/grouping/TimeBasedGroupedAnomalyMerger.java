package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TimeBasedGroupedAnomalyMerger {
  public static Map<DimensionMap, GroupedAnomalyResultsDTO> timeBasedMergeGroupedAnomalyResults(
      Map<DimensionMap, GroupedAnomalyResultsDTO> recentGroupedAnomaly,
      Map<DimensionMap, GroupedAnomalyResultsDTO> newGroupedAnomalies) {
    Map<DimensionMap, GroupedAnomalyResultsDTO> updatedGroupedAnomalies = new HashMap<>();
    for (Map.Entry<DimensionMap, GroupedAnomalyResultsDTO> groupedAnomalyEntry : newGroupedAnomalies.entrySet()) {
      // Retrieve the most recent anomaly for the same dimension
      GroupedAnomalyResultsDTO mostRecentGroupedAnomaly = recentGroupedAnomaly.get(groupedAnomalyEntry.getKey());

      // Merge grouped anomalies
      GroupedAnomalyResultsDTO updatedGroupedAnomaly;
      if (mostRecentGroupedAnomaly != null) {
        List<MergedAnomalyResultDTO> mergedAnomalyResults = mostRecentGroupedAnomaly.getAnomalyResults();
        Set<Long> existingMergedAnomalyId = new HashSet<>();
        for (MergedAnomalyResultDTO mergedAnomalyResult : mergedAnomalyResults) {
          existingMergedAnomalyId.add(mergedAnomalyResult.getId());
        }
        for (MergedAnomalyResultDTO newMergedAnomaly : groupedAnomalyEntry.getValue().getAnomalyResults()) {
          if (!existingMergedAnomalyId.contains(newMergedAnomaly.getId())) {
            mergedAnomalyResults.add(newMergedAnomaly);
            existingMergedAnomalyId.add(newMergedAnomaly.getId());
          }
        }
        updatedGroupedAnomaly = mostRecentGroupedAnomaly;
      } else {
        updatedGroupedAnomaly = groupedAnomalyEntry.getValue();
      }
      updatedGroupedAnomalies.put(groupedAnomalyEntry.getKey(), updatedGroupedAnomaly);
    }
    return updatedGroupedAnomalies;
  }
//  public static Map<DimensionMap, List<GroupedAnomalyResultsDTO>> timeBasedMergeGroupedAnomalyResults(
//      Map<DimensionMap, GroupedAnomalyResultsDTO> recentGroupedAnomalies,
//      Map<DimensionMap, GroupedAnomalyResultsDTO> newGroupedAnomalies) {
//    Map<DimensionMap, GroupedAnomalyResultsDTO> updatedGroupedAnomalies = new HashMap<>();
//    for (Map.Entry<DimensionMap, GroupedAnomalyResultsDTO> groupedAnomalyEntry : newGroupedAnomalies.entrySet()) {
//      // Retrieve the most recent anomaly for the same dimension
//      GroupedAnomalyResultsDTO recentGroupedAnomaly = recentGroupedAnomalies.get(groupedAnomalyEntry.getKey());
//
//      DimensionMap dimensions = groupedAnomalyEntry.getKey();
//      GroupedAnomalyResultsDTO newGroupedAnomaly = groupedAnomalyEntry.getValue();
//      List<MergedAnomalyResultDTO> mergedAnomalies = newGroupedAnomaly.getAnomalyResults();
//      long alertConfigId = newGroupedAnomaly.getAlertConfigId();
//
//      boolean veryFirstMergedAnomaly = true;
//      for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
//        // Merge grouped anomalies
//        GroupedAnomalyResultsDTO updatedGroupedAnomaly;
//        if (recentGroupedAnomaly != null) {
//
//          List<MergedAnomalyResultDTO> mergedAnomalyResults = recentGroupedAnomaly.getAnomalyResults();
//          Set<Long> existingMergedAnomalyId = new HashSet<>();
//          for (MergedAnomalyResultDTO mergedAnomalyResult : mergedAnomalyResults) {
//            existingMergedAnomalyId.add(mergedAnomalyResult.getId());
//          }
//          for (MergedAnomalyResultDTO newMergedAnomaly : groupedAnomalyEntry.getValue().getAnomalyResults()) {
//            if (!existingMergedAnomalyId.contains(newMergedAnomaly.getId())) {
//              mergedAnomalyResults.add(newMergedAnomaly);
//            }
//          }
//          updatedGroupedAnomaly = recentGroupedAnomaly;
//        } else {
//          GroupedAnomalyResultsDTO groupedAnomaly = populateGroupedAnomaly(dimensions, alertConfigId, mergedAnomaly);
//          updatedGroupedAnomalies.put(dimensions, groupedAnomaly);
//          recentGroupedAnomaly = groupedAnomaly;
//        }
//        updatedGroupedAnomalies.put(groupedAnomalyEntry.getKey(), updatedGroupedAnomaly);
//
//        veryFirstMergedAnomaly = false;
//      }
//    }
//    return updatedGroupedAnomalies;
//  }

  private static GroupedAnomalyResultsDTO populateGroupedAnomaly(DimensionMap dimensions, long alergConfigId,
      MergedAnomalyResultDTO mergedAnomaly) {
    GroupedAnomalyResultsDTO groupedAnomaly = new GroupedAnomalyResultsDTO();
    groupedAnomaly.setDimensions(dimensions);
    groupedAnomaly.setAlertConfigId(alergConfigId);
    List<MergedAnomalyResultDTO> newMergedAnomalies = new ArrayList<>();
    newMergedAnomalies.add(mergedAnomaly);
    groupedAnomaly.setAnomalyResults(newMergedAnomalies);
    return groupedAnomaly;
  }
}
