package com.linkedin.thirdeye.anomaly.merge;

import com.linkedin.thirdeye.api.dto.MergedAnomalyResult;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AnomalySummaryGenerator {

  private AnomalySummaryGenerator() {

  }

  /**
   * @param anomalies   : list of raw anomalies to be merged with last mergedAnomaly
   * @param mergeConfig : merge configuration params
   *
   * @return
   */
  public static List<MergedAnomalyResult> mergeAnomalies(List<AnomalyResult> anomalies,
      AnomalyMergeConfig mergeConfig) {
    return mergeAnomalies(null, anomalies, mergeConfig);
  }

  /**
   * @param mergedAnomaly : last merged anomaly
   * @param anomalies     : list of raw anomalies to be merged with last mergedAnomaly
   * @param mergeConfig   : merge configuration params
   *
   * @return
   */
  public static List<MergedAnomalyResult> mergeAnomalies(MergedAnomalyResult mergedAnomaly,
      List<AnomalyResult> anomalies, AnomalyMergeConfig mergeConfig) {

    // sort anomalies in natural order of start time
    Collections
        .sort(anomalies, (o1, o2) -> (int) ((o1.getStartTimeUtc() - o2.getStartTimeUtc()) / 1000));

    boolean applySequentialGapBasedSplit = false;
    boolean applyMaxDurationBasedSplit = false;

    if (mergeConfig.getMergeDuration() > 0) {
      applyMaxDurationBasedSplit = true;
    }

    if (mergeConfig.getSequentialAllowedGap() > 0) {
      applySequentialGapBasedSplit = true;
    }

    List<MergedAnomalyResult> mergedAnomalies = new ArrayList<>();

    for (int i = 0; i < anomalies.size(); i++) {
      AnomalyResult currentResult = anomalies.get(i);
      if (mergedAnomaly == null) {
        mergedAnomaly = new MergedAnomalyResult();
        populateMergedResult(mergedAnomaly, currentResult);
        mergedAnomaly.setMergeStrategy(mergeConfig.getMergeStrategy());
      } else {
        // check if Max Duration for merged has passed, if so, create new one
        if (applyMaxDurationBasedSplit) {
          if (mergedAnomaly.getEndTime() - mergedAnomaly.getStartTime() >= mergeConfig
              .getMergeDuration()) {
            // Split here
            mergedAnomalies.add(mergedAnomaly);
            mergedAnomaly = new MergedAnomalyResult();
            populateMergedResult(mergedAnomaly, currentResult);
            mergedAnomaly.setMergeStrategy(mergeConfig.getMergeStrategy());
          }
        }

        // compare current with merged and decide whether to merge the current result or create a new one
        if (applySequentialGapBasedSplit
            && (currentResult.getStartTimeUtc() - mergedAnomaly.getEndTime()) > mergeConfig
            .getSequentialAllowedGap()) {
          // Split here if not processed already
          mergedAnomalies.add(mergedAnomaly);
          mergedAnomaly = new MergedAnomalyResult();
          populateMergedResult(mergedAnomaly, currentResult);
          mergedAnomaly.setMergeStrategy(mergeConfig.getMergeStrategy());
        } else {
          // add the current result into mergedResult
          if (currentResult.getStartTimeUtc() < mergedAnomaly.getStartTime()) {
            mergedAnomaly.setStartTime(currentResult.getStartTimeUtc());
          }
          if (currentResult.getEndTimeUtc() > mergedAnomaly.getEndTime()) {
            mergedAnomaly.setEndTime(currentResult.getEndTimeUtc());
          }
          if (!mergedAnomaly.getAnomalyResults().contains(currentResult)) {
            mergedAnomaly.getAnomalyResults().add(currentResult);
          }
        }
      }
      if (i == (anomalies.size() - 1)) {
        if (!mergedAnomalies.contains(mergedAnomaly)) {
          mergedAnomalies.add(mergedAnomaly);
        }
      }
    }
    for (MergedAnomalyResult mergedAnomalyResult : mergedAnomalies) {
      populateMergedProperties(mergedAnomalyResult);
    }
    return mergedAnomalies;
  }

  private static void populateMergedProperties(MergedAnomalyResult mergedResult) {
    double totalScore = 0.0;
    double totalWeight = 0.0;
    Map<Integer, Set<String>> mergedDimensions = new HashMap<>();

    int n = mergedResult.getAnomalyResults().size();
    for (AnomalyResult anomalyResult : mergedResult.getAnomalyResults()) {
      totalScore += anomalyResult.getScore();
      totalWeight += anomalyResult.getWeight();

      // Merging dimensions
      String dimensions = anomalyResult.getDimensions();
      if (dimensions != null) {
        String[] dimArr = dimensions.split(",");
        if (dimArr.length > 0) {
          for (int i = 0; i < dimArr.length; i++) {
            if (dimArr[i] != null && !dimArr[i].equals("") && !dimArr[i].equals("*")) {
              if (!mergedDimensions.containsKey(i)) {
                mergedDimensions.put(i, new HashSet<>());
              }
              mergedDimensions.get(i).add(dimArr[i]);
            }
          }
        }
      }
    }
    mergedResult.setScore(totalScore / n);
    mergedResult.setWeight(totalWeight / n);
    StringBuffer dimBuff = new StringBuffer();
    int count = 0;
    for (Map.Entry<Integer, Set<String>> entry : mergedDimensions.entrySet()) {
      // add only non empty dimensions
      if (entry.getValue().size() > 0) {
        if (count != 0) {
          dimBuff.append(',');
        }
        dimBuff.append(entry.getValue().toString());
        count++;
      }
    }
    String dimensions = dimBuff.toString();
    if (!dimensions.equals("")) {
      mergedResult.setDimensions(dimensions);
    }
  }

  private static void populateMergedResult(MergedAnomalyResult mergedAnomaly,
      AnomalyResult currentResult) {
    mergedAnomaly.setFunction(currentResult.getFunction());
    if (!mergedAnomaly.getAnomalyResults().contains(currentResult)) {
      mergedAnomaly.getAnomalyResults().add(currentResult);
    }
    mergedAnomaly.setCreationTime(currentResult.getCreationTimeUtc());
    mergedAnomaly.setDimensions(currentResult.getDimensions());
    mergedAnomaly.setStartTime(currentResult.getStartTimeUtc());
    mergedAnomaly.setEndTime(currentResult.getEndTimeUtc());
    mergedAnomaly.setMessage(currentResult.getMessage());
    mergedAnomaly.setProperties(currentResult.getProperties());
  }
}
