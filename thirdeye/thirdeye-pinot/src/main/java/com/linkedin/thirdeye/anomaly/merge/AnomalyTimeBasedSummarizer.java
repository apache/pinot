package com.linkedin.thirdeye.anomaly.merge;

import com.linkedin.thirdeye.anomalydetection.AnomalyDetectionUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;



/**
 * Given list of {@link RawAnomalyResultDTO} and merge parameters, this utility performs time based merge
 */
public abstract class AnomalyTimeBasedSummarizer {
  private final static Logger LOG = LoggerFactory.getLogger(AnomalyTimeBasedSummarizer.class);

  private AnomalyTimeBasedSummarizer() {

  }

  /**
   * @param anomalies   : list of raw anomalies to be merged with last mergedAnomaly
   * @param mergeConfig : the configurations for merging, i.e. maxMergedDurationMillis, sequentialAllowedGap, mergeablePropertyKeys
   * @return
   */
  public static List<MergedAnomalyResultDTO> mergeAnomalies(List<RawAnomalyResultDTO> anomalies, AnomalyMergeConfig mergeConfig) {
    return mergeAnomalies(null, anomalies, mergeConfig);
  }

  /**
   * @param mergedAnomaly : last merged anomaly
   * @param anomalies     : list of raw anomalies to be merged with last mergedAnomaly
   * @param mergeConfig   : the configurations needed for time based merging: i.e. maxMergedDurationMillis, sequentialAllowedGap, mergeablePropertyKeys
   * @return
   */
  public static List<MergedAnomalyResultDTO> mergeAnomalies(MergedAnomalyResultDTO mergedAnomaly,
      List<RawAnomalyResultDTO> anomalies, AnomalyMergeConfig mergeConfig) {

    long maxMergedDurationMillis = mergeConfig.getMaxMergeDurationLength();
    long sequentialAllowedGap = mergeConfig.getSequentialAllowedGap();
    List<String> mergeablePropertyKeys = mergeConfig.getMergeablePropertyKeys();

    // sort anomalies in natural order of start time
    Collections.sort(anomalies, new Comparator<RawAnomalyResultDTO>() {
      @Override
      public int compare(RawAnomalyResultDTO o1, RawAnomalyResultDTO o2) {
        return (int) ((o1.getStartTime() - o2.getStartTime()) / 1000);
      }
    });

    boolean applySequentialGapBasedSplit = false;
    boolean applyMaxDurationBasedSplit = false;

    if (maxMergedDurationMillis > 0) {
      applyMaxDurationBasedSplit = true;
    }

    if (sequentialAllowedGap > 0) {
      applySequentialGapBasedSplit = true;
    }

    List<MergedAnomalyResultDTO> mergedAnomalies = new ArrayList<>();

    for (int i = 0; i < anomalies.size(); i++) {
      RawAnomalyResultDTO currentResult = anomalies.get(i);
      if (mergedAnomaly == null || currentResult.getEndTime() < mergedAnomaly.getStartTime()) {
        mergedAnomaly = new MergedAnomalyResultDTO();
        populateMergedResult(mergedAnomaly, currentResult);
      } else {
        // compare current with merged and decide whether to merge the current result or create a new one
        MergedAnomalyResultDTO currAnomaly = new MergedAnomalyResultDTO();
        populateMergedResult(currAnomaly, currentResult);
        // if the merging is applying sequential gap and current anomaly has gap time larger than sequentialAllowedGap
        // or the duration of the anomaly to be merged is longer than the maxMergedDurationMillis
        // or current anomaly is not equal on mergeable keys with mergedAnomaly
        // should not merge the two and split from here
        if ((applySequentialGapBasedSplit
            && (currentResult.getStartTime() - mergedAnomaly.getEndTime()) > sequentialAllowedGap)
            || ( applyMaxDurationBasedSplit
            && (currentResult.getEndTime() - mergedAnomaly.getStartTime()) > maxMergedDurationMillis)
            || (!isEqualOnMergeableKeys(mergedAnomaly, currAnomaly, mergeablePropertyKeys))) {

          // Split here
          // add previous merged result
          mergedAnomalies.add(mergedAnomaly);

          //set current raw result
          mergedAnomaly = currAnomaly;
        } else {
          // add the current raw result into mergedResult
          if (currentResult.getStartTime() < mergedAnomaly.getStartTime()) {
            mergedAnomaly.setStartTime(currentResult.getStartTime());
          }
          if (currentResult.getEndTime() > mergedAnomaly.getEndTime()) {
            mergedAnomaly.setEndTime(currentResult.getEndTime());
          }
          if (!mergedAnomaly.getAnomalyResults().contains(currentResult)) {
            mergedAnomaly.getAnomalyResults().add(currentResult);
            currentResult.setMerged(true);
          }
        }
      }

      if (i == (anomalies.size() - 1) && mergedAnomaly != null) {
        mergedAnomalies.add(mergedAnomaly);
      }
    }
    LOG.info("merging [{}] raw anomalies", anomalies.size());
    return mergedAnomalies;
  }


  private static void populateMergedResult(MergedAnomalyResultDTO mergedAnomaly,
      RawAnomalyResultDTO currentResult) {
    if (!mergedAnomaly.getAnomalyResults().contains(currentResult)) {
      mergedAnomaly.getAnomalyResults().add(currentResult);
      currentResult.setMerged(true);
    }
    // only set collection, keep metric, dimensions and function null
    mergedAnomaly.setCollection(currentResult.getCollection());
    mergedAnomaly.setMetric(currentResult.getMetric());
    mergedAnomaly.setStartTime(currentResult.getStartTime());
    mergedAnomaly.setEndTime(currentResult.getEndTime());
    mergedAnomaly.setCreatedTime(System.currentTimeMillis());
    // populate current result's property as well, will be used to identify if two anomalies can be merged by comparing their mergeable keys
    mergedAnomaly.setProperties(AnomalyDetectionUtils.decodeCompactedPropertyStringToMap(currentResult.getProperties()));
  }


  /**
   * Given property keys from anomaly function, comparing if two anomalies have same property on the mergeable keys when doing anomaly detection
   * If key set is empty, or both properties for the two anomalies are empty or if all of the values on mergeable keys are equal on anomalies return true
   * Otherwise return false
   * @param anomaly1 The first anomaly result
   * @param anomaly2 The second anomaly result
   * @param mergeableKeys keys that passed by AnomalyMergeConfig, which is defined by Anomaly Detection Function
   * @return true if two anomalies are equal on mergeable keys, otherwise return false
   */
  private static boolean isEqualOnMergeableKeys(MergedAnomalyResultDTO anomaly1, MergedAnomalyResultDTO anomaly2, List<String> mergeableKeys){
    Map<String, String> prop1 = anomaly1.getProperties();
    Map<String, String> prop2 = anomaly2.getProperties();
    // degenerate case
    if(mergeableKeys.size() == 0 ||
        (MapUtils.isEmpty(prop1) && MapUtils.isEmpty(prop2))){
      return true;
    }
    // If both of anomalies have mergeable keys and the contents are equal, they are mergeable;
    // Otherwise it's indicating the two anomalies are detected by different function configurations, they are not mergeable
    for (String key : mergeableKeys) {
      // If both prop1 and prop2 do not contain key, the mergeable keys are not properly defined or the anomalies are not generated by the anomaly function
      if (!prop1.containsKey(key) && !prop2.containsKey(key)) {
        LOG.warn("Mergeable key: {} does not exist in properties! The mergeable keys are not properly defined or the anomalies are not generated by the anomaly function", key);
      }
      // If prop1 and prop2 have different value on key, return false
      if (!ObjectUtils.equals(prop1.get(key), prop2.get(key))) {
        return false;
      }
    }
    return true;
  }
}
