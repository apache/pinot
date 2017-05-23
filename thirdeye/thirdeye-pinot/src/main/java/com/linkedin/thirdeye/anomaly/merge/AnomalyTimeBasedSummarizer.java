package com.linkedin.thirdeye.anomaly.merge;

import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
   * @param mergeDuration   : length of a merged anomaly
   * @param sequentialAllowedGap : allowed gap between two raw anomalies in order to merge
   * @param anomalyFunctionFactory
   * @return
   */
  public static List<MergedAnomalyResultDTO> mergeAnomalies(
      List<RawAnomalyResultDTO> anomalies,
      long mergeDuration, long sequentialAllowedGap,
      AnomalyFunctionFactory anomalyFunctionFactory) throws Exception {
    return mergeAnomalies(null, anomalies, mergeDuration, sequentialAllowedGap, anomalyFunctionFactory);
  }

  /**
   * @param mergedAnomaly : last merged anomaly
   * @param anomalies     : list of raw anomalies to be merged with last mergedAnomaly
   * @param maxMergedDurationMillis   : length of a merged anomaly
   * @param sequentialAllowedGap : allowed gap between two raw anomalies in order to merge
   * @return
   */
  public static List<MergedAnomalyResultDTO> mergeAnomalies(MergedAnomalyResultDTO mergedAnomaly,
      List<RawAnomalyResultDTO> anomalies, long maxMergedDurationMillis, long sequentialAllowedGap, AnomalyFunctionFactory anomalyFunctionFactory) {

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

        BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(mergedAnomaly.getFunction());
        MergedAnomalyResultDTO currMergeAnomaly = new MergedAnomalyResultDTO();
        populateMergedResult(currMergeAnomaly, currentResult);
        LOG.info("BaseAnomalyFunction Being initiated!, {}", anomalyFunction.getClass());
        if (applySequentialGapBasedSplit
            && (currentResult.getStartTime() - mergedAnomaly.getEndTime()) > sequentialAllowedGap
            && !anomalyFunction.isMergeable(mergedAnomaly, currMergeAnomaly)) {
          // Split here
          // add previous merged result
          mergedAnomalies.add(mergedAnomaly);

          //set current raw result
          mergedAnomaly = new MergedAnomalyResultDTO();
          populateMergedResult(mergedAnomaly, currentResult);
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

      // till this point merged result contains current raw result
      if (applyMaxDurationBasedSplit
          // check if Max Duration for merged has passed, if so, create new one
          && mergedAnomaly.getEndTime() - mergedAnomaly.getStartTime() >= maxMergedDurationMillis) {
        // check if next anomaly has same start time as current one, that should be merged with current one too
        if (i < (anomalies.size() - 1) && anomalies.get(i + 1).getStartTime()
            .equals(currentResult.getStartTime())) {
          // no need to split as we want to include the next raw anomaly into the current one
        } else {
          // Split here
          mergedAnomalies.add(mergedAnomaly);
          mergedAnomaly = null;
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
//    mergedAnomaly.setFunction(mergedAnomaly.getFunction());
    mergedAnomaly.setProperties(mergedAnomaly.getProperties());
  }
}
