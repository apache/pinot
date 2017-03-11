package com.linkedin.thirdeye.anomaly.merge;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.detection.TimeSeriesUtil;
import com.linkedin.thirdeye.anomaly.override.OverrideConfigHelper;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.detector.metric.transfer.MetricTransfer;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBasedAnomalyMerger {
  private final static Logger LOG = LoggerFactory.getLogger(TimeBasedAnomalyMerger.class);

  private final MergedAnomalyResultManager mergedResultDAO;
  private final OverrideConfigManager overrideConfigDAO;
  private final AnomalyFunctionFactory anomalyFunctionFactory;

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private final static AnomalyMergeConfig DEFAULT_TIME_BASED_MERGE_CONFIG;
  static {
    DEFAULT_TIME_BASED_MERGE_CONFIG = new AnomalyMergeConfig();
    DEFAULT_TIME_BASED_MERGE_CONFIG.setSequentialAllowedGap(TimeUnit.HOURS.toMillis(2)); // merge anomalies apart 2 hours
    DEFAULT_TIME_BASED_MERGE_CONFIG.setMaxMergeDurationLength(TimeUnit.DAYS.toMillis(7) - 3600_000); // break anomaly longer than 6 days 23 hours
    DEFAULT_TIME_BASED_MERGE_CONFIG.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);
  }

  public TimeBasedAnomalyMerger(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.mergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.overrideConfigDAO = DAO_REGISTRY.getOverrideConfigDAO();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  /**
   * Performs a time based merge, which merged anomalies that have the same function id and dimensions, etc.
   * This method is supposed to be performed by anomaly detectors right after their anomaly detection.
   *
   * Time based merge logic works as following:
   *
   * Step 1: for the given function, find all groups of raw (unprocessed) anomalies based on
   *         merge strategy (FunctionId and/or dimensions)
   *
   * Step 2: For each such group, find the base mergedAnomaly
   *
   * Step 3: perform time based merge
   *
   * Step 4: Recompute anomaly score / weight
   *
   * Step 5: persist merged anomalies
   *
   * @param functionSpec the spec of the function that detects anomalies
   * @param isBackfill set to true to disable the alert of the merged anomalies
   *
   * @return the number of merged anomalies after merging
   */
  public ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergeAnomalies(AnomalyFunctionDTO functionSpec,
      ListMultimap<DimensionMap, RawAnomalyResultDTO> unmergedAnomalies, boolean isBackfill) {

    int rawAnomaliesCount = 0;
    for (DimensionMap dimensionMap : unmergedAnomalies.keySet()) {
      rawAnomaliesCount += unmergedAnomalies.get(dimensionMap).size();
    }
    LOG.info("Running merge for function id : [{}], found [{}] raw anomalies", functionSpec.getId(), rawAnomaliesCount);

    AnomalyMergeConfig mergeConfig = functionSpec.getAnomalyMergeConfig();
    if (mergeConfig == null) {
      mergeConfig = DEFAULT_TIME_BASED_MERGE_CONFIG;
    }

    if (unmergedAnomalies.size() == 0) {
      return ArrayListMultimap.create();
    } else {
      ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies =
          dimensionalShuffleAndUnifyMerge(functionSpec, mergeConfig, unmergedAnomalies);

      // Update information of merged anomalies
      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : mergedAnomalies.values()) {
        if (isBackfill) {
          mergedAnomalyResultDTO.setNotified(isBackfill);
        } // else notified flag is left as is
        updateMergedAnomalyInfo(mergedAnomalyResultDTO, mergeConfig);
      }

      return mergedAnomalies;
    }
  }

  private ListMultimap<DimensionMap, MergedAnomalyResultDTO> dimensionalShuffleAndUnifyMerge(AnomalyFunctionDTO function,
      AnomalyMergeConfig mergeConfig, ListMultimap<DimensionMap, RawAnomalyResultDTO> dimensionsResultMap) {

    ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies = ArrayListMultimap.create();

    for (DimensionMap dimensionMap : dimensionsResultMap.keySet()) {
      List<RawAnomalyResultDTO> unmergedResultsByDimensions = dimensionsResultMap.get(dimensionMap);
      long anomalyWindowStart = Long.MAX_VALUE;
      long anomalyWindowEnd = Long.MIN_VALUE;
      for (RawAnomalyResultDTO unmergedResultsByDimension : unmergedResultsByDimensions) {
        anomalyWindowStart = Math.min(anomalyWindowStart, unmergedResultsByDimension.getStartTime());
        anomalyWindowEnd = Math.max(anomalyWindowEnd, unmergedResultsByDimension.getEndTime());
      }

      // NOTE: We get "latest overlapped (Conflict)" merged anomaly instead of "recent" merged anomaly in order to
      // prevent the merge results of current (online) detection interfere the merge results of back-fill (offline)
      // detection.
      // Moreover, the window start is modified by mergeConfig.getSequentialAllowedGap() in order to allow a gap between
      // anomalies to be merged.
      MergedAnomalyResultDTO latestOverlappedMergedResult =
          mergedResultDAO.findLatestConflictByFunctionIdDimensions(function.getId(), dimensionMap.toString(),
              anomalyWindowStart - mergeConfig.getSequentialAllowedGap(), anomalyWindowEnd);

      List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer
          .mergeAnomalies(latestOverlappedMergedResult, unmergedResultsByDimensions,
              mergeConfig.getMaxMergeDurationLength(), mergeConfig.getSequentialAllowedGap());
      for (MergedAnomalyResultDTO mergedResult : mergedResults) {
        mergedResult.setFunction(function);
        mergedResult.setDimensions(dimensionMap);
      }
      LOG.info(
          "Merging [{}] raw anomalies into [{}] merged anomalies for function id : [{}] and dimensions : [{}]",
          unmergedResultsByDimensions.size(), mergedResults.size(), function.getId(), dimensionMap);
      mergedAnomalies.putAll(dimensionMap, mergedResults);
    }

    return mergedAnomalies;
  }

  private void updateMergedAnomalyInfo(MergedAnomalyResultDTO mergedResult, AnomalyMergeConfig mergeConfig) {
    // Calculate default score and weight in case of the failure during updating score and weight through Pinot's value
    double weightedScoreSum = 0.0;
    double weightedWeightSum = 0.0;
    double totalBucketSize = 0.0;
    double avgCurrent = 0.0;
    double avgBaseline = 0.0;

    double normalizationFactor = 1000; // to prevent from double overflow
    List<RawAnomalyResultDTO> rawAnomalies = mergedResult.getAnomalyResults();
    String anomalyMessage = "";
    for (RawAnomalyResultDTO anomalyResult : rawAnomalies) {
      anomalyResult.setMerged(true);
      double bucketSizeSeconds = (anomalyResult.getEndTime() - anomalyResult.getStartTime()) / 1000;
      weightedScoreSum += (anomalyResult.getScore() / normalizationFactor) * bucketSizeSeconds;
      weightedWeightSum += (anomalyResult.getWeight() / normalizationFactor) * bucketSizeSeconds;
      totalBucketSize += bucketSizeSeconds;
      avgCurrent += anomalyResult.getAvgCurrentVal();
      avgBaseline += anomalyResult.getAvgBaselineVal();
      anomalyMessage = anomalyResult.getMessage();
    }
    if (totalBucketSize != 0) {
      mergedResult.setScore((weightedScoreSum / totalBucketSize) * normalizationFactor);
      mergedResult.setWeight((weightedWeightSum / totalBucketSize) * normalizationFactor);
      mergedResult.setAvgCurrentVal(avgCurrent / rawAnomalies.size());
      mergedResult.setAvgBaselineVal(avgBaseline / rawAnomalies.size());
    }

    mergedResult.setMessage(anomalyMessage);

    if (rawAnomalies.size() > 1) {
      // recompute weight using anomaly function specific method
      try {
        computeMergedAnomalyInfo(mergedResult, mergeConfig);
      } catch (Exception e) {
        AnomalyFunctionDTO function = mergedResult.getFunction();
        LOG.warn(
            "Unable to compute merged weight and the average weight of raw anomalies is used. Dataset: {}, Topic Metric: {}, Function: {}, Time:{} - {}, Exception: {}",
            function.getCollection(), function.getTopicMetric(), function.getFunctionName(), new DateTime(mergedResult.getStartTime()), new DateTime(mergedResult.getEndTime()), e);
      }
    }
//    try {
//      // persist the merged result
//      mergedResultDAO.update(mergedResult);
//      for (RawAnomalyResultDTO rawAnomalyResultDTO : mergedResult.getAnomalyResults()) {
//        anomalyResultDAO.update(rawAnomalyResultDTO);
//      }
//    } catch (Exception e) {
//      LOG.error("Could not persist merged result : [" + mergedResult.toString() + "]", e);
//    }
  }

  /**
   * Uses function-specific method to re-computes the weight of merged anomaly.
   *
   * @param mergedAnomalies the merged anomaly to be updated
   * @param mergeConfig the merge configuration that was applied when merge the merged anomaly
   * @throws Exception if error occurs when retrieving the time series for calculating the weight
   */
  private void computeMergedAnomalyInfo(MergedAnomalyResultDTO mergedAnomalies, AnomalyMergeConfig mergeConfig)
      throws Exception {
    AnomalyFunctionDTO anomalyFunctionSpec = mergedAnomalies.getFunction();
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

    List<Pair<Long, Long>> startEndTimeRanges =
        anomalyFunction.getDataRangeIntervals(mergedAnomalies.getStartTime(), mergedAnomalies.getEndTime());

    TimeGranularity timeGranularity = new TimeGranularity(anomalyFunctionSpec.getBucketSize(),
        anomalyFunctionSpec.getBucketUnit());

    MetricTimeSeries metricTimeSeries =
        TimeSeriesUtil.getTimeSeriesByDimension(anomalyFunctionSpec, startEndTimeRanges,
            mergedAnomalies.getDimensions(), timeGranularity);

    if (metricTimeSeries != null) {
      DateTime windowStart = new DateTime(mergedAnomalies.getStartTime());
      DateTime windowEnd = new DateTime(mergedAnomalies.getEndTime());

      List<MergedAnomalyResultDTO> knownAnomalies = Collections.emptyList();

      // Retrieve history merged anomalies
      if (anomalyFunction.useHistoryAnomaly()) {
        knownAnomalies = getHistoryMergedAnomalies(anomalyFunction, windowStart.getMillis(), windowEnd.getMillis(),
            mergedAnomalies.getDimensions());

        if (knownAnomalies.size() > 0) {
          LOG.info("Found {} history anomalies for computing the weight of current merged anomaly.", knownAnomalies.size());
          AnomalyUtils.logAnomaliesOverlapWithWindow(windowStart, windowEnd, knownAnomalies);
        }
      }

      // Transform Time Series
      List<ScalingFactor> scalingFactors = OverrideConfigHelper
          .getTimeSeriesScalingFactors(overrideConfigDAO, anomalyFunctionSpec.getCollection(),
              anomalyFunctionSpec.getTopicMetric(), anomalyFunctionSpec.getId(),
              anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis()));
      if (CollectionUtils.isNotEmpty(scalingFactors)) {
        Properties properties = anomalyFunction.getProperties();
        MetricTransfer.rescaleMetric(metricTimeSeries, windowStart.getMillis(), scalingFactors,
            anomalyFunctionSpec.getTopicMetric(), properties);
      }

      anomalyFunction.updateMergedAnomalyInfo(mergedAnomalies, metricTimeSeries, windowStart, windowEnd, knownAnomalies);
    }
  }

  /**
   * Returns history merged anomalies of the function id that are needed for computing the weight of the new merged
   * anomalies; history anomalies are anomalies that occurs before the window of current merged anomaly.
   *
   * @param anomalyFunction the anomaly function that detects the merged anomaly
   * @param anomalyWindowStart the start of the merged anomaly
   * @param anomalyWindowEnd the end of the merged anomaly
   * @param dimensions the dimensions of the merge anomaly
   *
   * @return history merged anomalies of the function id that are needed for computing the weight of the new merged
   * anomalies
   */
  private List<MergedAnomalyResultDTO> getHistoryMergedAnomalies(BaseAnomalyFunction anomalyFunction,
      long anomalyWindowStart, long anomalyWindowEnd, DimensionMap dimensions) {

    List<Pair<Long, Long>> startEndTimeRanges =
        anomalyFunction.getDataRangeIntervals(anomalyWindowStart, anomalyWindowEnd);

    List<MergedAnomalyResultDTO> results = new ArrayList<>();
    for (Pair<Long, Long> startEndTimeRange : startEndTimeRanges) {
      // we don't need the known anomalies on current window; we only need those on history data
      if (anomalyWindowStart <= startEndTimeRange.getFirst() && startEndTimeRange.getSecond() <= anomalyWindowEnd) {
        continue;
      }
      try {
        results.addAll(mergedResultDAO
            .findAllConflictByFunctionIdDimensions(anomalyFunction.getSpec().getId(), startEndTimeRange.getFirst(),
                startEndTimeRange.getSecond(), dimensions.toString()));
      } catch (Exception e) {
        LOG.error("Unable to get history merged anomalies for function {} on dimensions {} in the anomaly window {} -- {}: {}",
            anomalyFunction.getSpec().getId(), dimensions.toString(), anomalyWindowStart, anomalyWindowEnd, e);
      }
    }

    return results;
  }
}
