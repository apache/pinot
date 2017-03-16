package com.linkedin.thirdeye.anomaly.merge;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import com.linkedin.thirdeye.anomaly.detection.TimeSeriesUtil;
import com.linkedin.thirdeye.anomaly.override.OverrideConfigHelper;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.detector.metric.transfer.MetricTransfer;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBasedAnomalyMerger {
  private final static Logger LOG = LoggerFactory.getLogger(TimeBasedAnomalyMerger.class);
  private final static double NORMALIZATION_FACTOR = 1000; // to prevent from double overflow

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
    List<RawAnomalyResultDTO> rawAnomalies = mergedResult.getAnomalyResults();
    if (CollectionUtils.isEmpty(rawAnomalies)) {
      LOG.warn("Skip updating anomaly (id={}) because its does not have any children anomalies.", mergedResult.getId());
      return;
    }

    // Update the info of merged anomalies
    if (rawAnomalies.size() == 1) {
      RawAnomalyResultDTO rawAnomaly = rawAnomalies.get(0);
      mergedResult.setScore(rawAnomaly.getScore());
      mergedResult.setWeight(rawAnomaly.getWeight());
      mergedResult.setAvgCurrentVal(rawAnomaly.getAvgCurrentVal());
      mergedResult.setAvgBaselineVal(rawAnomaly.getAvgBaselineVal());
      mergedResult.setMessage(rawAnomaly.getMessage());
    } else {
      // Calculate default score and weight in case of any failure (e.g., DB exception) during the update
      double weightedScoreSum = 0.0;
      double weightedWeightSum = 0.0;
      double totalBucketSize = 0.0;
      double avgCurrent = 0.0;
      double avgBaseline = 0.0;
      String anomalyMessage = "";

      for (RawAnomalyResultDTO anomalyResult : rawAnomalies) {
        anomalyResult.setMerged(true);
        double bucketSizeSeconds = (anomalyResult.getEndTime() - anomalyResult.getStartTime()) / 1000;
        double normalizedBucketSize = getNormalizedBucketSize(bucketSizeSeconds);
        totalBucketSize += bucketSizeSeconds;
        weightedScoreSum += anomalyResult.getScore() * normalizedBucketSize;
        weightedWeightSum += anomalyResult.getWeight() * normalizedBucketSize;
        avgCurrent += anomalyResult.getAvgCurrentVal() * normalizedBucketSize;
        avgBaseline += anomalyResult.getAvgBaselineVal() * normalizedBucketSize;
        anomalyMessage = anomalyResult.getMessage();
      }
      if (totalBucketSize != 0) {
        double normalizedTotalBucketSize = getNormalizedBucketSize(totalBucketSize);
        mergedResult.setScore(weightedScoreSum / normalizedTotalBucketSize);
        mergedResult.setWeight(weightedWeightSum / normalizedTotalBucketSize);
        mergedResult.setAvgCurrentVal(avgCurrent / normalizedTotalBucketSize);
        mergedResult.setAvgBaselineVal(avgBaseline / normalizedTotalBucketSize);
      }
      mergedResult.setMessage(anomalyMessage);

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
  }

  private static double getNormalizedBucketSize(double secondsInABucket) {
    return secondsInABucket / NORMALIZATION_FACTOR;
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
    long windowStartMillis = mergedAnomalies.getStartTime();
    long windowEndMillis = mergedAnomalies.getEndTime();
    DimensionMap dimensions = mergedAnomalies.getDimensions();

    AnomalyDetectionInputContext adInputContext =
        fetchDataByDimension(windowStartMillis, windowEndMillis, dimensions, anomalyFunction, mergedResultDAO,
            overrideConfigDAO);

    MetricTimeSeries metricTimeSeries = adInputContext.getDimensionKeyMetricTimeSeriesMap().get(dimensions);

    if (metricTimeSeries != null) {
      List<MergedAnomalyResultDTO> knownAnomalies = adInputContext.getKnownMergedAnomalies().get(dimensions);
      // Transform time series with scaling factor
      List<ScalingFactor> scalingFactors = adInputContext.getScalingFactors();
      if (CollectionUtils.isNotEmpty(scalingFactors)) {
        Properties properties = anomalyFunction.getProperties();
        MetricTransfer.rescaleMetric(metricTimeSeries, windowStartMillis, scalingFactors,
            anomalyFunctionSpec.getTopicMetric(), properties);
      }

      DateTime windowStart = new DateTime(windowStartMillis);
      DateTime windowEnd = new DateTime(windowEndMillis);
      anomalyFunction.updateMergedAnomalyInfo(mergedAnomalies, metricTimeSeries, windowStart, windowEnd, knownAnomalies);
    }
  }


  /**
   * Fetch time series, known merged anomalies, and scaling factor for the specified dimension. Note that scaling
   * factor has no dimension information, so all scaling factor in the specified time range will be retrieved.
   *
   * @param windowStartTime the start time for retrieving the data
   * @param windowEndTime the end time for retrieving the data
   * @param dimensions the dimension of the data
   * @param anomalyFunction the anomaly function that produces the anomaly
   * @param mergedResultDAO DAO for merged anomalies
   * @param overrideConfigDAO DAO for override configuration
   * @return an anomaly detection input context that contains all the retrieved data
   * @throws Exception if it fails to retrieve time series from DB.
   */
  public static AnomalyDetectionInputContext fetchDataByDimension(long windowStartTime, long windowEndTime,
      DimensionMap dimensions, BaseAnomalyFunction anomalyFunction, MergedAnomalyResultManager mergedResultDAO,
      OverrideConfigManager overrideConfigDAO)
      throws Exception {
    AnomalyFunctionDTO functionSpec = anomalyFunction.getSpec();
    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(windowStartTime, windowEndTime);
    TimeGranularity timeGranularity = new TimeGranularity(functionSpec.getBucketSize(), functionSpec.getBucketUnit());

    AnomalyDetectionInputContext adInputContext = new AnomalyDetectionInputContext();

    // Retrieve Time Series
    MetricTimeSeries metricTimeSeries =
        TimeSeriesUtil.getTimeSeriesByDimension(functionSpec, startEndTimeRanges, dimensions, timeGranularity);
    Map<DimensionMap, MetricTimeSeries> metricTimeSeriesMap = new HashMap<>();
    metricTimeSeriesMap.put(dimensions, metricTimeSeries);
    adInputContext.setDimensionKeyMetricTimeSeriesMap(metricTimeSeriesMap);

    // Retrieve historical anomaly
    if (anomalyFunction.useHistoryAnomaly()) {
      List<MergedAnomalyResultDTO> knownAnomalies =
          getBaselineKnownAnomaliesByDimension(anomalyFunction, windowStartTime, windowEndTime, dimensions, mergedResultDAO);
      ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalyMap = ArrayListMultimap.create();
      mergedAnomalyMap.putAll(dimensions, knownAnomalies);
      adInputContext.setKnownMergedAnomalies(mergedAnomalyMap);
      if (knownAnomalies.size() > 0) {
        LOG.info("Found {} history anomalies for computing the weight of current merged anomaly.", knownAnomalies.size());
      }
    }

    // Retrieve scaling factor
    List<ScalingFactor> scalingFactors = OverrideConfigHelper
        .getTimeSeriesScalingFactors(overrideConfigDAO, functionSpec.getCollection(),
            functionSpec.getTopicMetric(), functionSpec.getId(),
            anomalyFunction.getDataRangeIntervals(windowStartTime, windowEndTime));
    adInputContext.setScalingFactors(scalingFactors);

    return adInputContext;
  }


  /**
   * Returns known merged anomalies of the function id that are needed for computing the weight of the new merged
   * anomalies; history anomalies are anomalies that occurs before the window of current merged anomaly.
   *
   * @param anomalyFunction the anomaly function that detects the merged anomaly
   * @param windowStart the start of time range to retrieve the known anomalies
   * @param windowEnd the end of time range to retrieve the known anomalies
   * @param dimensions the specified dimensions
   *
   * @return history merged anomalies of the function id that are needed for computing the weight of the new merged
   * anomalies
   */
  private static List<MergedAnomalyResultDTO> getBaselineKnownAnomaliesByDimension(BaseAnomalyFunction anomalyFunction,
      long windowStart, long windowEnd, DimensionMap dimensions, MergedAnomalyResultManager mergedResultDAO) {

    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(windowStart, windowEnd);

    List<MergedAnomalyResultDTO> results = new ArrayList<>();
    for (Pair<Long, Long> startEndTimeRange : startEndTimeRanges) {
      // we don't need the known anomalies on current window; we only need those on history data
      if (windowStart <= startEndTimeRange.getFirst() && startEndTimeRange.getSecond() <= windowEnd) {
        continue;
      }
      try {
        results.addAll(mergedResultDAO
            .findAllConflictByFunctionIdDimensions(anomalyFunction.getSpec().getId(), startEndTimeRange.getFirst(),
                startEndTimeRange.getSecond(), dimensions.toString()));
      } catch (Exception e) {
        LOG.error("Unable to get history merged anomalies for function {} on dimensions {} in the anomaly window {} -- {}: {}",
            anomalyFunction.getSpec().getId(), dimensions.toString(), windowStart, windowEnd, e);
      }
    }

    return results;
  }
}
