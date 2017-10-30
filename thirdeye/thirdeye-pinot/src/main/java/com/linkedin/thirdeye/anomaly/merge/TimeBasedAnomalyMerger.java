package com.linkedin.thirdeye.anomaly.merge;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.detector.metric.transfer.MetricTransfer;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeBasedAnomalyMerger {
  private final static Logger LOG = LoggerFactory.getLogger(TimeBasedAnomalyMerger.class);
  private final static Double NULL_DOUBLE = Double.NaN;
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
   *
   * @return the number of merged anomalies after merging
   */
  public ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergeAnomalies(AnomalyFunctionDTO functionSpec,
      ListMultimap<DimensionMap, RawAnomalyResultDTO> unmergedAnomalies) {

    int rawAnomaliesCount = 0;
    for (DimensionMap dimensionMap : unmergedAnomalies.keySet()) {
      rawAnomaliesCount += unmergedAnomalies.get(dimensionMap).size();
    }
    LOG.info("Running merge for function id : [{}], found [{}] raw anomalies", functionSpec.getId(), rawAnomaliesCount);

    AnomalyMergeConfig mergeConfig = functionSpec.getAnomalyMergeConfig();
    if (mergeConfig == null) {
      mergeConfig = DEFAULT_TIME_BASED_MERGE_CONFIG;
    }

    // From anomaly function spec initialize the anomaly function, and get its specified mergeable property keys if any
    // If failed to create anomaly function, default empty mergeable property keys will be used from mergeConfig
    try {
      BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(functionSpec);
      mergeConfig.setMergeablePropertyKeys(anomalyFunction.getMergeablePropertyKeys());
      LOG.info("Created anomaly function for class: {}, set mergeable keys as: {}", anomalyFunction.getClass(), anomalyFunction.getMergeablePropertyKeys());
    } catch (Exception e) {
      LOG.warn("Unsuccessfully create anomaly function from anomalyFunctionFactory, {}", e.getMessage());
    }


    if (unmergedAnomalies.size() == 0) {
      return ArrayListMultimap.create();
    } else {
      ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies =
          dimensionalShuffleAndUnifyMerge(functionSpec, mergeConfig, unmergedAnomalies);

      // Update information of merged anomalies
      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : mergedAnomalies.values()) {
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
          mergedResultDAO.findLatestOverlapByFunctionIdDimensions(function.getId(), dimensionMap.toString(),
              anomalyWindowStart - mergeConfig.getSequentialAllowedGap(), anomalyWindowEnd, true);

      List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer
          .mergeAnomalies(latestOverlappedMergedResult, unmergedResultsByDimensions,
              mergeConfig);
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

      // Decode Properties String to HashMap
      HashMap<String, String> props = new HashMap<>();
      String[] tokens = rawAnomaly.getProperties().split(";");
      for(String token : tokens) {
        String[] keyValues = token.split("=");
        String values = keyValues[1];
        for(int i = 2; i < keyValues.length; i++) {
          values = values + "=" + keyValues[i];
        }
        props.put(keyValues[0], values);
      }

      mergedResult.setProperties(props);
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
    }

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
    DateTime windowStart = new DateTime(mergedAnomalies.getStartTime());
    DateTime windowEnd = new DateTime(mergedAnomalies.getEndTime());
    DimensionMap dimensions = mergedAnomalies.getDimensions();

    AnomalyDetectionInputContextBuilder anomalyDetectionInputContextBuilder =
        new AnomalyDetectionInputContextBuilder(anomalyFunctionFactory);
    anomalyDetectionInputContextBuilder.setFunction(anomalyFunctionSpec)
        .fetchTimeSeriesDataByDimension(windowStart, windowEnd, dimensions, false)
        .fetchScalingFactors(windowStart, windowEnd)
        .fetchExistingMergedAnomaliesByDimension(windowStart, windowEnd, dimensions);
    if (anomalyFunctionSpec.isToCalculateGlobalMetric()) {
      anomalyDetectionInputContextBuilder.fetchTimeSeriesGlobalMetric(windowStart, windowEnd);
    }
    AnomalyDetectionInputContext adInputContext = anomalyDetectionInputContextBuilder.build();

    MetricTimeSeries metricTimeSeries = adInputContext.getDimensionMapMetricTimeSeriesMap().get(dimensions);

    if (metricTimeSeries != null) {
      List<MergedAnomalyResultDTO> knownAnomalies = adInputContext.getKnownMergedAnomalies().get(dimensions);
      // Transform time series with scaling factor
      List<ScalingFactor> scalingFactors = adInputContext.getScalingFactors();
      if (CollectionUtils.isNotEmpty(scalingFactors)) {
        Properties properties = anomalyFunction.getProperties();
        MetricTransfer.rescaleMetric(metricTimeSeries, windowStart.getMillis(), scalingFactors,
            anomalyFunctionSpec.getTopicMetric(), properties);
      }
      anomalyFunction.updateMergedAnomalyInfo(mergedAnomalies, metricTimeSeries, windowStart, windowEnd, knownAnomalies);

      if(anomalyFunctionSpec.isToCalculateGlobalMetric()) {
        MetricTimeSeries subMetricTimeSeries = adInputContext.getDimensionMapMetricTimeSeriesMap().get(dimensions);
        computeImpactToGlobalMetric(adInputContext.getGlobalMetric(), subMetricTimeSeries, mergedAnomalies);
      }
    }
  }

  /**
   * Calculate the impact to global metric for the MergedAnomalyResult instance
   *
   * impact_to_global = severity * traffic_contribution
   *   \- severity is the change ratio in monitoring window
   *   \- trafic_contribution is the ratio of the average traffic of subMetric and global metric in training window, that is
   *        traffic_contribution = avg(subMetric)/avg(globalMetric)
   * @param globalMetricTimeSerise
   *      The time series data in global metric
   * @param subMetricTimeSeries
   *      The time series data in sub-metric
   * @param mergedAnomaly
   *      The instance of merged anomaly
   */
  public static double computeImpactToGlobalMetric(MetricTimeSeries globalMetricTimeSerise, MetricTimeSeries subMetricTimeSeries,
      MergedAnomalyResultDTO mergedAnomaly) {
    double impactToTotal = NULL_DOUBLE;
    if (globalMetricTimeSerise == null || globalMetricTimeSerise.getTimeWindowSet().isEmpty()) {
      return impactToTotal;
    }

    String globalMetric = mergedAnomaly.getFunction().getGlobalMetric();
    String anomalyMetric = mergedAnomaly.getMetric();
    if (StringUtils.isNotBlank(globalMetric)) {
      globalMetric = mergedAnomaly.getFunction().getMetric();
    }
    // Calculate the traffic contribution
    double avgGlobal = 0.0;
    double avgSub = 0.0;
    int countGlobal = 0;
    int countSub = 0;
    // Calculate the average traffic
    for (long timestamp : globalMetricTimeSerise.getTimeWindowSet()) {
      if (timestamp < mergedAnomaly.getStartTime()) {
        double globalMetricValue = globalMetricTimeSerise.getOrDefault(timestamp, globalMetric, NULL_DOUBLE).doubleValue();
        double subMetricValue = subMetricTimeSeries.getOrDefault(timestamp, anomalyMetric, NULL_DOUBLE).doubleValue();
        if (Double.compare(globalMetricValue, NULL_DOUBLE) != 0) {
          avgGlobal += globalMetricValue;
          countGlobal++;
        }
        if (Double.compare(subMetricValue, NULL_DOUBLE) != 0) {
          avgSub += subMetricValue;
          countSub++;
        }
      }
    }
    avgGlobal /= (double) countGlobal;
    avgSub /= (double) countSub;

    if (avgGlobal > 0) {
      double trafficContribution = avgSub / avgGlobal;
      impactToTotal = mergedAnomaly.getWeight() * trafficContribution;
    }
    mergedAnomaly.setImpactToGlobal(impactToTotal);
    return impactToTotal;
  }
}
