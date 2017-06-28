package com.linkedin.thirdeye.anomaly.merge;

import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.detector.metric.transfer.MetricTransfer;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * finds raw anomalies grouped by a strategy and merges them with an existing (in the same group) or
 * new {@link com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO}
 */
@Deprecated
public class AnomalyMergeExecutor implements Runnable {
  private final MergedAnomalyResultManager mergedResultDAO;
  private final RawAnomalyResultManager anomalyResultDAO;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final OverrideConfigManager overrideConfigDAO;
  private final ScheduledExecutorService executorService;
  private final AnomalyFunctionFactory anomalyFunctionFactory;

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private final static Logger LOG = LoggerFactory.getLogger(AnomalyMergeExecutor.class);

  private final static AnomalyMergeConfig DEFAULT_MERGE_CONFIG;
  static {
    DEFAULT_MERGE_CONFIG = new AnomalyMergeConfig();
    DEFAULT_MERGE_CONFIG.setSequentialAllowedGap(TimeUnit.HOURS.toMillis(2)); // merge anomalies apart 2 hours
    DEFAULT_MERGE_CONFIG.setMaxMergeDurationLength(TimeUnit.DAYS.toMillis(7) - 3600_000); // break anomaly longer than 6 days 23 hours
    DEFAULT_MERGE_CONFIG.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);
  }

  private final static AnomalyMergeConfig DEFAULT_SYNCHRONIZED_MERGE_CONFIG;
  static {
    DEFAULT_SYNCHRONIZED_MERGE_CONFIG = new AnomalyMergeConfig();
    DEFAULT_SYNCHRONIZED_MERGE_CONFIG.setSequentialAllowedGap(DEFAULT_MERGE_CONFIG.getSequentialAllowedGap());
    DEFAULT_SYNCHRONIZED_MERGE_CONFIG.setMaxMergeDurationLength(DEFAULT_MERGE_CONFIG.getMaxMergeDurationLength());
    // Synchronized merge always use FUNCTION_DIMENSIONS merge strategy
    DEFAULT_SYNCHRONIZED_MERGE_CONFIG.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);
  }

  public AnomalyMergeExecutor(ScheduledExecutorService executorService, AnomalyFunctionFactory anomalyFunctionFactory) {
    this.mergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.anomalyResultDAO = DAO_REGISTRY.getRawAnomalyResultDAO();
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.overrideConfigDAO = DAO_REGISTRY.getOverrideConfigDAO();
    this.executorService = executorService;
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  public void start() {
    // running every 15 mins
    executorService.scheduleWithFixedDelay(this, 0, 15, TimeUnit.MINUTES);
  }

  public void stop() {
    executorService.shutdown();
  }

  /**
   * Performs asynchronous merge base on function id and dimensions.
   */
  public void run() {
    ExecutorService taskExecutorService = Executors.newFixedThreadPool(5);
    List<Future<Integer>> taskCallbacks = new ArrayList<>();
    List<AnomalyFunctionDTO> activeFunctions = anomalyFunctionDAO.findAllActiveFunctions();

    // for each anomaly function, find raw unmerged results and perform merge
    for (final AnomalyFunctionDTO function : activeFunctions) {
      Callable<Integer> task = new Callable<Integer>() {
        @Override public Integer call() throws Exception {
          final boolean isBackfill = false;
          // TODO : move merge config within the AnomalyFunction; Every function should have its own merge config.
          AnomalyMergeConfig anomalyMergeConfig = function.getAnomalyMergeConfig();
          if (anomalyMergeConfig == null) {
            anomalyMergeConfig = DEFAULT_MERGE_CONFIG;
          }
          return AnomalyMergeExecutor.this.mergeAnomalies(function, anomalyMergeConfig, isBackfill);
        }
      };
      Future<Integer> taskFuture = taskExecutorService.submit(task);
      taskCallbacks.add(taskFuture);
    }

    // wait till all the tasks complete
    try {
      for (Future<Integer> future : taskCallbacks) {
        future.get();
      }
    } catch (Exception e) {
      LOG.error("Error in merge execution", e);
    }
  }

  /**
   * Performs a time based merge, which merged anomalies that have the same function id and dimensions.
   * This method is supposed to be performed by anomaly detectors right after their anomaly detection.
   *
   * @param functionSpec the spec of the function that detects anomalies
   * @param isBackfill set to true to disable the alert of the merged anomalies
   *
   * @return the number of merged anomalies after merging
   */
  public int synchronousMergeBasedOnFunctionIdAndDimension(AnomalyFunctionDTO functionSpec, boolean isBackfill) {
    if (functionSpec.getIsActive()) {
      AnomalyMergeConfig anomalyMergeConfig = functionSpec.getAnomalyMergeConfig();
      if (anomalyMergeConfig == null) {
        anomalyMergeConfig = DEFAULT_SYNCHRONIZED_MERGE_CONFIG;
      }
      return mergeAnomalies(functionSpec, anomalyMergeConfig, isBackfill);
    } else {
      return 0;
    }
  }

  /**
   * Merges raw anomalies according to the given merge configuration. The merge logic works as following:
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
   * @param functionSpc the spec of the function that has unmerged anomalies
   * @param mergeConfig the merge strategy
   *
   * @return the number of merged anomalies
   */
  private int mergeAnomalies(AnomalyFunctionDTO functionSpc, AnomalyMergeConfig mergeConfig, boolean isBackfill) {
    List<RawAnomalyResultDTO> unmergedResults = anomalyResultDAO.findUnmergedByFunctionId(functionSpc.getId());

    LOG.info("Running merge for function id : [{}], found [{}] raw anomalies", functionSpc.getId(), unmergedResults.size());

    if (unmergedResults.size() > 0) {
      List<MergedAnomalyResultDTO> output = new ArrayList<>();
      switch (mergeConfig.getMergeStrategy()) {
        case FUNCTION:
          performMergeBasedOnFunctionId(functionSpc, mergeConfig, unmergedResults, output);
          break;
        case FUNCTION_DIMENSIONS:
          performMergeBasedOnFunctionIdAndDimensions(functionSpc, mergeConfig, unmergedResults, output);
          break;
        default:
          throw new IllegalArgumentException("Merge strategy " + mergeConfig.getMergeStrategy() + " not supported");
      }
      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : output) {
        if (isBackfill) {
          mergedAnomalyResultDTO.setNotified(isBackfill);
        } // else notified flag is left as is
        updateMergedScoreAndPersist(mergedAnomalyResultDTO, mergeConfig);
      }
      return output.size();
    } else {
      return 0;
    }
  }

  private void updateMergedScoreAndPersist(MergedAnomalyResultDTO mergedResult, AnomalyMergeConfig mergeConfig) {
    // Calculate default score and weight in case of the failure during updating score and weight through Pinot's value
    double weightedScoreSum = 0.0;
    double weightedWeightSum = 0.0;
    double totalBucketSize = 0.0;

    double normalizationFactor = 1000; // to prevent from double overflow
    String anomalyMessage = "";
    for (RawAnomalyResultDTO anomalyResult : mergedResult.getAnomalyResults()) {
      anomalyResult.setMerged(true);
      double bucketSizeSeconds = (anomalyResult.getEndTime() - anomalyResult.getStartTime()) / 1000;
      weightedScoreSum += (anomalyResult.getScore() / normalizationFactor) * bucketSizeSeconds;
      weightedWeightSum += (anomalyResult.getWeight() / normalizationFactor) * bucketSizeSeconds;
      totalBucketSize += bucketSizeSeconds;
      anomalyMessage = anomalyResult.getMessage();
    }
    if (totalBucketSize != 0) {
      mergedResult.setScore((weightedScoreSum / totalBucketSize) * normalizationFactor);
      mergedResult.setWeight((weightedWeightSum / totalBucketSize) * normalizationFactor);
    }

    mergedResult.setMessage(anomalyMessage);

    if (mergedResult.getAnomalyResults().size() > 1) {
      // recompute weight using anomaly function specific method
      try {
        updateMergedAnomalyWeight(mergedResult, mergeConfig);
      } catch (Exception e) {
        AnomalyFunctionDTO function = mergedResult.getFunction();
        LOG.warn(
            "Unable to compute merged weight and the average weight of raw anomalies is used. Dataset: {}, Topic Metric: {}, Function: {}, Time:{} - {}, Exception: {}",
            function.getCollection(), function.getTopicMetric(), function.getFunctionName(), new DateTime(mergedResult.getStartTime()), new DateTime(mergedResult.getEndTime()), e);
      }
    }
    try {
      // persist the merged result
      mergedResultDAO.update(mergedResult);
      for (RawAnomalyResultDTO rawAnomalyResultDTO : mergedResult.getAnomalyResults()) {
        anomalyResultDAO.update(rawAnomalyResultDTO);
      }
    } catch (Exception e) {
      LOG.error("Could not persist merged result : [" + mergedResult.toString() + "]", e);
    }
  }

  /**
   * Uses function-specific method to re-computes the weight of merged anomaly.
   *
   * @param anomalyMergedResult the merged anomaly to be updated
   * @param mergeConfig the merge configuration that was applied when merge the merged anomaly
   * @throws Exception if error occurs when retrieving the time series for calculating the weight
   */
  private void updateMergedAnomalyWeight(MergedAnomalyResultDTO anomalyMergedResult, AnomalyMergeConfig mergeConfig)
      throws Exception {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyMergedResult.getFunction();
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

    DateTime anomalyResultStart = new DateTime(anomalyMergedResult.getStartTime());
    DateTime anomalyResultEnd = new DateTime(anomalyMergedResult.getEndTime());
    DimensionMap dimensions = anomalyMergedResult.getDimensions();
    AnomalyDetectionInputContextBuilder anomalyDetectionInputContextBuilder =
        new AnomalyDetectionInputContextBuilder(anomalyFunctionFactory);
    anomalyDetectionInputContextBuilder.setFunction(anomalyFunctionSpec)
        .fetchTimeSeriesDataByDimension(anomalyResultStart, anomalyResultEnd, dimensions, false)
        .fetchExistingMergedAnomaliesByDimension(anomalyResultStart, anomalyResultEnd, dimensions)
        .fetchScalingFactors(anomalyResultStart, anomalyResultEnd);
    AnomalyDetectionInputContext anomalyDetectionInputContext = anomalyDetectionInputContextBuilder.build();
    MetricTimeSeries metricTimeSeries = anomalyDetectionInputContext.getDimensionMapMetricTimeSeriesMap().get(dimensions);

    if (metricTimeSeries != null) {
      DateTime windowStart = new DateTime(anomalyMergedResult.getStartTime());
      DateTime windowEnd = new DateTime(anomalyMergedResult.getEndTime());
      List<MergedAnomalyResultDTO> knownAnomalies = Collections.emptyList();

      // Retrieve history merged anomalies
      if (anomalyFunction.useHistoryAnomaly()) {
        switch (mergeConfig.getMergeStrategy()) {
          case FUNCTION:
            knownAnomalies = anomalyDetectionInputContext.getKnownMergedAnomalies().get(dimensions);
            break;
          case FUNCTION_DIMENSIONS:
            knownAnomalies = anomalyDetectionInputContext.getKnownMergedAnomalies().get(dimensions);
            break;
          default:
            throw new IllegalArgumentException("Merge strategy " + mergeConfig.getMergeStrategy() + " not supported");
        }

        if (knownAnomalies.size() > 0) {
          LOG.info("Found {} history anomalies for computing the weight of current merged anomaly.", knownAnomalies.size());
          LOG.info("Checking if any known anomalies overlap with the monitoring window of anomaly detection, which could result in unwanted holes in current values.");
          AnomalyUtils.logAnomaliesOverlapWithWindow(windowStart, windowEnd, knownAnomalies);
        }
      }

      // Transform Time Series
      List<ScalingFactor> scalingFactors = anomalyDetectionInputContext.getScalingFactors();
      if (CollectionUtils.isNotEmpty(scalingFactors)) {
        Properties properties = anomalyFunction.getProperties();
        MetricTransfer.rescaleMetric(metricTimeSeries, windowStart.getMillis(), scalingFactors,
            anomalyFunctionSpec.getTopicMetric(), properties);
      }

      anomalyFunction.updateMergedAnomalyInfo(anomalyMergedResult, metricTimeSeries, windowStart, windowEnd,
          knownAnomalies);
    }
  }

  @Deprecated
  private void performMergeBasedOnFunctionId(AnomalyFunctionDTO function,
      AnomalyMergeConfig mergeConfig, List<RawAnomalyResultDTO> unmergedResults,
      List<MergedAnomalyResultDTO> output) {
    // Now find last MergedAnomalyResult in same category
    MergedAnomalyResultDTO latestMergedResult =
        mergedResultDAO.findLatestByFunctionIdOnly(function.getId(), true);
    // TODO : get mergeConfig from function
    List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer
        .mergeAnomalies(latestMergedResult, unmergedResults, mergeConfig);
    for (MergedAnomalyResultDTO mergedResult : mergedResults) {
      mergedResult.setFunction(function);
    }
    LOG.info("Merging [{}] raw anomalies into [{}] merged anomalies for function id : [{}]",
        unmergedResults.size(), mergedResults.size(), function.getId());
    output.addAll(mergedResults);
  }

  private void performMergeBasedOnFunctionIdAndDimensions(AnomalyFunctionDTO function,
      AnomalyMergeConfig mergeConfig, List<RawAnomalyResultDTO> unmergedResults,
      List<MergedAnomalyResultDTO> output) {
    Map<DimensionMap, List<RawAnomalyResultDTO>> dimensionsResultMap = new HashMap<>();
    for (RawAnomalyResultDTO anomalyResult : unmergedResults) {
      DimensionMap exploredDimensions = anomalyResult.getDimensions();
      if (!dimensionsResultMap.containsKey(exploredDimensions)) {
        dimensionsResultMap.put(exploredDimensions, new ArrayList<RawAnomalyResultDTO>());
      }
      dimensionsResultMap.get(exploredDimensions).add(anomalyResult);
    }
    for (DimensionMap exploredDimensions : dimensionsResultMap.keySet()) {
      List<RawAnomalyResultDTO> unmergedResultsByDimensions = dimensionsResultMap.get(exploredDimensions);
      long anomalyWindowStart = Long.MAX_VALUE;
      long anomalyWindowEnd = Long.MIN_VALUE;
      for (RawAnomalyResultDTO unmergedResultsByDimension : unmergedResultsByDimensions) {
        anomalyWindowStart = Math.min(anomalyWindowStart, unmergedResultsByDimension.getStartTime());
        anomalyWindowEnd = Math.max(anomalyWindowEnd, unmergedResultsByDimension.getEndTime());
      }

      // NOTE: We get "latest overlapped (Conflict)" merged anomaly instead of "latest" merged anomaly in order to
      // prevent the merge results of current (online) detection interfere the merge results of back-fill (offline)
      // detection.
      // Moreover, the window start is modified by mergeConfig.getSequentialAllowedGap() in order to allow a gap between
      // anomalies to be merged.
      MergedAnomalyResultDTO latestOverlappedMergedResult =
          mergedResultDAO.findLatestOverlapByFunctionIdDimensions(function.getId(), exploredDimensions.toString(),
              anomalyWindowStart - mergeConfig.getSequentialAllowedGap(), anomalyWindowEnd, true);

      List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer
          .mergeAnomalies(latestOverlappedMergedResult, unmergedResultsByDimensions,
              mergeConfig);
      for (MergedAnomalyResultDTO mergedResult : mergedResults) {
        mergedResult.setFunction(function);
        mergedResult.setDimensions(exploredDimensions);
      }
      LOG.info(
          "Merging [{}] raw anomalies into [{}] merged anomalies for function id : [{}] and dimensions : [{}]",
          unmergedResultsByDimensions.size(), mergedResults.size(), function.getId(), exploredDimensions);
      output.addAll(mergedResults);
    }
  }

  private String createMessage(double severity, Double currentVal, Double baseLineVal) {
    return String.format("change : %.2f %%, currentVal : %.2f, baseLineVal : %.2f", severity * 100,
        currentVal, baseLineVal);
  }
}
