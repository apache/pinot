package com.linkedin.thirdeye.anomaly.merge;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.detection.TimeSeriesUtil;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;


/**
 * finds raw anomalies grouped by a strategy and merges them with an existing (in the same group) or
 * new {@link com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO}
 */
public class AnomalyMergeExecutor implements Runnable {
  private final MergedAnomalyResultManager mergedResultDAO;
  private final RawAnomalyResultManager anomalyResultDAO;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final ScheduledExecutorService executorService;
  private final AnomalyFunctionFactory anomalyFunctionFactory;

  private final QueryCache queryCache;
  private final TimeSeriesHandler timeSeriesHandler;

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private final static Logger LOG = LoggerFactory.getLogger(AnomalyMergeExecutor.class);

  public AnomalyMergeExecutor(ScheduledExecutorService executorService, AnomalyFunctionFactory anomalyFunctionFactory) {
    this.mergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.anomalyResultDAO = DAO_REGISTRY.getRawAnomalyResultDAO();
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.executorService = executorService;
    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    this.timeSeriesHandler = new TimeSeriesHandler(queryCache);
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  public void start() {
    // running every 15 mins
    executorService.scheduleWithFixedDelay(this, 0, 15, TimeUnit.MINUTES);
  }

  public void stop() {
    executorService.shutdown();
  }

  public void run() {
    ExecutorService taskExecutorService = Executors.newFixedThreadPool(5);
    List<Future<Integer>> taskCallbacks = new ArrayList<>();

    try {
      /**
       * Step 0: find all active functions
       *
       * Step 1: for each function :
       *        find all groups of raw (unprocessed) anomalies based on
       *        merge strategy (FunctionId and/or dimensions)
       *
       * Step 2: For each such group, find the base mergedAnomaly
       *
       * Step 3: perform time based merge
       *
       * Step 4: Recompute anomaly score / weight
       *
       * Step 5: persist merged anomalies
       */
      List<AnomalyFunctionDTO> activeFunctions = anomalyFunctionDAO.findAllActiveFunctions();

      // for each anomaly function, find raw unmerged results and perform merge
      for (AnomalyFunctionDTO function : activeFunctions) {
        Callable<Integer> task = () -> {
          List<RawAnomalyResultDTO> unmergedResults =
              anomalyResultDAO.findUnmergedByFunctionId(function.getId());
          LOG.info("Running merge for function id : [{}], found [{}] raw anomalies",
              function.getId(), unmergedResults.size());

          // TODO : move merge config within the AnomalyFunction; Every function should have its own merge config.
          AnomalyMergeConfig mergeConfig = new AnomalyMergeConfig();
          mergeConfig.setSequentialAllowedGap(2 * 60 * 60_000); // 2 hours
          mergeConfig.setMaxMergeDurationLength(-1); // no time based split
          mergeConfig.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);

          List<MergedAnomalyResultDTO> output = new ArrayList<>();

          if (unmergedResults.size() > 0) {
            switch (mergeConfig.getMergeStrategy()) {
            case FUNCTION:
              performMergeBasedOnFunctionId(function, mergeConfig, unmergedResults, output);
              break;
            case FUNCTION_DIMENSIONS:
              performMergeBasedOnFunctionIdAndDimensions(function, mergeConfig, unmergedResults, output);
              break;
            default:
              throw new IllegalArgumentException(
                  "Merge strategy " + mergeConfig.getMergeStrategy() + " not supported");
            }
          }
          for (MergedAnomalyResultDTO mergedAnomalyResultDTO : output) {
            updateMergedScoreAndPersist(mergedAnomalyResultDTO);
          }
          return output.size();
        };
        Future<Integer> taskFuture = taskExecutorService.submit(task);
        taskCallbacks.add(taskFuture);
      }
      for (Future<Integer> future : taskCallbacks) {
        // now wait till all the tasks complete
        future.get();
      }
    } catch (Exception e) {
      LOG.error("Error in merge execution", e);
    }
  }

  private void updateMergedScoreAndPersist(MergedAnomalyResultDTO mergedResult) {
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

    // recompute weight using anomaly function specific method
    try {
      updateMergedAnomalyWeight(mergedResult);
    } catch (Exception e) {
      AnomalyFunctionDTO function = mergedResult.getFunction();
      LOG.warn(
          "Failed to compute merged weight and the average weight of raw anomalies is used. Dataset: {}, Metric: {}, Function: {}, Time:{} - {}, Exception: {}",
          function.getCollection(), function.getMetric(), function.getFunctionName(),
          new DateTime(mergedResult.getStartTime()), new DateTime(mergedResult.getEndTime()), e);
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

  private void updateMergedAnomalyWeight(MergedAnomalyResultDTO anomalyMergedResult) throws Exception {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyMergedResult.getFunction();
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

    List<Pair<Long, Long>> startEndTimeRanges =
        anomalyFunction.getDataRangeIntervals(anomalyMergedResult.getStartTime(), anomalyMergedResult.getEndTime());

    TimeGranularity timeGranularity = new TimeGranularity(anomalyFunctionSpec.getBucketSize(),
        anomalyFunctionSpec.getBucketUnit());

    MetricTimeSeries metricTimeSeries =
        TimeSeriesUtil.getTimeSeriesByDimension(anomalyFunctionSpec, startEndTimeRanges,
            anomalyMergedResult.getDimensions(), timeGranularity);

    if (metricTimeSeries != null) {
      List<MergedAnomalyResultDTO> knownAnomalies = null;
      if (anomalyFunction.useHistoryAnomaly()) {
        knownAnomalies = getKnownMergedAnomalies(anomalyFunction, anomalyMergedResult.getStartTime(),
            anomalyMergedResult.getEndTime());
      }
      anomalyFunction.updateMergedAnomalyInfo(anomalyMergedResult, metricTimeSeries,
          new DateTime(anomalyMergedResult.getStartTime()), new DateTime(anomalyMergedResult.getEndTime()),
          knownAnomalies);
    }
  }

  private List<MergedAnomalyResultDTO> getKnownMergedAnomalies(BaseAnomalyFunction anomalyFunction,
      long monitoringWindowStart, long monitoringWindowEnd) {

    List<Pair<Long, Long>> startEndTimeRanges =
        anomalyFunction.getDataRangeIntervals(monitoringWindowStart, monitoringWindowEnd);

    List<MergedAnomalyResultDTO> results = new ArrayList<>();
    for (Pair<Long, Long> startEndTimeRange : startEndTimeRanges) {
      // we don't need the known anomalies on current window; we only need those on history data
      if (monitoringWindowStart <= startEndTimeRange.getFirst()
          && startEndTimeRange.getSecond() <= monitoringWindowEnd) {
        continue;
      }
      try {
        results.addAll(
            mergedResultDAO.findAllConflictByFunctionId(anomalyFunction.getSpec().getId(), startEndTimeRange.getFirst(),
                startEndTimeRange.getSecond()));
      } catch (Exception e) {
        LOG.error("Exception in getting existing anomalies", e);
      }
    }

    return results;
  }

  private void performMergeBasedOnFunctionId(AnomalyFunctionDTO function,
      AnomalyMergeConfig mergeConfig, List<RawAnomalyResultDTO> unmergedResults,
      List<MergedAnomalyResultDTO> output) {
    // Now find last MergedAnomalyResult in same category
    MergedAnomalyResultDTO latestMergedResult =
        mergedResultDAO.findLatestByFunctionIdOnly(function.getId());
    // TODO : get mergeConfig from function
    List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer
        .mergeAnomalies(latestMergedResult, unmergedResults, mergeConfig.getMaxMergeDurationLength(),
            mergeConfig.getSequentialAllowedGap());
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
        dimensionsResultMap.put(exploredDimensions, new ArrayList<>());
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
      MergedAnomalyResultDTO latestOverlappedMergedResult =
          mergedResultDAO.findLatestConflictByFunctionIdDimensions(function.getId(), exploredDimensions.toString(),
              anomalyWindowStart, anomalyWindowEnd, mergeConfig.getSequentialAllowedGap());

      // TODO : get mergeConfig from MergeStrategy
      List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer
          .mergeAnomalies(latestOverlappedMergedResult, unmergedResultsByDimensions,
              mergeConfig.getMaxMergeDurationLength(), mergeConfig.getSequentialAllowedGap());
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

  /**
   * Performs a light weight merge based on function id and dimensions. This method is supposed to be performed by
   * anomaly detectors right after their anomaly detection. For complex merge logics which merge anomalies across
   * different dimensions, function, metrics, etc., the tasks should be performed by a dedicated merger.
   *
   * The light weight merge logic works as follows:
   *
   * Step 1: find all unmerged raw (unprocessed) anomalies of the given function and group them according to their
   *         dimensions
   *
   * Step 2: find the latest merged anomaly for each group of unmerged anomalies
   *
   * Step 3: perform time based merge
   *
   * Step 4: recompute anomaly score / weight
   *
   * Step 5: persist merged anomalies
   */
  public int performSynchronousMergeBasedOnFunctionIdAndDimension(AnomalyFunctionDTO functionSpec, boolean isNotified) {
    if (functionSpec.getIsActive()) {
      List<RawAnomalyResultDTO> unmergedResults = anomalyResultDAO.findUnmergedByFunctionId(functionSpec.getId());

      if (unmergedResults.size() > 0) {
        LOG.info("Running merge for function id : [{}], found [{}] raw anomalies", functionSpec.getId(),
            unmergedResults.size());

        // TODO : move merge config within the AnomalyFunction; Every function should have its own merge config.
        AnomalyMergeConfig mergeConfig = new AnomalyMergeConfig();
        mergeConfig.setSequentialAllowedGap(2 * 60 * 60_000); // 2 hours
        mergeConfig.setMaxMergeDurationLength(-1); // no time based split
        mergeConfig.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);

        List<MergedAnomalyResultDTO> output = new ArrayList<>();
        performMergeBasedOnFunctionIdAndDimensions(functionSpec, mergeConfig, unmergedResults, output);
        for (MergedAnomalyResultDTO mergedAnomalyResultDTO : output) {
          mergedAnomalyResultDTO.setNotified(isNotified);
          updateMergedScoreAndPersist(mergedAnomalyResultDTO);
        }
        return output.size();
      }
    }
    return 0;
  }
}
