package com.linkedin.thirdeye.anomaly.merge;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.persistence.PersistenceException;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRequest;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionUtils;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

/**
 * finds raw anomalies grouped by a strategy and merges them with an existing (in the same group) or
 * new {@link com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO}
 */
public class AnomalyMergeExecutor implements Runnable {
  private final MergedAnomalyResultManager mergedResultDAO;
  private final RawAnomalyResultManager anomalyResultDAO;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final ScheduledExecutorService executorService;

  private final QueryCache queryCache;
  private final TimeSeriesHandler timeSeriesHandler;

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE =
      ThirdEyeCacheRegistry.getInstance();

  private final static Logger LOG = LoggerFactory.getLogger(AnomalyMergeExecutor.class);

  public AnomalyMergeExecutor(MergedAnomalyResultManager mergedResultDAO,
      AnomalyFunctionManager anomalyFunctionDAO, RawAnomalyResultManager anomalyResultDAO,
      ScheduledExecutorService executorService) {
    this.mergedResultDAO = mergedResultDAO;
    this.anomalyResultDAO = anomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.executorService = executorService;
    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    this.timeSeriesHandler = new TimeSeriesHandler(queryCache);
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
          mergeConfig.setMergeDuration(-1); // no time based split
          mergeConfig.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);

          List<MergedAnomalyResultDTO> output = new ArrayList<>();

          if (unmergedResults.size() > 0) {
            switch (mergeConfig.getMergeStrategy()) {
            case FUNCTION:
              performMergeBasedOnFunctionId(function, mergeConfig, unmergedResults, output);
              break;
            case FUNCTION_DIMENSIONS:
              performMergeBasedOnFunctionIdAndDimensions(function, mergeConfig, unmergedResults,
                  output);
              break;
            default:
              throw new IllegalArgumentException(
                  "Merge strategy " + mergeConfig.getMergeStrategy() + " not supported");
            }
          }

          Set<MergedAnomalyResultDTO> existingMergedResults = new HashSet<>();

          existingMergedResults.addAll(mergedResultDAO.findByFunctionId(function.getId()));

          for (MergedAnomalyResultDTO mergedAnomalyResultDTO : output) {
            updateMergedScoreAndPersist(mergedAnomalyResultDTO, existingMergedResults);
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

  private void updateMergedScoreAndPersist(MergedAnomalyResultDTO mergedResult,
      Set<MergedAnomalyResultDTO> existingResults) {
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

    // recompute severity
    try {
      updateMergedSeverity(mergedResult);
    } catch (Exception e) {
      LOG.error("Could not recompute severity", e);
    }
    try {
      if (!existingResults.contains(mergedResult)) {
        // persist the merged result
        mergedResultDAO.update(mergedResult);
      } else {
        LOG.info("MergedResult [{}] is already present", mergedResult);
      }
      for (RawAnomalyResultDTO rawAnomalyResultDTO : mergedResult.getAnomalyResults()) {
        anomalyResultDAO.update(rawAnomalyResultDTO);
      }
    } catch (PersistenceException e) {
      LOG.error("Could not persist merged result : [" + mergedResult.toString() + "]", e);
    }
  }

  private void updateMergedSeverity(MergedAnomalyResultDTO anomalyMergedResult) throws Exception {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyMergedResult.getFunction();

    // create time series request
    TimeSeriesRequest timeSeriesRequest = new TimeSeriesRequest();
    timeSeriesRequest.setCollectionName(anomalyFunctionSpec.getCollection());

    List<MetricExpression> metricExpressions = Utils
        .convertToMetricExpressions(anomalyFunctionSpec.getMetric(),
            anomalyFunctionSpec.getMetricFunction(), anomalyFunctionSpec.getCollection());

    timeSeriesRequest.setMetricExpressions(metricExpressions);
    TimeGranularity timeBucket = new TimeGranularity(anomalyFunctionSpec.getBucketSize(),
        anomalyFunctionSpec.getBucketUnit());
    timeSeriesRequest.setAggregationTimeGranularity(timeBucket);

    timeSeriesRequest.setEndDateInclusive(false);

    Multimap<String, String> filters = ArrayListMultimap.create();
    if (StringUtils.isNotBlank(anomalyFunctionSpec.getFilters())) {
      filters.putAll(ThirdEyeUtils.getFilterSet(anomalyFunctionSpec.getFilters()));
    }
    String exploreDimension = anomalyFunctionSpec.getExploreDimensions();
    if (StringUtils.isNotBlank(exploreDimension)) {
      timeSeriesRequest.setGroupByDimensions(Collections.singletonList(exploreDimension));
      String anomalyDimensions = anomalyMergedResult.getDimensions();
      String[] dimArr = anomalyDimensions.split(",");
      for (String dim : dimArr) {
        if (!StringUtils.isBlank(dim) && !"*".equals(dim)) {
          filters.removeAll(exploreDimension);
          if (dim.equalsIgnoreCase("other")) {
            filters.put(exploreDimension, dim);
            filters.put(exploreDimension, dim.toLowerCase());
            filters.put(exploreDimension, "");
          } else {
            // Only add a specific dimension value filter if there are more values present for the same dimension
            filters.put(exploreDimension, dim);
          }
          LOG.info("Adding filter : [{} = {}] in the query", exploreDimension, dim);
        }
      }
    }

    LOG.info("Applying final filter : {}", filters.toString());
    // Set filters including anomaly-dimension
    timeSeriesRequest.setFilterSet(filters);

    // TODO : fix the pinot query interface to accept time in millis e
    // Fetch current time series data
    timeSeriesRequest.setStart(new DateTime(anomalyMergedResult.getStartTime()));
    timeSeriesRequest.setEnd(new DateTime(anomalyMergedResult.getEndTime()));
    TimeSeriesResponse responseCurrent = timeSeriesHandler.handle(timeSeriesRequest);

    LOG.info("printing current start end millis : {} {}, joda {} {}", anomalyMergedResult.getStartTime(),
        anomalyMergedResult.getEndTime(), new DateTime(anomalyMergedResult.getStartTime()),
        new DateTime(anomalyMergedResult.getEndTime()));

    // Fetch baseline time series data
    long baselineOffset = AnomalyFunctionUtils.getBaselineOffset(anomalyFunctionSpec);
    timeSeriesRequest.setStart(new DateTime(anomalyMergedResult.getStartTime() - baselineOffset));
    timeSeriesRequest.setEnd(new DateTime(anomalyMergedResult.getEndTime() - baselineOffset));
    TimeSeriesResponse responseBaseline = timeSeriesHandler.handle(timeSeriesRequest);

    LOG.info("printing baseline start end millis : {} {}, joda {} {}", anomalyMergedResult.getStartTime() - baselineOffset,
        anomalyMergedResult.getEndTime() - baselineOffset, new DateTime(anomalyMergedResult.getStartTime() - baselineOffset),
        new DateTime(anomalyMergedResult.getEndTime() - baselineOffset));

    Double currentValue;
    Double baselineValue;

    if (Utils
        .isDerievedMetric(anomalyFunctionSpec.getCollection(), anomalyFunctionSpec.getMetric())) {
      LOG.info("Found derived metric [{}], assigning avg value per bucket in the message",
          anomalyFunctionSpec.getMetric());
      currentValue = getAvgMetricValuePerBucket(responseCurrent, anomalyFunctionSpec.getMetric());
      baselineValue = getAvgMetricValuePerBucket(responseBaseline, anomalyFunctionSpec.getMetric());
    } else {
      LOG.info("Assigning total value in the message for metric : [{}]",
          anomalyFunctionSpec.getMetric());
      currentValue = getMetricValueSum(responseCurrent, anomalyFunctionSpec.getMetric());
      baselineValue = getMetricValueSum(responseBaseline, anomalyFunctionSpec.getMetric());
    }

    Double severity;
    if (baselineValue != 0) {
      severity = (currentValue - baselineValue) / baselineValue;
      anomalyMergedResult.setWeight(severity);
    } else {
      LOG.error("Could not recompute severity for merged anomaly [{}], assigning weighted avg",
          anomalyMergedResult.toString());
    }
    anomalyMergedResult
        .setMessage(createMessage(anomalyMergedResult.getWeight(), currentValue, baselineValue));
  }

  private Double getMetricValueSum(TimeSeriesResponse response, String metricName) {
    Double totalVal = 0.0;
    for (int i = 0; i < response.getNumRows(); i++) {
      for (TimeSeriesRow.TimeSeriesMetric metricData : response.getRow(i).getMetrics()) {
        if (metricName.equals(metricData.getMetricName())) {
          totalVal += metricData.getValue();
        }
      }
    }
    return totalVal;
  }

  /**
   * Returns average metrics value (per bucket) for given time series
   *
   * @param response
   * @param metricName
   *
   * @return
   */
  private Double getAvgMetricValuePerBucket(TimeSeriesResponse response, String metricName) {
    Double totalVal = 0.0;
    int numBuckets = 0;
    for (int i = 0; i < response.getNumRows(); i++) {
      for (TimeSeriesRow.TimeSeriesMetric metricData : response.getRow(i).getMetrics()) {
        if (metricName.equals(metricData.getMetricName())) {
          totalVal += metricData.getValue();
          numBuckets++;
        }
      }
    }
    if (numBuckets == 0) {
      return totalVal;
    }
    return totalVal / numBuckets;
  }

  private void performMergeBasedOnFunctionId(AnomalyFunctionDTO function,
      AnomalyMergeConfig mergeConfig, List<RawAnomalyResultDTO> unmergedResults,
      List<MergedAnomalyResultDTO> output) {
    // Now find last MergedAnomalyResult in same category
    MergedAnomalyResultDTO latestMergedResult =
        mergedResultDAO.findLatestByFunctionIdOnly(function.getId());
    // TODO : get mergeConfig from function
    List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer
        .mergeAnomalies(latestMergedResult, unmergedResults, mergeConfig.getMergeDuration(),
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
    Map<String, List<RawAnomalyResultDTO>> dimensionsResultMap = new HashMap<>();
    for (RawAnomalyResultDTO anomalyResult : unmergedResults) {
      String dimensions = anomalyResult.getDimensions();
      if (!dimensionsResultMap.containsKey(dimensions)) {
        dimensionsResultMap.put(dimensions, new ArrayList<>());
      }
      dimensionsResultMap.get(dimensions).add(anomalyResult);
    }
    for (String dimensions : dimensionsResultMap.keySet()) {
      MergedAnomalyResultDTO latestMergedResult =
          mergedResultDAO.findLatestByFunctionIdDimensions(function.getId(), dimensions);
      List<RawAnomalyResultDTO> unmergedResultsByDimensions = dimensionsResultMap.get(dimensions);

      // TODO : get mergeConfig from function
      List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer
          .mergeAnomalies(latestMergedResult, unmergedResultsByDimensions,
              mergeConfig.getMergeDuration(), mergeConfig.getSequentialAllowedGap());
      for (MergedAnomalyResultDTO mergedResult : mergedResults) {
        mergedResult.setFunction(function);
        mergedResult.setDimensions(dimensions);
      }
      LOG.info(
          "Merging [{}] raw anomalies into [{}] merged anomalies for function id : [{}] and dimensions : [{}]",
          unmergedResults.size(), mergedResults.size(), function.getId(), dimensions);
      output.addAll(mergedResults);
    }
  }

  private String createMessage(double severity, Double currentVal, Double baseLineVal) {
    return String.format("change : %.2f %%, currentVal : %.2f, baseLineVal : %.2f", severity * 100,
        currentVal, baseLineVal);
  }
}
