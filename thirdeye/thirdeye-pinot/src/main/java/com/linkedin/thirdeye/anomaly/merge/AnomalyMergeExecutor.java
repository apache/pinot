package com.linkedin.thirdeye.anomaly.merge;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRequest;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.db.dao.AnomalyMergedResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionUtils;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.persistence.PersistenceException;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * finds raw anomalies grouped by a strategy and merges them with an existing (in the same group) or
 * new {@link com.linkedin.thirdeye.db.entity.AnomalyMergedResult}
 */
public class AnomalyMergeExecutor implements Runnable {
  private final AnomalyMergedResultDAO mergedResultDAO;
  private final AnomalyResultDAO anomalyResultDAO;
  private final AnomalyFunctionDAO anomalyFunctionDAO;
  private final ScheduledExecutorService executorService;

  private final QueryCache queryCache;
  private final TimeSeriesHandler timeSeriesHandler;

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE =
      ThirdEyeCacheRegistry.getInstance();

  private final static Logger LOG = LoggerFactory.getLogger(AnomalyMergeExecutor.class);

  public AnomalyMergeExecutor(AnomalyMergedResultDAO mergedResultDAO,
      AnomalyFunctionDAO anomalyFunctionDAO, AnomalyResultDAO anomalyResultDAO,
      ScheduledExecutorService executorService) {
    this.mergedResultDAO = mergedResultDAO;
    this.anomalyResultDAO = anomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.executorService = executorService;
    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    this.timeSeriesHandler = new TimeSeriesHandler(queryCache);
  }

  public void start() {
    executorService.scheduleWithFixedDelay(this, 1, 2, TimeUnit.MINUTES);
  }

  public void stop() {
    executorService.shutdown();
  }

  public void run() {
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
      List<AnomalyFunctionSpec> activeFunctions = anomalyFunctionDAO.findAllActiveFunctions();

      // for each anomaly function, find raw unmerged results and perform merge
      for (AnomalyFunctionSpec function : activeFunctions) {

        List<AnomalyResult> unmergedResults =
            anomalyResultDAO.findUnmergedByFunctionId(function.getId());

        LOG.info("Running merge for function id : [{}], found [{}] raw anomalies", function.getId(),
            unmergedResults.size());

        // TODO : move merge config within the AnomalyFunction; Every function should have its own merge config.
        AnomalyMergeConfig mergeConfig = new AnomalyMergeConfig();
        mergeConfig.setSequentialAllowedGap(2 * 60 * 60_000); // 2 hours
        mergeConfig.setMergeDuration(-1); // no time based split
        mergeConfig.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);

        List<AnomalyMergedResult> output = new ArrayList<>();

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
        output.forEach(this::updateMergedScoreAndPersist);
      }
    } catch (Exception e) {
      LOG.error("Error in merge execution", e);
    }
  }

  private void updateMergedScoreAndPersist(AnomalyMergedResult mergedResult) {
    double weightedScoreSum = 0.0;
    double weightedWeightSum = 0.0;
    double totalBucketSize = 0.0;

    double normalizationFactor = 1000; // to prevent from double overflow

    for (AnomalyResult anomalyResult : mergedResult.getAnomalyResults()) {
      anomalyResult.setMerged(true);
      double bucketSizeSeconds =
          (anomalyResult.getEndTimeUtc() - anomalyResult.getStartTimeUtc()) / 1000;
      weightedScoreSum += (anomalyResult.getScore() / normalizationFactor) * bucketSizeSeconds;
      weightedWeightSum += (anomalyResult.getWeight() / normalizationFactor) * bucketSizeSeconds;
      totalBucketSize += bucketSizeSeconds;
    }
    if (totalBucketSize != 0) {
      mergedResult.setScore((weightedScoreSum / totalBucketSize) * normalizationFactor);
      // TODO: recompute weight by querying Pinot
      mergedResult.setWeight((weightedWeightSum / totalBucketSize) * normalizationFactor);
    }

    if (mergedResult.getAnomalyResults().size() > 1) {
      try {
        updateMergedSeverity(mergedResult);
      } catch (Exception e) {
        LOG.error("Could not recompute severity", e);
      }
    }

    try {
      // persist the merged result
      mergedResultDAO.update(mergedResult);
      anomalyResultDAO.updateAll(mergedResult.getAnomalyResults());
    } catch (PersistenceException e) {
      LOG.error("Could not persist merged result : [" + mergedResult.toString() + "]", e);
    }
  }

  private void updateMergedSeverity(AnomalyMergedResult anomalyMergedResult) throws Exception {
    AnomalyFunctionSpec anomalyFunctionSpec = anomalyMergedResult.getFunction();

    // create time series request
    TimeSeriesRequest timeSeriesRequest = new TimeSeriesRequest();
    timeSeriesRequest.setCollectionName(anomalyFunctionSpec.getCollection());
    MetricFunction metricFunction = new MetricFunction(anomalyFunctionSpec.getMetricFunction(),
        anomalyFunctionSpec.getMetric());
    List<MetricFunction> metricFunctions = Collections.singletonList(metricFunction);
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metricFunctions);
    timeSeriesRequest.setMetricExpressions(metricExpressions);
    TimeGranularity timeBucket = new TimeGranularity(anomalyFunctionSpec.getBucketSize(),
        anomalyFunctionSpec.getBucketUnit());
    timeSeriesRequest.setAggregationTimeGranularity(timeBucket);

    timeSeriesRequest.setEndDateInclusive(false);
    if (StringUtils.isNotBlank(anomalyFunctionSpec.getFilters())) {
      timeSeriesRequest.setFilterSet(ThirdEyeUtils.getFilterSet(anomalyFunctionSpec.getFilters()));
    }
    String exploreDimension = anomalyFunctionSpec.getExploreDimensions();
    if (StringUtils.isNotBlank(exploreDimension)) {
      timeSeriesRequest.setGroupByDimensions(Collections.singletonList(exploreDimension));
    }

    // Fetch current time series data
    timeSeriesRequest.setStart(new DateTime(anomalyMergedResult.getStartTime()));
    timeSeriesRequest.setEnd(new DateTime(anomalyMergedResult.getEndTime()));
    TimeSeriesResponse responseCurrent = timeSeriesHandler.handle(timeSeriesRequest);

    // Fetch baseline time series data
    long baselineOffset = AnomalyFunctionUtils.getBaselineOffset(anomalyFunctionSpec);
    timeSeriesRequest.setStart(new DateTime(anomalyMergedResult.getStartTime() - baselineOffset));
    timeSeriesRequest.setEnd(new DateTime(anomalyMergedResult.getEndTime() - baselineOffset));
    TimeSeriesResponse responseBaseline = timeSeriesHandler.handle(timeSeriesRequest);

    Double currentValue = getMetricValueSum(responseCurrent, anomalyFunctionSpec.getMetric());
    Double baselineValue = getMetricValueSum(responseBaseline, anomalyFunctionSpec.getMetric());
    Double severity = anomalyMergedResult.getWeight();
    if (baselineValue != 0) {
      severity = (currentValue - baselineValue) / baselineValue;
    }
    anomalyMergedResult.setWeight(severity);
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

  private void performMergeBasedOnFunctionId(AnomalyFunctionSpec function,
      AnomalyMergeConfig mergeConfig, List<AnomalyResult> unmergedResults,
      List<AnomalyMergedResult> output) {
    // Now find last MergedAnomalyResult in same category
    AnomalyMergedResult latestMergedResult =
        mergedResultDAO.findLatestByFunctionIdOnly(function.getId());
    // TODO : get mergeConfig from function
    List<AnomalyMergedResult> mergedResults = AnomalyTimeBasedSummarizer
        .mergeAnomalies(latestMergedResult, unmergedResults, mergeConfig.getMergeDuration(),
            mergeConfig.getSequentialAllowedGap());
    for (AnomalyMergedResult mergedResult : mergedResults) {
      mergedResult.setFunction(function);
    }
    LOG.info("Merging [{}] raw anomalies into [{}] merged anomalies for function id : [{}]",
        unmergedResults.size(), mergedResults.size(), function.getId());
    output.addAll(mergedResults);
  }

  private void performMergeBasedOnFunctionIdAndDimensions(AnomalyFunctionSpec function,
      AnomalyMergeConfig mergeConfig, List<AnomalyResult> unmergedResults,
      List<AnomalyMergedResult> output) {
    Map<String, List<AnomalyResult>> dimensionsResultMap = new HashMap<>();
    for (AnomalyResult anomalyResult : unmergedResults) {
      String dimensions = anomalyResult.getDimensions();
      if (!dimensionsResultMap.containsKey(dimensions)) {
        dimensionsResultMap.put(dimensions, new ArrayList<>());
      }
      dimensionsResultMap.get(dimensions).add(anomalyResult);
    }
    for (String dimensions : dimensionsResultMap.keySet()) {
      AnomalyMergedResult latestMergedResult =
          mergedResultDAO.findLatestByFunctionIdDimensions(function.getId(), dimensions);
      List<AnomalyResult> unmergedResultsByDimensions = dimensionsResultMap.get(dimensions);

      // TODO : get mergeConfig from function
      List<AnomalyMergedResult> mergedResults = AnomalyTimeBasedSummarizer
          .mergeAnomalies(latestMergedResult, unmergedResultsByDimensions,
              mergeConfig.getMergeDuration(), mergeConfig.getSequentialAllowedGap());
      for (AnomalyMergedResult mergedResult : mergedResults) {
        mergedResult.setFunction(function);
        mergedResult.setDimensions(dimensions);
      }
      LOG.info(
          "Merging [{}] raw anomalies into [{}] merged anomalies for function id : [{}] and dimensions : [{}]",
          unmergedResults.size(), mergedResults.size(), function.getId(), dimensions);
      output.addAll(mergedResults);
    }
  }
}
