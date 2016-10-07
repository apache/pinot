package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.ArrayListMultimap;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class DetectionTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionTaskRunner.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE =
      ThirdEyeCacheRegistry.getInstance();

  private TimeSeriesResponseConverter timeSeriesResponseConverter;

  private RawAnomalyResultManager resultDAO;

  private List<String> collectionDimensions;
  private DateTime windowStart;
  private DateTime windowEnd;
  private List<RawAnomalyResultDTO> knownAnomalies;
  private BaseAnomalyFunction anomalyFunction;

  public DetectionTaskRunner() {
    timeSeriesResponseConverter = TimeSeriesResponseConverter.getInstance();
  }

  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    DetectionTaskInfo detectionTaskInfo = (DetectionTaskInfo) taskInfo;
    List<TaskResult> taskResult = new ArrayList<>();
    LOG.info("Begin executing task {}", taskInfo);
    resultDAO = taskContext.getResultDAO();
    AnomalyFunctionFactory anomalyFunctionFactory = taskContext.getAnomalyFunctionFactory();
    AnomalyFunctionDTO anomalyFunctionSpec = detectionTaskInfo.getAnomalyFunctionSpec();
    anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);
    windowStart = detectionTaskInfo.getWindowStartTime();
    windowEnd = detectionTaskInfo.getWindowEndTime();

    LOG.info(
        "Running anomaly detection job with metricFunction: [{}], metric [{}], collection: [{}]",
        anomalyFunctionSpec.getFunctionName(), anomalyFunctionSpec.getMetric(),
        anomalyFunctionSpec.getCollection());

    CollectionSchema collectionSchema = null;
    try {
      collectionSchema = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache()
          .get(anomalyFunctionSpec.getCollection());
      collectionDimensions = collectionSchema.getDimensionNames();
    } catch (Exception e) {
      LOG.error("Exception when reading collection schema cache", e);
    }

    // Get existing anomalies for this time range and this function id
    knownAnomalies = getExistingAnomalies();
    TimeSeriesResponse finalResponse = TimeSeriesUtil
        .getTimeSeriesResponse(anomalyFunctionSpec, anomalyFunction,
            detectionTaskInfo.getGroupByDimension(), windowStart.getMillis(),
            windowEnd.getMillis());

    exploreDimensionsAndAnalyze(finalResponse);
    return taskResult;
  }

  private void exploreDimensionsAndAnalyze(TimeSeriesResponse finalResponse) {
    int anomalyCounter = 0;
    Map<DimensionKey, MetricTimeSeries> res =
        timeSeriesResponseConverter.toMap(finalResponse, collectionDimensions);

    // Sort the known anomalies by their dimension names
    ArrayListMultimap<String, RawAnomalyResultDTO> dimensionNamesToKnownAnomalies = ArrayListMultimap.create();
    for (RawAnomalyResultDTO knownAnomaly : knownAnomalies) {
      dimensionNamesToKnownAnomalies.put(knownAnomaly.getDimensions(), knownAnomaly);
    }

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : res.entrySet()) {
      if (entry.getValue().getTimeWindowSet().size() < 1) {
        LOG.warn("Insufficient data for {} to run anomaly detection function", entry.getKey());
        continue;
      }

      // Get current entry's knownAnomalies, which should have the same dimension names.
      String dimensionKeyString = entry.getKey().toCommaSeparatedString();
      List<RawAnomalyResultDTO> knownAnomaliesOfAnEntry = dimensionNamesToKnownAnomalies.get(dimensionKeyString);

      try {
        // Run algorithm
        DimensionKey dimensionKey = entry.getKey();
        MetricTimeSeries metricTimeSeries = entry.getValue();
        LOG.info("Analyzing anomaly function with dimensionKey: {}, windowStart: {}, windowEnd: {}",
            dimensionKey, windowStart, windowEnd);

        List<RawAnomalyResultDTO> resultsOfAnEntry = anomalyFunction
            .analyze(dimensionKey, metricTimeSeries, windowStart, windowEnd, knownAnomaliesOfAnEntry);

        // Remove any known anomalies
        resultsOfAnEntry.removeAll(knownAnomaliesOfAnEntry);

        // Handle results
        handleResults(resultsOfAnEntry);

        LOG.info("{} has {} anomalies in window {} to {}", entry.getKey(), resultsOfAnEntry.size(),
            windowStart, windowEnd);
        anomalyCounter += resultsOfAnEntry.size();
      } catch (Exception e) {
        LOG.error("Could not compute for {}", entry.getKey(), e);
      }
    }
    LOG.info("{} anomalies found in total", anomalyCounter);
  }

  private List<RawAnomalyResultDTO> getExistingAnomalies() {
    List<RawAnomalyResultDTO> results = new ArrayList<>();
    try {
      results.addAll(resultDAO
          .findAllByTimeAndFunctionId(windowStart.getMillis(), windowEnd.getMillis(),
              anomalyFunction.getSpec().getId()));
    } catch (Exception e) {
      LOG.error("Exception in getting existing anomalies", e);
    }
    return results;
  }

  private void handleResults(List<RawAnomalyResultDTO> results) {
    for (RawAnomalyResultDTO result : results) {
      try {
        // Properties that always come from the function spec
        AnomalyFunctionDTO spec = anomalyFunction.getSpec();
        // make sure score and weight are valid numbers
        result.setScore(normalize(result.getScore()));
        result.setWeight(normalize(result.getWeight()));
        result.setFunction(spec);
        resultDAO.save(result);
      } catch (Exception e) {
        LOG.error("Exception in saving anomaly result : " + result.toString(), e);
      }
    }
  }

  /**
   * Handle any infinite or NaN values by replacing them with +/- max value or 0
   */
  private double normalize(double value) {
    if (Double.isInfinite(value)) {
      return (value > 0.0 ? 1 : -1) * Double.MAX_VALUE;
    } else if (Double.isNaN(value)) {
      return 0.0; // default?
    } else {
      return value;
    }
  }
}
