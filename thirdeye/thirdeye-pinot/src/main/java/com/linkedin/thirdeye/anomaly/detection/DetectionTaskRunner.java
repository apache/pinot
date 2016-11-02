package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.ArrayListMultimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class DetectionTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionTaskRunner.class);

  private TimeSeriesResponseConverter timeSeriesResponseConverter;

  private MergedAnomalyResultManager mergedResultDAO;
  private RawAnomalyResultManager rawAnomalyDAO;

  private List<String> collectionDimensions;
  private DateTime windowStart;
  private DateTime windowEnd;
  private List<MergedAnomalyResultDTO> knownMergedAnomalies;
  private List<RawAnomalyResultDTO> existingRawAnomalies;
  private BaseAnomalyFunction anomalyFunction;
  private DatasetConfigManager datasetConfigDAO;

  public DetectionTaskRunner() {
    timeSeriesResponseConverter = TimeSeriesResponseConverter.getInstance();
  }

  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    DetectionTaskInfo detectionTaskInfo = (DetectionTaskInfo) taskInfo;
    List<TaskResult> taskResult = new ArrayList<>();
    LOG.info("Begin executing task {}", taskInfo);
    mergedResultDAO = taskContext.getMergedResultDAO();
    rawAnomalyDAO = taskContext.getResultDAO();
    datasetConfigDAO = taskContext.getDatasetConfigDAO();
    AnomalyFunctionFactory anomalyFunctionFactory = taskContext.getAnomalyFunctionFactory();
    AnomalyFunctionDTO anomalyFunctionSpec = detectionTaskInfo.getAnomalyFunctionSpec();
    anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);
    windowStart = detectionTaskInfo.getWindowStartTime();
    windowEnd = detectionTaskInfo.getWindowEndTime();

    LOG.info(
        "Running anomaly detection job with metricFunction: [{}], metric [{}], collection: [{}]",
        anomalyFunctionSpec.getFunctionName(), anomalyFunctionSpec.getMetric(),
        anomalyFunctionSpec.getCollection());

    collectionDimensions = datasetConfigDAO.findByDataset(anomalyFunctionSpec.getCollection()).getDimensions();

    // Get existing anomalies for this time range and this function id
    knownMergedAnomalies = getKnownMergedAnomalies(anomalyFunction, windowStart.getMillis(), windowEnd.getMillis());
    existingRawAnomalies = getExistingRawAnomalies(anomalyFunction, windowStart.getMillis(), windowEnd.getMillis());

    TimeSeriesResponse finalResponse =
        TimeSeriesUtil.getTimeSeriesResponseForAnomalyDetection(anomalyFunction, windowStart.getMillis(), windowEnd.getMillis());

    exploreDimensionsAndAnalyze(finalResponse);
    return taskResult;
  }

  private void exploreDimensionsAndAnalyze(TimeSeriesResponse finalResponse) {
    int anomalyCounter = 0;
    Map<DimensionKey, MetricTimeSeries> res =
        timeSeriesResponseConverter.toMap(finalResponse, collectionDimensions);

    // Sort the known anomalies by their dimension names
    ArrayListMultimap<DimensionMap, MergedAnomalyResultDTO> dimensionNamesToKnownMergedAnomalies = ArrayListMultimap.create();
    for (MergedAnomalyResultDTO knownMergedAnomaly : knownMergedAnomalies) {
      dimensionNamesToKnownMergedAnomalies.put(knownMergedAnomaly.getDimensions(), knownMergedAnomaly);
    }

    ArrayListMultimap<DimensionMap, RawAnomalyResultDTO> dimensionNamesToKnownRawAnomalies = ArrayListMultimap.create();
    for (RawAnomalyResultDTO existingRawAnomaly : existingRawAnomalies) {
      dimensionNamesToKnownRawAnomalies.put(existingRawAnomaly.getDimensions(), existingRawAnomaly);
    }

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : res.entrySet()) {
      DimensionKey dimensionKey = entry.getKey();
      DimensionMap exploredDimensions = DimensionMap.fromDimensionKey(dimensionKey, collectionDimensions);

      if (entry.getValue().getTimeWindowSet().size() < 1) {
        LOG.warn("Insufficient data for {} to run anomaly detection function", exploredDimensions);
        continue;
      }

      // Get current entry's knownMergedAnomalies, which should have the same explored dimensions
      List<MergedAnomalyResultDTO> knownMergedAnomaliesOfAnEntry = dimensionNamesToKnownMergedAnomalies.get(exploredDimensions);
      List<MergedAnomalyResultDTO> historyMergedAnomalies = retainHistoryMergedAnomalies(windowStart.getMillis(), knownMergedAnomaliesOfAnEntry);

      try {
        // Run algorithm
        MetricTimeSeries metricTimeSeries = entry.getValue();
        LOG.info("Analyzing anomaly function with explored dimensions: {}, windowStart: {}, windowEnd: {}",
            exploredDimensions, windowStart, windowEnd);

        List<RawAnomalyResultDTO> resultsOfAnEntry = anomalyFunction
            .analyze(exploredDimensions, metricTimeSeries, windowStart, windowEnd, historyMergedAnomalies);

        // Remove any known anomalies
        if (CollectionUtils.isNotEmpty(resultsOfAnEntry)) {
          List<RawAnomalyResultDTO> existingRawAnomaliesOfAnEntry =
              dimensionNamesToKnownRawAnomalies.get(exploredDimensions);
          resultsOfAnEntry = removeFromExistingRawAnomalies(resultsOfAnEntry, existingRawAnomaliesOfAnEntry);
        }
        if (CollectionUtils.isNotEmpty(resultsOfAnEntry)) {
          List<MergedAnomalyResultDTO> existingMergedAnomalies =
              retainExistingMergedAnomalies(windowStart.getMillis(), windowEnd.getMillis(), knownMergedAnomaliesOfAnEntry);
          resultsOfAnEntry = removeFromExistingMergedAnomalies(resultsOfAnEntry, existingMergedAnomalies);
        }

        // Handle results
        handleResults(resultsOfAnEntry);

        LOG.info("{} has {} anomalies in window {} to {}", exploredDimensions, resultsOfAnEntry.size(),
            windowStart, windowEnd);
        anomalyCounter += resultsOfAnEntry.size();
      } catch (Exception e) {
        LOG.error("Could not compute for {}", exploredDimensions, e);
      }
    }
    LOG.info("{} anomalies found in total", anomalyCounter);
  }

  /**
   * Returns existing raw anomalies in the given monitoring window
   *
   * @param anomalyFunction
   * @param monitoringWindowStart inclusive
   * @param monitoringWindowEnd inclusive but it doesn't matter
   * @return known raw anomalies in monitoring window and history data
   */
  private List<RawAnomalyResultDTO> getExistingRawAnomalies(BaseAnomalyFunction anomalyFunction,
      long monitoringWindowStart, long monitoringWindowEnd) {
    List<RawAnomalyResultDTO> results = new ArrayList<>();
    try {
      results.addAll(rawAnomalyDAO.findAllByTimeAndFunctionId(monitoringWindowStart, monitoringWindowEnd,
          anomalyFunction.getSpec().getId()));
    } catch (Exception e) {
      LOG.error("Exception in getting existing anomalies", e);
    }
    return results;
  }

  /**
   * Returns 1. history merged anomalies and 2. existing merged anomalies that are overlapped with the given monitoring
   * window and its history data
   *
   * History and existing merged anomalies are defined as follows:
   *   1. History merged anomalies are referring to the known merged anomalies that are located in training (history) data
   *   2. Existing merged anomalies are referring to the known merged anomalies that are located in monitoring data
   *
   * @param anomalyFunction
   * @param monitoringWindowStart inclusive
   * @param monitoringWindowEnd inclusive but it doesn't matter
   * @return known merged anomalies that are overlapped with monitoring window and history data
   */
  private List<MergedAnomalyResultDTO> getKnownMergedAnomalies(BaseAnomalyFunction anomalyFunction,
      long monitoringWindowStart, long monitoringWindowEnd) {

    List<Pair<Long, Long>> startEndTimeRanges =
        anomalyFunction.getDataRangeIntervals(monitoringWindowStart, monitoringWindowEnd);

    List<MergedAnomalyResultDTO> results = new ArrayList<>();
    for (Pair<Long, Long> startEndTimeRange : startEndTimeRanges) {
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

  /**
   * Returns all anomalies that happen before the monitoring window
   *
   * @param monitoringWindowStart
   * @param knownAnomalies
   * @return all anomalies that happen before the monitoring window
   */
  private List<MergedAnomalyResultDTO> retainHistoryMergedAnomalies(long monitoringWindowStart,
      List<MergedAnomalyResultDTO> knownAnomalies) {
    List<MergedAnomalyResultDTO> historyAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO knownAnomaly : knownAnomalies) {
      if (knownAnomaly.getEndTime() < monitoringWindowStart) {
        historyAnomalies.add(knownAnomaly);
      }
    }
    return historyAnomalies;
  }

  /**
   * Returns all anomalies that overlap with the monitoring window
   *
   * @param monitoringWindowStart
   * @param knownAnomalies
   * @return all anomalies that happen in the monitoring window
   */
  private List<MergedAnomalyResultDTO> retainExistingMergedAnomalies(long monitoringWindowStart, long monitoringWindowEnd,
      List<MergedAnomalyResultDTO> knownAnomalies) {
    List<MergedAnomalyResultDTO> existingAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO knownAnomaly : knownAnomalies) {
      if (knownAnomaly.getStartTime() <= monitoringWindowEnd && knownAnomaly.getEndTime() >= monitoringWindowStart) {
        existingAnomalies.add(knownAnomaly);
      }
    }
    return existingAnomalies;
  }

  /**
   * Given a list of raw anomalies, this method returns a list of raw anomalies that are not contained in any existing
   * merged anomalies.
   *
   * @param rawAnomalies
   * @param existingAnomalies
   * @return
   */
  private List<RawAnomalyResultDTO> removeFromExistingMergedAnomalies(List<RawAnomalyResultDTO> rawAnomalies,
      List<MergedAnomalyResultDTO> existingAnomalies) {
    if (CollectionUtils.isEmpty(rawAnomalies) || CollectionUtils.isEmpty(existingAnomalies)) {
      return rawAnomalies;
    }
    List<RawAnomalyResultDTO> newRawAnomalies = new ArrayList<>();

    for (RawAnomalyResultDTO rawAnomaly : rawAnomalies) {
      boolean isContained = false;
      for (MergedAnomalyResultDTO existingAnomaly : existingAnomalies) {
        if (existingAnomaly.getStartTime().compareTo(rawAnomaly.getStartTime()) <= 0
            && rawAnomaly.getEndTime().compareTo(existingAnomaly.getEndTime()) <= 0) {
          isContained = true;
          break;
        }
      }
      if (!isContained) {
        newRawAnomalies.add(rawAnomaly);
      }
    }

    return newRawAnomalies;
  }

  /**
   * Given a list of raw anomalies, this method returns a list of raw anomalies that are not contained in any existing
   * raw anomalies.
   *
   * @param rawAnomalies
   * @param existingRawAnomalies
   * @return
   */
  private List<RawAnomalyResultDTO> removeFromExistingRawAnomalies(List<RawAnomalyResultDTO> rawAnomalies,
      List<RawAnomalyResultDTO> existingRawAnomalies) {
    List<RawAnomalyResultDTO> newRawAnomalies = new ArrayList<>();

    for (RawAnomalyResultDTO rawAnomaly : rawAnomalies) {
      boolean matched = false;
      for (RawAnomalyResultDTO existingAnomaly : existingRawAnomalies) {
        if (existingAnomaly.getStartTime().compareTo(rawAnomaly.getStartTime()) <= 0
            && rawAnomaly.getEndTime().compareTo(existingAnomaly.getEndTime()) <= 0) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        newRawAnomalies.add(rawAnomaly);
      }
    }

    return newRawAnomalies;
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
        rawAnomalyDAO.save(result);
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
