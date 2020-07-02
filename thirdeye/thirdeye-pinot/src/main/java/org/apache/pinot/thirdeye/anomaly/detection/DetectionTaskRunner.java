/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.anomaly.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.pinot.thirdeye.anomaly.detection.DetectionJobContext.DetectionJobType;
import org.apache.pinot.thirdeye.anomaly.merge.TimeBasedAnomalyMerger;
import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.anomaly.task.TaskInfo;
import org.apache.pinot.thirdeye.anomaly.task.TaskResult;
import org.apache.pinot.thirdeye.anomaly.task.TaskRunner;
import org.apache.pinot.thirdeye.anomaly.utils.AnomalyUtils;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.anomalydetection.datafilter.DataFilter;
import org.apache.pinot.thirdeye.anomalydetection.datafilter.DataFilterFactory;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.common.metric.MetricTimeSeries;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;
import org.apache.pinot.thirdeye.detector.function.BaseAnomalyFunction;
import org.apache.pinot.thirdeye.detector.metric.transfer.MetricTransfer;
import org.apache.pinot.thirdeye.detector.metric.transfer.ScalingFactor;

import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil.*;


@Deprecated
public class DetectionTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionTaskRunner.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public static final String BACKFILL_PREFIX = "adhoc_";

  private static final double DIMENSION_ERROR_THRESHOLD = 0.3; // 3 0%
  private static final int MAX_ERROR_MESSAGE_WORD_COUNT = 10_000; // 10k bytes
  // This is used to throttle MySQL write.
  private static final int ANOMALY_BATCH_WRITE_SIZE = 20;

  private List<DateTime> windowStarts;
  private List<DateTime> windowEnds;
  private AnomalyFunctionDTO anomalyFunctionSpec;
  private long jobExecutionId;
  private DetectionJobType detectionJobType;

  private List<String> collectionDimensions;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private BaseAnomalyFunction anomalyFunction;
  private List<BaseAnomalyFunction> secondaryAnomalyFunctions;

  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    detectionTaskCounter.inc();
    List<TaskResult> taskResult = new ArrayList<>();

    LOG.info("Setting up task {}", taskInfo);
    setupTask(taskInfo, taskContext);

    // Run for all pairs of window start and window end
    for (int i = 0; i < windowStarts.size(); i ++) {
      runTask(windowStarts.get(i), windowEnds.get(i));
    }

    return taskResult;
  }

  private void setupTask(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    DetectionTaskInfo detectionTaskInfo = (DetectionTaskInfo) taskInfo;
    windowStarts = detectionTaskInfo.getWindowStartTime();
    windowEnds = detectionTaskInfo.getWindowEndTime();
    anomalyFunctionSpec = detectionTaskInfo.getAnomalyFunctionSpec();
    jobExecutionId = detectionTaskInfo.getJobExecutionId();
    anomalyFunctionFactory = taskContext.getAnomalyFunctionFactory();
    anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);
    secondaryAnomalyFunctions = anomalyFunctionFactory.getSecondaryAnomalyFunctions(anomalyFunctionSpec);
    detectionJobType = detectionTaskInfo.getDetectionJobType();

    String dataset = anomalyFunctionSpec.getCollection();
    DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);

    if (datasetConfig == null) {
      LOG.error("Dataset [" + dataset + "] is not found");
      throw new IllegalArgumentException(
          "Dataset [" + dataset + "] is not found with function : " + anomalyFunctionSpec
              .toString());
    }
    collectionDimensions = datasetConfig.getDimensions();

    LOG.info(
        "Running anomaly detection job with metricFunction: [{}], topic metric [{}], collection: [{}]",
        anomalyFunctionSpec.getFunctionName(), anomalyFunctionSpec.getTopicMetric(),
        anomalyFunctionSpec.getCollection());
  }


  private void runTask(DateTime windowStart, DateTime windowEnd) throws Exception {
    AnomalyResultSource anomalyResultSource = AnomalyResultSource.DEFAULT_ANOMALY_DETECTION;
    LOG.info("Running anomaly detection for time range {} to  {}", windowStart, windowEnd);

    AnomalyDetectionInputContextBuilder anomalyDetectionInputContextBuilder =
        new AnomalyDetectionInputContextBuilder(anomalyFunctionFactory);

    // TODO: Change to DataFetchers/DataSources
    anomalyDetectionInputContextBuilder = anomalyDetectionInputContextBuilder
        .setFunction(anomalyFunctionSpec)
        .fetchTimeSeriesData(windowStart, windowEnd)
        .fetchExistingMergedAnomalies(windowStart, windowEnd, true)
        .fetchScalingFactors(windowStart, windowEnd);
    if (anomalyFunctionSpec.isToCalculateGlobalMetric()) {
      anomalyDetectionInputContextBuilder.fetchTimeSeriesGlobalMetric(windowStart, windowEnd);
    }
    AnomalyDetectionInputContext adContext = anomalyDetectionInputContextBuilder.build();

    ListMultimap<DimensionMap, AnomalyResult> resultRawAnomalies = dimensionalShuffleAndUnifyAnalyze(windowStart, windowEnd, adContext);
    detectionTaskSuccessCounter.inc();

    // If the current job is a backfill (adhoc) detection job, set notified flag to true so the merged anomalies do not
    // induce alerts and emails.
    if (detectionJobType != null && (detectionJobType.equals(DetectionJobType.BACKFILL) ||
        detectionJobType.equals(DetectionJobType.OFFLINE))) {
      LOG.info("BACKFILL is triggered for Detection Job {}. Notified flag is set to be true", jobExecutionId);
      anomalyResultSource = AnomalyResultSource.ANOMALY_REPLAY;
    }

    // Update merged anomalies
    TimeBasedAnomalyMerger timeBasedAnomalyMerger = new TimeBasedAnomalyMerger(anomalyFunctionFactory);
    ListMultimap<DimensionMap, MergedAnomalyResultDTO> resultMergedAnomalies =
      timeBasedAnomalyMerger.mergeAnomalies(anomalyFunctionSpec, resultRawAnomalies);

    // Set anomaly source on merged anomaly results
    for (MergedAnomalyResultDTO mergedAnomaly : resultMergedAnomalies.values()) {
      mergedAnomaly.setAnomalyResultSource(anomalyResultSource);
    }

    detectionTaskSuccessCounter.inc();

    // TODO: Change to DataSink
    AnomalyDetectionOutputContext adOutputContext = new AnomalyDetectionOutputContext();
    adOutputContext.setMergedAnomalies(resultMergedAnomalies);
    storeData(adOutputContext);
  }

  private void storeData(AnomalyDetectionOutputContext anomalyDetectionOutputContext) throws InterruptedException {
    MergedAnomalyResultManager mergedAmomalyDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();

    int savedAnomalies = 0;
    for (MergedAnomalyResultDTO mergedAnomalyResultDTO : anomalyDetectionOutputContext.getMergedAnomalies().values()) {
      mergedAmomalyDAO.update(mergedAnomalyResultDTO);
      // Add additional delay when storing anomalies.
      if (++savedAnomalies % ANOMALY_BATCH_WRITE_SIZE == 0) {
        Thread.sleep(100);
      }
    }
  }

  private ListMultimap<DimensionMap, AnomalyResult> dimensionalShuffleAndUnifyAnalyze(DateTime windowStart,
      DateTime windowEnd, AnomalyDetectionInputContext anomalyDetectionInputContext) throws Exception {
    int anomalyCounter = 0;
    ListMultimap<DimensionMap, AnomalyResult> resultRawAnomalies = ArrayListMultimap.create();

    List<Exception> exceptions = new ArrayList<>();
    DataFilter dataFilter = DataFilterFactory.fromSpec(anomalyFunctionSpec.getDataFilter());
    Set<DimensionMap> dimensions = anomalyDetectionInputContext.getDimensionMapMetricTimeSeriesMap().keySet();
    for (DimensionMap dimensionMap : dimensions) {
      if (Thread.interrupted()) {
        throw new IllegalStateException("Thread interrupted. Aborting.");
      }

      // Skip anomaly detection if the current time series does not pass data filter, which may check if the traffic
      // or total count of the data has enough volume for produce sufficient confidence anomaly results
      MetricTimeSeries metricTimeSeries =
          anomalyDetectionInputContext.getDimensionMapMetricTimeSeriesMap().get(dimensionMap);
      if (!dataFilter.isQualified(metricTimeSeries, dimensionMap, windowStart.getMillis(), windowEnd.getMillis())) {
        continue;
      }

      List<AnomalyResult> resultsOfAnEntry = Collections.emptyList();
      try {
        resultsOfAnEntry = runAnalyze(windowStart, windowEnd, anomalyDetectionInputContext, dimensionMap);
      } catch (Exception e) {
        String message = String.format("Unable to detection anomaly for dimension: %s, function id: %d", dimensionMap,
            anomalyFunctionSpec.getId());
        LOG.error(message, e);
        exceptions.add(new Exception(message, e));

        if (Double.compare((double) exceptions.size() / (double) dimensions.size(), DIMENSION_ERROR_THRESHOLD) >= 0) {
          String errorMessage = ThirdEyeUtils.exceptionsToString(exceptions, MAX_ERROR_MESSAGE_WORD_COUNT);
          throw new Exception(errorMessage);
        }
      }

      // Set raw anomalies' properties
      normalizeRawResults(resultsOfAnEntry);

      LOG.info("Dimension {} has {} anomalies in window {} to {}", dimensionMap, resultsOfAnEntry.size(),
          windowStart, windowEnd);
      anomalyCounter += resultsOfAnEntry.size();
      resultRawAnomalies.putAll(dimensionMap, resultsOfAnEntry);
    }

    if (anomalyCounter != 0) {
      LOG.info("{} anomalies found in total", anomalyCounter);
    } else {
      LOG.info("No anomlay is found");
    }
    return resultRawAnomalies;
  }

  private List<AnomalyResult> runAnalyze(DateTime windowStart, DateTime windowEnd,
      AnomalyDetectionInputContext anomalyDetectionInputContext, DimensionMap dimensionMap) throws Exception {

    List<AnomalyResult> resultsOfAnEntry = Collections.emptyList();

    String metricName = anomalyFunction.getSpec().getTopicMetric();
    MetricTimeSeries metricTimeSeries = anomalyDetectionInputContext.getDimensionMapMetricTimeSeriesMap().get(dimensionMap);

    /*
    Check if current task is running offline analysis
     */
    boolean isOffline = false;
    if (detectionJobType != null && detectionJobType.equals(DetectionJobType.OFFLINE)) {
      LOG.info("Detection Job {} is running under OFFLINE mode", jobExecutionId);
      isOffline = true;
    }

    // Get current entry's knownMergedAnomalies, which should have the same explored dimensions
    List<MergedAnomalyResultDTO> knownMergedAnomaliesOfAnEntry =
        anomalyDetectionInputContext.getKnownMergedAnomalies().get(dimensionMap);
    List<MergedAnomalyResultDTO> historyMergedAnomalies;
    if (anomalyFunction.useHistoryAnomaly()) {
      historyMergedAnomalies = retainHistoryMergedAnomalies(windowStart.getMillis(), knownMergedAnomaliesOfAnEntry);
    } else {
      historyMergedAnomalies = Collections.emptyList();
    }

    LOG.info("[Old pipeline] Analyzing anomaly function with explored dimensions: {}, windowStart: {}, windowEnd: {}",
        dimensionMap, windowStart, windowEnd);

    AnomalyUtils.logAnomaliesOverlapWithWindow(windowStart, windowEnd, historyMergedAnomalies);

    // Scaling time series according to the scaling factor
    List<ScalingFactor> scalingFactors = anomalyDetectionInputContext.getScalingFactors();
    if (CollectionUtils.isNotEmpty(scalingFactors)) {
      Properties properties = anomalyFunction.getProperties();
      MetricTransfer.rescaleMetric(metricTimeSeries, windowStart.getMillis(), scalingFactors, metricName, properties);
    }

    // Run detection algorithm
    if (isOffline) {
      resultsOfAnEntry = anomalyFunction.offlineAnalyze(dimensionMap, metricTimeSeries, windowStart, windowEnd,
          historyMergedAnomalies);
    } else {
      resultsOfAnEntry =
          anomalyFunction.analyze(dimensionMap, metricTimeSeries, windowStart, windowEnd, historyMergedAnomalies);
    }
    if (secondaryAnomalyFunctions != null) {
      for (BaseAnomalyFunction secondaryAnomalyFunction : secondaryAnomalyFunctions) {
        resultsOfAnEntry.addAll(secondaryAnomalyFunction.analyze(dimensionMap, metricTimeSeries, windowStart, windowEnd,
            historyMergedAnomalies));
      }
    }

    // Clean up duplicate and overlapped raw anomalies
    if (CollectionUtils.isNotEmpty(resultsOfAnEntry)) {
      resultsOfAnEntry = cleanUpDuplicateRawAnomalies(resultsOfAnEntry);
    }
    // Remove detected anomalies that have existed in database
    if (CollectionUtils.isNotEmpty(resultsOfAnEntry)) {
      List<MergedAnomalyResultDTO> existingMergedAnomalies =
          retainExistingMergedAnomalies(windowStart.getMillis(), windowEnd.getMillis(), knownMergedAnomaliesOfAnEntry);
      resultsOfAnEntry = removeFromExistingMergedAnomalies(resultsOfAnEntry, existingMergedAnomalies);
    }

    return resultsOfAnEntry;
  }

  /**
   * Returns history anomalies of the monitoring window from the given known anomalies.
   *
   * Definition of history anomaly: An anomaly that starts before the monitoring window starts.
   *
   * @param monitoringWindowStart the start of the monitoring window
   * @param knownAnomalies the list of known anomalies
   *
   * @return all history anomalies of the monitoring window
   */
  private List<MergedAnomalyResultDTO> retainHistoryMergedAnomalies(long monitoringWindowStart,
      List<MergedAnomalyResultDTO> knownAnomalies) {
    List<MergedAnomalyResultDTO> historyAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO knownAnomaly : knownAnomalies) {
      if (knownAnomaly.getStartTime() < monitoringWindowStart) {
        historyAnomalies.add(knownAnomaly);
      }
    }
    return historyAnomalies;
  }

  /**
   * Returns anomalies that overlap with the monitoring window from the given known anomalies
   *
   * Definition of existing anomaly: An anomaly that happens in the monitoring window
   *
   * @param monitoringWindowStart the start of the monitoring window
   * @param monitoringWindowEnd the end of the monitoring window
   * @param knownAnomalies the list of known anomalies
   *
   * @return anomalies that happen in the monitoring window from the given known anomalies
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
  private List<AnomalyResult> removeFromExistingMergedAnomalies(List<AnomalyResult> rawAnomalies,
      List<MergedAnomalyResultDTO> existingAnomalies) {
    if (CollectionUtils.isEmpty(rawAnomalies) || CollectionUtils.isEmpty(existingAnomalies)) {
      return rawAnomalies;
    }
    List<AnomalyResult> newRawAnomalies = new ArrayList<>();

    for (AnomalyResult rawAnomaly : rawAnomalies) {
      boolean isContained = false;
      for (MergedAnomalyResultDTO existingAnomaly : existingAnomalies) {
        if (Long.compare(existingAnomaly.getStartTime(), rawAnomaly.getStartTime()) <= 0
            && Long.compare(rawAnomaly.getEndTime(), existingAnomaly.getEndTime()) <= 0) {
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
   * Remove duplicated raw anomalies (assuming the given list of anomalies are sorted by start time).
   *
   * @param rawAnomalies the list of raw anomalies.
   *
   * @return a list of raw anomalies that are not duplicated.
   */
  public static List<AnomalyResult> cleanUpDuplicateRawAnomalies(List<AnomalyResult> rawAnomalies) {
    if (rawAnomalies.size() < 2) {
      return rawAnomalies;
    } else {
      // Sort raw anomalies by their start time (in acending order) and
      // then window size (in decending order)
      Collections.sort(rawAnomalies, new Comparator<AnomalyResult>() {
        @Override
        public int compare(AnomalyResult o1, AnomalyResult o2) {
          if (o1.getStartTime() == o2.getStartTime()) {
            long anomaly1Range = o1.getEndTime() - o1.getStartTime();
            long anomaly2Range = o2.getEndTime() - o2.getStartTime();
            return Long.compare(anomaly2Range, anomaly1Range);
          } else {
            return Long.compare(o1.getStartTime(), o2.getStartTime());
          }
        }
      });

      List<AnomalyResult> cleanedUpRawAnomalies = new ArrayList<>();
      cleanedUpRawAnomalies.add(rawAnomalies.get(0));
      for (int suspect = 1; suspect < rawAnomalies.size(); suspect++) {
        AnomalyResult rawAnomaly = rawAnomalies.get(suspect);
        boolean duplicated = false;
        for (int idxToCompare = 0; idxToCompare < suspect; ++idxToCompare) {
          AnomalyResult rawAnomalyToCompare = rawAnomalies.get(idxToCompare);
          if (Long.compare(rawAnomalyToCompare.getStartTime(), rawAnomaly.getStartTime()) <= 0
              && Long.compare(rawAnomaly.getEndTime(), rawAnomalyToCompare.getEndTime()) <= 0) {
            duplicated = true;
            break;
          }
        }
        if (!duplicated) {
          cleanedUpRawAnomalies.add(rawAnomaly);
        }
      }
      return cleanedUpRawAnomalies;
    }
  }

  /**
   * Makes sure score and weight of anomaly results are valid numbers.
   *
   * @param results the anomalies to be checked.
   */
  private void normalizeRawResults(List<AnomalyResult> results) {
    if (CollectionUtils.isNotEmpty(results)) {
      for (AnomalyResult result : results) {
        try {
          result.setScore(normalize(result.getScore()));
          result.setWeight(normalize(result.getWeight()));
          result.setAvgCurrentVal(normalize(result.getAvgCurrentVal()));
          result.setAvgBaselineVal(normalize(result.getAvgBaselineVal()));
        } catch (Exception e) {
          LOG.warn("Exception when normalizing anomaly result : {}.", result.toString(), e);
        }
      }
    }
  }

  /**
   * Handle any infinite or NaN values by replacing them with +/- max value or 0
   */
  public static double normalize(double value) {
    if (Double.isInfinite(value)) {
      return (value > 0.0 ? 1 : -1) * Double.MAX_VALUE;
    } else if (Double.isNaN(value)) {
      return 0.0; // default?
    } else {
      return value;
    }
  }
}
