/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.classification;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.linkedin.thirdeye.anomaly.classification.classifier.AnomalyClassifier;
import com.linkedin.thirdeye.anomaly.classification.classifier.AnomalyClassifierFactory;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.anomaly.detection.DetectionTaskRunner;
import com.linkedin.thirdeye.anomaly.merge.TimeBasedAnomalyMerger;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.anomalydetection.context.RawAnomalyResult;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.detector.metric.transfer.MetricTransfer;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class determines the issue type of the anomalies from main anomaly function in two steps:
 * 1. Get anomalies for all anomaly functions.
 *    There are two cases to consider: A. activated anomaly functions and B. deactivated functions.
 *    We read the anomalies in the window from DB for case A and trigger adhoc anomaly detection for case B.
 * 2. Afterwards, a classification logic takes as input the anomalies and updates the issue type of the anomalies
 *    from main anomaly function.
 */
public class ClassificationTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(ClassificationTaskRunner.class);
  private static final MergeAnomalyEndTimeComparator MERGE_ANOMALY_END_TIME_COMPARATOR =
      new MergeAnomalyEndTimeComparator();
  public static final String ISSUE_TYPE_KEY = "issue_type";

  private AnomalyFunctionManager anomalyFunctionDAO = DAORegistry.getInstance().getAnomalyFunctionDAO();
  private MergedAnomalyResultManager mergedAnomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
  private ClassificationConfigManager classificationConfigDAO = DAORegistry.getInstance().getClassificationConfigDAO();

  private long windowStart;
  private long windowEnd;
  private ClassificationConfigDTO classificationConfig;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AlertFilterFactory alertFilterFactory;
  private AnomalyClassifierFactory anomalyClassifierFactory;

  private Map<Long, AnomalyFunctionDTO> anomalyFunctionSpecMap = new HashMap<>();
  private Map<Long, AlertFilter> alertFilterMap = new HashMap<>();

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    List<TaskResult> taskResults = new ArrayList<>();

    LOG.info("Setting up task {}", taskInfo);
    setupTask(taskInfo, taskContext);

    runTask(windowStart, windowEnd);

    return taskResults;
  }

  private void setupTask(TaskInfo taskInfo, TaskContext taskContext) {
    ClassificationTaskInfo classificationTaskInfo = (ClassificationTaskInfo) taskInfo;
    windowStart = classificationTaskInfo.getWindowStartTime();
    windowEnd = classificationTaskInfo.getWindowEndTime();
    classificationConfig = classificationTaskInfo.getClassificationConfigDTO();
    anomalyFunctionFactory = taskContext.getAnomalyFunctionFactory();
    alertFilterFactory = taskContext.getAlertFilterFactory();
    anomalyClassifierFactory = taskContext.getAnomalyClassifierFactory();
  }

  private void runTask(long windowStart, long windowEnd) {
    List<MergedAnomalyResultDTO> filteredMainAnomalies = new ArrayList<>();
    List<Long> mainFunctionIdList = classificationConfig.getMainFunctionIdList();
    for (Long mainFunctionId : mainFunctionIdList) {
      addAnomalyFunctionAndAlertConfig(mainFunctionId);
      AlertFilter alertFilter = alertFilterMap.get(mainFunctionId);
      // Get the anomalies from the main anomaly function
      List<MergedAnomalyResultDTO> mainAnomalies =
          mergedAnomalyDAO.findOverlappingByFunctionId(mainFunctionId, windowStart, windowEnd);
      filteredMainAnomalies.addAll(filterAnomalies(alertFilter, mainAnomalies));
    }
    // Run classification on each main anomaly that passes through alert filter
    if (CollectionUtils.isNotEmpty(filteredMainAnomalies)) {
      LOG.info("Classification config {} gets {} anomalies to identify issue type.", classificationConfig.getId(),
          filteredMainAnomalies.size());

      // Sort merged anomalies by the natural order of the end time
      Collections.sort(filteredMainAnomalies, MERGE_ANOMALY_END_TIME_COMPARATOR);
      // Run classifier for each dimension of the anomalies
      List<MergedAnomalyResultDTO> updatedMainAnomaliesByDimension =
          dimensionalShuffleAndUnifyClassification(filteredMainAnomalies);
      // Update anomalies whose issue type is updated.
      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : updatedMainAnomaliesByDimension) {
        mergedAnomalyDAO.update(mergedAnomalyResultDTO);
      }
    }
    // Update watermark of window end time
    if (windowEnd > classificationConfig.getEndTimeWatermark()) {
      classificationConfig.setEndTimeWatermark(windowEnd);
      classificationConfigDAO.update(classificationConfig);
    }
  }

  /**
   * For each dimension of the main anomalies, this method collects its correlated anomalies from other metrics and
   * invokes the classification logic on those anomalies. The correlated anomalies could come from activated or
   * deactivated functions. For the former functions, this method retrieves its anomalies from backend DB. For the
   * latter one, it invokes adhoc anomaly detections on the time window of main anomalies. The time window is determined
   * by the min. start time and max. end time of main anomalies; in addition, the window is bounded by the start and
   * end time of this classification job just in case of long main anomalies.
   *
   * @param mainAnomalies the collection of main anomalies.
   *
   * @return a collection of main anomalies, which has issue type updated, that is grouped by dimensions.
   */
  private List<MergedAnomalyResultDTO> dimensionalShuffleAndUnifyClassification(
      List<MergedAnomalyResultDTO> mainAnomalies) {

    List<MergedAnomalyResultDTO> updatedMainAnomaliesByDimension = new ArrayList<>();
    // Terminate if the main anomaly function has not detected any anomalies in the given window
    if (CollectionUtils.isEmpty(mainAnomalies)) {
      return updatedMainAnomaliesByDimension;
    }

    // Sort anomalies by their dimensions
    ListMultimap<DimensionMap, MergedAnomalyResultDTO> mainAnomaliesByDimensionMap = ArrayListMultimap.create();
    for (MergedAnomalyResultDTO mainAnomaly : mainAnomalies) {
      mainAnomaliesByDimensionMap.put(mainAnomaly.getDimensions(), mainAnomaly);
    }

    // Set up maps of function id to anomaly function config and alert filter
    List<Long> auxFunctionIdList = classificationConfig.getAuxFunctionIdList();
    for (Long auxFunctionId : auxFunctionIdList) {
      addAnomalyFunctionAndAlertConfig(auxFunctionId);
    }

    // For each dimension, we get the anomalies from the correlated metric
    for (DimensionMap dimensionMap : mainAnomaliesByDimensionMap.keySet()) {

      // Determine the smallest time window that could enclose all main anomalies. In addition, this window is bounded
      // by windowStart and windowEnd because we don't want to grab too many correlated anomalies for classification.
      // The start and end time of this window is used to retrieve the anomalies on the correlated metrics.
      List<MergedAnomalyResultDTO> mainAnomaliesByDimension = mainAnomaliesByDimensionMap.get(dimensionMap);
      long startTimeForCorrelatedAnomalies = windowStart;
      long endTimeForCorrelatedAnomalies = windowEnd;
      for (MergedAnomalyResultDTO mainAnomaly : mainAnomaliesByDimension) {
        startTimeForCorrelatedAnomalies = Math.max(startTimeForCorrelatedAnomalies, mainAnomaly.getStartTime());
        endTimeForCorrelatedAnomalies = Math.min(endTimeForCorrelatedAnomalies, mainAnomaly.getEndTime());
      }

      Map<String, List<MergedAnomalyResultDTO>> metricNameToAuxAnomalies = new HashMap<>();

      // Get the anomalies from other anomaly function that are activated
      for (Long auxFunctionId : auxFunctionIdList) {
        AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionSpecMap.get(auxFunctionId);
        AlertFilter alertFilter = alertFilterMap.get(auxFunctionId);
        List<MergedAnomalyResultDTO> anomalies;
        if (anomalyFunctionDTO.getIsActive()) {
          // Get existing anomalies from DB
          anomalies = mergedAnomalyDAO.findOverlappingByFunctionIdDimensions(auxFunctionId, startTimeForCorrelatedAnomalies,
              endTimeForCorrelatedAnomalies, dimensionMap.toString());
        } else {
          LOG.info("Invoking ad-hoc anomaly detection for anomaly function {} at window ({}--{}).", auxFunctionId,
              new DateTime(windowStart), new DateTime(windowEnd));
          // Trigger ad-hoc anomaly detection
          try {
            anomalies = adhocAnomalyDetection(auxFunctionId, startTimeForCorrelatedAnomalies,
                endTimeForCorrelatedAnomalies, dimensionMap);
          } catch (Exception e) {
            anomalies = Collections.emptyList();
            LOG.warn(
                "Failed to fetch data for the auxiliary anomaly function ({}); Omitting anomalies from this function.",
                auxFunctionId, e);
          }
        }
        // Filter anomalies for current anomaly function
        List<MergedAnomalyResultDTO> filteredAnomalies = filterAnomalies(alertFilter, anomalies);
        if (CollectionUtils.isNotEmpty(filteredAnomalies)) {
          Collections.sort(filteredAnomalies, MERGE_ANOMALY_END_TIME_COMPARATOR);
          String metricName = anomalyFunctionDTO.getMetric();
          if (metricNameToAuxAnomalies.containsKey(metricName)) {
            metricNameToAuxAnomalies.get(metricName).addAll(filteredAnomalies);
          } else {
            metricNameToAuxAnomalies.put(metricName, filteredAnomalies);
          }
        }
      }

      // Invoke classification logic for each main anomaly of this dimension
      for (MergedAnomalyResultDTO mainAnomalyResult : mainAnomaliesByDimension) {
        // Initiate classifier
        Map<String, String> classifierConfig = new HashMap<>(classificationConfig.getClassifierConfig());
        AnomalyClassifier anomalyClassifier = anomalyClassifierFactory.fromSpec(classifierConfig);
        anomalyClassifier.setParameters(classifierConfig);
        // Construct map of metric name to auxiliary anomalies that are overlapping with the current main anomaly
        Map<String, List<MergedAnomalyResultDTO>> metricNameToOverlappingAuxAnomalies = new HashMap<>();
        for (Map.Entry<String, List<MergedAnomalyResultDTO>> metricNameToAuxAnomaliesEntry : metricNameToAuxAnomalies
            .entrySet()) {
          List<MergedAnomalyResultDTO> overlappingAuxAnomalies = new ArrayList<>();
          for (MergedAnomalyResultDTO auxAnomalyResult : metricNameToAuxAnomaliesEntry.getValue()) {
            if (auxAnomalyResult.getEndTime() > mainAnomalyResult.getStartTime()
                && auxAnomalyResult.getStartTime() < mainAnomalyResult.getEndTime()) {
              overlappingAuxAnomalies.add(auxAnomalyResult);
            }
          }
          String metricName = metricNameToAuxAnomaliesEntry.getKey();
          if (metricNameToOverlappingAuxAnomalies.containsKey(metricName)) {
            metricNameToOverlappingAuxAnomalies.get(metricName).addAll(overlappingAuxAnomalies);
          } else {
            metricNameToOverlappingAuxAnomalies.put(metricName, overlappingAuxAnomalies);
          }
        }
        // Get and update issue type for the current main anomalies
        String issueType = anomalyClassifier.classify(mainAnomalyResult, metricNameToOverlappingAuxAnomalies);
        if (updateIssueTypeForAnomalyResult(mainAnomalyResult, issueType)) {
          updatedMainAnomaliesByDimension.add(mainAnomalyResult);
        }
      }
    }

    return updatedMainAnomaliesByDimension;
  }

  /**
   * Adds or updates the issue type to the property field of the given anomaly. If the given issue type is null, then
   * no addition or update is performed.
   *
   * @param mainAnomaly the anomaly to be updated.
   * @param issueType   the issue type to be added or updated to the given anomaly. If the given issue type is null,
   *                    then no addition or update is performed.
   *
   * @return if the issue type is added/updated successfully, i.e., if the anomaly should be written back to DB.
   */
  private boolean updateIssueTypeForAnomalyResult(MergedAnomalyResultDTO mainAnomaly, String issueType) {
    if (mainAnomaly == null || issueType == null) {
      return false;
    }

    Map<String, String> anomalyProperties = mainAnomaly.getProperties();
    if (anomalyProperties == null) {
      anomalyProperties = new HashMap<>();
      mainAnomaly.setProperties(anomalyProperties);
    }
    String originalIssueType = null;
    if (anomalyProperties.containsKey(ISSUE_TYPE_KEY)) {
      originalIssueType = anomalyProperties.get(ISSUE_TYPE_KEY);
    }
    if (!Objects.equals(issueType, originalIssueType)) {
      anomalyProperties.put(ISSUE_TYPE_KEY, issueType);
      return true;
    }
    return false;
  }

  /**
   * Initiates anomaly function spec and alert filter config for the given function id.
   *
   * @param functionId the id of the function to be initiated.
   */
  private void addAnomalyFunctionAndAlertConfig(long functionId) {
    AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(functionId);
    AlertFilter alertFilter = alertFilterFactory.fromSpec(anomalyFunctionDTO.getAlertFilter());
    anomalyFunctionSpecMap.put(functionId, anomalyFunctionDTO);
    alertFilterMap.put(functionId, alertFilter);
  }

  /**
   * Filter the list of anomalies by the given alert filter.
   *
   * @param alertFilter the filter to apply on the list of anomalies.
   * @param anomalies a list of anomalies.
   *
   * @return a list of anomalies that pass through the alert filter.
   */
  private static List<MergedAnomalyResultDTO> filterAnomalies(AlertFilter alertFilter,
      List<MergedAnomalyResultDTO> anomalies) {
    List<MergedAnomalyResultDTO> filteredMainAnomalies = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(anomalies)) {
      for (MergedAnomalyResultDTO mainAnomaly : anomalies) {
        if (alertFilter.isQualified(mainAnomaly)) {
          filteredMainAnomalies.add(mainAnomaly);
        }
      }
    }
    return filteredMainAnomalies;
  }

  /**
   * A comparator to sort merged anomalies in the natural order of their end time.
   */
  private static class MergeAnomalyEndTimeComparator implements Comparator<MergedAnomalyResultDTO> {
    @Override
    public int compare(MergedAnomalyResultDTO lhs, MergedAnomalyResultDTO rhs) {
      return (int) (lhs.getEndTime() - rhs.getEndTime());
    }
  }

  // TODO: Refine the almost duplicated code in anomaly detection task runner
  /**
   * Invokes an ad-hoc anomalies detection for the given anomaly function on the given monitoring window and dimensions.
   * The monitoring window might be extended to satisfy the min detection window of the anomaly function.
   *
   * @param functionId            the function id of the anomaly function.
   * @param monitoringWindowStart the original monitoring window start, which might be changed in order to satisfy the
   *                              min detection window size.
   * @param monitoringWindowEnd   the monitoring window end.
   * @param dimensions            the dimension on which the anomaly detection is executed.
   *
   * @return A list of merged anomalies that are detected in the given monitoring window.
   *
   * @throws Exception If the ad-hoc anomaly detection cannot be finished successfully.
   */
  private List<MergedAnomalyResultDTO> adhocAnomalyDetection(long functionId, long monitoringWindowStart,
      long monitoringWindowEnd, DimensionMap dimensions) throws Exception {
    // Initiate anomaly function
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionSpecMap.get(functionId);
    AlertFilter alertFilter = alertFilterMap.get(functionId);
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

    DateTime windowStart = new DateTime(monitoringWindowStart);
    DateTime windowEnd = new DateTime(monitoringWindowEnd);
    // Increase window size if it is smaller than the detection window size defined by anomaly function.
    // The reason is that an anomaly function could have minimal detection window size, i.e., detection does not
    // happen if monitoring window size is smaller than min size.
    Integer detectionWindowSize = anomalyFunctionSpec.getWindowSize();
    TimeUnit detectionWindowUnit = anomalyFunctionSpec.getWindowUnit();
    TimeGranularity minDetectionWindowSize = new TimeGranularity(detectionWindowSize, detectionWindowUnit);
    if (monitoringWindowEnd - monitoringWindowStart < minDetectionWindowSize.toMillis()) {
      windowStart = windowEnd.minus(minDetectionWindowSize.toPeriod());
    }

    // The ad-hoc anomaly detection pipeline starts here
    // Gathering input context, e.g., time series, known anomalies, etc.
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

      // Starts anomaly detection
      List<AnomalyResult> adhocRawAnomalies =
          anomalyFunction.analyze(dimensions, metricTimeSeries, windowStart, windowEnd, knownAnomalies);

      // Generate merged anomalies from the adhoc raw anomalies
      if (CollectionUtils.isNotEmpty(adhocRawAnomalies)) {
        for (AnomalyResult rawAnomaly : adhocRawAnomalies) {
          // Make sure score and weight are valid numbers
          rawAnomaly.setScore(DetectionTaskRunner.normalize(rawAnomaly.getScore()));
          rawAnomaly.setWeight(DetectionTaskRunner.normalize(rawAnomaly.getWeight()));
        }

        ListMultimap<DimensionMap, AnomalyResult> resultRawAnomalies = ArrayListMultimap.create();
        resultRawAnomalies.putAll(dimensions, adhocRawAnomalies);
        TimeBasedAnomalyMerger timeBasedAnomalyMerger = new TimeBasedAnomalyMerger(anomalyFunctionFactory);
        ListMultimap<DimensionMap, MergedAnomalyResultDTO> resultMergedAnomalies =
            timeBasedAnomalyMerger.mergeAnomalies(anomalyFunctionSpec, resultRawAnomalies);
        // The list of mergedAnomalies might contain the anomalies that are located inside the original monitoring
        // window, because the window might be extended to satisfy the min detection window.
        List<MergedAnomalyResultDTO> mergedAnomalies = resultMergedAnomalies.get(dimensions);
        LOG.info("{} anomalies are detected by ad-hoc anomaly detection for function {})", mergedAnomalies.size(), functionId);
        if (CollectionUtils.isNotEmpty(mergedAnomalies)) {
          List<MergedAnomalyResultDTO> mergedAnomaliesInMonitoringWindow = new ArrayList<>();
          for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
            if ((mergedAnomaly.getEndTime() > monitoringWindowStart) && (mergedAnomaly.getStartTime()
                < monitoringWindowEnd) && alertFilter.isQualified(mergedAnomaly)) {
              mergedAnomaliesInMonitoringWindow.add(mergedAnomaly);
            }
          }
          LOG.info("{} anomalies are located in monitoring window for function {}", mergedAnomaliesInMonitoringWindow.size(), functionId);
          return mergedAnomaliesInMonitoringWindow;
        }
      }
    }

    return Collections.emptyList();
  }
}
