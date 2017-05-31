package com.linkedin.thirdeye.anomaly.classification;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
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
  private AnomalyFunctionManager anomalyFunctionDAO = DAORegistry.getInstance().getAnomalyFunctionDAO();
  private MergedAnomalyResultManager mergedAnomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();

  private long windowStart;
  private long windowEnd;
  private ClassificationConfigDTO classificationConfig;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AlertFilterFactory alertFilterFactory;

  Map<Long, AnomalyFunctionDTO> anomalyFunctionConfigMap = new HashMap<>();
  Map<Long, AlertFilter> alertFilterMap = new HashMap<>();

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
  }

  private void runTask(long windowStart, long windowEnd) {
    long mainFunctionId = classificationConfig.getMainFunctionId();
    addAnomalyFunctionAndAlertConfig(mainFunctionId);
    AlertFilter alertFilter = alertFilterMap.get(mainFunctionId);

    // Get the anomalies from the main anomaly function
    List<MergedAnomalyResultDTO> mainAnomalies =
        mergedAnomalyDAO.findAllOverlapByFunctionId(mainFunctionId, windowStart, windowEnd, false);
    List<MergedAnomalyResultDTO> filteredMainAnomalies = filterAnomalies(alertFilter, mainAnomalies);

    // Sort merged anomalies by the natural order of the end time
    Collections.sort(filteredMainAnomalies, new MergeAnomalyEndTimeComparator());
    // Run classifier for each dimension of the anomalies
    ListMultimap<DimensionMap, MergedAnomalyResultDTO> updatedMainAnomaliesByDimension =
        dimensionalShuffleAndUnifyClassification(filteredMainAnomalies);

  }

  private ListMultimap<DimensionMap, MergedAnomalyResultDTO> dimensionalShuffleAndUnifyClassification(
      List<MergedAnomalyResultDTO> mainAnomalies) {

    ListMultimap<DimensionMap, MergedAnomalyResultDTO> updatedMainAnomaliesByDimension = ArrayListMultimap.create();
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
    List<Long> functionIds = classificationConfig.getFunctionIdList();
    for (Long functionId : functionIds) {
      addAnomalyFunctionAndAlertConfig(functionId);
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

      Map<Long, List<MergedAnomalyResultDTO>> functionIdToAnomalyResult = new HashMap<>();
      // Get the anomalies from other anomaly function that are activated
      for (Long functionId : functionIds) {
        AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionConfigMap.get(functionId);
        AlertFilter alertFilter = alertFilterMap.get(functionId);
        if (anomalyFunctionDTO.getIsActive()) {
          List<MergedAnomalyResultDTO> anomalies = mergedAnomalyDAO
              .findAllOverlapByFunctionIdDimensions(functionId, startTimeForCorrelatedAnomalies,
                  endTimeForCorrelatedAnomalies, dimensionMap.toString(), false);
          List<MergedAnomalyResultDTO> filteredAnomalies = filterAnomalies(alertFilter, anomalies);
          Collections.sort(filteredAnomalies, new MergeAnomalyEndTimeComparator());
          functionIdToAnomalyResult.put(functionId, filteredAnomalies);
        } else {
          // TODO: Trigger adhoc anomaly detection
        }
      }

      // TODO: Invoke classification logic for this dimension

    }

    return updatedMainAnomaliesByDimension;
  }

  private void addAnomalyFunctionAndAlertConfig(long functionId) {
    AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(functionId);
    anomalyFunctionConfigMap.put(functionId, anomalyFunctionDTO);
    AlertFilter alertFilter = alertFilterFactory.fromSpec(anomalyFunctionDTO.getAlertFilter());
    alertFilterMap.put(functionId, alertFilter);
  }

  private static List<MergedAnomalyResultDTO> filterAnomalies(AlertFilter alertFilter,
      List<MergedAnomalyResultDTO> anomalies) {
    List<MergedAnomalyResultDTO> filteredMainAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO mainAnomaly : anomalies) {
      if (alertFilter.isQualified(mainAnomaly)) {
        filteredMainAnomalies.add(mainAnomaly);
      }
    }
    return filteredMainAnomalies;
  }

  private static class MergeAnomalyEndTimeComparator implements Comparator<MergedAnomalyResultDTO> {
    @Override
    public int compare(MergedAnomalyResultDTO lhs, MergedAnomalyResultDTO rhs) {
      return (int) (lhs.getEndTime() - rhs.getEndTime());
    }
  }
}
