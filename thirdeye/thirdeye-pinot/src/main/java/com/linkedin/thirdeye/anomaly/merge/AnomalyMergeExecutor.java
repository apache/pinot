package com.linkedin.thirdeye.anomaly.merge;

import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.db.dao.AnomalyMergedResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * finds raw anomalies grouped by a strategy and merges them with an existing (in the same group) or
 * new {@link com.linkedin.thirdeye.db.entity.AnomalyMergedResult}
 */
public class AnomalyMergeExecutor {
  final AnomalyMergedResultDAO mergedResultDAO;
  final AnomalyResultDAO anomalyResultDAO;
  final AnomalyFunctionDAO anomalyFunctionDAO;
  final AnomalyMergeConfig mergeConfig;

  private final static Logger LOG = LoggerFactory.getLogger(AnomalyMergeExecutor.class);

  public AnomalyMergeExecutor(AnomalyMergedResultDAO mergedResultDAO,
      AnomalyFunctionDAO anomalyFunctionDAO, AnomalyResultDAO anomalyResultDAO,
      AnomalyMergeConfig mergeConfig) {
    this.mergedResultDAO = mergedResultDAO;
    this.anomalyResultDAO = anomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.mergeConfig = mergeConfig;
  }

  public void updateMergedResults() {
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

      // TODO : move merge config within the AnomalyFunction; Every function should have its own merge config.
      List<AnomalyMergedResult> output = new ArrayList<>();

      if (unmergedResults.size() > 0) {
        switch (mergeConfig.getMergeStrategy()) {
        case FUNCTION:
          performMergeBasedOnFunctionId(function, unmergedResults, output);
          break;
        case FUNCTION_DIMENSIONS:
          performMergeBasedOnFunctionIdAndDimensions(function, unmergedResults, output);
          break;
        default:
          throw new IllegalArgumentException(
              "Merge strategy " + mergeConfig.getMergeStrategy() + " not supported");
        }
      }
      output.forEach(this::updateMergedScoreAndPersist);
    }
  }

  private void updateMergedScoreAndPersist(AnomalyMergedResult mergedResult) {
    double weightedScoreSum = 0.0;
    double weightSum = 0.0;

    for (AnomalyResult anomalyResult : mergedResult.getAnomalyResults()) {
      anomalyResult.setMerged(true);
      double weight = (anomalyResult.getEndTimeUtc() - anomalyResult.getStartTimeUtc()) / 1000;
      weightedScoreSum += anomalyResult.getScore() * weight;
      weightSum += weight;
    }
    if (weightSum != 0) {
      mergedResult.setScore(weightedScoreSum / weightSum);
    }
    try {
      // persist the merged result
      mergedResultDAO.update(mergedResult);
      anomalyResultDAO.updateAll(mergedResult.getAnomalyResults());
    } catch (PersistenceException e) {
      LOG.error("Could not persist merged result : [" + mergedResult.toString() + "]", e);
    }
  }

  private void performMergeBasedOnFunctionId(AnomalyFunctionSpec function,
      List<AnomalyResult> unmergedResults, List<AnomalyMergedResult> output) {
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
    output.addAll(mergedResults);
  }

  private void performMergeBasedOnFunctionIdAndDimensions(AnomalyFunctionSpec function,
      List<AnomalyResult> unmergedResults, List<AnomalyMergedResult> output) {
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
      output.addAll(mergedResults);
    }
  }
}
