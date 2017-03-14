package com.linkedin.thirdeye.anomalydetection.performanceEvaluation;

import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import org.joda.time.Interval;


public class PerformanceEvaluateHelper {
  public static PerformanceEvaluate getPerformanceEvaluator(PerformanceEvaluationMethod performanceEvaluationMethod,
      long functionId, long clonedFunctionId, Interval windowInterval,
      MergedAnomalyResultManager mergedAnomalyResultDAO) {
    PerformanceEvaluate performanceEvaluator = null;
    List<MergedAnomalyResultDTO> knownAnomalies = mergedAnomalyResultDAO.findAllConflictByFunctionId(functionId,
        windowInterval.getStartMillis(), windowInterval.getEndMillis());
    List<MergedAnomalyResultDTO> detectedMergedAnomalies = mergedAnomalyResultDAO.findAllConflictByFunctionId(
        clonedFunctionId, windowInterval.getStartMillis(), windowInterval.getEndMillis());
    switch (performanceEvaluationMethod){
      case ANOMALY_PERCENTAGE:
      default:
        performanceEvaluator = new AnomalyPercentagePerformanceEvaluation(windowInterval, detectedMergedAnomalies);
    }
    return performanceEvaluator;
  }
}