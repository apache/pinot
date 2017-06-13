package com.linkedin.thirdeye.anomalydetection.performanceEvaluation;

import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import org.joda.time.Interval;


public class PerformanceEvaluateHelper {
  /**
   * This helper initialize the performance evaluator with requested performance evaluation method.
   * @param performanceEvaluationMethod
   * The enum of performance evaluation method; if null or not found, then ANOMALY_PERCENTAGE
   * @param functionId
   * the original function id. It is for providing user labeled anomaly information for supervised performance
   * evaluation, such as precision, recall and f1 score.
   * @param clonedFunctionId
   * the cloned function id. It is the function id to be evaluated. If functionId == cloneFunctionId, we evaluate the
   * performance of the original function.
   * @param windowInterval
   * the time interval to be evaluated.
   * @param mergedAnomalyResultDAO
   * @return
   * A proper initiated performance evaluator.
   */
  public static PerformanceEvaluate getPerformanceEvaluator(PerformanceEvaluationMethod performanceEvaluationMethod,
      long functionId, long clonedFunctionId, Interval windowInterval,
      MergedAnomalyResultManager mergedAnomalyResultDAO) {
    PerformanceEvaluate performanceEvaluator = null;
    List<MergedAnomalyResultDTO> knownAnomalies = mergedAnomalyResultDAO.findOverlappingByFunctionId(functionId,
        windowInterval.getStartMillis(), windowInterval.getEndMillis(), true);
    List<MergedAnomalyResultDTO> detectedMergedAnomalies = mergedAnomalyResultDAO.findOverlappingByFunctionId(
        clonedFunctionId, windowInterval.getStartMillis(), windowInterval.getEndMillis(), true);
    switch (performanceEvaluationMethod){
      case F1_SCORE:
        performanceEvaluator = new F1ScoreByTimePerformanceEvaluation(knownAnomalies, detectedMergedAnomalies);
        break;
      case RECALL:
        performanceEvaluator = new RecallByTimePreformanceEvaluation(knownAnomalies, detectedMergedAnomalies);
        break;
      case PRECISION:
        performanceEvaluator = new PrecisionByTimePerformanceEvaluation(knownAnomalies, detectedMergedAnomalies);
        break;
      case ANOMALY_PERCENTAGE:
      default:
        performanceEvaluator = new AnomalyPercentagePerformanceEvaluation(windowInterval, detectedMergedAnomalies);
    }
    return performanceEvaluator;
  }
}
