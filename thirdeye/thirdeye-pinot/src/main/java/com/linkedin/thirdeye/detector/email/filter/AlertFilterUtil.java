package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;


/**
 * Utility class to evaluate alert filter
 */
public class AlertFilterUtil {
  public static final Integer PRECISION_INDEX = 0;
  public static final Integer RECALL_INDEX = 1;
  public static final Integer TOTAL_ANOMALIES_INDEX = 0;
  public static final Integer TOTAL_RESPONSES_INDEX = 1;
  public static final Integer TRUE_ANOMALY_INDEX = 2;
  public static final Integer FALSE_ALARM_INDEX = 3;
  public static final Integer NONACTIONALBLE_INDEX = 4;

  private static final Integer NUM_EVALUATE_METRICS = 2;
  private static final Integer NUM_EVALUTATE_FEEDBACK_METRICS = 5;

  private AlertFilter alertFilter;

  public AlertFilterUtil(AlertFilter alertFilter){
    this.alertFilter = alertFilter;
  }

  /**
   * Evaluate alert filter given merged anomalies and output precision and recall
   * @param mergedAnomalyResultDTOS
   * @return Array of precision and recall
   * @throws Exception
   */
  public Double[] getEvalResults(List<MergedAnomalyResultDTO> mergedAnomalyResultDTOS) throws Exception {
    int TP = 0;
    int FP = 0;
    int FN = 0;
    Double[] evals = new Double[NUM_EVALUATE_METRICS];
    for (MergedAnomalyResultDTO anomaly: mergedAnomalyResultDTOS) {
      boolean predLabel = alertFilter.isQualified(anomaly);
      AnomalyFeedbackDTO feedback = anomaly.getFeedback();
      boolean label = !(feedback == null || feedback.getFeedbackType() == AnomalyFeedbackType.NOT_ANOMALY);
      //predicted true
      if (predLabel) {
        if (label) {
          TP++;
        } else {
          FP++;
        }
      } else if (label) {
        // else if predicted false but label is true
        FN++;
      }
    }
    if (TP + FN == 0) {
      throw new Exception("No true labels in dataset. Check data");
    }
    if (TP + FP == 0) {
      throw new Exception("No predicted true labels. Check model input");
    }
    evals[PRECISION_INDEX] = 1.000 * TP / (TP + FP);
    evals[RECALL_INDEX] = 1.000 * TP / (TP + FN);
    return evals;
  }

  //TODO: put it somewhere not in alertfilterUtil?

  /**
   * Provide feedback summary give a list of merged anomalies
   * @param anomalies
   * @return summary array of feedbacks
   */
  public Integer[] getEvalFeedbacks(List<MergedAnomalyResultDTO> anomalies){
    Integer[] evals = new Integer[NUM_EVALUTATE_FEEDBACK_METRICS];
    int totalAnomalies = 0;
    int totalResponses = 0;
    int trueAnomalies = 0;
    int falseAlarm = 0;
    int nonActionable = 0;
    for(MergedAnomalyResultDTO anomaly: anomalies) {
      totalAnomalies++;
      // evaluate feedbacks
      AnomalyFeedbackDTO feedback = anomaly.getFeedback();
      if (feedback != null) {
        totalResponses++;
        AnomalyFeedbackType feedbackType = feedback.getFeedbackType();
        switch (feedbackType) {
          case ANOMALY:
            trueAnomalies++;
            break;
          case ANOMALY_NO_ACTION:
            nonActionable++;
            break;
          case NOT_ANOMALY:
            falseAlarm++;
            break;
        }
      }
    }
    evals[TOTAL_ANOMALIES_INDEX] = totalAnomalies;
    evals[TOTAL_RESPONSES_INDEX] = totalResponses;
    evals[TRUE_ANOMALY_INDEX] = trueAnomalies;
    evals[FALSE_ALARM_INDEX] = falseAlarm;
    evals[NONACTIONALBLE_INDEX] = nonActionable;
    return evals;
  }
}
