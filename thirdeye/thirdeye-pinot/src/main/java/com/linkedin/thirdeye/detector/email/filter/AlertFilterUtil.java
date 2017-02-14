package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;


public class AlertFilterUtil {
  public static final Integer PRECISION_INDEX = 0;
  public static final Integer RECALL_INDEX = 1;

  private static final Integer NUM_EVALUATE_METRICS = 2;
  private AlertFilter alertFilter;

  public AlertFilterUtil(AlertFilter alertFilter){
    this.alertFilter = alertFilter;
  }

  public double[] getEvalResults(List<MergedAnomalyResultDTO> mergedAnomalyResultDTOS) throws Exception {
    int TP = 0;
    int FP = 0;
    int FN = 0;
    double[] evals = new double[NUM_EVALUATE_METRICS];
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
    evals[PRECISION_INDEX] = 1.0 * TP / (TP + FP);
    evals[RECALL_INDEX] = 1.0 * TP / (TP + FN);
    return evals;
  }
}
