package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;


public class AlertUtils {
  private AlertUtils() {
    //left blank
  }

  public static boolean hasFeedback(MergedAnomalyResultDTO anomaly) {
    return anomaly.getFeedback() != null
        && !AnomalyFeedbackType.NO_FEEDBACK.equals(anomaly.getFeedback().getFeedbackType());
  }
}
