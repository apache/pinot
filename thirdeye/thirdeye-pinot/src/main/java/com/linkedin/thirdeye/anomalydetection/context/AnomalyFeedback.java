package com.linkedin.thirdeye.anomalydetection.context;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;

public interface AnomalyFeedback {
  /**
   * Set feedback type (e.g., anomaly, anomaly no action, etc.)
   * @param feedbackType feedback type
   */
  void setFeedbackType(AnomalyFeedbackType feedbackType);

  /**
   * Get feedback type (e.g., anomaly, anomaly no action, etc.)
   * @return feedback type
   */
  AnomalyFeedbackType getFeedbackType();

  /**
   * Set comment for this feedback.
   * @param comment comment for this feedback.
   */
  void setComment(String comment);

  /**
   * Get comment of this feedback.
   * @return comment of this feedback.
   */
  String getComment();
}
