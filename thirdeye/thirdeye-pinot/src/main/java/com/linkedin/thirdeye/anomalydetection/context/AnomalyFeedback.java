package com.linkedin.thirdeye.anomalydetection.context;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;

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
   * Set status (e.g., in progress, resolve, etc.) of this feedback.
   * @param status status of this feedback
   */
  void setStatus(FeedbackStatus status);

  /**
   * Get status (e.g., in progress, resolve, etc.) of this feedback.
   * @return status of this feedback
   */
  FeedbackStatus getStatus();

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
