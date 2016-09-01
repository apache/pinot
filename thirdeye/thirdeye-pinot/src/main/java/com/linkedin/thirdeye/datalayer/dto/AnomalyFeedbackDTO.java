package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;

public class AnomalyFeedbackDTO extends AbstractDTO {

  private AnomalyFeedbackType feedbackType;

  private FeedbackStatus status;

  private String comment;

  public AnomalyFeedbackType getFeedbackType() {
    return feedbackType;
  }

  public void setFeedbackType(AnomalyFeedbackType feedbackType) {
    this.feedbackType = feedbackType;
  }

  public FeedbackStatus getStatus() {
    return status;
  }

  public void setStatus(FeedbackStatus status) {
    this.status = status;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }
}
