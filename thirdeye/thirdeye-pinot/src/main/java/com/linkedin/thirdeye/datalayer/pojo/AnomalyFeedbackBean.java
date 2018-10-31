package com.linkedin.thirdeye.datalayer.pojo;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import java.io.Serializable;
import java.util.Objects;

public class AnomalyFeedbackBean extends AbstractBean implements Serializable {
  private static final long serialVersionUID = 1L;

  private AnomalyFeedbackType feedbackType;

  private String comment;

  public AnomalyFeedbackType getFeedbackType() {
    return feedbackType;
  }

  public void setFeedbackType(AnomalyFeedbackType feedbackType) {
    this.feedbackType = feedbackType;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AnomalyFeedbackBean that = (AnomalyFeedbackBean) o;
    return Objects.equals(getId(), that.getId()) && Objects.equals(feedbackType, that.getFeedbackType())
        && Objects.equals(comment, that.getComment());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), feedbackType, comment);
  }
}
