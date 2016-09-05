package com.linkedin.thirdeye.datalayer.pojo;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;
import java.io.Serializable;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Table;

@Entity
@Table(name = "anomaly_feedback")
public class AnomalyFeedbackBean extends AbstractBean implements Serializable {
  private static final long serialVersionUID = 1L;

  @Column(name = "feedback_type", nullable = false)
  @Enumerated(EnumType.STRING)
  private AnomalyFeedbackType feedbackType;

  @Column(name = "status", nullable = false)
  @Enumerated(EnumType.STRING)
  private FeedbackStatus status;

  @Column(name = "comment", length = 500)
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
        && Objects.equals(status, that.getStatus()) && Objects.equals(comment, that.getComment());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), feedbackType, status, comment);
  }

  @Override
  public String toString() {
    return "AnomalyFeedback{" + "id=" + getId() + ", feedbackType=" + feedbackType + ", status=" + status
        + ", comment='" + comment + '\'' + '}';
  }
}
