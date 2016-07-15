package com.linkedin.thirdeye.detector.api;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;
import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "anomaly_feedback")
public class AnomalyFeedback implements Serializable{
  private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @Column(name = "feedback_type", nullable = false)
  @Enumerated(EnumType.STRING)
  private AnomalyFeedbackType feedbackType;

  @Column(name = "status", nullable = false)
  @Enumerated(EnumType.STRING)
  private FeedbackStatus status;

  @Column(name = "comment", length = 500)
  private String comment;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

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

    AnomalyFeedback that = (AnomalyFeedback) o;

    if (id != that.id) {
      return false;
    }
    if (feedbackType != that.feedbackType) {
      return false;
    }
    if (status != that.status) {
      return false;
    }
    return comment != null ? comment.equals(that.comment) : that.comment == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (id ^ (id >>> 32));
    result = 31 * result + (feedbackType != null ? feedbackType.hashCode() : 0);
    result = 31 * result + (status != null ? status.hashCode() : 0);
    result = 31 * result + (comment != null ? comment.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "AnomalyFeedback{" + "id=" + id + ", feedback=" + feedbackType + ", status=" + status
        + ", comment='" + comment + '\'' + '}';
  }
}
