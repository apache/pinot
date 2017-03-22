package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import java.io.Serializable;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;

public class AnomalyFeedbackDTO extends AnomalyFeedbackBean implements AnomalyFeedback, Serializable {
  private static final long serialVersionUID = 1L;

  public AnomalyFeedbackDTO() { }

  public AnomalyFeedbackDTO(AnomalyFeedback anomalyFeedback) {
    this.setFeedbackType(anomalyFeedback.getFeedbackType());
    this.setStatus(anomalyFeedback.getStatus());
    this.setComment(anomalyFeedback.getComment());
  }
}
