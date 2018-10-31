package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import java.io.Serializable;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import org.apache.commons.lang3.StringUtils;

public class AnomalyFeedbackDTO extends AnomalyFeedbackBean implements AnomalyFeedback, Serializable {
  private static final long serialVersionUID = 1L;

  public AnomalyFeedbackDTO() {
    this.setFeedbackType(AnomalyFeedbackType.NO_FEEDBACK);
    this.setComment("");
  }

  public AnomalyFeedbackDTO(AnomalyFeedback anomalyFeedback) {
    this();
    if (anomalyFeedback != null) {
      if (anomalyFeedback.getFeedbackType() != null) {
        this.setFeedbackType(anomalyFeedback.getFeedbackType());
      }
      if (StringUtils.isNotBlank(anomalyFeedback.getComment())) {
        this.setComment(anomalyFeedback.getComment());
      }
    }
  }
}
