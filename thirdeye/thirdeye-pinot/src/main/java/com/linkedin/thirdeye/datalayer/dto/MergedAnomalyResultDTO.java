package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MergedAnomalyResultDTO extends MergedAnomalyResultBean implements AnomalyResult {

  private AnomalyFeedbackDTO feedback;

  private List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();

  private AnomalyFunctionDTO function;

  @Override
  public void setFeedback(AnomalyFeedback anomalyFeedback) {
    if (anomalyFeedback == null) {
      this.feedback = null;
    } else if (anomalyFeedback instanceof AnomalyFeedbackDTO) {
      this.feedback = (AnomalyFeedbackDTO) anomalyFeedback;
    } else {
      this.feedback = new AnomalyFeedbackDTO(anomalyFeedback);
    }
  }

  @Override
  public AnomalyFeedback getFeedback() {
    return this.feedback;
  }

  public List<RawAnomalyResultDTO> getAnomalyResults() {
    return anomalyResults;
  }

  public void setAnomalyResults(List<RawAnomalyResultDTO> anomalyResults) {
    this.anomalyResults = anomalyResults;
  }

  public AnomalyFunctionDTO getFunction() {
    return function;
  }

  public void setFunction(AnomalyFunctionDTO function) {
    this.function = function;
  }
}
