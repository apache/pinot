package com.linkedin.thirdeye.datalayer.dto;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MergedAnomalyResultDTO extends MergedAnomalyResultBean {

  private AnomalyFeedbackDTO feedback;

  private List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();

  private AnomalyFunctionDTO function;

  public AnomalyFeedbackDTO getFeedback() {
    return feedback;
  }

  public void setFeedback(AnomalyFeedbackDTO feedback) {
    this.feedback = feedback;
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
