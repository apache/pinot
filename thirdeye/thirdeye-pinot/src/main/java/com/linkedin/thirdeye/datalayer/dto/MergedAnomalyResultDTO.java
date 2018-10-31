package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MergedAnomalyResultDTO extends MergedAnomalyResultBean implements AnomalyResult {

  private AnomalyFeedbackDTO feedback;

  private AnomalyFunctionDTO function;

  public MergedAnomalyResultDTO() {
    setCreatedTime(System.currentTimeMillis());
  }

  public MergedAnomalyResultDTO(AnomalyResult anomalyResult) {
    setCreatedTime(System.currentTimeMillis());
    populateFrom(anomalyResult);
  }

  public void populateFrom(AnomalyResult anomalyResult) {
    setStartTime(anomalyResult.getStartTime());
    setEndTime(anomalyResult.getEndTime());
    setDimensions(anomalyResult.getDimensions());
    setScore(anomalyResult.getScore());
    setWeight(anomalyResult.getWeight());
    setAvgCurrentVal(anomalyResult.getAvgCurrentVal());
    setAvgBaselineVal(anomalyResult.getAvgBaselineVal());
    setFeedback(anomalyResult.getFeedback());
    setProperties(anomalyResult.getProperties());
  }

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

  public AnomalyFunctionDTO getFunction() {
    return function;
  }

  public void setFunction(AnomalyFunctionDTO function) {
    this.function = function;
  }
}
