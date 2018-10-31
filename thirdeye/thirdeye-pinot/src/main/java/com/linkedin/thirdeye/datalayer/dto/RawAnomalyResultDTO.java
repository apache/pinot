package com.linkedin.thirdeye.datalayer.dto;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;

@Deprecated
public class RawAnomalyResultDTO extends RawAnomalyResultBean {

  private AnomalyFeedbackDTO feedback;

  @JsonIgnore
  private AnomalyFunctionDTO function;

  public RawAnomalyResultDTO() {
    super();
  }

  public AnomalyFunctionDTO getFunction() {
    return function;
  }

  public void setFunction(AnomalyFunctionDTO function) {
    this.function = function;
  }

  // TODO: rename to getMetric and update the column name in DB?
  public String getMetric() {
    return function.getTopicMetric();
  }

  public String getCollection() {
    return function.getCollection();
  }

  public AnomalyFeedbackDTO getFeedback() {
    return feedback;
  }

  public void setFeedback(AnomalyFeedbackDTO feedback) {
    this.feedback = feedback;
  }

}
