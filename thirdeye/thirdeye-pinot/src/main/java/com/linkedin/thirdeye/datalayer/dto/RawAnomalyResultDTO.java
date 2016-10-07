package com.linkedin.thirdeye.datalayer.dto;

import java.util.Objects;

import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;

public class RawAnomalyResultDTO extends RawAnomalyResultBean {

  private AnomalyFeedbackDTO feedback;

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

  public String getMetric() {
    return function.getMetric();
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

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RawAnomalyResultDTO)) {
      return false;
    }
    RawAnomalyResultDTO r = (RawAnomalyResultDTO) o;
    return Objects.equals(getFunction(), function)
        && Objects.equals(getStartTime(), r.getStartTime())
        && Objects.equals(getDimensions(), r.getDimensions()) && Objects.equals(getEndTime(), r.getEndTime())
        && Objects.equals(getScore(), r.getScore()) && Objects.equals(getWeight(), r.getWeight())
        && Objects.equals(getProperties(), r.getProperties()) && Objects.equals(getMessage(), r.getMessage());
    // Intentionally omit creationTimeUtc, since start/end are the truly significant dates for
    // anomalies
  }

  @Override
  public int hashCode() {
    return Objects.hash(function, getDimensions(), getStartTime(), getEndTime(), getScore(), getWeight(), getProperties(),
        getMessage());
    // Intentionally omit creationTimeUtc, since start/end are the truly significant dates for
    // anomalies
  }

}
