package com.linkedin.thirdeye.datalayer.dto;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;

@Entity
@Table(name = "anomaly_results")
public class RawAnomalyResultDTO extends RawAnomalyResultBean {

  @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER, orphanRemoval = true)
  @JoinColumn(name = "anomaly_feedback_id")
  private AnomalyFeedbackDTO feedback;


  @ManyToOne(fetch = FetchType.EAGER)
  @JoinColumn(name = "function_id")
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
}
