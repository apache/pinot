package com.linkedin.thirdeye.datalayer.dto;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "anomaly_merged_results")
public class MergedAnomalyResultDTO extends MergedAnomalyResultBean {

  @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER, orphanRemoval = true)
  @JoinColumn(name = "anomaly_feedback_id")
  private AnomalyFeedbackDTO feedback;

  @OneToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
  @JoinTable(name = "anomaly_merged_results_mapping", joinColumns = @JoinColumn(name = "anomaly_merged_result_id"),
      inverseJoinColumns = @JoinColumn(name = "anomaly_result_id"))
  private List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();

  @ManyToOne(fetch = FetchType.EAGER, cascade = CascadeType.DETACH)
  @JoinColumn(name = "function_id")
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
