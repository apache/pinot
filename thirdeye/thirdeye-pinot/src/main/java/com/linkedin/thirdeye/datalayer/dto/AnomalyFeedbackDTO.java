package com.linkedin.thirdeye.datalayer.dto;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.Table;

import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;

@Entity
@Table(name = "anomaly_feedback")
public class AnomalyFeedbackDTO extends AnomalyFeedbackBean implements Serializable {
  private static final long serialVersionUID = 1L;


}
