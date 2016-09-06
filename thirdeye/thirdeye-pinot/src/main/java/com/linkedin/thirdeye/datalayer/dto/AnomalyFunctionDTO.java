package com.linkedin.thirdeye.datalayer.dto;

import javax.persistence.Entity;
import javax.persistence.Table;

import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;

@Entity
@Table(name = "anomaly_functions")
public class AnomalyFunctionDTO extends AnomalyFunctionBean {



}
