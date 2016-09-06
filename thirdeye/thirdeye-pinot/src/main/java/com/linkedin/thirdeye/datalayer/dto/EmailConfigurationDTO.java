package com.linkedin.thirdeye.datalayer.dto;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

import com.linkedin.thirdeye.datalayer.pojo.EmailConfigurationBean;

@Entity
@Table(name = "email_configurations")
public class EmailConfigurationDTO extends EmailConfigurationBean {

  @ManyToMany(fetch = FetchType.EAGER, cascade = CascadeType.MERGE)
  @JoinTable(name = "email_function_dependencies", joinColumns = {@JoinColumn(name = "email_id")},
      inverseJoinColumns = {@JoinColumn(name = "function_id")})
  private List<AnomalyFunctionDTO> functions = new ArrayList<>();

  public List<AnomalyFunctionDTO> getFunctions() {
    return functions;
  }

  public void setFunctions(List<AnomalyFunctionDTO> functions) {
    this.functions = functions;
  }

}
