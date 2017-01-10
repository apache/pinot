package com.linkedin.thirdeye.datalayer.dto;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.datalayer.pojo.EmailConfigurationBean;

@Deprecated
public class EmailConfigurationDTO extends EmailConfigurationBean {

  private List<AnomalyFunctionDTO> functions = new ArrayList<>();

  public List<AnomalyFunctionDTO> getFunctions() {
    return functions;
  }

  public void setFunctions(List<AnomalyFunctionDTO> functions) {
    this.functions = functions;
  }

}
