package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFunction;
import java.util.List;

public class AnomalyFunctionManager extends AbstractManager<AnomalyFunctionDTO, AnomalyFunction> {

  public List<AnomalyFunctionDTO> findAllByCollection(String collection) {
    return null;
  }

  public List<String> findDistinctMetricsByCollection(String collection) {
    return null;
  }

  public List<AnomalyFunctionDTO> findAllActiveFunctions() {
    return null;
  }
}
