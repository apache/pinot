package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;


public interface AnomalyFunctionManager extends AbstractManager<AnomalyFunctionDTO> {

  List<AnomalyFunctionDTO> findAllByCollection(String collection);

  List<String> findDistinctMetricsByCollection(String collection);

  List<AnomalyFunctionDTO> findAllActiveFunctions();

}
