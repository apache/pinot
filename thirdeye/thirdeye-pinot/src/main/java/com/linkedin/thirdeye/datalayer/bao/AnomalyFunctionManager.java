package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;


public interface AnomalyFunctionManager extends AbstractManager<AnomalyFunctionDTO> {

  List<AnomalyFunctionDTO> findAllByCollection(String collection);

  List<String> findDistinctTopicMetricsByCollection(String collection);

  List<AnomalyFunctionDTO> findAllActiveFunctions();

  List<AnomalyFunctionDTO> findWhereNameLike(String name);

  AnomalyFunctionDTO findWhereNameEquals(String name);

}
