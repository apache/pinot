package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;


public interface AnomalyFunctionManager extends AbstractManager<AnomalyFunctionDTO> {

  List<AnomalyFunctionDTO> findAllByCollection(String collection);

  /**
   * Get the list of anomaly functions under the given application
   * @param application name of the application
   * @return return the list of anomaly functions under the application
   */
  List<AnomalyFunctionDTO> findAllByApplication(String application);

  List<String> findDistinctTopicMetricsByCollection(String collection);

  List<AnomalyFunctionDTO> findAllActiveFunctions();

  List<AnomalyFunctionDTO> findWhereNameLike(String name);

  AnomalyFunctionDTO findWhereNameEquals(String name);

}
