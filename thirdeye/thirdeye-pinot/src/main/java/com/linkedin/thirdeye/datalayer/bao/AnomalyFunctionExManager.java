package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;
import java.util.List;


public interface AnomalyFunctionExManager extends AbstractManager<AnomalyFunctionExDTO> {

  List<AnomalyFunctionExDTO> findByActive(boolean active);

  List<AnomalyFunctionExDTO> findByName(String name);

  List<AnomalyFunctionExDTO> findByClassName(String className);

}
