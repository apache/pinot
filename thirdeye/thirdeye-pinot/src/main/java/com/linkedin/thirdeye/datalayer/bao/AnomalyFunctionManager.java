package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFunction;
import java.util.List;

public class AnomalyFunctionManager extends AbstractManager<AnomalyFunctionDTO, AnomalyFunction> {

  // TODO: inject this
  private AnomalyFunctionDAO anomalyFunctionDAO;

  public AnomalyFunctionManager(AnomalyFunctionDAO functionDAO) {
    this.anomalyFunctionDAO = functionDAO;
  }

  public List<AnomalyFunctionDTO> findAllByCollection(String collection) {
    return null;
  }

  public List<String> findDistinctMetricsByCollection(String collection) {
    return null;
  }

  public List<AnomalyFunctionDTO> findAllActiveFunctions() {
    return null;
  }

  @Override
  protected AnomalyFunctionDTO covertToDTO(AnomalyFunction e, AnomalyFunctionDTO d) {
    super.covertToDTO(e, d);
    // // TODO: 9/2/16 populate dto here
    return d;
  }

  @Override
  protected AnomalyFunction covertToEntity(AnomalyFunctionDTO d, AnomalyFunction e) {
    super.covertToEntity(d, e);
    // TODO : populate entitiy here
    return e;
  }
}
