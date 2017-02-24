package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class AnomalyFunctionManagerImpl extends AbstractManagerImpl<AnomalyFunctionDTO>
    implements AnomalyFunctionManager {

  public AnomalyFunctionManagerImpl() {
    super(AnomalyFunctionDTO.class, AnomalyFunctionBean.class);
  }

  @Override
  public List<AnomalyFunctionDTO> findAllByCollection(String collection) {
    Predicate predicate = Predicate.EQ("collection", collection);
    List<AnomalyFunctionBean> list = genericPojoDao.get(predicate, AnomalyFunctionBean.class);
    List<AnomalyFunctionDTO> result = new ArrayList<>();
    for (AnomalyFunctionBean abstractBean : list) {
      AnomalyFunctionDTO dto = MODEL_MAPPER.map(abstractBean, AnomalyFunctionDTO.class);
      result.add(dto);
    }
    return result;
  }

  @Override
  public List<String> findDistinctTopicMetricsByCollection(String collection) {
    Predicate predicate = Predicate.EQ("collection", collection);
    List<AnomalyFunctionBean> list = genericPojoDao.get(predicate, AnomalyFunctionBean.class);
    Set<String> metrics = new HashSet<>();
    for (AnomalyFunctionBean anomalyFunctionBean : list) {
      AnomalyFunctionDTO dto = MODEL_MAPPER.map(anomalyFunctionBean, AnomalyFunctionDTO.class);
      metrics.add(dto.getTopicMetric());
    }
    return new ArrayList<>(metrics);
  }

  /*
   * (non-Javadoc)
   *
   * @see com.linkedin.thirdeye.datalayer.bao.IAnomalyFunctionManager# findAllActiveFunctions()
   */
  @Override
  public List<AnomalyFunctionDTO> findAllActiveFunctions() {
    Predicate predicate = Predicate.EQ("active", true);
    List<AnomalyFunctionBean> list = genericPojoDao.get(predicate, AnomalyFunctionBean.class);
    List<AnomalyFunctionDTO> result = new ArrayList<>();
    for (AnomalyFunctionBean abstractBean : list) {
      AnomalyFunctionDTO dto = MODEL_MAPPER.map(abstractBean, AnomalyFunctionDTO.class);
      result.add(dto);
    }
    return result;
  }

}
