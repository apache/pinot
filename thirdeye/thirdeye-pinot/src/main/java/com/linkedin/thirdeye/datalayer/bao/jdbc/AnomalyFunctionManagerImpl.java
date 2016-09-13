package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class AnomalyFunctionManagerImpl extends AbstractManagerImpl<AnomalyFunctionDTO>
    implements AnomalyFunctionManager {

  private static final String FIND_DISTINCT_METRIC_BY_COLLECTION =
      "SELECT DISTINCT(af.metric) FROM AnomalyFunctionDTO af WHERE af.collection = :collection";

  public AnomalyFunctionManagerImpl() {
    super(AnomalyFunctionDTO.class, AnomalyFunctionBean.class);
  }

  @Override
  @Transactional
  public List<AnomalyFunctionDTO> findAllByCollection(String collection) {
    // return super.findByParams(ImmutableMap.of("collection", collection));
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
  @Transactional
  public List<String> findDistinctMetricsByCollection(String collection) {
    // return
    // getEntityManager().createQuery(FIND_DISTINCT_METRIC_BY_COLLECTION,
    // String.class)
    // .setParameter("collection", collection).getResultList();
    Predicate predicate = Predicate.EQ("collection", collection);
    List<AnomalyFunctionBean> list = genericPojoDao.get(predicate, AnomalyFunctionBean.class);
    Set<String> metrics = new HashSet<>();
    for (AnomalyFunctionBean anomalyFunctionBean : list) {
      metrics.add(anomalyFunctionBean.getMetric());
    }
    return new ArrayList<>(metrics);
  }

  /*
   * (non-Javadoc)
   *
   * @see com.linkedin.thirdeye.datalayer.bao.IAnomalyFunctionManager# findAllActiveFunctions()
   */
  @Override
  @Transactional
  public List<AnomalyFunctionDTO> findAllActiveFunctions() {
    // return super.findByParams(ImmutableMap.of("isActive", true));
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
