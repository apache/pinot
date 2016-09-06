package com.linkedin.thirdeye.datalayer.bao.hibernate;

import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

public class AnomalyFunctionManagerImpl extends AbstractManagerImpl<AnomalyFunctionDTO>
    implements AnomalyFunctionManager {

  private static final String FIND_DISTINCT_METRIC_BY_COLLECTION =
      "SELECT DISTINCT(af.metric) FROM AnomalyFunctionDTO af WHERE af.collection = :collection";

  public AnomalyFunctionManagerImpl() {
    super(AnomalyFunctionDTO.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.linkedin.thirdeye.datalayer.bao.IAnomalyFunctionManager#findAllByCollection(java.lang.
   * String)
   */
  @Override
  @Transactional
  public List<AnomalyFunctionDTO> findAllByCollection(String collection) {
    return super.findByParams(ImmutableMap.of("collection", collection));
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.linkedin.thirdeye.datalayer.bao.IAnomalyFunctionManager#findDistinctMetricsByCollection(
   * java.lang.String)
   */
  @Override
  @Transactional
  public List<String> findDistinctMetricsByCollection(String collection) {
    return getEntityManager().createQuery(FIND_DISTINCT_METRIC_BY_COLLECTION, String.class)
        .setParameter("collection", collection).getResultList();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.linkedin.thirdeye.datalayer.bao.IAnomalyFunctionManager#findAllActiveFunctions()
   */
  @Override
  @Transactional
  public List<AnomalyFunctionDTO> findAllActiveFunctions() {
    return super.findByParams(ImmutableMap.of("isActive", true));
  }
}
