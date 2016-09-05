package com.linkedin.thirdeye.datalayer.bao;

import com.google.common.collect.ImmutableMap;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

import java.util.List;

public class AnomalyFunctionManager extends AbstractManager<AnomalyFunctionDTO> {

  private static final String FIND_DISTINCT_METRIC_BY_COLLECTION =
      "SELECT DISTINCT(af.metric) FROM AnomalyFunctionSpec af WHERE af.collection = :collection";

  public AnomalyFunctionManager() {
    super(AnomalyFunctionDTO.class);
  }

  @Transactional
  public List<AnomalyFunctionDTO> findAllByCollection(String collection) {
    return super.findByParams(ImmutableMap.of("collection", collection));
  }

  @Transactional
  public List<String> findDistinctMetricsByCollection(String collection) {
    return getEntityManager().createQuery(FIND_DISTINCT_METRIC_BY_COLLECTION, String.class)
        .setParameter("collection", collection).getResultList();
  }

  @Transactional
  public List<AnomalyFunctionDTO> findAllActiveFunctions() {
    return super.findByParams(ImmutableMap.of("isActive", true));
  }
}
