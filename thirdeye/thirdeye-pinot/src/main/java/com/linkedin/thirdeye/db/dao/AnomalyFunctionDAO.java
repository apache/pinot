package com.linkedin.thirdeye.db.dao;

import com.google.common.collect.ImmutableMap;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;

import java.util.List;

public class AnomalyFunctionDAO extends AbstractJpaDAO<AnomalyFunctionSpec> {

  private static final String FIND_DISTINCT_METRIC_BY_COLLECTION =
      "SELECT DISTINCT(af.metric) FROM AnomalyFunctionSpec af WHERE af.collection = :collection";

  public AnomalyFunctionDAO() {
    super(AnomalyFunctionSpec.class);
  }

  @Transactional
  public List<AnomalyFunctionSpec> findAllByCollection(String collection) {
    return super.findByParams(ImmutableMap.of("collection", collection));
  }

  @Transactional
  public List<String> findDistinctMetricsByCollection(String collection) {
    return getEntityManager().createQuery(FIND_DISTINCT_METRIC_BY_COLLECTION, String.class)
        .setParameter("collection", collection).getResultList();
  }

  @Transactional
  public List<AnomalyFunctionSpec> findAllActiveFunctions() {
    return super.findByParams(ImmutableMap.of("isActive", true));
  }
}
