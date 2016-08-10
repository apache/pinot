package com.linkedin.thirdeye.db.dao;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnomalyFunctionDAO extends AbstractJpaDAO<AnomalyFunctionSpec> {

  private static final String FIND_DISTINCT_METRIC_BY_COLLECTION =
      "SELECT DISTINCT(af.metric) FROM AnomalyFunctionSpec af WHERE af.collection = :collection";

  public AnomalyFunctionDAO() {
    super(AnomalyFunctionSpec.class);
  }

  @Transactional
  public AnomalyFunctionSpec findById(Long id) {
    return super.findById(id);
  }

  @Transactional
  public List<AnomalyFunctionSpec> findAllByCollection(String collection) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    return super.findByParams(filterParams);
  }

  public List<String> findDistinctMetricsByCollection(String collection) {
    return getEntityManager().createQuery(FIND_DISTINCT_METRIC_BY_COLLECTION, String.class)
        .setParameter("collection", collection).getResultList();
  }
}
