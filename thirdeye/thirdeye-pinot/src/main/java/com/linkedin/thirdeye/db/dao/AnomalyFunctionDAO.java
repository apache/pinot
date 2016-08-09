package com.linkedin.thirdeye.db.dao;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;

import java.util.List;

import javax.persistence.NoResultException;

public class AnomalyFunctionDAO extends AbstractJpaDAO<AnomalyFunctionSpec> {

  private static final String GET_BY_COLLECTION =
      "SELECT af FROM AnomalyFunctionSpec af WHERE af.collection = :collection";

  private static final String FIND_DISTINCT_METRIC_BY_COLLECTION =
      "SELECT DISTINCT(af.metric) FROM AnomalyFunctionSpec af WHERE af.collection = :collection";

  private static final String FIND_BY_ID =
      "SELECT af FROM AnomalyFunctionSpec af WHERE af.id = :id";

  public AnomalyFunctionDAO() {
    super(AnomalyFunctionSpec.class);
  }

  @Transactional
  public AnomalyFunctionSpec findById(Long id) {
    try {
      return getEntityManager().createQuery(FIND_BY_ID, AnomalyFunctionSpec.class)
          .setParameter("id", id).getSingleResult();
    } catch (NoResultException e) {
      return null;
    }
  }

  @Transactional
  public List<AnomalyFunctionSpec> findAllByCollection(String collection) {
    return getEntityManager().createQuery(GET_BY_COLLECTION, entityClass)
        .setParameter("collection", collection).getResultList();
  }

  public List<String> findDistinctMetricsByCollection(String collection) {
    return getEntityManager().createQuery(FIND_DISTINCT_METRIC_BY_COLLECTION, String.class)
        .setParameter("collection", collection).getResultList();
  }
}
