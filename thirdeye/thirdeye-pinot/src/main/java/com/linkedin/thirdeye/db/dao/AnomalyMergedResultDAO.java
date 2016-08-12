package com.linkedin.thirdeye.db.dao;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.NoResultException;

public class AnomalyMergedResultDAO extends AbstractJpaDAO<AnomalyMergedResult> {

  private static final String FIND_BY_COLLECTION_METRIC_DIMENSIONS_ORDER_BY_END_TIME =
      "from AnomalyMergedResult amr where amr.collection=:collection and amr.metric=:metric "
          + "and amr.dimensions=:dimensions order by amr.endTime desc";

  private static final String FIND_BY_FUNCTION_DIMENSIONS =
      "from AnomalyMergedResult amr where amr.function.id=:functionId "
          + "and amr.dimensions=:dimensions order by amr.endTime desc";

  private static final String FIND_BY_FUNCTION_ONLY =
      "from AnomalyMergedResult amr where amr.function.id=:functionId "
          + "and amr.dimensions is null order by amr.endTime desc";

  public AnomalyMergedResultDAO() {
    super(AnomalyMergedResult.class);
  }

  @Transactional
  public List<AnomalyMergedResult> findByCollectionMetricDimensions(String collection,
      String metric, String dimensions) {
    Map<String, Object> params = new HashMap<>();
    params.put("collection", collection);
    params.put("metric", metric);
    params.put("dimensions", dimensions);
    return super.findByParams(params);
  }

  @Transactional
  public AnomalyMergedResult findLatestByCollectionMetricDimensions(
      String collection, String metric, String dimensions) {
    try {
      return getEntityManager()
          .createQuery(FIND_BY_COLLECTION_METRIC_DIMENSIONS_ORDER_BY_END_TIME, entityClass)
          .setParameter("collection", collection).setParameter("metric", metric)
          .setParameter("dimensions", dimensions).setMaxResults(1).getSingleResult();
    } catch (NoResultException e) {
      return null;
    }
  }

  @Transactional
  public AnomalyMergedResult findLatestByFunctionIdDimensions(Long functionId, String dimensions) {
    try {
      return getEntityManager().createQuery(FIND_BY_FUNCTION_DIMENSIONS, entityClass)
          .setParameter("functionId", functionId).setParameter("dimensions", dimensions)
          .setMaxResults(1).getSingleResult();
    } catch (NoResultException e) {
      return null;
    }
  }

  @Transactional
  public AnomalyMergedResult findLatestByFunctionIdOnly(Long functionId) {
    try {
      return getEntityManager().createQuery(FIND_BY_FUNCTION_ONLY, entityClass)
          .setParameter("functionId", functionId).setMaxResults(1).getSingleResult();
    } catch (NoResultException e) {
      return null;
    }
  }
}
