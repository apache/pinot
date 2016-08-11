package com.linkedin.thirdeye.db.dao;

import com.google.common.collect.ImmutableMap;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;
import java.util.List;

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
    return super.findByParams(
        ImmutableMap.of("collection", collection, "metric", metric, "dimensions", dimensions));
  }

  @Transactional
  public AnomalyMergedResult findLatestByCollectionMetricDimensions(
      String collection, String metric, String dimensions) {
    return getEntityManager()
        .createQuery(FIND_BY_COLLECTION_METRIC_DIMENSIONS_ORDER_BY_END_TIME, entityClass)
        .setParameter("collection", collection).setParameter("metric", metric)
        .setParameter("dimensions", dimensions).setMaxResults(1).getSingleResult();
  }

  @Transactional
  public AnomalyMergedResult findLatestByFunctionIdDimensions(Long functionId, String dimensions) {
    return getEntityManager().createQuery(FIND_BY_FUNCTION_DIMENSIONS, entityClass)
        .setParameter("functionId", functionId).setParameter("dimensions", dimensions)
        .setMaxResults(1).getSingleResult();
  }

  @Transactional
  public AnomalyMergedResult findLatestByFunctionIdOnly(Long functionId) {
    return getEntityManager().createQuery(FIND_BY_FUNCTION_ONLY, entityClass)
        .setParameter("functionId", functionId).setMaxResults(1).getSingleResult();
  }
}
