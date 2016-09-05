package com.linkedin.thirdeye.datalayer.bao;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

import java.util.Arrays;
import java.util.List;
import javax.persistence.NoResultException;

public class MergedAnomalyResultManager extends AbstractManager<MergedAnomalyResultDTO> {

  private static final String FIND_BY_COLLECTION_METRIC_DIMENSIONS_ORDER_BY_END_TIME =
      "from AnomalyMergedResult amr where amr.collection=:collection and amr.metric=:metric "
          + "and amr.dimensions=:dimensions order by amr.endTime desc";

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_METRIC_DIMENSIONS_TIME =
      "from AnomalyMergedResult r where r.collection=:collection and r.metric=:metric "
          + "and r.dimensions in :dimensions "
          + "and (r.startTime < :endTime and r.endTime > :startTime) "
          + "order by r.endTime desc";

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_METRIC_TIME =
      "from AnomalyMergedResult r where r.collection=:collection and r.metric=:metric "
          + "and (r.startTime < :endTime and r.endTime > :startTime) order by r.endTime desc";

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_TIME =
      "from AnomalyMergedResult r where r.collection=:collection "
          + "and (r.startTime < :endTime and r.endTime > :startTime) order by r.endTime desc";

  private static final String FIND_BY_FUNCTION_AND_DIMENSIONS =
      "from AnomalyMergedResult amr where amr.function.id=:functionId "
          + "and amr.dimensions=:dimensions order by amr.endTime desc";

  private static final String FIND_BY_FUNCTION_AND_NULL_DIMENSION =
      "from AnomalyMergedResult amr where amr.function.id=:functionId "
          + "and amr.dimensions is null order by amr.endTime desc";

  private static final String FIND_BY_TIME =
      "from AnomalyMergedResult r WHERE (r.startTime < :endTime and r.endTime > :startTime) order by r.endTime desc ";

  private static final String FIND_BY_TIME_EMAIL_NOTIFIED_FALSE =
      "SELECT r FROM EmailConfiguration d JOIN d.functions f, AnomalyMergedResult r "
          + "WHERE r.function.id=f.id AND d.id = :emailId and r.notified=false "
          + "and (r.startTime < :endTime and r.endTime > :startTime) order by r.endTime desc ";

  public MergedAnomalyResultManager() {
    super(MergedAnomalyResultDTO.class);
  }

  @Transactional
  public List<MergedAnomalyResultDTO> getAllByTime(long startTime, long endTime) {
    return getEntityManager().createQuery(FIND_BY_TIME, entityClass)
        .setParameter("startTime", startTime).setParameter("endTime", endTime).getResultList();
  }

  @Transactional
  public List<MergedAnomalyResultDTO> getAllByTimeEmailIdAndNotifiedFalse(long startTime, long endTime, long emailId) {
    return getEntityManager().createQuery(FIND_BY_TIME_EMAIL_NOTIFIED_FALSE, entityClass)
        .setParameter("emailId", emailId).setParameter("startTime", startTime)
        .setParameter("endTime", endTime).getResultList();
  }

  @Transactional
  public List<MergedAnomalyResultDTO> findByCollectionMetricDimensionsTime(String collection,
      String metric, String [] dimensions, long startTime, long endTime) {
    List<String> dimList = Arrays.asList(dimensions);
    return getEntityManager()
        .createQuery(FIND_BY_COLLECTION_METRIC_DIMENSIONS_TIME, entityClass)
        .setParameter("collection", collection).setParameter("metric", metric)
        .setParameter("dimensions", dimList).setParameter("startTime", startTime)
        .setParameter("endTime", endTime).getResultList();
  }

  @Transactional
  public List<MergedAnomalyResultDTO> findByCollectionMetricTime(String collection,
      String metric, long startTime, long endTime) {
    return getEntityManager().createQuery(FIND_BY_COLLECTION_METRIC_TIME, entityClass)
        .setParameter("collection", collection).setParameter("metric", metric)
        .setParameter("startTime", startTime).setParameter("endTime", endTime).getResultList();
  }

  @Transactional
  public List<MergedAnomalyResultDTO> findByCollectionTime(String collection,
      long startTime, long endTime) {
    return getEntityManager().createQuery(FIND_BY_COLLECTION_TIME, entityClass)
        .setParameter("collection", collection).setParameter("startTime", startTime)
        .setParameter("endTime", endTime).getResultList();
  }

  @Transactional
  public MergedAnomalyResultDTO findLatestByCollectionMetricDimensions(
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
  public MergedAnomalyResultDTO findLatestByFunctionIdDimensions(Long functionId, String dimensions) {
    try {
      return getEntityManager().createQuery(FIND_BY_FUNCTION_AND_DIMENSIONS, entityClass)
          .setParameter("functionId", functionId).setParameter("dimensions", dimensions)
          .setMaxResults(1).getSingleResult();
    } catch (NoResultException e) {
      return null;
    }
  }

  @Transactional
  public MergedAnomalyResultDTO findLatestByFunctionIdOnly(Long functionId) {
    try {
      return getEntityManager().createQuery(FIND_BY_FUNCTION_AND_NULL_DIMENSION, entityClass)
          .setParameter("functionId", functionId).setMaxResults(1).getSingleResult();
    } catch (NoResultException e) {
      return null;
    }
  }
}
