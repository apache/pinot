package com.linkedin.thirdeye.datalayer.bao.hibernate;

import java.util.Arrays;
import java.util.List;

import javax.persistence.NoResultException;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public class MergedAnomalyResultManagerImpl extends AbstractManagerImpl<MergedAnomalyResultDTO> implements MergedAnomalyResultManager {

  private static final String FIND_BY_COLLECTION_METRIC_DIMENSIONS_ORDER_BY_END_TIME =
      "from MergedAnomalyResultDTO amr where amr.collection=:collection and amr.metric=:metric "
          + "and amr.dimensions=:dimensions order by amr.endTime desc";

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_METRIC_DIMENSIONS_TIME =
      "from MergedAnomalyResultDTO r where r.collection=:collection and r.metric=:metric "
          + "and r.dimensions in :dimensions "
          + "and (r.startTime < :endTime and r.endTime > :startTime) "
          + "order by r.endTime desc";

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_METRIC_TIME =
      "from MergedAnomalyResultDTO r where r.collection=:collection and r.metric=:metric "
          + "and (r.startTime < :endTime and r.endTime > :startTime) order by r.endTime desc";

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_TIME =
      "from MergedAnomalyResultDTO r where r.collection=:collection "
          + "and (r.startTime < :endTime and r.endTime > :startTime) order by r.endTime desc";

  private static final String FIND_BY_FUNCTION_AND_DIMENSIONS =
      "from MergedAnomalyResultDTO amr where amr.function.id=:functionId "
          + "and amr.dimensions=:dimensions order by amr.endTime desc";

  private static final String FIND_BY_FUNCTION_AND_NULL_DIMENSION =
      "from MergedAnomalyResultDTO amr where amr.function.id=:functionId "
          + "and amr.dimensions is null order by amr.endTime desc";

  private static final String FIND_BY_TIME =
      "from MergedAnomalyResultDTO r WHERE (r.startTime < :endTime and r.endTime > :startTime) order by r.endTime desc ";

  private static final String FIND_BY_TIME_EMAIL_NOTIFIED_FALSE =
      "SELECT r FROM EmailConfiguration d JOIN d.functions f, MergedAnomalyResultDTO r "
          + "WHERE r.function.id=f.id AND d.id = :emailId and r.notified=false "
          + "and (r.startTime < :endTime and r.endTime > :startTime) order by r.endTime desc ";

  public MergedAnomalyResultManagerImpl() {
    super(MergedAnomalyResultDTO.class);
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#getAllByTime(long, long)
   */
  @Override
  @Transactional
  public List<MergedAnomalyResultDTO> getAllByTime(long startTime, long endTime) {
    return getEntityManager().createQuery(FIND_BY_TIME, entityClass)
        .setParameter("startTime", startTime).setParameter("endTime", endTime).getResultList();
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#getAllByTimeEmailIdAndNotifiedFalse(long, long, long)
   */
  @Override
  @Transactional
  public List<MergedAnomalyResultDTO> getAllByTimeEmailIdAndNotifiedFalse(long startTime, long endTime, long emailId) {
    return getEntityManager().createQuery(FIND_BY_TIME_EMAIL_NOTIFIED_FALSE, entityClass)
        .setParameter("emailId", emailId).setParameter("startTime", startTime)
        .setParameter("endTime", endTime).getResultList();
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#findByCollectionMetricDimensionsTime(java.lang.String, java.lang.String, java.lang.String[], long, long)
   */
  @Override
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

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#findByCollectionMetricTime(java.lang.String, java.lang.String, long, long)
   */
  @Override
  @Transactional
  public List<MergedAnomalyResultDTO> findByCollectionMetricTime(String collection,
      String metric, long startTime, long endTime) {
    return getEntityManager().createQuery(FIND_BY_COLLECTION_METRIC_TIME, entityClass)
        .setParameter("collection", collection).setParameter("metric", metric)
        .setParameter("startTime", startTime).setParameter("endTime", endTime).getResultList();
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#findByCollectionTime(java.lang.String, long, long)
   */
  @Override
  @Transactional
  public List<MergedAnomalyResultDTO> findByCollectionTime(String collection,
      long startTime, long endTime) {
    return getEntityManager().createQuery(FIND_BY_COLLECTION_TIME, entityClass)
        .setParameter("collection", collection).setParameter("startTime", startTime)
        .setParameter("endTime", endTime).getResultList();
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#findLatestByCollectionMetricDimensions(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
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

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#findLatestByFunctionIdDimensions(java.lang.Long, java.lang.String)
   */
  @Override
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

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#findLatestByFunctionIdOnly(java.lang.Long)
   */
  @Override
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
