package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.List;

import javax.persistence.NoResultException;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;

public class MergedAnomalyResultManagerImpl extends AbstractManagerImpl<MergedAnomalyResultDTO> implements MergedAnomalyResultManager {

  public MergedAnomalyResultManagerImpl() {
    super(MergedAnomalyResultDTO.class, MergedAnomalyResultBean.class);
  }


  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#getAllByTimeEmailIdAndNotifiedFalse(long, long, long)
   */
  @Override
  @Transactional
  public List<MergedAnomalyResultDTO> getAllByTimeEmailIdAndNotifiedFalse(long startTime, long endTime, long emailId) {
//    return getEntityManager().createQuery(FIND_BY_TIME_EMAIL_NOTIFIED_FALSE, entityClass)
//        .setParameter("emailId", emailId).setParameter("startTime", startTime)
//        .setParameter("endTime", endTime).getResultList();
    return null;
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#findByCollectionMetricDimensionsTime(java.lang.String, java.lang.String, java.lang.String[], long, long)
   */
  @Override
  public List<MergedAnomalyResultDTO> findByFunctionId(Long functionId) {
//    return getEntityManager().createQuery(FIND_BY_FUNCTION_ID, entityClass)
//        .setParameter("functionId", functionId).getResultList();
    return null;
  }

  @Transactional
  public List<MergedAnomalyResultDTO> findByCollectionMetricDimensionsTime(String collection,
      String metric, String [] dimensions, long startTime, long endTime) {
//    List<String> dimList = Arrays.asList(dimensions);
//    return getEntityManager()
//        .createQuery(FIND_BY_COLLECTION_METRIC_DIMENSIONS_TIME, entityClass)
//        .setParameter("collection", collection).setParameter("metric", metric)
//        .setParameter("dimensions", dimList).setParameter("startTime", startTime)
//        .setParameter("endTime", endTime).getResultList();
    return null;

  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#findByCollectionMetricTime(java.lang.String, java.lang.String, long, long)
   */
  @Override
  @Transactional
  public List<MergedAnomalyResultDTO> findByCollectionMetricTime(String collection,
      String metric, long startTime, long endTime) {
//    return getEntityManager().createQuery(FIND_BY_COLLECTION_METRIC_TIME, entityClass)
//        .setParameter("collection", collection).setParameter("metric", metric)
//        .setParameter("startTime", startTime).setParameter("endTime", endTime).getResultList();
    return null;

  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#findByCollectionTime(java.lang.String, long, long)
   */
  @Override
  @Transactional
  public List<MergedAnomalyResultDTO> findByCollectionTime(String collection,
      long startTime, long endTime) {
//    return getEntityManager().createQuery(FIND_BY_COLLECTION_TIME, entityClass)
//        .setParameter("collection", collection).setParameter("startTime", startTime)
//        .setParameter("endTime", endTime).getResultList();
    return null;
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IMergedAnomalyResultManager#findLatestByFunctionIdDimensions(java.lang.Long, java.lang.String)
   */
  @Override
  @Transactional
  public MergedAnomalyResultDTO findLatestByFunctionIdDimensions(Long functionId, String dimensions) {
    try {
//      return getEntityManager().createQuery(FIND_BY_FUNCTION_AND_DIMENSIONS, entityClass)
//          .setParameter("functionId", functionId).setParameter("dimensions", dimensions)
//          .setMaxResults(1).getSingleResult();
      return null;
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
//      return getEntityManager().createQuery(FIND_BY_FUNCTION_AND_NULL_DIMENSION, entityClass)
//          .setParameter("functionId", functionId).setMaxResults(1).getSingleResult();
      return null;
    } catch (NoResultException e) {
      return null;
    }
  }
}
