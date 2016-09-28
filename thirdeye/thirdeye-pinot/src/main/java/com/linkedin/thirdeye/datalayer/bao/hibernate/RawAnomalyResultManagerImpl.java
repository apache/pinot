package com.linkedin.thirdeye.datalayer.bao.hibernate;

import java.util.List;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

public class RawAnomalyResultManagerImpl extends AbstractManagerImpl<RawAnomalyResultDTO> implements RawAnomalyResultManager {

  private static final String FIND_BY_TIME_AND_FUNCTION_ID =
      "SELECT r FROM RawAnomalyResultDTO r WHERE r.function.id = :functionId "
          + "AND ((r.startTime >= :startTime AND r.startTime <= :endTime) "
          + "OR (r.endTime >= :startTime AND r.endTime <= :endTime))";

  private static final String FIND_UNMERGED_BY_FUNCTION =
      "select r from RawAnomalyResultDTO r where r.function.id = :functionId and r.merged=false "
          + "and r.dataMissing=:dataMissing";

  private static final String FIND_BY_FUNCTION_ID =
      "select r from RawAnomalyResultDTO r where r.function.id = :functionId";

  public RawAnomalyResultManagerImpl() {
    super(RawAnomalyResultDTO.class);
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IRawAnomalyResultManager#findAllByTimeAndFunctionId(long, long, long)
   */
  @Override
  @Transactional
  public List<RawAnomalyResultDTO> findAllByTimeAndFunctionId(long startTime, long endTime,
      long functionId) {
    return getEntityManager().createQuery(FIND_BY_TIME_AND_FUNCTION_ID, entityClass)
        .setParameter("startTime", startTime).setParameter("endTime", endTime)
        .setParameter("functionId", functionId).getResultList();
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IRawAnomalyResultManager#findUnmergedByFunctionId(java.lang.Long)
   */
  @Override
  @Transactional
  public List<RawAnomalyResultDTO> findUnmergedByFunctionId(Long functionId) {
    return getEntityManager().createQuery(FIND_UNMERGED_BY_FUNCTION, entityClass)
        .setParameter("functionId", functionId).setParameter("dataMissing", false).getResultList();
  }

  @Override
  public List<RawAnomalyResultDTO> findByFunctionId(Long functionId) {
    return getEntityManager().createQuery(FIND_BY_FUNCTION_ID, entityClass)
        .setParameter("functionId", functionId).getResultList();
  }
}
