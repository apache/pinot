package com.linkedin.thirdeye.datalayer.bao.hibernate;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.TypedQuery;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.api.dto.GroupByKey;
import com.linkedin.thirdeye.api.dto.GroupByRow;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

public class RawAnomalyResultManagerImpl extends AbstractManagerImpl<RawAnomalyResultDTO> implements RawAnomalyResultManager {

  private static final String FIND_BY_TIME_AND_FUNCTION_ID =
      "SELECT r FROM RawAnomalyResultDTO r WHERE r.function.id = :functionId "
          + "AND ((r.startTime >= :startTime AND r.startTime <= :endTime) "
          + "OR (r.endTime >= :startTime AND r.endTime <= :endTime))";

  private static final String FIND_BY_TIME_FUNCTION_ID_DIMENSIONS =
      "SELECT r FROM RawAnomalyResultDTO r WHERE r.function.id = :functionId and r.dimensions = :dimensions "
          + "AND ((r.startTime >= :startTime AND r.startTime <= :endTime) "
          + "OR (r.endTime >= :startTime AND r.endTime <= :endTime))";

  private static final String COUNT_GROUP_BY_FUNCTION = "select count(r.id) as num, r.function.id,"
      + "r.function.functionName, r.function.collection, r.function.metric from RawAnomalyResultDTO r "
      + "where r.function.isActive=true "
      + "and ((r.startTime >= :startTime and r.startTime <= :endTime) "
      + "or (r.endTime >= :startTime and r.endTime <= :endTime))"
      + "group by r.function.id, r.function.functionName, r.function.collection, r.function.metric "
      + "order by r.function.collection, num desc";

  private static final String COUNT_GROUP_BY_FUNCTION_DIMENSIONS = "select count(r.id) as num, r.function.id,"
      + "r.function.functionName, r.function.collection, r.function.metric, r.dimensions from RawAnomalyResultDTO r "
      + "where r.function.isActive=true "
      + "and ((r.startTime >= :startTime and r.startTime <= :endTime) "
      + "or (r.endTime >= :startTime and r.endTime <= :endTime))"
      + "group by r.function.id, r.function.functionName, r.function.collection, r.function.metric, r.dimensions "
      + "order by r.function.collection, num desc";

  private static final String FIND_UNMERGED_BY_COLLECTION_METRIC_DIMENSION =
      "from RawAnomalyResultDTO r where r.function.collection = :collection and r.function.metric = :metric "
          + "and r.dimensions=:dimensions and r.merged=false and r.dataMissing=:dataMissing";

  private static final String FIND_UNMERGED_BY_FUNCTION =
      "select r from RawAnomalyResultDTO r where r.function.id = :functionId and r.merged=false "
          + "and r.dataMissing=:dataMissing";

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
   * @see com.linkedin.thirdeye.datalayer.bao.IRawAnomalyResultManager#findAllByTimeFunctionIdAndDimensions(long, long, long, java.lang.String)
   */
  @Override
  @Transactional
  public List<RawAnomalyResultDTO> findAllByTimeFunctionIdAndDimensions(long startTime, long endTime,
      long functionId, String dimensions) {
    return getEntityManager().createQuery(FIND_BY_TIME_FUNCTION_ID_DIMENSIONS, entityClass)
        .setParameter("startTime", startTime).setParameter("endTime", endTime)
        .setParameter("functionId", functionId).setParameter("dimensions", dimensions)
        .getResultList();
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IRawAnomalyResultManager#getCountByFunction(long, long)
   */
  @Override
  @Transactional
  public List<GroupByRow<GroupByKey, Long>> getCountByFunction(long startTime, long endTime) {
    List<GroupByRow<GroupByKey, Long>> groupByRecords = new ArrayList<>();
    TypedQuery<Object[]> q = getEntityManager().createQuery(COUNT_GROUP_BY_FUNCTION, Object[].class)
        .setParameter("startTime", startTime).setParameter("endTime", endTime);
    List<Object[]> results = q.getResultList();
    for (int i = 0; i < results.size(); i++) {
      Long count = (Long) results.get(i)[0];
      GroupByKey functionKey = new GroupByKey();
      functionKey.setFunctionId((Long) results.get(i)[1]);
      functionKey.setFunctionName((String) results.get(i)[2]);
      functionKey.setCollection((String) results.get(i)[3]);
      functionKey.setMetric((String) results.get(i)[4]);
      GroupByRow<GroupByKey, Long> row = new GroupByRow<>();
      row.setGroupBy(functionKey);
      row.setValue(count);
      groupByRecords.add(row);
    }
    return groupByRecords;
  }

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IRawAnomalyResultManager#getCountByFunctionDimensions(long, long)
   */
  @Override
  @Transactional
  public List<GroupByRow<GroupByKey, Long>> getCountByFunctionDimensions(long startTime, long endTime) {
    List<GroupByRow<GroupByKey, Long>> groupByRecords = new ArrayList<>();
    TypedQuery<Object[]> q = getEntityManager().createQuery(COUNT_GROUP_BY_FUNCTION_DIMENSIONS, Object[].class)
        .setParameter("startTime", startTime).setParameter("endTime", endTime);
    List<Object[]> results = q.getResultList();
    for (int i = 0; i < results.size(); i++) {
      Long count = (Long) results.get(i)[0];
      GroupByKey functionKey = new GroupByKey();
      functionKey.setFunctionId((Long) results.get(i)[1]);
      functionKey.setFunctionName((String) results.get(i)[2]);
      functionKey.setCollection((String) results.get(i)[3]);
      functionKey.setMetric((String) results.get(i)[4]);
      functionKey.setDimensions((String)results.get(i)[5]);
      GroupByRow<GroupByKey, Long> row = new GroupByRow<>();
      row.setGroupBy(functionKey);
      row.setValue(count);
      groupByRecords.add(row);
    }
    return groupByRecords;
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

  /* (non-Javadoc)
   * @see com.linkedin.thirdeye.datalayer.bao.IRawAnomalyResultManager#findUnmergedByCollectionMetricAndDimensions(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  @Transactional
  public List<RawAnomalyResultDTO> findUnmergedByCollectionMetricAndDimensions(String collection,
      String metric, String dimensions) {
    return getEntityManager().createQuery(FIND_UNMERGED_BY_COLLECTION_METRIC_DIMENSION, entityClass)
        .setParameter("collection", collection).setParameter("metric", metric)
        .setParameter("dimensions", dimensions).setParameter("dataMissing", false).getResultList();
  }
}
