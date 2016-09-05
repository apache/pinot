package com.linkedin.thirdeye.datalayer.bao;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.api.dto.GroupByKey;
import com.linkedin.thirdeye.api.dto.GroupByRow;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

import java.util.ArrayList;
import java.util.List;
import javax.persistence.TypedQuery;

public class RawAnomalyResultManager extends AbstractManager<RawAnomalyResultDTO> {

  private static final String FIND_BY_TIME_AND_FUNCTION_ID =
      "SELECT r FROM AnomalyResult r WHERE r.function.id = :functionId "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_TIME_FUNCTION_ID_DIMENSIONS =
      "SELECT r FROM AnomalyResult r WHERE r.function.id = :functionId and r.dimensions = :dimensions "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String COUNT_GROUP_BY_FUNCTION = "select count(r.id) as num, r.function.id,"
      + "r.function.functionName, r.function.collection, r.function.metric from AnomalyResult r "
      + "where r.function.isActive=true "
      + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
      + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc))"
      + "group by r.function.id, r.function.functionName, r.function.collection, r.function.metric "
      + "order by r.function.collection, num desc";

  private static final String COUNT_GROUP_BY_FUNCTION_DIMENSIONS = "select count(r.id) as num, r.function.id,"
      + "r.function.functionName, r.function.collection, r.function.metric, r.dimensions from AnomalyResult r "
      + "where r.function.isActive=true "
      + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
      + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc))"
      + "group by r.function.id, r.function.functionName, r.function.collection, r.function.metric, r.dimensions "
      + "order by r.function.collection, num desc";

  private static final String FIND_UNMERGED_BY_COLLECTION_METRIC_DIMENSION =
      "from AnomalyResult r where r.function.collection = :collection and r.function.metric = :metric "
          + "and r.dimensions=:dimensions and r.merged=false and r.dataMissing=:dataMissing";

  private static final String FIND_UNMERGED_BY_FUNCTION =
      "select r from AnomalyResult r where r.function.id = :functionId and r.merged=false "
          + "and r.dataMissing=:dataMissing";

  public RawAnomalyResultManager() {
    super(RawAnomalyResultDTO.class);
  }

  @Transactional
  public List<RawAnomalyResultDTO> findAllByTimeAndFunctionId(long startTime, long endTime,
      long functionId) {
    return getEntityManager().createQuery(FIND_BY_TIME_AND_FUNCTION_ID, entityClass)
        .setParameter("startTimeUtc", startTime).setParameter("endTimeUtc", endTime)
        .setParameter("functionId", functionId).getResultList();
  }

  @Transactional
  public List<RawAnomalyResultDTO> findAllByTimeFunctionIdAndDimensions(long startTime, long endTime,
      long functionId, String dimensions) {
    return getEntityManager().createQuery(FIND_BY_TIME_FUNCTION_ID_DIMENSIONS, entityClass)
        .setParameter("startTimeUtc", startTime).setParameter("endTimeUtc", endTime)
        .setParameter("functionId", functionId).setParameter("dimensions", dimensions)
        .getResultList();
  }

  @Transactional
  public List<GroupByRow<GroupByKey, Long>> getCountByFunction(long startTime, long endTime) {
    List<GroupByRow<GroupByKey, Long>> groupByRecords = new ArrayList<>();
    TypedQuery<Object[]> q = getEntityManager().createQuery(COUNT_GROUP_BY_FUNCTION, Object[].class)
        .setParameter("startTimeUtc", startTime).setParameter("endTimeUtc", endTime);
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

  @Transactional
  public List<GroupByRow<GroupByKey, Long>> getCountByFunctionDimensions(long startTime, long endTime) {
    List<GroupByRow<GroupByKey, Long>> groupByRecords = new ArrayList<>();
    TypedQuery<Object[]> q = getEntityManager().createQuery(COUNT_GROUP_BY_FUNCTION_DIMENSIONS, Object[].class)
        .setParameter("startTimeUtc", startTime).setParameter("endTimeUtc", endTime);
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

  @Transactional
  public List<RawAnomalyResultDTO> findUnmergedByFunctionId(Long functionId) {
    return getEntityManager().createQuery(FIND_UNMERGED_BY_FUNCTION, entityClass)
        .setParameter("functionId", functionId).setParameter("dataMissing", false).getResultList();
  }

  @Transactional
  public List<RawAnomalyResultDTO> findUnmergedByCollectionMetricAndDimensions(String collection,
      String metric, String dimensions) {
    return getEntityManager().createQuery(FIND_UNMERGED_BY_COLLECTION_METRIC_DIMENSION, entityClass)
        .setParameter("collection", collection).setParameter("metric", metric)
        .setParameter("dimensions", dimensions).setParameter("dataMissing", false).getResultList();
  }
}
