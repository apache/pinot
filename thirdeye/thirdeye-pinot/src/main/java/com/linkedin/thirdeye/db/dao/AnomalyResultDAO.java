package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.api.dto.GroupByKey;
import com.linkedin.thirdeye.api.dto.GroupByRow;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.persistence.TypedQuery;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class AnomalyResultDAO extends AbstractJpaDAO<AnomalyResult> {

  private static final String FIND_BY_COLLECTION_TIME =
      "SELECT r FROM AnomalyResult r WHERE r.function.collection = :collection "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_COLLECTION_TIME_METRIC =
      "SELECT r FROM AnomalyResult r WHERE r.function.collection = :collection AND r.function.metric = :metric "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_COLLECTION_TIME_METRIC_DIMENSION =
      "SELECT r FROM AnomalyResult r WHERE r.function.collection = :collection AND r.function.metric = :metric "
          + "AND r.dimensions IN :dimensions "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc)) ";

  private static final String FIND_BY_COLLECTION_TIME_FILTERS =
      "SELECT r FROM AnomalyResult r WHERE r.function.collection = :collection "
          + "AND ((r.function.filters = :filters) or (r.function.filters is NULL and :filters is NULL)) "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_COLLECTION_TIME_METRIC_FILTERS =
      "SELECT r FROM AnomalyResult r WHERE r.function.collection = :collection AND r.function.metric = :metric "
          + "AND ((r.function.filters = :filters) or (r.function.filters is NULL and :filters is NULL)) "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_TIME_AND_FUNCTION_ID =
      "SELECT r FROM AnomalyResult r WHERE r.function.id = :functionId "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_VALID_BY_TIME_EMAIL_ID =
      "SELECT r FROM EmailConfiguration d JOIN d.functions f JOIN f.anomalies r "
          + "WHERE d.id = :emailId AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc)) "
          + "AND r.dataMissing=:dataMissing";

  private static final String COUNT_GROUP_BY_COLLECTION_METRIC_DIMENSION =
      "select count(r.id) as num, r.function.collection, "
          + "r.function.metric, r.dimensions from AnomalyResult r "
          + "where r.function.isActive=true "
          + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
          + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc)) "
          + "group by r.function.collection, r.function.metric, r.dimensions "
          + "order by r.function.collection, num desc";

  private static final String COUNT_GROUP_BY_FUNCTION = "select count(r.id) as num, r.function.id,"
      + "r.function.functionName, r.function.collection, r.function.metric from AnomalyResult r "
      + "where r.function.isActive=true "
      + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
      + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc))"
      + "group by r.function.id, r.function.functionName, r.function.collection, r.function.metric "
      + "order by r.function.collection, num desc";

  private static final String COUNT_GROUP_BY_COLLECTION_METRIC = "select count(r.id) as num, "
      + "r.function.collection, r.function.metric from AnomalyResult r "
      + "where r.function.isActive=true "
      + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
      + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc))"
      + "group by r.function.collection, r.function.metric  "
      + "order by r.function.collection, num desc";

  private static final String COUNT_GROUP_BY_COLLECTION =
      "select count(r.id) as num, " + "r.function.collection from AnomalyResult r "
          + "where r.function.isActive=true "
          + "and ((r.startTimeUtc >= :startTimeUtc and r.startTimeUtc <= :endTimeUtc) "
          + "or (r.endTimeUtc >= :startTimeUtc and r.endTimeUtc <= :endTimeUtc))"
          + "group by r.function.collection order by r.function.collection, num desc";

  public AnomalyResultDAO() {
    super(AnomalyResult.class);
  }

  public List<AnomalyResult> findAllByCollectionAndTime(String collection, DateTime startTime,
      DateTime endTime) {
    return getEntityManager().createQuery(FIND_BY_COLLECTION_TIME, entityClass)
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .getResultList();
  }

  public List<AnomalyResult> findAllByCollectionTimeAndMetric(String collection, String metric,
      DateTime startTime, DateTime endTime) {
    return getEntityManager().createQuery(FIND_BY_COLLECTION_TIME_METRIC, entityClass)
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("metric", metric).getResultList();
  }

  public List<AnomalyResult> findAllByCollectionTimeMetricAndDimensions(String collection,
      String metric, DateTime startTime, DateTime endTime, String[] dimensions) {
    List<String> dimList = Arrays.asList(dimensions);
    return getEntityManager().createQuery(FIND_BY_COLLECTION_TIME_METRIC_DIMENSION, entityClass)
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("metric", metric).setParameter("dimensions", dimList).getResultList();
  }

  public List<AnomalyResult> findAllByTimeAndFunctionId(long startTime, long endTime,
      long functionId) {
    return getEntityManager().createQuery(FIND_BY_TIME_AND_FUNCTION_ID, entityClass)
        .setParameter("startTimeUtc", startTime).setParameter("endTimeUtc", endTime)
        .setParameter("functionId", functionId).getResultList();
  }

  public List<AnomalyResult> findAllByCollectionTimeAndFilters(String collection,
      DateTime startTime, DateTime endTime, String filters) {
    return getEntityManager().createQuery(FIND_BY_COLLECTION_TIME_FILTERS, entityClass)
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("filters", filters).getResultList();
  }

  public List<AnomalyResult> findAllByCollectionTimeMetricAndFilters(String collection,
      String metric, DateTime startTime, DateTime endTime, String filters) {
    return getEntityManager().createQuery(FIND_BY_COLLECTION_TIME_METRIC_FILTERS, entityClass)
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("metric", metric).setParameter("filters", filters).getResultList();
  }

  public List<AnomalyResult> findValidAllByTimeAndEmailId(DateTime startTime, DateTime endTime,
      long emailId) {
    return getEntityManager().createQuery(FIND_VALID_BY_TIME_EMAIL_ID, entityClass)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("emailId", emailId).setParameter("dataMissing", false).getResultList();
  }

  public List<GroupByRow<GroupByKey, Long>> getCountByCollectionMetricDimension(long startTime, long endTime) {
    List<GroupByRow<GroupByKey, Long>> groupByRecords = new ArrayList<>();
    TypedQuery<Object[]> q = getEntityManager().createQuery(
        COUNT_GROUP_BY_COLLECTION_METRIC_DIMENSION, Object[].class)
        .setParameter("startTimeUtc", startTime).setParameter("endTimeUtc", endTime);
    List<Object[]> results = q.getResultList();
    for (int i = 0; i < results.size(); i++) {
      Long count = (Long) results.get(i)[0];
      GroupByKey groupByKey = new GroupByKey();
      groupByKey.setDataset((String) results.get(i)[1]);
      groupByKey.setMetric((String) results.get(i)[2]);
      groupByKey.setDimensions((String) results.get(i)[3]);
      GroupByRow<GroupByKey, Long> row = new GroupByRow<>();
      row.setGroupBy(groupByKey);
      row.setValue(count);
      groupByRecords.add(row);
    }
    return groupByRecords;
  }

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
      functionKey.setDataset((String) results.get(i)[3]);
      functionKey.setMetric((String) results.get(i)[4]);
      GroupByRow<GroupByKey, Long> row = new GroupByRow<>();
      row.setGroupBy(functionKey);
      row.setValue(count);
      groupByRecords.add(row);
    }
    return groupByRecords;
  }

  public List<GroupByRow<GroupByKey, Long>> getCountByCollection(long startTime, long endTime) {
    List<GroupByRow<GroupByKey, Long>> groupByRecords = new ArrayList<>();
    TypedQuery<Object[]> q =
        getEntityManager().createQuery(COUNT_GROUP_BY_COLLECTION, Object[].class)
            .setParameter("startTimeUtc", startTime).setParameter("endTimeUtc", endTime);
    List<Object[]> results = q.getResultList();
    for (int i = 0; i < results.size(); i++) {
      Long count = (Long) results.get(i)[0];
      GroupByKey groupByKey = new GroupByKey();
      groupByKey.setDataset((String) results.get(i)[1]);
      GroupByRow<GroupByKey, Long> row = new GroupByRow<>();
      row.setGroupBy(groupByKey);
      row.setValue(count);
      groupByRecords.add(row);
    }
    return groupByRecords;
  }

  public List<GroupByRow<GroupByKey, Long>> getCountByCollectionMetric(long startTime,
      long endTime) {
    List<GroupByRow<GroupByKey, Long>> groupByRecords = new ArrayList<>();
    TypedQuery<Object[]> q =
        getEntityManager().createQuery(COUNT_GROUP_BY_COLLECTION_METRIC, Object[].class)
            .setParameter("startTimeUtc", startTime).setParameter("endTimeUtc", endTime);
    List<Object[]> results = q.getResultList();
    for (int i = 0; i < results.size(); i++) {
      Long count = (Long) results.get(i)[0];
      GroupByKey groupByKey = new GroupByKey();
      groupByKey.setDataset((String) results.get(i)[1]);
      groupByKey.setMetric((String) results.get(i)[2]);
      GroupByRow<GroupByKey, Long> row = new GroupByRow<>();
      row.setGroupBy(groupByKey);
      row.setValue(count);
      groupByRecords.add(row);
    }
    return groupByRecords;
  }
}
