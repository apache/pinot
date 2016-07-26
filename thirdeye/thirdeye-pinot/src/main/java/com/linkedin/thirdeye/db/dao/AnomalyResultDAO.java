package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.db.entity.AnomalyResult;
import java.util.List;
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

  private static final String FIND_BY_TIME_AND_FUNCTION_ID = "SELECT r FROM AnomalyResult r WHERE r.function.id = :functionId "
      + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
      + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_COLLECTION_TIME_AND_FUNCTION_ID = "SELECT r FROM AnomalyResult r WHERE r.function.collection = :collection "
      + "AND r.function.id = :functionId "
      + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
      + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_TIME_EMAIL_ID = "SELECT r FROM EmailConfiguration d JOIN d.functions f JOIN f.anomalies r "
      + "WHERE d.id = :emailId AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
      + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

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
    return getEntityManager().createQuery(FIND_BY_COLLECTION_TIME_METRIC_DIMENSION, entityClass)
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("metric", metric).setParameter("dimensions", dimensions).getResultList();
  }

  public List<AnomalyResult> findAllByTimeAndFunctionId(DateTime startTime, DateTime endTime,
      long functionId) {
    return getEntityManager().createQuery(FIND_BY_TIME_AND_FUNCTION_ID, entityClass)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
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

  public List<AnomalyResult> findAllByTimeAndEmailId(DateTime startTime, DateTime endTime,
      long emailId) {
    return getEntityManager().createQuery(FIND_BY_TIME_EMAIL_ID, entityClass)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("emailId", emailId).getResultList();
  }

  public List<AnomalyResult> findAllByCollectionTimeAndFunction(String collection,
      DateTime startTime, DateTime endTime, long functionId) {
    return getEntityManager().createQuery(FIND_BY_COLLECTION_TIME_AND_FUNCTION_ID, entityClass)
            .setParameter("collection", collection)
            .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
            .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
            .setParameter("functionId", functionId).getResultList();
  }
}
