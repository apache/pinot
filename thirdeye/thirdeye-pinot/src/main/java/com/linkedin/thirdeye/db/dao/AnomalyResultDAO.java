package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.db.entity.AnomalyResult;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class AnomalyResultDAO extends AbstractJpaDAO<AnomalyResult> {

  private static final String FIND_BY_COLLECTION_TIME =
      "SELECT r FROM AnomalyResult r WHERE r.collection = :collection "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_COLLECTION_TIME_METRIC =
      "SELECT r FROM AnomalyResult r WHERE r.collection = :collection AND r.metric = :metric "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))";

  private static final String FIND_BY_COLLECTION_TIME_METRIC_DIMENSION =
      "SELECT r FROM AnomalyResult r WHERE r.collection = :collection" + "AND r.metric = :metric "
          + "AND r.dimensions IN :dimensions "
          + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
          + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc)) ";

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
    return getEntityManager()
        .createQuery(FIND_BY_COLLECTION_TIME_METRIC_DIMENSION, entityClass)
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("metric", metric).setParameter("dimensions", dimensions).getResultList();
  }
}
