package com.linkedin.thirdeye.db;

import com.linkedin.thirdeye.api.AnomalyResult;
import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AnomalyResultDAO extends AbstractDAO<AnomalyResult> {
  public AnomalyResultDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public AnomalyResult findById(Long id) {
    return get(id);
  }

  public Long create(AnomalyResult anomalyResult) {
    return persist(anomalyResult).getId();
  }

  public void delete(Long id) {
    AnomalyResult anomalyResult = new AnomalyResult();
    anomalyResult.setId(id);
    currentSession().delete(id);
  }

  public List<AnomalyResult> findAllByCollectionAndTime(String collection, DateTime startTime, DateTime endTime) {
    return list(namedQuery("com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionAndTime")
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis()));
  }

  public List<AnomalyResult> findAllByCollectionTimeAndMetric(String collection, String metric, DateTime startTime, DateTime endTime) {
    return list(namedQuery("com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionTimeAndMetric")
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("metric", metric));
  }

  public List<AnomalyResult> findAllByCollectionTimeAndFunction(String collection, DateTime startTime, DateTime endTime, long functionId) {
    return list(namedQuery("com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionTimeAndFunction")
        .setParameter("collection", collection)
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("functionId", functionId));
  }
}
