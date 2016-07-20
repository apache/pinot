package com.linkedin.thirdeye.detector.db;

import java.util.List;

import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.linkedin.thirdeye.detector.db.entity.ContextualEvent;

import io.dropwizard.hibernate.AbstractDAO;

public class ContextualEventDAO extends AbstractDAO<ContextualEvent> {
  public ContextualEventDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public ContextualEvent findById(Long id) {
    return get(id);
  }

  public Long create(ContextualEvent anomalyResult) {
    return persist(anomalyResult).getId();
  }

  public void delete(Long id) {
    ContextualEvent anomalyResult = new ContextualEvent();
    anomalyResult.setId(id);
    currentSession().delete(id);
  }

  public List<ContextualEvent> findAllByTime(DateTime startTime, DateTime endTime) {
    return list(namedQuery("com.linkedin.thirdeye.api.ContextualEvent#findAllByTime")
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis()));
  }
}
