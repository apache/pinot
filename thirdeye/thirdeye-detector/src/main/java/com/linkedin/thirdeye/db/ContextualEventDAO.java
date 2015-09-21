package com.linkedin.thirdeye.db;

import com.linkedin.thirdeye.api.ContextualEvent;
import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;

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
