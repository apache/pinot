package com.linkedin.thirdeye.detector.db.dao;

import java.util.List;

import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.linkedin.thirdeye.detector.db.entity.ContextualEvent;

public class ContextualEventDAO extends AbstractBaseDAO<ContextualEvent> {
  public ContextualEventDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public List<ContextualEvent> findAllByTime(DateTime startTime, DateTime endTime) {
    return list(namedQuery("com.linkedin.thirdeye.api.ContextualEvent#findAllByTime")
        .setParameter("startTimeUtc", startTime.toDateTime(DateTimeZone.UTC).getMillis())
        .setParameter("endTimeUtc", endTime.toDateTime(DateTimeZone.UTC).getMillis()));
  }
}
