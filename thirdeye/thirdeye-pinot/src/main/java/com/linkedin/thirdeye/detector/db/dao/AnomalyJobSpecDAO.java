package com.linkedin.thirdeye.detector.db.dao;

import java.sql.Timestamp;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.detector.db.entity.AnomalyJobSpec;

public class AnomalyJobSpecDAO extends AbstractBaseDAO<AnomalyJobSpec> {

  private static final Logger LOG = LoggerFactory.getLogger(AnomalyJobSpecDAO.class);
  public AnomalyJobSpecDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public List<AnomalyJobSpec> findAll() {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyJobSpec#findAll"));
  }

  public List<AnomalyJobSpec> findByStatus(JobStatus status) {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyJobSpec#findByStatus")
        .setParameter("status", status));
  }

  public boolean updateStatusAndJobEndTime(Long id, JobStatus status, Long jobEndTime) {
    try {
      int numRowsUpdated = namedQuery("com.linkedin.thirdeye.anomaly.AnomalyJobSpec#updateStatusAndJobEndTime")
          .setParameter("id", id)
          .setParameter("status", status)
          .setParameter("jobEndTime", jobEndTime)
          .executeUpdate();
      return numRowsUpdated == 1;
    } catch (HibernateException e) {
      LOG.error("Exception in updateStatusAndJobEndTime", e);
      return false;
    }
  }

  public int deleteRecordsOlderThanDaysWithStatus(int days, JobStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    int numRowsUpdated = namedQuery("com.linkedin.thirdeye.anomaly.AnomalyJobSpec#deleteRecordsOlderThanDaysWithStatus")
        .setParameter("expireTimestamp", expireTimestamp)
        .setParameter("status", status)
        .executeUpdate();
    return numRowsUpdated;
  }
}
