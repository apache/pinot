package com.linkedin.thirdeye.detector.db.dao;

import java.sql.Timestamp;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.joda.time.DateTime;

import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.db.entity.AnomalyTaskSpec;

public class AnomalyTaskSpecDAO extends AbstractBaseDAO<AnomalyTaskSpec> {
  public AnomalyTaskSpecDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public List<AnomalyTaskSpec> findAll() {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findAll"));
  }

  public List<AnomalyTaskSpec> findByJobIdAndStatusNotIn(Long jobId, TaskStatus status) {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByJobIdAndStatusNotIn")
        .setParameter("jobId", jobId)
        .setParameter("status", status));
  }

  public List<AnomalyTaskSpec> findByStatusOrderByCreateTimeAscending(TaskStatus status) {
    return list(namedQuery(
        "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByStatusOrderByCreateTimeAscending")
            .setParameter("status", status));
  }

  public boolean updateStatusAndTaskEndTime(Long id, TaskStatus oldStatus, TaskStatus newStatus, Long taskEndTime) {
    try {
      int numRowsUpdated = namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#updateStatusAndTaskEndTime")
          .setParameter("id", id)
          .setParameter("oldStatus", oldStatus)
          .setParameter("newStatus", newStatus)
          .setParameter("taskEndTime", taskEndTime)
          .executeUpdate();
      return numRowsUpdated == 1;
    } catch (HibernateException exception) {
      exception.printStackTrace();
      return false;
    }
  }

  //also update the worker id that is picking up the task
  public boolean updateStatusAndWorkerId(Long workerId, Long id, TaskStatus oldStatus, TaskStatus newStatus) {
    try {
      int numRowsUpdated = namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#updateStatusAndWorkerId")
          .setParameter("id", id).setParameter("workerId", workerId)
          .setParameter("oldStatus", oldStatus).setParameter("newStatus", newStatus)
          .executeUpdate();
      return numRowsUpdated == 1;
    } catch (HibernateException exception) {
      exception.printStackTrace();
      return false;
    }
  }

  public int deleteRecordsOlderThanDaysWithStatus(int days, TaskStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());
    int numRowsUpdated = namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#deleteRecordsOlderThanDaysWithStatus")
        .setParameter("expireTimestamp", expireTimestamp)
        .setParameter("status", status)
        .executeUpdate();
    return numRowsUpdated;
  }
}
