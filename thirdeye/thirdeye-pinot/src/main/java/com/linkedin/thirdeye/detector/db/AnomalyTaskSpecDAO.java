package com.linkedin.thirdeye.detector.db;

import java.util.List;

import io.dropwizard.hibernate.AbstractDAO;

import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.detector.db.entity.AnomalyTaskSpec;

public class AnomalyTaskSpecDAO extends AbstractDAO<AnomalyTaskSpec> {
  public AnomalyTaskSpecDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public AnomalyTaskSpec findById(Long id) {
    AnomalyTaskSpec anomalyTasksSpec = get(id);
    return anomalyTasksSpec;
  }

  public List<AnomalyTaskSpec> findAll() {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findAll"));
  }

  public List<AnomalyTaskSpec> findByJobId(Long jobId) {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByJobId")
        .setParameter("jobId", jobId));
  }

  public List<AnomalyTaskSpec> findByStatusOrderByCreateTimeAscending(TaskStatus status) {
    return list(namedQuery(
        "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByStatusOrderByCreateTimeAscending")
            .setParameter("status", status));
  }

  public boolean updateStatus(Long id, TaskStatus oldStatus, TaskStatus newStatus) {
    try {
      int numRowsUpdated = namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#updateStatus")
          .setParameter("id", id).setParameter("oldStatus", oldStatus).setParameter("newStatus", newStatus)
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

  public Long createOrUpdate(AnomalyTaskSpec anomalyTasksSpec) {
    long id = persist(anomalyTasksSpec).getId();
    currentSession().getTransaction().commit();
    return id;
  }

  public void delete(Long id) {
    AnomalyTaskSpec anomalyTasksSpec = new AnomalyTaskSpec();
    anomalyTasksSpec.setId(id);
    currentSession().delete(anomalyTasksSpec);
  }

  public void delete(AnomalyTaskSpec anomalyTasksSpec) {
    currentSession().delete(anomalyTasksSpec);
  }
}
