package com.linkedin.thirdeye.detector.db;

import java.util.List;

import io.dropwizard.hibernate.AbstractDAO;

import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.anomaly.ThirdeyeAnomalyConstants.TaskStatus;
import com.linkedin.thirdeye.detector.api.AnomalyTaskSpec;

public class AnomalyTaskSpecDAO extends AbstractDAO<AnomalyTaskSpec> {
  public AnomalyTaskSpecDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public AnomalyTaskSpec findById(Long taskId) {
    AnomalyTaskSpec anomalyTasksSpec = get(taskId);
    return anomalyTasksSpec;
  }

  public List<AnomalyTaskSpec> findAll() {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findAll"));
  }

  public List<AnomalyTaskSpec> findByJobExecutionId(Long jobExecutionId) {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByJobExecutionId")
        .setParameter("jobExecutionId", jobExecutionId));
  }

  public List<AnomalyTaskSpec> findByStatusOrderByCreateTimeAscending(TaskStatus status) {
    return list(namedQuery(
        "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByStatusOrderByCreateTimeAscending")
            .setParameter("status", status));
  }

  public boolean updateStatus(Long taskId, TaskStatus oldStatus, TaskStatus newStatus) {
    try {
      int numRowsUpdated = namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#updateStatus")
          .setParameter("taskId", taskId).setParameter("oldStatus", oldStatus).setParameter("newStatus", newStatus)
          .executeUpdate();
      return numRowsUpdated == 1;
    } catch (HibernateException exception) {
      exception.printStackTrace();
      return false;
    }
  }

  //also update the worker id that is picking up the task
  public boolean updateStatusAndWorkerId(Long workerId, Long taskId, TaskStatus oldStatus, TaskStatus newStatus) {
    try {
      int numRowsUpdated = namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#updateStatusAndWorkerId")
          .setParameter("taskId", taskId).setParameter("workerId", workerId)
          .setParameter("oldStatus", oldStatus).setParameter("newStatus", newStatus)
          .executeUpdate();
      return numRowsUpdated == 1;
    } catch (HibernateException exception) {
      exception.printStackTrace();
      return false;
    }
  }

  public Long createOrUpdate(AnomalyTaskSpec anomalyTasksSpec) {
    long id = persist(anomalyTasksSpec).getTaskId();
    currentSession().getTransaction().commit();
    return id;
  }

  public void delete(Long taskId) {
    AnomalyTaskSpec anomalyTasksSpec = new AnomalyTaskSpec();
    anomalyTasksSpec.setTaskId(taskId);
    currentSession().delete(anomalyTasksSpec);
  }

  public void delete(AnomalyTaskSpec anomalyTasksSpec) {
    currentSession().delete(anomalyTasksSpec);
  }
}
