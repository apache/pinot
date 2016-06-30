package com.linkedin.thirdeye.detector.db;

import java.util.List;

import io.dropwizard.hibernate.AbstractDAO;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.anomaly.JobRunner.JobStatus;
import com.linkedin.thirdeye.detector.api.AnomalyTaskSpec;

public class AnomalyTaskSpecDAO extends AbstractDAO<AnomalyTaskSpec> {
  public AnomalyTaskSpecDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public AnomalyTaskSpec findById(Long taskId) {
    AnomalyTaskSpec anomalyTasksSpec = get(taskId);
    return anomalyTasksSpec;
  }

  public List<AnomalyTaskSpec> findByJobExecutionId(Long jobExecutionId) {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByJobExecutionId")
        .setParameter("jobExecutionId", jobExecutionId));
  }

  public List<AnomalyTaskSpec> findByStatusForUpdate(JobStatus status) {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByStatusForUpdate")
        .setParameter("status", status));
  }

  public List<AnomalyTaskSpec> updateStatus() {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#updateStatus"));
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
