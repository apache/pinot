package com.linkedin.thirdeye.anomaly;

import io.dropwizard.hibernate.AbstractDAO;

import org.hibernate.SessionFactory;

public class AnomalyTaskSpecDAO extends AbstractDAO<AnomalyTaskSpec> {
  public AnomalyTaskSpecDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public AnomalyTaskSpec findById(Long taskId) {
    AnomalyTaskSpec anomalyTasksSpec = get(taskId);
    return anomalyTasksSpec;
  }

  public AnomalyTaskSpec findByJobExecutionId(Long jobExecutionId) {
    AnomalyTaskSpec anomalyTasksSpec = get(jobExecutionId);
    return anomalyTasksSpec;
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
