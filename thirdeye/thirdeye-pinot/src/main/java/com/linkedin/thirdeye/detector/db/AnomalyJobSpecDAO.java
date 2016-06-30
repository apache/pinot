package com.linkedin.thirdeye.detector.db;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.api.AnomalyJobSpec;

import io.dropwizard.hibernate.AbstractDAO;
import io.dropwizard.hibernate.UnitOfWork;

public class AnomalyJobSpecDAO extends AbstractDAO<AnomalyJobSpec> {
  public AnomalyJobSpecDAO(SessionFactory sessionFactory) {
    super(sessionFactory);
  }

  public AnomalyJobSpec findById(Long jobExecutionId) {
    AnomalyJobSpec anomalyJobsSpec = get(jobExecutionId);
    return anomalyJobsSpec;
  }

  public List<AnomalyJobSpec> findAll() {
    return list(namedQuery("com.linkedin.thirdeye.anomaly.AnomalyJobSpec#findAll"));
  }

  @UnitOfWork
  public Long createOrUpdate(AnomalyJobSpec anomalyJobsSpec) {
    long id = persist(anomalyJobsSpec).getJobExecutionId();
    currentSession().getTransaction().commit();
    return id;
  }

  public void delete(Long id) {
    AnomalyJobSpec anomalyJobsSpec = new AnomalyJobSpec();
    anomalyJobsSpec.setJobExecutionId(id);
    currentSession().delete(anomalyJobsSpec);
  }

  public void delete(AnomalyJobSpec anomalyJobsSpec) {
    currentSession().delete(anomalyJobsSpec);
  }
}
