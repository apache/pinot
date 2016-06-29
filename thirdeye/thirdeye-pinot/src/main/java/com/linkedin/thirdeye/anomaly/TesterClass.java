package com.linkedin.thirdeye.anomaly;

import io.dropwizard.hibernate.UnitOfWork;

import com.linkedin.thirdeye.anomaly.JobRunner.JobStatus;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;


public class TesterClass {

  AnomalyJobSpecDAO dao;

  TesterClass(AnomalyJobSpecDAO dao) {
    this.dao = dao;
  }

  @UnitOfWork
  public void start() {
    System.out.println("starting");
    AnomalyJobSpec anomalyJobSpec = new AnomalyJobSpec();
    anomalyJobSpec.setJobName("text");
    anomalyJobSpec.setScheduleStartTime(System.currentTimeMillis()); // rename
    anomalyJobSpec.setStatus(JobStatus.WAITING);
    long jobExecutionId = dao.createOrUpdate(anomalyJobSpec);
  }

  public void stop() {
    System.out.println("stopping");
  }

}
