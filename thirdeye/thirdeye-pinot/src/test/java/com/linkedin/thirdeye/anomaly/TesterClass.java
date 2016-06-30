package com.linkedin.thirdeye.anomaly;

import java.util.List;

import io.dropwizard.hibernate.UnitOfWork;

import com.linkedin.thirdeye.anomaly.JobRunner.JobStatus;
import com.linkedin.thirdeye.detector.api.AnomalyJobSpec;
import com.linkedin.thirdeye.detector.api.AnomalyTaskSpec;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;


public class TesterClass {

  AnomalyTaskSpecDAO dao;

  TesterClass(AnomalyTaskSpecDAO dao) {
    this.dao = dao;
  }

  @UnitOfWork
  public void start() {
    System.out.println("starting");
//    List<AnomalyTaskSpec> list = dao.findByStatusForUpdate(JobStatus.WAITING);
//    System.out.println("Result : " + list);
  }

  public void stop() {
    System.out.println("stopping");
  }

}
