package com.linkedin.thirdeye.anomaly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.hibernate.UnitOfWork;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.detector.db.dao.AnomalyTaskSpecDAO;

public class TestAnomalyApplication {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(TestAnomalyApplication.class);
  AnomalyTaskSpecDAO dao;

  TestAnomalyApplication(AnomalyTaskSpecDAO dao) {
    this.dao = dao;
  }

  @UnitOfWork
  public void start() {
    dao.deleteRecordsOlderThanDaysWithStatus(7, TaskStatus.COMPLETED);
  }

  public void stop() {
  }

}
