package com.linkedin.thirdeye.anomaly;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.hibernate.UnitOfWork;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.detector.api.AnomalyTaskSpec;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;


public class TestAnomalyApplication {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(TestAnomalyApplication.class);
  AnomalyTaskSpecDAO dao;

  TestAnomalyApplication(AnomalyTaskSpecDAO dao) {
    this.dao = dao;
  }

  @UnitOfWork
  public void start() {
    List<AnomalyTaskSpec> anomalyTasks = dao.findAll();
    AnomalyTaskSpec anomalyTaskSpec = anomalyTasks.get(0);
    String taskInfoString = anomalyTaskSpec.getTaskInfo();
    try {
      TaskInfo taskInfo = OBJECT_MAPPER.readValue(taskInfoString, TaskInfo.class);
    } catch (Exception e) {
      LOG.error("Exception", e);
    }
  }

  public void stop() {
  }

}
