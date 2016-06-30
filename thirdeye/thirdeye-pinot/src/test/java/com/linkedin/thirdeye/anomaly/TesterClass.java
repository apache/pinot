package com.linkedin.thirdeye.anomaly;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.hibernate.UnitOfWork;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.JobRunner.JobStatus;
import com.linkedin.thirdeye.detector.api.AnomalyJobSpec;
import com.linkedin.thirdeye.detector.api.AnomalyTaskSpec;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;


public class TesterClass {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(TesterClass.class);
  AnomalyTaskSpecDAO dao;

  TesterClass(AnomalyTaskSpecDAO dao) {
    this.dao = dao;
  }

  @UnitOfWork
  public void start() {
    System.out.println("starting");
    List<AnomalyTaskSpec> anomalyTasks = dao.findAll();
    AnomalyTaskSpec anomalyTaskSpec = anomalyTasks.get(0);
    String taskInfoString = anomalyTaskSpec.getTaskInfo();
    try {
      TaskInfo taskInfo = OBJECT_MAPPER.readValue(taskInfoString, TaskInfo.class);
    } catch (Exception e) {
      LOG.error("Exception", e);
    }
    System.out.println("tasks " + anomalyTasks);
  }

  public void stop() {
    System.out.println("stopping");
  }

}
