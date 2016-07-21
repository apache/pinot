package com.linkedin.thirdeye.anomaly.monitor;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.anomaly.task.TaskGenerator;
import com.linkedin.thirdeye.detector.db.dao.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.detector.db.entity.AnomalyJobSpec;
import com.linkedin.thirdeye.detector.db.entity.AnomalyTaskSpec;

public class MonitorJobRunner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private SessionFactory sessionFactory;
  private MonitorConfiguration monitorConfiguration;
  private TaskGenerator taskGenerator;
  private JobContext jobContext;

  public MonitorJobRunner(AnomalyJobSpecDAO anomalyJobSpecDAO, AnomalyTaskSpecDAO anomalyTaskSpecDAO,
      SessionFactory sessionFactory, MonitorConfiguration monitorConfiguration) {
    this.anomalyJobSpecDAO = anomalyJobSpecDAO;
    this.anomalyTaskSpecDAO = anomalyTaskSpecDAO;
    this.sessionFactory = sessionFactory;
    this.monitorConfiguration = monitorConfiguration;

    taskGenerator = new TaskGenerator();
    jobContext = new JobContext();
  }

  @Override
  public void run() {
    try {
      LOG.info("Starting monitor job");

      List<AnomalyJobSpec> anomalyJobSpecs = findAnomalyJobsWithStatusScheduled();
      jobContext.setJobName(TaskType.MONITOR.toString());
      long jobExecutionId = createAnomalyJob();
      jobContext.setJobExecutionId(jobExecutionId);
      List<Long> taskIds = createAnomalyTasks(anomalyJobSpecs);

    } catch (Exception e) {
      LOG.error("Exception in monitor job runner", e);
    }
  }

  private long createAnomalyJob() {
    Session session = sessionFactory.openSession();
    Long jobExecutionId = null;
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {
        LOG.info("Creating monitor job");
        AnomalyJobSpec anomalyJobSpec = new AnomalyJobSpec();
        anomalyJobSpec.setJobName(jobContext.getJobName());
        anomalyJobSpec.setScheduleStartTime(System.currentTimeMillis());
        anomalyJobSpec.setStatus(JobStatus.SCHEDULED);
        jobExecutionId = anomalyJobSpecDAO.save(anomalyJobSpec);
        if (!transaction.wasCommitted()) {
          transaction.commit();
        }
        LOG.info("Created anomalyJobSpec {} with jobExecutionId {}", anomalyJobSpec,
            jobExecutionId);
      } catch (Exception e) {
        transaction.rollback();
        throw new RuntimeException(e);
      }
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
    return jobExecutionId;
  }


  private List<Long> createAnomalyTasks(List<AnomalyJobSpec> anomalyJobSpecs) {
    Session session = sessionFactory.openSession();
    List<Long> taskIds = new ArrayList<>();
    try {
      ManagedSessionContext.bind(session);
      LOG.info("Creating monitor tasks");
      List<MonitorTaskInfo> monitorTasks = taskGenerator.createMonitorTasks(anomalyJobSpecs, monitorConfiguration);
      LOG.info("Monitor tasks {}", monitorTasks);
      for (MonitorTaskInfo taskInfo : monitorTasks) {
        String taskInfoJson = null;
        try {
          taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
        } catch (JsonProcessingException e) {
          LOG.error("Exception when converting MonitorTaskInfo {} to jsonString", taskInfo, e);
        }

        Transaction transaction = session.beginTransaction();
        try {
          AnomalyTaskSpec anomalyTaskSpec = new AnomalyTaskSpec();
          anomalyTaskSpec.setJobId(jobContext.getJobExecutionId());
          anomalyTaskSpec.setTaskType(TaskType.MONITOR);
          anomalyTaskSpec.setJobName(jobContext.getJobName());
          anomalyTaskSpec.setStatus(TaskStatus.WAITING);
          anomalyTaskSpec.setTaskStartTime(System.currentTimeMillis());
          anomalyTaskSpec.setTaskInfo(taskInfoJson);
          long taskId = anomalyTaskSpecDAO.save(anomalyTaskSpec);
          taskIds.add(taskId);
          LOG.info("Created monitorTask {} with taskId {}", anomalyTaskSpec, taskId);
          if (!transaction.wasCommitted()) {
            transaction.commit();
          }
        } catch (Exception e) {
          transaction.rollback();
          throw new RuntimeException(e);
        }
      }
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
    return taskIds;

  }

  private List<AnomalyJobSpec> findAnomalyJobsWithStatusScheduled() {
    List<AnomalyJobSpec> anomalyJobSpecs = null;
    Session session = sessionFactory.openSession();
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {
        anomalyJobSpecs = anomalyJobSpecDAO.findByStatus(JobStatus.SCHEDULED);
        if (!transaction.wasCommitted()) {
          transaction.commit();
        }
      } catch (Exception e) {
        transaction.rollback();
        throw new RuntimeException(e);
      }
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
    return anomalyJobSpecs;
  }

}
