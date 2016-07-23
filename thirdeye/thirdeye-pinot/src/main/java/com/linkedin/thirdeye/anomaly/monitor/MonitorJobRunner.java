package com.linkedin.thirdeye.anomaly.monitor;

import io.dropwizard.hibernate.UnitOfWork;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.context.internal.ManagedSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.job.JobRunner;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.anomaly.task.TaskGenerator;
import com.linkedin.thirdeye.detector.db.dao.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;
import com.linkedin.thirdeye.db.entity.AnomalyTaskSpec;

public class MonitorJobRunner implements JobRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private SessionFactory sessionFactory;
  private TaskGenerator taskGenerator;
  private MonitorJobContext monitorJobContext;

  public MonitorJobRunner(MonitorJobContext monitorJobContext) {
    this.monitorJobContext = monitorJobContext;
    this.anomalyJobSpecDAO = monitorJobContext.getAnomalyJobSpecDAO();
    this.anomalyTaskSpecDAO = monitorJobContext.getAnomalyTaskSpecDAO();
    this.sessionFactory = monitorJobContext.getSessionFactory();

    taskGenerator = new TaskGenerator();
  }

  @Override
  public void run() {
    try {
      LOG.info("Starting monitor job");

      List<AnomalyJobSpec> anomalyJobSpecs = findAnomalyJobsWithStatusScheduled();
      monitorJobContext.setJobName(TaskType.MONITOR.toString());
      monitorJobContext.setAnomalyJobSpecs(anomalyJobSpecs);
      long jobExecutionId = createJob();
      monitorJobContext.setJobExecutionId(jobExecutionId);
      List<Long> taskIds = createTasks();

    } catch (Exception e) {
      LOG.error("Exception in monitor job runner", e);
    }
  }

  @UnitOfWork
  public long createJob() {
    Session session = sessionFactory.openSession();
    Long jobExecutionId = null;
    try {
      ManagedSessionContext.bind(session);

      LOG.info("Creating monitor job");
      AnomalyJobSpec anomalyJobSpec = new AnomalyJobSpec();
      anomalyJobSpec.setJobName(monitorJobContext.getJobName());
      anomalyJobSpec.setScheduleStartTime(System.currentTimeMillis());
      anomalyJobSpec.setStatus(JobStatus.SCHEDULED);
      jobExecutionId = anomalyJobSpecDAO.save(anomalyJobSpec);
      LOG.info("Created anomalyJobSpec {} with jobExecutionId {}", anomalyJobSpec,
          jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating monitor job", e);
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
    return jobExecutionId;
  }


  @UnitOfWork
  public List<Long> createTasks() {
    Session session = sessionFactory.openSession();
    List<Long> taskIds = new ArrayList<>();
    try {
      ManagedSessionContext.bind(session);
      LOG.info("Creating monitor tasks");
      List<MonitorTaskInfo> monitorTasks = taskGenerator.createMonitorTasks(monitorJobContext);
      LOG.info("Monitor tasks {}", monitorTasks);
      for (MonitorTaskInfo taskInfo : monitorTasks) {
        String taskInfoJson = null;
        try {
          taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
        } catch (JsonProcessingException e) {
          LOG.error("Exception when converting MonitorTaskInfo {} to jsonString", taskInfo, e);
        }

        AnomalyTaskSpec anomalyTaskSpec = new AnomalyTaskSpec();
        anomalyTaskSpec.setJobId(monitorJobContext.getJobExecutionId());
        anomalyTaskSpec.setTaskType(TaskType.MONITOR);
        anomalyTaskSpec.setJobName(monitorJobContext.getJobName());
        anomalyTaskSpec.setStatus(TaskStatus.WAITING);
        anomalyTaskSpec.setTaskStartTime(System.currentTimeMillis());
        anomalyTaskSpec.setTaskInfo(taskInfoJson);
        long taskId = anomalyTaskSpecDAO.save(anomalyTaskSpec);
        taskIds.add(taskId);
        LOG.info("Created monitorTask {} with taskId {}", anomalyTaskSpec, taskId);
      }
    } catch (Exception e) {
      LOG.error("Exception in creating anomaly tasks", e);
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
    return taskIds;

  }

  @UnitOfWork
  private List<AnomalyJobSpec> findAnomalyJobsWithStatusScheduled() {
    List<AnomalyJobSpec> anomalyJobSpecs = null;
    Session session = sessionFactory.openSession();
    try {
      ManagedSessionContext.bind(session);
      anomalyJobSpecs = anomalyJobSpecDAO.findByStatus(JobStatus.SCHEDULED);
    } catch (Exception e) {
      LOG.error("Exception in finding anomaly jobs with status scheduled", e);
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
    return anomalyJobSpecs;
  }

}
