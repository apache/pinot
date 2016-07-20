package com.linkedin.thirdeye.anomaly.detection;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.api.AnomalyJobSpec;
import com.linkedin.thirdeye.detector.api.AnomalyTaskSpec;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.anomaly.task.TaskGenerator;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;

public class DetectionJobRunner implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String DETECTION_JOB_CONTEXT = "DETECTION_JOB_CONTEXT";

  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTasksSpecDAO;
  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private SessionFactory sessionFactory;
  private long anomalyFunctionId;
  private String jobName;
  private DateTime windowStart;
  private DateTime windowEnd;
  private JobContext jobContext;

  private TaskGenerator taskGenerator;

  public DetectionJobRunner() {
    taskGenerator = new TaskGenerator();
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOG.info("Running " + jobExecutionContext.getJobDetail().getKey().toString());

    jobContext = (JobContext) jobExecutionContext.getJobDetail().getJobDataMap()
        .get(DETECTION_JOB_CONTEXT);
    sessionFactory = jobContext.getSessionFactory();
    anomalyJobSpecDAO = jobContext.getAnomalyJobSpecDAO();
    anomalyTasksSpecDAO = jobContext.getAnomalyTaskSpecDAO();
    anomalyFunctionSpecDAO = jobContext.getAnomalyFunctionSpecDAO();
    anomalyFunctionId = jobContext.getAnomalyFunctionId();
    jobName = jobContext.getJobName();

    AnomalyFunctionSpec anomalyFunctionSpec = getAnomalyFunctionSpec(anomalyFunctionId);
    String windowEndProp = jobContext.getWindowEndIso();
    String windowStartProp = jobContext.getWindowStartIso();

    // Compute window end
    if (windowEndProp == null) {
      long delayMillis = 0;
      if (anomalyFunctionSpec.getWindowDelay() != null) {
        delayMillis = TimeUnit.MILLISECONDS.convert(anomalyFunctionSpec.getWindowDelay(),
            anomalyFunctionSpec.getWindowDelayUnit());
      }
      Date scheduledFireTime = jobExecutionContext.getScheduledFireTime();
      windowEnd = new DateTime(scheduledFireTime).minus(delayMillis);
    } else {
      windowEnd = ISODateTimeFormat.dateTimeParser().parseDateTime(windowEndProp);
    }

    // Compute window start
    if (windowStartProp == null) {
      int windowSize = anomalyFunctionSpec.getWindowSize();
      TimeUnit windowUnit = anomalyFunctionSpec.getWindowUnit();
      long windowMillis = TimeUnit.MILLISECONDS.convert(windowSize, windowUnit);
      windowStart = windowEnd.minus(windowMillis);
    } else {
      windowStart = ISODateTimeFormat.dateTimeParser().parseDateTime(windowStartProp);
    }
    jobContext.setWindowStart(windowStart);
    jobContext.setWindowEnd(windowEnd);

    // write to anomaly_jobs
    Long jobExecutionId = createAnomalyJob(jobName);
    jobContext.setJobExecutionId(jobExecutionId);

    // write to anomaly_tasks
    List<Long> taskIds = createAnomalyTasks(anomalyFunctionSpec);

  }

  private long createAnomalyJob(String jobName) {
    Session session = sessionFactory.openSession();
    Long jobExecutionId = null;
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {
        AnomalyJobSpec anomalyJobSpec = new AnomalyJobSpec();
        anomalyJobSpec.setJobName(jobName);
        anomalyJobSpec.setWindowStartTime(jobContext.getWindowStart().getMillis());
        anomalyJobSpec.setWindowEndTime(jobContext.getWindowEnd().getMillis());
        anomalyJobSpec.setScheduleStartTime(System.currentTimeMillis());
        anomalyJobSpec.setStatus(JobStatus.WAITING);
        jobExecutionId = anomalyJobSpecDAO.createOrUpdate(anomalyJobSpec);
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

  private List<Long> createAnomalyTasks(AnomalyFunctionSpec anomalyFunctionSpec) {
    Session session = sessionFactory.openSession();
    List<Long> taskIds = new ArrayList<>();
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {

        List<TaskInfo> tasks = taskGenerator.createTasks(jobContext, anomalyFunctionSpec);

        for (TaskInfo taskInfo : tasks) {
          String taskInfoJson = null;
          try {
            taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
          } catch (JsonProcessingException e) {
            LOG.error("Exception when converting TaskInfo {} to jsonString", taskInfo, e);
          }
          AnomalyTaskSpec anomalyTaskSpec = new AnomalyTaskSpec();
          anomalyTaskSpec.setJobId(jobContext.getJobExecutionId());
          anomalyTaskSpec.setTaskType(TaskType.ANOMALY_DETECTION);
          anomalyTaskSpec.setJobName(jobContext.getJobName());
          anomalyTaskSpec.setStatus(TaskStatus.WAITING);
          anomalyTaskSpec.setTaskStartTime(System.currentTimeMillis());
          anomalyTaskSpec.setTaskInfo(taskInfoJson);
          long taskId = anomalyTasksSpecDAO.createOrUpdate(anomalyTaskSpec);
          taskIds.add(taskId);
          LOG.info("Created anomalyTask {} with taskId {}", anomalyTaskSpec, taskId);
        }
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

    return taskIds;
  }

  private AnomalyFunctionSpec getAnomalyFunctionSpec(Long anomalyFunctionId) {
    Session session = sessionFactory.openSession();
    AnomalyFunctionSpec anomalyFunctionSpec = null;
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {
        anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(anomalyFunctionId);
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
    return anomalyFunctionSpec;
  }

}