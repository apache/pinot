package com.linkedin.thirdeye.anomaly.detection;

import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import io.dropwizard.hibernate.UnitOfWork;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
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
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;
import com.linkedin.thirdeye.db.entity.AnomalyTaskSpec;
import com.linkedin.thirdeye.detector.db.dao.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.anomaly.task.TaskGenerator;

public class DetectionJobRunner implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String DETECTION_JOB_CONTEXT = "DETECTION_JOB_CONTEXT";

  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTasksSpecDAO;
  private AnomalyFunctionDAO anomalyFunctionSpecDAO;
  private SessionFactory sessionFactory;
  private long anomalyFunctionId;
  private DateTime windowStart;
  private DateTime windowEnd;
  private DetectionJobContext detectionJobContext;

  private TaskGenerator taskGenerator;

  public DetectionJobRunner() {
    taskGenerator = new TaskGenerator();
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOG.info("Running " + jobExecutionContext.getJobDetail().getKey().toString());

    detectionJobContext = (DetectionJobContext) jobExecutionContext.getJobDetail().getJobDataMap()
        .get(DETECTION_JOB_CONTEXT);
    sessionFactory = detectionJobContext.getSessionFactory();
    anomalyJobSpecDAO = detectionJobContext.getAnomalyJobSpecDAO();
    anomalyTasksSpecDAO = detectionJobContext.getAnomalyTaskSpecDAO();
    anomalyFunctionSpecDAO = detectionJobContext.getAnomalyFunctionSpecDAO();
    anomalyFunctionId = detectionJobContext.getAnomalyFunctionId();

    AnomalyFunctionSpec anomalyFunctionSpec = getAnomalyFunctionSpec(anomalyFunctionId);
    detectionJobContext.setAnomalyFunctionSpec(anomalyFunctionSpec);

    String windowEndProp = detectionJobContext.getWindowEndIso();
    String windowStartProp = detectionJobContext.getWindowStartIso();

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
    detectionJobContext.setWindowStart(windowStart);
    detectionJobContext.setWindowEnd(windowEnd);

    // write to anomaly_jobs
    Long jobExecutionId = createJob();
    detectionJobContext.setJobExecutionId(jobExecutionId);

    // write to anomaly_tasks
    List<Long> taskIds = createTasks();

  }


  @UnitOfWork
  private long createJob() {
    Session session = sessionFactory.openSession();
    Long jobExecutionId = null;
    try {
      ManagedSessionContext.bind(session);

      AnomalyJobSpec anomalyJobSpec = new AnomalyJobSpec();
      anomalyJobSpec.setJobName(detectionJobContext.getJobName());
      anomalyJobSpec.setWindowStartTime(detectionJobContext.getWindowStart().getMillis());
      anomalyJobSpec.setWindowEndTime(detectionJobContext.getWindowEnd().getMillis());
      anomalyJobSpec.setScheduleStartTime(System.currentTimeMillis());
      anomalyJobSpec.setStatus(JobStatus.SCHEDULED);
      jobExecutionId = anomalyJobSpecDAO.save(anomalyJobSpec);

      LOG.info("Created anomalyJobSpec {} with jobExecutionId {}", anomalyJobSpec,
          jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating detection job", e);
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }

    return jobExecutionId;
  }

  @UnitOfWork
  private List<Long> createTasks() {
    Session session = sessionFactory.openSession();
    List<Long> taskIds = new ArrayList<>();
    try {
      ManagedSessionContext.bind(session);

      List<DetectionTaskInfo> tasks = taskGenerator.createDetectionTasks(detectionJobContext);

      for (DetectionTaskInfo taskInfo : tasks) {
        String taskInfoJson = null;
        try {
          taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
        } catch (JsonProcessingException e) {
          LOG.error("Exception when converting DetectionTaskInfo {} to jsonString", taskInfo, e);
        }
        AnomalyTaskSpec anomalyTaskSpec = new AnomalyTaskSpec();
        anomalyTaskSpec.setJobId(detectionJobContext.getJobExecutionId());
        anomalyTaskSpec.setTaskType(TaskType.ANOMALY_DETECTION);
        anomalyTaskSpec.setJobName(detectionJobContext.getJobName());
        anomalyTaskSpec.setStatus(TaskStatus.WAITING);
        anomalyTaskSpec.setTaskStartTime(System.currentTimeMillis());
        anomalyTaskSpec.setTaskInfo(taskInfoJson);
        long taskId = anomalyTasksSpecDAO.save(anomalyTaskSpec);
        taskIds.add(taskId);
        LOG.info("Created anomalyTask {} with taskId {}", anomalyTaskSpec, taskId);
      }
    } catch (Exception e) {
      LOG.error("Exception in creating detection tasks", e);
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }

    return taskIds;
  }

  @UnitOfWork
  private AnomalyFunctionSpec getAnomalyFunctionSpec(Long anomalyFunctionId) {
    Session session = sessionFactory.openSession();
    AnomalyFunctionSpec anomalyFunctionSpec = null;
    try {
      ManagedSessionContext.bind(session);
      anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(anomalyFunctionId);
    } catch (Exception e)  {
      LOG.error("Exception in getting anomalyFunctionSpec by id", e);
    } finally {
      session.close();
      ManagedSessionContext.unbind(sessionFactory);
    }
    return anomalyFunctionSpec;
  }

}
