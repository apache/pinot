package com.linkedin.thirdeye.anomaly;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
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

public class JobRunner implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(JobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
  }

  public static final String THIRDEYE_JOB_CONTEXT = "THIRDEYE_JOB_CONTEXT";

  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTasksSpecDAO;
  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private SessionFactory sessionFactory;
  private long anomalyFunctionId;
  private String jobName;
  private ThirdEyeJobContext thirdEyeJobContext;

  private TaskGenerator taskGenerator;

  public enum JobStatus {
    WAITING,
    RUNNING,
    COMPLETED,
    FAILED
  };

  public JobRunner() {
    taskGenerator = new TaskGenerator();
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOG.info("Running " + jobExecutionContext.getJobDetail().getKey().toString());

    thirdEyeJobContext = (ThirdEyeJobContext) jobExecutionContext.getJobDetail().getJobDataMap()
        .get(THIRDEYE_JOB_CONTEXT);
    sessionFactory = thirdEyeJobContext.getSessionFactory();
    anomalyJobSpecDAO = thirdEyeJobContext.getAnomalyJobSpecDAO();
    anomalyTasksSpecDAO = thirdEyeJobContext.getAnomalyTaskSpecDAO();
    anomalyFunctionSpecDAO = thirdEyeJobContext.getAnomalyFunctionSpecDAO();
    anomalyFunctionId = thirdEyeJobContext.getAnomalyFunctionId();
    jobName = thirdEyeJobContext.getJobName();

    thirdEyeJobContext.setScheduledFireTime(jobExecutionContext.getScheduledFireTime());

    // write to anomaly_jobs
    Long jobExecutionId = createAnomlyJob(jobName);

    // write to anomaly_tasks
    List<Long> taskIds = createAnomlyTasks(jobExecutionId);

  }

  private long createAnomlyJob(String jobName) {
    Session session = sessionFactory.openSession();
    Long jobExecutionId = null;
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {
        AnomalyJobSpec anomalyJobSpec = new AnomalyJobSpec();
        anomalyJobSpec.setJobName(jobName);
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

  private List<Long> createAnomlyTasks(Long jobExecutionId) {
    Session session = sessionFactory.openSession();
    List<Long> taskIds = new ArrayList<>();
    try {
      ManagedSessionContext.bind(session);
      Transaction transaction = session.beginTransaction();
      try {
        AnomalyFunctionSpec anomalyFunctionSpec =
            anomalyFunctionSpecDAO.findById(anomalyFunctionId);
        List<TaskInfo> tasks = taskGenerator.createTasks(thirdEyeJobContext, anomalyFunctionSpec);

        for (TaskInfo taskInfo : tasks) {
          String taskInfoJson = null;
          try {
            taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
          } catch (JsonProcessingException e) {
            LOG.error("Exception when converting TaskInfo {} to jsonString", taskInfo, e);
          }
          AnomalyTaskSpec anomalyTaskSpec = new AnomalyTaskSpec();
          anomalyTaskSpec.setJobExecutionId(jobExecutionId);
          anomalyTaskSpec.setStatus(JobStatus.WAITING);
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

}
