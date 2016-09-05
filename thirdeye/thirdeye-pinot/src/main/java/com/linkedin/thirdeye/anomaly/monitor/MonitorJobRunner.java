package com.linkedin.thirdeye.anomaly.monitor;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.job.JobRunner;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.anomaly.task.TaskGenerator;

public class MonitorJobRunner implements JobRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private JobManager anomalyJobDAO;
  private TaskManager anomalyTaskDAO;
  private TaskGenerator taskGenerator;
  private MonitorJobContext monitorJobContext;

  public MonitorJobRunner(MonitorJobContext monitorJobContext) {
    this.monitorJobContext = monitorJobContext;
    this.anomalyJobDAO = monitorJobContext.getAnomalyJobDAO();
    this.anomalyTaskDAO = monitorJobContext.getAnomalyTaskDAO();

    taskGenerator = new TaskGenerator();
  }

  @Override
  public void run() {
    try {
      LOG.info("Starting monitor job");

      List<JobDTO> anomalyJobSpecs = findAnomalyJobsWithStatusScheduled();
      monitorJobContext.setJobName(TaskType.MONITOR.toString());
      monitorJobContext.setAnomalyJobSpecs(anomalyJobSpecs);
      Long jobExecutionId = createJob();
      if (jobExecutionId != null) {
        monitorJobContext.setJobExecutionId(jobExecutionId);
        List<Long> taskIds = createTasks();
      }
    } catch (Exception e) {
      LOG.error("Exception in monitor job runner", e);
    }
  }

  public Long createJob() {
    Long jobExecutionId = null;
    try {

      LOG.info("Creating monitor job");
      JobDTO anomalyJobSpec = new JobDTO();
      anomalyJobSpec.setJobName(monitorJobContext.getJobName());
      anomalyJobSpec.setScheduleStartTime(System.currentTimeMillis());
      anomalyJobSpec.setStatus(JobStatus.SCHEDULED);
      jobExecutionId = anomalyJobDAO.save(anomalyJobSpec);
      LOG.info("Created anomalyJobSpec {} with jobExecutionId {}", anomalyJobSpec,
          jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating monitor job", e);
    }
    return jobExecutionId;
  }


  public List<Long> createTasks() {
    List<Long> taskIds = new ArrayList<>();
    try {
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

        TaskDTO anomalyTaskSpec = new TaskDTO();
        anomalyTaskSpec.setTaskType(TaskType.MONITOR);
        anomalyTaskSpec.setJobName(monitorJobContext.getJobName());
        anomalyTaskSpec.setStatus(TaskStatus.WAITING);
        anomalyTaskSpec.setTaskStartTime(System.currentTimeMillis());
        anomalyTaskSpec.setTaskInfo(taskInfoJson);
        JobDTO anomalyJobSpec = anomalyJobDAO.findById(monitorJobContext.getJobExecutionId());
        anomalyTaskSpec.setJob(anomalyJobSpec);
        long taskId = anomalyTaskDAO.save(anomalyTaskSpec);
        taskIds.add(taskId);
        LOG.info("Created monitorTask {} with taskId {}", anomalyTaskSpec, taskId);
      }
    } catch (Exception e) {
      LOG.error("Exception in creating anomaly tasks", e);
    }
    return taskIds;

  }

  private List<JobDTO> findAnomalyJobsWithStatusScheduled() {
    List<JobDTO> anomalyJobSpecs = null;
    try {
      anomalyJobSpecs = anomalyJobDAO.findByStatus(JobStatus.SCHEDULED);
    } catch (Exception e) {
      LOG.error("Exception in finding anomaly jobs with status scheduled", e);
    }
    return anomalyJobSpecs;
  }

}
