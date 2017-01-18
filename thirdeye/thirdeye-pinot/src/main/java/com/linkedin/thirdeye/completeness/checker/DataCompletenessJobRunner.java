package com.linkedin.thirdeye.completeness.checker;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.job.JobRunner;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.anomaly.task.TaskGenerator;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;

public class DataCompletenessJobRunner implements JobRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DataCompletenessJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private JobManager jobDAO;
  private TaskManager taskDAO;
  private TaskGenerator taskGenerator;
  private DataCompletenessJobContext dataCompletenessJobContext;
  private DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmm");

  public DataCompletenessJobRunner(DataCompletenessJobContext dataCompletenessJobContext) {
    this.dataCompletenessJobContext = dataCompletenessJobContext;

    taskGenerator = new TaskGenerator();
    jobDAO = DAO_REGISTRY.getJobDAO();
    taskDAO = DAO_REGISTRY.getTaskDAO();
  }

  @Override
  public void run() {
    DateTime now = new DateTime();
    long checkDurationEndTime = now.getMillis();
    long checkDurationStartTime = now.minus(TimeUnit.MILLISECONDS.convert(
        DataCompletenessConstants.LOOKBACK_TIME_DURATION, DataCompletenessConstants.LOOKBACK_TIMEUNIT)).getMillis();

    String checkerEndTime = dateTimeFormatter.print(checkDurationEndTime);
    String checkerStartTime = dateTimeFormatter.print(checkDurationStartTime);
    String jobName =
        String.format("%s-%s-%s", TaskType.DATA_COMPLETENESS.toString(), checkerStartTime, checkerEndTime);

    dataCompletenessJobContext.setCheckDurationStartTime(checkDurationStartTime);
    dataCompletenessJobContext.setCheckDurationEndTime(checkDurationEndTime);
    dataCompletenessJobContext.setJobName(jobName);

    // create data completeness job
    long jobExecutionId = createJob();
    dataCompletenessJobContext.setJobExecutionId(jobExecutionId);

    // create data completeness tasks
    createTasks();

  }

  public Long createJob() {
    Long jobExecutionId = null;
    try {
      LOG.info("Creating data completeness job");
      JobDTO jobSpec = new JobDTO();
      jobSpec.setJobName(dataCompletenessJobContext.getJobName());
      jobSpec.setScheduleStartTime(System.currentTimeMillis());
      jobSpec.setStatus(JobStatus.SCHEDULED);
      jobExecutionId = jobDAO.save(jobSpec);
      LOG.info("Created JobSpec {} with jobExecutionId {}", jobSpec,
          jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating data completeness job", e);
    }
    return jobExecutionId;
  }


  public List<Long> createTasks() {
    List<Long> taskIds = new ArrayList<>();
    try {
      LOG.info("Creating data completeness checker tasks");
      List<DataCompletenessTaskInfo> dataCompletenessTasks =
          taskGenerator.createDataCompletenessTasks(dataCompletenessJobContext);
      LOG.info("Monitor tasks {}", dataCompletenessTasks);
      for (DataCompletenessTaskInfo taskInfo : dataCompletenessTasks) {
        String taskInfoJson = null;
        try {
          taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
        } catch (JsonProcessingException e) {
          LOG.error("Exception when converting MonitorTaskInfo {} to jsonString", taskInfo, e);
        }

        TaskDTO taskSpec = new TaskDTO();
        taskSpec.setTaskType(TaskType.DATA_COMPLETENESS);
        taskSpec.setJobName(dataCompletenessJobContext.getJobName());
        taskSpec.setStatus(TaskStatus.WAITING);
        taskSpec.setStartTime(System.currentTimeMillis());
        taskSpec.setTaskInfo(taskInfoJson);
        taskSpec.setJobId(dataCompletenessJobContext.getJobExecutionId());
        long taskId = taskDAO.save(taskSpec);
        taskIds.add(taskId);
        LOG.info("Created dataCompleteness task {} with taskId {}", taskSpec, taskId);
      }
    } catch (Exception e) {
      LOG.error("Exception in creating data completeness tasks", e);
    }
    return taskIds;

  }


}
