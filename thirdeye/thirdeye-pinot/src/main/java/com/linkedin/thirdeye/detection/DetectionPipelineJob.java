package com.linkedin.thirdeye.detection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DetectionPipelineJob implements Job {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionPipelineJob.class);
  private TaskManager taskDAO = DAORegistry.getInstance().getTaskDAO();
  private DetectionConfigManager detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    JobKey jobKey = jobExecutionContext.getJobDetail().getKey();
    Long id = Long.parseLong(jobKey.getName());
    DetectionConfigDTO configDTO = detectionDAO.findById(id);
    DetectionPipelineTaskInfo taskInfo = new DetectionPipelineTaskInfo(configDTO.getId(), configDTO.getLastTimestamp() + 1, System.currentTimeMillis());

    String taskInfoJson = null;
    try {
      taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
    } catch (JsonProcessingException e) {
      LOG.error("Exception when converting AlertTaskInfo {} to jsonString", taskInfo, e);
    }

    TaskDTO taskDTO = new TaskDTO();
    taskDTO.setTaskType(TaskConstants.TaskType.DETECTION);
    taskDTO.setJobName(String.format("%s_%d", TaskConstants.TaskType.DETECTION, id));
    taskDTO.setStatus(TaskConstants.TaskStatus.WAITING);
    taskDTO.setStartTime(System.currentTimeMillis());
    taskDTO.setTaskInfo(taskInfoJson);

    long taskId = taskDAO.save(taskDTO);
    LOG.info("Created detection pipeline task {} with taskId {}", taskDTO, taskId);

  }
}


