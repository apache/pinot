package com.linkedin.thirdeye.anomaly.task;

import com.linkedin.thirdeye.anomaly.classification.ClassificationTaskInfo;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.alert.AlertTaskInfo;
import com.linkedin.thirdeye.anomaly.detection.DetectionTaskInfo;
import com.linkedin.thirdeye.anomaly.monitor.MonitorTaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessTaskInfo;

/**
 * This class returns deserializes the task info json and returns the TaskInfo,
 * depending on the task type
 */
public class TaskInfoFactory {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(TaskInfoFactory.class);

  public static TaskInfo getTaskInfoFromTaskType(TaskType taskType, String taskInfoString) throws IOException {
    TaskInfo taskInfo = null;
    try {
      switch(taskType) {
        case ANOMALY_DETECTION:
          taskInfo = OBJECT_MAPPER.readValue(taskInfoString, DetectionTaskInfo.class);
          break;
        case MERGE:
          LOG.error("TaskType MERGE not supported");
          break;
        case MONITOR:
          taskInfo = OBJECT_MAPPER.readValue(taskInfoString, MonitorTaskInfo.class);
          break;
        case ALERT:
        case ALERT2:
          taskInfo = OBJECT_MAPPER.readValue(taskInfoString, AlertTaskInfo.class);
          break;
        case DATA_COMPLETENESS:
          taskInfo = OBJECT_MAPPER.readValue(taskInfoString, DataCompletenessTaskInfo.class);
          break;
        case CLASSIFICATION:
          taskInfo = OBJECT_MAPPER.readValue(taskInfoString, ClassificationTaskInfo.class);
          break;
        default:
          LOG.error("TaskType must be one of ANOMALY_DETECTION, MONITOR, ALERT, ALERT2, DATA_COMPLETENESS, CLASSIFICATION");
          break;
      }
    } catch (Exception e) {
      LOG.error("Exception in converting taskInfoString {} to taskType {}", taskInfoString, taskType, e);
    }
    return taskInfo;
  }

}
