/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.alert.AlertTaskInfo;
import com.linkedin.thirdeye.anomaly.classification.ClassificationTaskInfo;
import com.linkedin.thirdeye.anomaly.detection.DetectionTaskInfo;
import com.linkedin.thirdeye.anomaly.monitor.MonitorTaskInfo;
import com.linkedin.thirdeye.anomaly.onboard.ReplayTaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessTaskInfo;
import com.linkedin.thirdeye.detection.DetectionPipelineTaskInfo;
import com.linkedin.thirdeye.detection.alert.DetectionAlertTaskInfo;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        case DETECTION:
          taskInfo = OBJECT_MAPPER.readValue(taskInfoString, DetectionPipelineTaskInfo.class);
          break;
        case DETECTION_ALERT:
          taskInfo = OBJECT_MAPPER.readValue(taskInfoString, DetectionAlertTaskInfo.class);
          break;
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
        case REPLAY:
          taskInfo = OBJECT_MAPPER.readValue(taskInfoString, ReplayTaskInfo.class);
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
