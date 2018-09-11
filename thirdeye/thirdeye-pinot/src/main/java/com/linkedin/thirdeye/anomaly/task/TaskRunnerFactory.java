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

import com.linkedin.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import com.linkedin.thirdeye.anomaly.classification.ClassificationTaskRunner;
import com.linkedin.thirdeye.anomaly.detection.DetectionTaskRunner;
import com.linkedin.thirdeye.anomaly.monitor.MonitorTaskRunner;
import com.linkedin.thirdeye.anomaly.onboard.ReplayTaskRunner;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessTaskRunner;
import com.linkedin.thirdeye.detection.DetectionPipelineTaskRunner;
import com.linkedin.thirdeye.detection.alert.DetectionAlertTaskRunner;


/**
 * This class returns an instance of the task runner depending on the task type
 */
public class TaskRunnerFactory {

  public static TaskRunner getTaskRunnerFromTaskType(TaskType taskType) {
    TaskRunner taskRunner = null;
    switch (taskType) {
      case DETECTION:
        taskRunner = new DetectionPipelineTaskRunner();
        break;
      case DETECTION_ALERT:
        taskRunner = new DetectionAlertTaskRunner();
        break;
      case ANOMALY_DETECTION:
        taskRunner = new DetectionTaskRunner();
        break;
      case MERGE:
        break;
      case MONITOR:
        taskRunner = new MonitorTaskRunner();
        break;
      case DATA_COMPLETENESS:
        taskRunner = new DataCompletenessTaskRunner();
        break;
      case ALERT:
      case ALERT2:
        taskRunner = new AlertTaskRunnerV2();
        break;
      case CLASSIFICATION:
        taskRunner = new ClassificationTaskRunner();
        break;
      case REPLAY:
        taskRunner = new ReplayTaskRunner();
      default:
    }
    return taskRunner;
  }
}
