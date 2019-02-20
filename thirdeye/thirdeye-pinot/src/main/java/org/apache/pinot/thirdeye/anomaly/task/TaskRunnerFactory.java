/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.anomaly.task;

import org.apache.pinot.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import org.apache.pinot.thirdeye.anomaly.classification.ClassificationTaskRunner;
import org.apache.pinot.thirdeye.anomaly.detection.DetectionTaskRunner;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorTaskRunner;
import org.apache.pinot.thirdeye.anomaly.onboard.ReplayTaskRunner;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskType;
import org.apache.pinot.thirdeye.completeness.checker.DataCompletenessTaskRunner;
import org.apache.pinot.thirdeye.detection.DetectionPipelineTaskRunner;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertTaskRunner;
import org.apache.pinot.thirdeye.detection.onboard.YamlOnboardingTaskRunner;


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
      case YAML_DETECTION_ONBOARD:
        taskRunner = new YamlOnboardingTaskRunner();
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
