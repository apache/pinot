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

import com.linkedin.thirdeye.anomaly.classification.ClassificationJobContext;
import com.linkedin.thirdeye.anomaly.classification.ClassificationTaskInfo;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import com.linkedin.thirdeye.anomaly.alert.AlertJobContext;
import com.linkedin.thirdeye.anomaly.alert.AlertTaskInfo;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobContext;
import com.linkedin.thirdeye.anomaly.detection.DetectionTaskInfo;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConfiguration;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConstants.MonitorType;
import com.linkedin.thirdeye.anomaly.monitor.MonitorJobContext;
import com.linkedin.thirdeye.anomaly.monitor.MonitorTaskInfo;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;


/**
 * Generates tasks for a job depending on the task type
 */
public class TaskGenerator {

  public List<DetectionTaskInfo> createDetectionTasks(DetectionJobContext detectionJobContext,
      List<DateTime> monitoringWindowStartTimes, List<DateTime> monitoringWindowEndTimes)
      throws Exception {

    List<DetectionTaskInfo> tasks = new ArrayList<>();
    AnomalyFunctionDTO anomalyFunctionSpec = detectionJobContext.getAnomalyFunctionSpec();

    long jobExecutionId = detectionJobContext.getJobExecutionId();
    // generate tasks
    String exploreDimensionsString = anomalyFunctionSpec.getExploreDimensions();
    if (StringUtils.isBlank(exploreDimensionsString)) {
      DetectionTaskInfo taskInfo = new DetectionTaskInfo(jobExecutionId,
          monitoringWindowStartTimes, monitoringWindowEndTimes, anomalyFunctionSpec, null,
          detectionJobContext.getDetectionJobType());
      tasks.add(taskInfo);
    } else {
      DetectionTaskInfo taskInfo =
          new DetectionTaskInfo(jobExecutionId, monitoringWindowStartTimes, monitoringWindowEndTimes, anomalyFunctionSpec,
              exploreDimensionsString, detectionJobContext.getDetectionJobType());
        tasks.add(taskInfo);
    }

    return tasks;

  }

  public List<AlertTaskInfo> createAlertTasksV2(AlertJobContext alertJobContext,
      DateTime monitoringWindowStartTime, DateTime monitoringWindowEndTime) throws Exception {

    List<AlertTaskInfo> tasks = new ArrayList<>();
    AlertConfigDTO alertConfig = alertJobContext.getAlertConfigDTO();
    long jobExecutionId = alertJobContext.getJobExecutionId();

    AlertTaskInfo taskInfo =
        new AlertTaskInfo(jobExecutionId, monitoringWindowStartTime, monitoringWindowEndTime,
            alertConfig);
    tasks.add(taskInfo);
    return tasks;
  }


  public List<MonitorTaskInfo> createMonitorTasks(MonitorJobContext monitorJobContext) {
    List<MonitorTaskInfo> tasks = new ArrayList<>();
    MonitorConfiguration monitorConfiguration = monitorJobContext.getMonitorConfiguration();

    // Generates the task to updating the status of all jobs and tasks
    MonitorTaskInfo updateTaskInfo = new MonitorTaskInfo();
    updateTaskInfo.setMonitorType(MonitorType.UPDATE);
    updateTaskInfo.setCompletedJobRetentionDays(monitorConfiguration.getCompletedJobRetentionDays());
    updateTaskInfo.setDefaultRetentionDays(monitorConfiguration.getDefaultRetentionDays());
    updateTaskInfo.setDetectionStatusRetentionDays(monitorConfiguration.getDetectionStatusRetentionDays());
    updateTaskInfo.setRawAnomalyRetentionDays(monitorConfiguration.getRawAnomalyRetentionDays());
    tasks.add(updateTaskInfo);

    // Generates the task to expire (delete) old jobs and tasks in DB
    MonitorTaskInfo expireTaskInfo = new MonitorTaskInfo();
    expireTaskInfo.setMonitorType(MonitorType.EXPIRE);
    expireTaskInfo.setCompletedJobRetentionDays(monitorConfiguration.getCompletedJobRetentionDays());
    expireTaskInfo.setDefaultRetentionDays(monitorConfiguration.getDefaultRetentionDays());
    expireTaskInfo.setDetectionStatusRetentionDays(monitorConfiguration.getDetectionStatusRetentionDays());
    expireTaskInfo.setRawAnomalyRetentionDays(monitorConfiguration.getRawAnomalyRetentionDays());
    tasks.add(expireTaskInfo);

    return tasks;
  }

  public List<ClassificationTaskInfo> createGroupingTasks(ClassificationJobContext classificationJobContext,
      long monitoringWindowStartTime, long monitoringWindowEndTime) {
    long jobExecutionId = classificationJobContext.getJobExecutionId();
    ClassificationConfigDTO groupingConfig = classificationJobContext.getConfigDTO();
    ClassificationTaskInfo classificationTaskInfo =
        new ClassificationTaskInfo(jobExecutionId, monitoringWindowStartTime, monitoringWindowEndTime,
            groupingConfig);

    List<ClassificationTaskInfo> tasks = new ArrayList<>();
    tasks.add(classificationTaskInfo);
    return tasks;
  }

}
