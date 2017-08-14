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

    // TODO: Currently generates 1 task for updating all the completed jobs
    // We might need to create more tasks and assign only certain number of updations to each (say 5k)
    MonitorTaskInfo updateTaskInfo = new MonitorTaskInfo();
    updateTaskInfo.setMonitorType(MonitorType.UPDATE);
    tasks.add(updateTaskInfo);

    MonitorConfiguration monitorConfiguration = monitorJobContext.getMonitorConfiguration();
    MonitorTaskInfo expireTaskInfo = new MonitorTaskInfo();
    expireTaskInfo.setMonitorType(MonitorType.EXPIRE);
    expireTaskInfo.setExpireDaysAgo(monitorConfiguration.getExpireDaysAgo());
    tasks.add(expireTaskInfo);

    return tasks;
  }

  public List<ClassificationTaskInfo> createGroupingTasks(ClassificationJobContext classificationJobContext,
      long monitoringWindowStartTime, long monitoringWindowEndTime) throws Exception {
    long jobexecutionId = classificationJobContext.getJobExecutionId();
    ClassificationConfigDTO groupingConfig = classificationJobContext.getConfigDTO();
    ClassificationTaskInfo classificationTaskInfo =
        new ClassificationTaskInfo(jobexecutionId, monitoringWindowStartTime, monitoringWindowEndTime,
            groupingConfig);

    List<ClassificationTaskInfo> tasks = new ArrayList<>();
    tasks.add(classificationTaskInfo);
    return tasks;
  }

}
