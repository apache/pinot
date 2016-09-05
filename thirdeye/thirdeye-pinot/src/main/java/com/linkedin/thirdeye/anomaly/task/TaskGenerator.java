package com.linkedin.thirdeye.anomaly.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.alert.AlertJobContext;
import com.linkedin.thirdeye.anomaly.alert.AlertTaskInfo;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobContext;
import com.linkedin.thirdeye.anomaly.detection.DetectionTaskInfo;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConfiguration;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConstants.MonitorType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.anomaly.monitor.MonitorJobContext;
import com.linkedin.thirdeye.anomaly.monitor.MonitorTaskInfo;

/**
 * Generates tasks for a job depending on the task type
 */
public class TaskGenerator {

  private static Logger LOG = LoggerFactory.getLogger(TaskGenerator.class);

  public List<DetectionTaskInfo> createDetectionTasks(DetectionJobContext detectionJobContext)
      throws Exception{

    List<DetectionTaskInfo> tasks = new ArrayList<>();
    AnomalyFunctionDTO anomalyFunctionSpec = detectionJobContext.getAnomalyFunctionSpec();

    DateTime windowStartTime = detectionJobContext.getWindowStartTime();
    DateTime windowEndTime = detectionJobContext.getWindowEndTime();
    long jobExecutionId = detectionJobContext.getJobExecutionId();
    // generate tasks
    String exploreDimensionsString = anomalyFunctionSpec.getExploreDimensions();
    if (StringUtils.isBlank(exploreDimensionsString)) {
      DetectionTaskInfo taskInfo = new DetectionTaskInfo(jobExecutionId,
          windowStartTime, windowEndTime, anomalyFunctionSpec, null);
      tasks.add(taskInfo);
    } else {
      List<String> exploreDimensions = Arrays.asList(exploreDimensionsString.split(","));
      for (String exploreDimension : exploreDimensions) {
        DetectionTaskInfo taskInfo = new DetectionTaskInfo(jobExecutionId, windowStartTime, windowEndTime,
            anomalyFunctionSpec, exploreDimension);
        tasks.add(taskInfo);
      }
    }

    return tasks;

  }

  public List<AlertTaskInfo> createAlertTasks(AlertJobContext alertJobContext)
      throws Exception{

    List<AlertTaskInfo> tasks = new ArrayList<>();
    EmailConfigurationDTO alertConfig = alertJobContext.getAlertConfig();
    DateTime windowStartTime = alertJobContext.getWindowStartTime();
    DateTime windowEndTime = alertJobContext.getWindowEndTime();
    long jobExecutionId = alertJobContext.getJobExecutionId();


    AlertTaskInfo taskInfo = new AlertTaskInfo(jobExecutionId, windowStartTime, windowEndTime, alertConfig);
    tasks.add(taskInfo);
    return tasks;
  }


  public List<MonitorTaskInfo> createMonitorTasks(MonitorJobContext monitorJobContext) {
    List<MonitorTaskInfo> tasks = new ArrayList<>();
    List<JobDTO> anomalyJobSpecs = monitorJobContext.getAnomalyJobSpecs();
    for (JobDTO anomalyJobSpec : anomalyJobSpecs) {
      MonitorTaskInfo updateTaskInfo = new MonitorTaskInfo();
      updateTaskInfo.setJobExecutionId(anomalyJobSpec.getId());
      updateTaskInfo.setMonitorType(MonitorType.UPDATE);
      tasks.add(updateTaskInfo);
    }
    MonitorConfiguration monitorConfiguration = monitorJobContext.getMonitorConfiguration();
    MonitorTaskInfo expireTaskInfo = new MonitorTaskInfo();
    expireTaskInfo.setMonitorType(MonitorType.EXPIRE);
    expireTaskInfo.setExpireDaysAgo(monitorConfiguration.getExpireDaysAgo());
    tasks.add(expireTaskInfo);

    return tasks;
  }
}
