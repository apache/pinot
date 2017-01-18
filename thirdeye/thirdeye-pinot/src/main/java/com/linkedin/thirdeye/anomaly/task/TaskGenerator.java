package com.linkedin.thirdeye.anomaly.task;

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
import com.linkedin.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessType;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessJobContext;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessTaskInfo;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

/**
 * Generates tasks for a job depending on the task type
 */
public class TaskGenerator {


  public List<DetectionTaskInfo> createDetectionTasks(DetectionJobContext detectionJobContext,
      DateTime monitoringWindowStartTime, DateTime monitoringWindowEndTime)
      throws Exception {

    List<DetectionTaskInfo> tasks = new ArrayList<>();
    AnomalyFunctionDTO anomalyFunctionSpec = detectionJobContext.getAnomalyFunctionSpec();

    long jobExecutionId = detectionJobContext.getJobExecutionId();
    // generate tasks
    String exploreDimensionsString = anomalyFunctionSpec.getExploreDimensions();
    if (StringUtils.isBlank(exploreDimensionsString)) {
      DetectionTaskInfo taskInfo = new DetectionTaskInfo(jobExecutionId,
          monitoringWindowStartTime, monitoringWindowEndTime, anomalyFunctionSpec, null);
      tasks.add(taskInfo);
    } else {
      DetectionTaskInfo taskInfo =
          new DetectionTaskInfo(jobExecutionId, monitoringWindowStartTime, monitoringWindowEndTime, anomalyFunctionSpec,
              exploreDimensionsString);
        tasks.add(taskInfo);
    }

    return tasks;

  }

  public List<AlertTaskInfo> createAlertTasks(AlertJobContext alertJobContext, DateTime monitoringWindowStartTime,
      DateTime monitoringWindowEndTime)
      throws Exception{

    List<AlertTaskInfo> tasks = new ArrayList<>();
    EmailConfigurationDTO alertConfig = alertJobContext.getAlertConfig();
    long jobExecutionId = alertJobContext.getJobExecutionId();


    AlertTaskInfo taskInfo = new AlertTaskInfo(jobExecutionId, monitoringWindowStartTime, monitoringWindowEndTime, alertConfig);
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


  public List<DataCompletenessTaskInfo> createDataCompletenessTasks(DataCompletenessJobContext dataCompletenessJobContext) {
    List<DataCompletenessTaskInfo> tasks = new ArrayList<>();

    // create 1 task, which will get data and perform check
    DataCompletenessTaskInfo dataCompletenessCheck = new DataCompletenessTaskInfo();
    dataCompletenessCheck.setDataCompletenessType(DataCompletenessType.CHECKER);
    dataCompletenessCheck.setDataCompletenessStartTime(dataCompletenessJobContext.getCheckDurationStartTime());
    dataCompletenessCheck.setDataCompletenessEndTime(dataCompletenessJobContext.getCheckDurationEndTime());
    tasks.add(dataCompletenessCheck);

    // create 1 task, for cleanup
    DataCompletenessTaskInfo cleanup = new DataCompletenessTaskInfo();
    cleanup.setDataCompletenessType(DataCompletenessType.CLEANUP);
    tasks.add(cleanup);

    return tasks;
  }
}
