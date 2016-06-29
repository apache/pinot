package com.linkedin.thirdeye.anomaly;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;

public class TaskGenerator {

  private static Logger LOG = LoggerFactory.getLogger(TaskGenerator.class);

  private DateTime windowStart;
  private DateTime windowEnd;


  public List<TaskInfo> createTasks(AnomalyFunctionSpec anomalyFunctionSpec, ThirdEyeJobContext thirdeyeJobContext, long jobExecutionId){

    List<TaskInfo> tasks = new ArrayList<>();

    String windowEndProp = thirdeyeJobContext.getWindowEndIso();
    String windowStartProp = thirdeyeJobContext.getWindowStartIso();

    // Compute window end
    if (windowEndProp == null) {
      long delayMillis = 0;
      if (anomalyFunctionSpec.getWindowDelay() != null) {
        delayMillis =
            TimeUnit.MILLISECONDS.convert(anomalyFunctionSpec.getWindowDelay(), anomalyFunctionSpec.getWindowDelayUnit());
      }
      Date scheduledFireTime = thirdeyeJobContext.getScheduledFireTime();
      windowEnd = new DateTime(scheduledFireTime).minus(delayMillis);
    } else {
      windowEnd = ISODateTimeFormat.dateTimeParser().parseDateTime(windowEndProp);
    }

    // Compute window start
    if (windowStartProp == null) {
      int windowSize = anomalyFunctionSpec.getWindowSize();
      TimeUnit windowUnit = anomalyFunctionSpec.getWindowUnit();
      long windowMillis = TimeUnit.MILLISECONDS.convert(windowSize, windowUnit);
      windowStart = windowEnd.minus(windowMillis);
    } else {
      windowStart = ISODateTimeFormat.dateTimeParser().parseDateTime(windowStartProp);
    }

    // generate tasks
    String exploreDimensionsString = anomalyFunctionSpec.getExploreDimensions();
    if (StringUtils.isBlank(exploreDimensionsString)) {
      TaskInfo taskInfo = new TaskInfo();
      taskInfo.setJobExecutionId(jobExecutionId);
      taskInfo.setJobName(String.format("function_id_%d_job_execution_id_%d", anomalyFunctionSpec.getId(), jobExecutionId));
      taskInfo.setWindowStartTime(windowStart);
      taskInfo.setWindowEndTime(windowEnd);
      taskInfo.setAnomalyFunctionSpec(anomalyFunctionSpec);
      tasks.add(taskInfo);
    } else {
      List<String> exploreDimensions = Arrays.asList(exploreDimensionsString.split(","));
      for (String exploreDimension : exploreDimensions) {
        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setJobExecutionId(jobExecutionId);
        taskInfo.setJobName(String.format("function_id_%d_job_execution_id_%d", anomalyFunctionSpec.getId(), jobExecutionId));
        taskInfo.setWindowStartTime(windowStart);
        taskInfo.setWindowEndTime(windowEnd);
        taskInfo.setAnomalyFunctionSpec(anomalyFunctionSpec);
        taskInfo.setGroupByDimension(exploreDimension);
        tasks.add(taskInfo);
      }
    }

    return tasks;

  }

}
