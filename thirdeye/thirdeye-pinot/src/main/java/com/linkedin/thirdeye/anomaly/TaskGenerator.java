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
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskGenerator {

  private static Logger LOG = LoggerFactory.getLogger(TaskGenerator.class);



  public List<TaskInfo> createTasks(ThirdEyeJobContext thirdeyeJobContext, AnomalyFunctionSpec anomalyFunctionSpec)
      throws Exception{

    List<TaskInfo> tasks = new ArrayList<>();

    DateTime windowStart = thirdeyeJobContext.getWindowStart();
    DateTime windowEnd = thirdeyeJobContext.getWindowEnd();
    long jobExecutionId = thirdeyeJobContext.getJobExecutionId();
    // generate tasks
    String exploreDimensionsString = anomalyFunctionSpec.getExploreDimensions();
    if (StringUtils.isBlank(exploreDimensionsString)) {
      TaskInfo taskInfo = new TaskInfo();
      taskInfo.setJobExecutionId(jobExecutionId);
      taskInfo.setWindowStartTime(windowStart);
      taskInfo.setWindowEndTime(windowEnd);
      taskInfo.setAnomalyFunctionSpec(anomalyFunctionSpec);
      tasks.add(taskInfo);
    } else {
      List<String> exploreDimensions = Arrays.asList(exploreDimensionsString.split(","));
      for (String exploreDimension : exploreDimensions) {
        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setJobExecutionId(jobExecutionId);
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
