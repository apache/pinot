package com.linkedin.thirdeye.anomaly.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.detector.db.entity.AnomalyFunctionSpec;

public class TaskGenerator {

  private static Logger LOG = LoggerFactory.getLogger(TaskGenerator.class);



  public List<TaskInfo> createTasks(JobContext thirdeyeJobContext, AnomalyFunctionSpec anomalyFunctionSpec)
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
