package com.linkedin.thirdeye.anomaly.grouping;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import java.util.ArrayList;
import java.util.List;

public class GroupingTaskRunner implements TaskRunner {
  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    List<TaskResult> taskResults = new ArrayList<>();

    return taskResults;
  }
}
