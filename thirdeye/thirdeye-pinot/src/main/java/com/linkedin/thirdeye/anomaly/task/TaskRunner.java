package com.linkedin.thirdeye.anomaly.task;

import java.util.List;

public interface TaskRunner {

  List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception;


}
