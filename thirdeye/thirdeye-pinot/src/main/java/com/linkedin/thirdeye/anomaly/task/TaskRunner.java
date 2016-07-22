package com.linkedin.thirdeye.anomaly.task;

import java.util.List;

/**
 * Interface for task runner of various types of executors
 */
public interface TaskRunner {

  List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception;


}
