package org.apache.pinot.spi.config.table.task;

import java.util.Map;


/**
 * To add a new task type,
 * 1. Add the new enum value here.
 * 2. Extend the {@link TableTaskTypeConfig} class to create a new task config class.
 *
 * To test that a new task type is correctly added,
 * 1. <TODO>
 */
public enum TaskType {
  FileIngestionTask;

  public static boolean isValidTaskType(String taskType) {
    try {
      TaskType.valueOf(taskType);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static TableTaskTypeConfig createTaskTypeConfig(String taskType, Map<String, String> configs) {
    if (!isValidTaskType(taskType)) {
      throw new IllegalArgumentException("Invalid task type: " + taskType);
    }
    switch (TaskType.valueOf(taskType)) {
      case FileIngestionTask:
        return new SegmentRefreshTaskConfig(configs);
      default:
        throw new IllegalStateException("Unexpected task type: " + taskType);
    }
  }

}
