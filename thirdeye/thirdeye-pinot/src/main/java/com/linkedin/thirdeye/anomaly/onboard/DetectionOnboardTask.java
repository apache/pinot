package com.linkedin.thirdeye.anomaly.onboard;


public interface DetectionOnboardTask {

  /**
   * Returns the unique name of the task.
   *
   * @return the unique name of the task.
   */
  String getTaskName();

  /**
   * Sets the task context of this task.
   *
   * @param taskContext the task context of this task.
   */
  void setTaskContext(DetectionOnboardTaskContext taskContext);

  /**
   * Returns the task context of this task.
   * @return the task context of this task.
   */
  DetectionOnboardTaskContext getTaskContext();

  /**
   * Executes the task. To fail this task, throw exceptions. The job executor will catch the exception and store
   * it in the message in the execution status of this task.
   */
  void run();
}
