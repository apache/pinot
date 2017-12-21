package com.linkedin.thirdeye.anomaly.onboard;

import java.util.List;
import org.apache.commons.configuration.Configuration;

public interface DetectionOnboardJob {

  /**
   * Returns the unique name of this job.
   * @return the unique name of this job.
   */
  String getName();

  /**
   * Returns the configuration for the tasks in this job execution. The configuration should be built from the
   * properties map that is given in the initialized method. The property for each task in the built configuration
   * should has the corresponding task's name. Assume that a job has two tasks with names: "task1" and "task2",
   * respectively. The property for "task1" must have the prefix "task1.". Similarly, the configuration for "task2" have
   * the prefix "task2".
   *
   * @return the configuration for the tasks in this job execution.
   */
  Configuration getTaskConfiguration();

  /**
   * Returns the list of tasks of this job. The tasks will be executed following their order in the list.
   *
   * @return the list of tasks of this job.
   */
  List<DetectionOnboardTask> getTasks();
}
