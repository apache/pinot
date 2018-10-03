/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.onboard.framework;

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
