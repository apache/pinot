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
