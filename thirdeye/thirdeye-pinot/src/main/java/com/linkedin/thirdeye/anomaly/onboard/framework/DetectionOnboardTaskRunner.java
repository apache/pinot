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

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import java.util.concurrent.Callable;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DetectionOnboardTaskRunner implements Callable<DetectionOnboardTaskStatus> {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionOnboardTaskRunner.class);

  private final DetectionOnboardTask task;

  public DetectionOnboardTaskRunner(DetectionOnboardTask task) {
    Preconditions.checkNotNull(task);
    this.task = task;
  }

  @Override
  public DetectionOnboardTaskStatus call() throws Exception {
    DetectionOnboardTaskStatus taskStatus = new DetectionOnboardTaskStatus(task.getTaskName());
    taskStatus.setTaskStatus(TaskConstants.TaskStatus.RUNNING);

    try {
      task.run();
      taskStatus.setTaskStatus(TaskConstants.TaskStatus.COMPLETED);
    } catch (Exception e) {
      taskStatus.setTaskStatus(TaskConstants.TaskStatus.FAILED);
      taskStatus.setMessage(ExceptionUtils.getStackTrace(e));
      LOG.error("Error encountered when running task: {}", task.getTaskName(), e);
    }

    return taskStatus;
  }
}
