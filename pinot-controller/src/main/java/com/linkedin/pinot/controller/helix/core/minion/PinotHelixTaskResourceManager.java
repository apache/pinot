/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix.core.minion;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>PinotHelixTaskResourceManager</code> manages all the task resources in Pinot cluster.
 */
public class PinotHelixTaskResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixTaskResourceManager.class);

  public static final String TASK_NAME_SEPARATOR = "_";

  private static final String TASK_QUEUE_PREFIX = "TaskQueue" + TASK_NAME_SEPARATOR;
  private static final String TASK_PREFIX = "Task" + TASK_NAME_SEPARATOR;

  private final TaskDriver _taskDriver;

  public PinotHelixTaskResourceManager(@Nonnull TaskDriver taskDriver) {
    _taskDriver = taskDriver;
  }

  /**
   * Create a task queue of the given task type.
   *
   * @param taskType Task type
   */
  public void createTaskQueue(@Nonnull String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Creating task queue: {} for task type: {}", helixJobQueueName, taskType);

    // Set full parallelism
    JobQueue jobQueue = new JobQueue.Builder(helixJobQueueName).setWorkflowConfig(
        new WorkflowConfig.Builder().setParallelJobs(Integer.MAX_VALUE).build()).build();
    _taskDriver.createQueue(jobQueue);
  }

  /**
   * Stop a task queue of the given task type.
   *
   * @param taskType Task type
   */
  public void stopTaskQueue(@Nonnull String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Stopping task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.stop(helixJobQueueName);
  }

  /**
   * Resume a task queue of the given task type.
   *
   * @param taskType Task type
   */
  public void resumeTaskQueue(@Nonnull String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Resuming task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.resume(helixJobQueueName);
  }

  /**
   * Delete a task queue of the given task type.
   *
   * @param taskType Task type
   */
  public void deleteTaskQueue(@Nonnull String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Deleting task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.delete(helixJobQueueName);
  }

  /**
   * Submit a task to the Minion instances with the default tag.
   *
   * @param pinotTaskConfig Task config of the task to be submitted
   * @return Name of the submitted task
   */
  @Nonnull
  public String submitTask(@Nonnull PinotTaskConfig pinotTaskConfig) {
    return submitTask(pinotTaskConfig, CommonConstants.Helix.UNTAGGED_MINION_INSTANCE);
  }

  /**
   * Submit a task to the Minion instances with the given tag.
   *
   * @param pinotTaskConfig Task config of the task to be submitted
   * @param minionInstanceTag Tag of the Minion instances to submit the task to
   * @return Name of the submitted task
   */
  @Nonnull
  public String submitTask(@Nonnull PinotTaskConfig pinotTaskConfig, @Nonnull String minionInstanceTag) {
    String taskType = pinotTaskConfig.getTaskType();
    String taskName = TASK_PREFIX + taskType + TASK_NAME_SEPARATOR + System.nanoTime();
    LOGGER.info("Submitting task: {} of type: {} with config: {} to Minion instances with tag: {}", taskName, taskType,
        pinotTaskConfig, minionInstanceTag);
    JobConfig.Builder jobBuilder = new JobConfig.Builder().setInstanceGroupTag(minionInstanceTag)
        .addTaskConfigs(Collections.singletonList(pinotTaskConfig.toHelixTaskConfig(taskName)));
    _taskDriver.enqueueJob(getHelixJobQueueName(taskType), taskName, jobBuilder);
    return taskName;
  }

  /**
   * Get all task states for the given task type.
   *
   * @param taskType Task type
   * @return Map from task name to task state
   */
  @Nonnull
  public Map<String, TaskState> getTaskStates(@Nonnull String taskType) {
    Map<String, TaskState> helixJobStates =
        _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getJobStates();
    Map<String, TaskState> taskStates = new HashMap<>(helixJobStates.size());
    for (Map.Entry<String, TaskState> entry : helixJobStates.entrySet()) {
      taskStates.put(getPinotTaskName(taskType, entry.getKey()), entry.getValue());
    }
    return taskStates;
  }

  /**
   * Get the task config for the given submitted task name.
   *
   * @param taskName Name of the submitted task
   * @return Task config of the submitted task
   */
  @Nonnull
  public PinotTaskConfig getTaskConfig(@Nonnull String taskName) {
    return PinotTaskConfig.fromHelixTaskConfig(
        _taskDriver.getJobConfig(getHelixJobName(getTaskType(taskName), taskName)).getTaskConfig(taskName));
  }

  /**
   * Helper method to convert task type to Helix JobQueue name.
   * <p>E.g. DummyTask -> TaskQueue_DummyTask
   */
  @Nonnull
  protected static String getHelixJobQueueName(@Nonnull String taskType) {
    return TASK_QUEUE_PREFIX + taskType;
  }

  /**
   * Helper method to convert Pinot task name to Helix Job name with JobQueue prefix based on the given task type.
   * <p>E.g. Task_DummyTask_12345 (type DummyTask) -> TaskQueue_DummyTask_Task_DummyTask_12345
   */
  @Nonnull
  private static String getHelixJobName(@Nonnull String taskType, @Nonnull String pinotTaskName) {
    return TASK_QUEUE_PREFIX + taskType + "_" + pinotTaskName;
  }

  /**
   * Helper method to convert Helix Job name with JobQueue prefix to Pinot task name based on the given task type.
   * <p>E.g. TaskQueue_DummyTask_Task_DummyTask_12345 (type DummyTask) -> Task_DummyTask_12345
   */
  @Nonnull
  private static String getPinotTaskName(@Nonnull String taskType, @Nonnull String helixJobName) {
    return helixJobName.substring(TASK_QUEUE_PREFIX.length() + taskType.length() + 1);
  }

  /**
   * Helper method to extract task type from Pinot task name.
   * <p>E.g. Task_DummyTask_12345 -> DummyTask
   */
  @Nonnull
  private static String getTaskType(@Nonnull String pinotTaskName) {
    return pinotTaskName.split(TASK_NAME_SEPARATOR)[1];
  }
}
