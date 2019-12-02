/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.minion;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.pinot.common.config.PinotTaskConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>PinotHelixTaskResourceManager</code> manages all the task resources in Pinot cluster.
 */
public class PinotHelixTaskResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixTaskResourceManager.class);

  // Do not change this because Helix uses the same separator
  public static final String TASK_NAME_SEPARATOR = "_";

  private static final String TASK_QUEUE_PREFIX = "TaskQueue" + TASK_NAME_SEPARATOR;
  private static final String TASK_PREFIX = "Task" + TASK_NAME_SEPARATOR;

  private final TaskDriver _taskDriver;

  public PinotHelixTaskResourceManager(@Nonnull TaskDriver taskDriver) {
    _taskDriver = taskDriver;
  }

  /**
   * Get all task types.
   *
   * @return Set of all task types
   */
  @Nonnull
  public synchronized Set<String> getTaskTypes() {
    Set<String> helixJobQueues = _taskDriver.getWorkflows().keySet();
    Set<String> taskTypes = new HashSet<>(helixJobQueues.size());
    for (String helixJobQueue : helixJobQueues) {
      taskTypes.add(getTaskType(helixJobQueue));
    }
    return taskTypes;
  }

  /**
   * Ensure the task queue for the given task type exists.
   *
   * @param taskType Task type
   */
  public void ensureTaskQueueExists(String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    WorkflowConfig workflowConfig = _taskDriver.getWorkflowConfig(helixJobQueueName);

    if (workflowConfig == null) {
      // Task queue does not exist
      LOGGER.info("Creating task queue: {} for task type: {}", helixJobQueueName, taskType);

      // Set full parallelism
      // Don't allow overlap job assignment so that we can control number of concurrent tasks per instance
      JobQueue jobQueue = new JobQueue.Builder(helixJobQueueName)
          .setWorkflowConfig(new WorkflowConfig.Builder().setParallelJobs(Integer.MAX_VALUE).build()).build();
      _taskDriver.createQueue(jobQueue);
    }

    // Wait until task queue context shows up
    while (_taskDriver.getWorkflowContext(helixJobQueueName) == null) {
      Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Clean up a task queue for the given task type.
   *
   * @param taskType Task type
   */
  public synchronized void cleanUpTaskQueue(@Nonnull String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Cleaning up task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.cleanupQueue(helixJobQueueName);
  }

  /**
   * Stop the task queue for the given task type.
   *
   * @param taskType Task type
   */
  public synchronized void stopTaskQueue(@Nonnull String taskType)
      throws InterruptedException {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Stopping task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.stop(helixJobQueueName);
  }

  /**
   * Resume the task queue for the given task type.
   *
   * @param taskType Task type
   */
  public synchronized void resumeTaskQueue(@Nonnull String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Resuming task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.resume(helixJobQueueName);
  }

  /**
   * Delete the task queue for the given task type.
   *
   * @param taskType Task type
   */
  public synchronized void deleteTaskQueue(@Nonnull String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Deleting task queue: {} for task type: {}", helixJobQueueName, taskType);
    // NOTE: set force delete to true to remove the task queue from ZooKeeper immediately
    _taskDriver.delete(helixJobQueueName, true);
  }

  /**
   * Get all task queues.
   *
   * @return Set of task queue names
   */
  public synchronized Set<String> getTaskQueues() {
    return _taskDriver.getWorkflows().keySet();
  }

  /**
   * Get the task queue state for the given task type.
   *
   * @param taskType Task type
   * @return Task queue state
   */
  public synchronized TaskState getTaskQueueState(@Nonnull String taskType) {
    return _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getWorkflowState();
  }

  /**
   * Submit a list of child tasks with same task type to the Minion instances with the default tag.
   *
   * @param pinotTaskConfigs List of child task configs to be submitted
   * @param numConcurrentTasksPerInstance Maximum number of concurrent tasks allowed per instance
   * @return Name of the submitted parent task
   */
  @Nonnull
  public synchronized String submitTask(@Nonnull List<PinotTaskConfig> pinotTaskConfigs,
      int numConcurrentTasksPerInstance) {
    return submitTask(pinotTaskConfigs, Helix.UNTAGGED_MINION_INSTANCE, numConcurrentTasksPerInstance);
  }

  /**
   * Submit a list of child tasks with same task type to the Minion instances with the given tag.
   *
   * @param pinotTaskConfigs List of child task configs to be submitted
   * @param minionInstanceTag Tag of the Minion instances to submit the task to
   * @param numConcurrentTasksPerInstance Maximum number of concurrent tasks allowed per instance
   * @return Name of the submitted parent task
   */
  @Nonnull
  public synchronized String submitTask(@Nonnull List<PinotTaskConfig> pinotTaskConfigs,
      @Nonnull String minionInstanceTag, int numConcurrentTasksPerInstance) {
    int numChildTasks = pinotTaskConfigs.size();
    Preconditions.checkState(numChildTasks > 0);
    Preconditions.checkState(numConcurrentTasksPerInstance > 0);

    String taskType = pinotTaskConfigs.get(0).getTaskType();
    String parentTaskName = TASK_PREFIX + taskType + TASK_NAME_SEPARATOR + System.currentTimeMillis();
    LOGGER
        .info("Submitting parent task: {} of type: {} with {} child task configs: {} to Minion instances with tag: {}",
            parentTaskName, taskType, numChildTasks, pinotTaskConfigs, minionInstanceTag);
    List<TaskConfig> helixTaskConfigs = new ArrayList<>(numChildTasks);
    for (int i = 0; i < numChildTasks; i++) {
      PinotTaskConfig pinotTaskConfig = pinotTaskConfigs.get(i);
      Preconditions.checkState(pinotTaskConfig.getTaskType().equals(taskType));
      helixTaskConfigs.add(pinotTaskConfig.toHelixTaskConfig(parentTaskName + TASK_NAME_SEPARATOR + i));
    }

    // Run each task only once no matter whether it succeeds or not, and never fail the job
    // The reason for this is that: we put multiple independent tasks into one job to get them run in parallel, so we
    // don't want one task failure affects other tasks. Also, if one task failed, next time we will re-schedule it
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().addTaskConfigs(helixTaskConfigs).setInstanceGroupTag(minionInstanceTag)
            .setNumConcurrentTasksPerInstance(numConcurrentTasksPerInstance).setIgnoreDependentJobFailure(true)
            .setMaxAttemptsPerTask(1).setFailureThreshold(Integer.MAX_VALUE);
    _taskDriver.enqueueJob(getHelixJobQueueName(taskType), parentTaskName, jobBuilder);

    // Wait until task state is available
    while (getTaskState(parentTaskName) == null) {
      Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    return parentTaskName;
  }

  /**
   * Get all tasks for the given task type.
   *
   * @param taskType Task type
   * @return Set of task names
   */
  @Nonnull
  public synchronized Set<String> getTasks(@Nonnull String taskType) {
    Set<String> helixJobs = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getJobStates().keySet();
    Set<String> tasks = new HashSet<>(helixJobs.size());
    for (String helixJobName : helixJobs) {
      tasks.add(getPinotTaskName(helixJobName));
    }
    return tasks;
  }

  /**
   * Get all task states for the given task type.
   *
   * @param taskType Task type
   * @return Map from task name to task state
   */
  @Nonnull
  public synchronized Map<String, TaskState> getTaskStates(@Nonnull String taskType) {
    Map<String, TaskState> helixJobStates =
        _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getJobStates();
    Map<String, TaskState> taskStates = new HashMap<>(helixJobStates.size());
    for (Map.Entry<String, TaskState> entry : helixJobStates.entrySet()) {
      taskStates.put(getPinotTaskName(entry.getKey()), entry.getValue());
    }
    return taskStates;
  }

  /**
   * Get the task state for the given task name.
   *
   * @param taskName Task name
   * @return Task state
   */
  public synchronized TaskState getTaskState(@Nonnull String taskName) {
    String taskType = getTaskType(taskName);
    return _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getJobState(getHelixJobName(taskName));
  }

  /**
   * Get the child task configs for the given task name.
   *
   * @param taskName Task name
   * @return List of child task configs
   */
  @Nonnull
  public synchronized List<PinotTaskConfig> getTaskConfigs(@Nonnull String taskName) {
    Collection<TaskConfig> helixTaskConfigs =
        _taskDriver.getJobConfig(getHelixJobName(taskName)).getTaskConfigMap().values();
    List<PinotTaskConfig> taskConfigs = new ArrayList<>(helixTaskConfigs.size());
    for (TaskConfig helixTaskConfig : helixTaskConfigs) {
      taskConfigs.add(PinotTaskConfig.fromHelixTaskConfig(helixTaskConfig));
    }
    return taskConfigs;
  }

  /**
   * Helper method to convert task type to Helix JobQueue name.
   * <p>E.g. DummyTask -> TaskQueue_DummyTask
   *
   * @param taskType Task type
   * @return Helix JobQueue name
   */
  @Nonnull
  public static String getHelixJobQueueName(@Nonnull String taskType) {
    return TASK_QUEUE_PREFIX + taskType;
  }

  /**
   * Helper method to convert Pinot task name to Helix Job name with JobQueue prefix.
   * <p>E.g. Task_DummyTask_12345 -> TaskQueue_DummyTask_Task_DummyTask_12345
   *
   * @param pinotTaskName Pinot task name
   * @return helixJobName Helix Job name
   */
  @Nonnull
  private static String getHelixJobName(@Nonnull String pinotTaskName) {
    return getHelixJobQueueName(getTaskType(pinotTaskName)) + TASK_NAME_SEPARATOR + pinotTaskName;
  }

  /**
   * Helper method to convert Helix Job name with JobQueue prefix to Pinot task name.
   * <p>E.g. TaskQueue_DummyTask_Task_DummyTask_12345 -> Task_DummyTask_12345
   *
   * @param helixJobName Helix Job name
   * @return Pinot task name
   */
  @Nonnull
  private static String getPinotTaskName(@Nonnull String helixJobName) {
    return helixJobName.substring(TASK_QUEUE_PREFIX.length() + getTaskType(helixJobName).length() + 1);
  }

  /**
   * Helper method to extract task type from Pinot task name, Helix JobQueue name or Helix Job name.
   * <p>E.g. Task_DummyTask_12345 -> DummyTask (from Pinot task name)
   * <p>E.g. TaskQueue_DummyTask -> DummyTask (from Helix JobQueue name)
   * <p>E.g. TaskQueue_DummyTask_Task_DummyTask_12345 -> DummyTask (from Helix Job name)
   *
   * @param name Pinot task name, Helix JobQueue name or Helix Job name
   * @return Task type
   */
  @Nonnull
  private static String getTaskType(@Nonnull String name) {
    return name.split(TASK_NAME_SEPARATOR)[1];
  }
}
