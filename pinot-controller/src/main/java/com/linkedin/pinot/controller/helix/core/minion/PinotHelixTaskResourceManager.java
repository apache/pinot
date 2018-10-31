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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskConstants;
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

  // Do not change this because Helix uses the same separator
  public static final String TASK_NAME_SEPARATOR = "_";

  private static final String TASK_QUEUE_PREFIX = "TaskQueue" + TASK_NAME_SEPARATOR;
  private static final String TASK_PREFIX = "Task" + TASK_NAME_SEPARATOR;

  private final TaskDriver _taskDriver;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public PinotHelixTaskResourceManager(@Nonnull HelixManager helixManager) {
    _taskDriver = new TaskDriver(helixManager);
    _propertyStore = helixManager.getHelixPropertyStore();
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
      JobQueue jobQueue = new JobQueue.Builder(helixJobQueueName).setWorkflowConfig(
          new WorkflowConfig.Builder().setParallelJobs(Integer.MAX_VALUE).build()).build();
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
    // NOTE: There is a Helix bug that causes task contexts not removed properly. Need to explicitly remove them
    // TODO: After Helix bug gets fixed, remove the extra logic

    String helixJobQueueName = getHelixJobQueueName(taskType);
    Set<String> helixJobsBeforeCleaningUp = _taskDriver.getWorkflowConfig(helixJobQueueName).getJobDag().getAllNodes();
    if (helixJobsBeforeCleaningUp.isEmpty()) {
      return;
    }

    LOGGER.info("Cleaning up task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.cleanupJobQueue(helixJobQueueName);

    // Explicitly remove the task contexts
    Set<String> helixJobsAfterCleaningUp = _taskDriver.getWorkflowConfig(helixJobQueueName).getJobDag().getAllNodes();
    for (String helixJobName : helixJobsBeforeCleaningUp) {
      if (!helixJobsAfterCleaningUp.contains(helixJobName)) {
        _propertyStore.remove(TaskConstants.REBALANCER_CONTEXT_ROOT + "/" + helixJobName, AccessOption.PERSISTENT);
      }
    }
  }

  /**
   * Stop the task queue for the given task type.
   *
   * @param taskType Task type
   */
  public synchronized void stopTaskQueue(@Nonnull String taskType) throws InterruptedException {
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
    _taskDriver.delete(helixJobQueueName);
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
   * @return Name of the submitted parent task
   */
  @Nonnull
  public synchronized String submitTask(@Nonnull List<PinotTaskConfig> pinotTaskConfigs) {
    return submitTask(pinotTaskConfigs, CommonConstants.Minion.UNTAGGED_INSTANCE);
  }

  /**
   * Submit a list of child tasks with same task type to the Minion instances with the given tag.
   *
   * @param pinotTaskConfigs List of child task configs to be submitted
   * @param minionInstanceTag Tag of the Minion instances to submit the task to
   * @return Name of the submitted parent task
   */
  @Nonnull
  public synchronized String submitTask(@Nonnull List<PinotTaskConfig> pinotTaskConfigs,
      @Nonnull String minionInstanceTag) {
    int numChildTasks = pinotTaskConfigs.size();
    Preconditions.checkState(numChildTasks > 0);

    String taskType = pinotTaskConfigs.get(0).getTaskType();
    String parentTaskName = TASK_PREFIX + taskType + TASK_NAME_SEPARATOR + System.nanoTime();
    LOGGER.info(
        "Submitting parent task: {} of type: {} with {} child task configs: {} to Minion instances with tag: {}",
        parentTaskName, taskType, numChildTasks, pinotTaskConfigs, minionInstanceTag);
    List<TaskConfig> helixTaskConfigs = new ArrayList<>(numChildTasks);
    for (int i = 0; i < numChildTasks; i++) {
      PinotTaskConfig pinotTaskConfig = pinotTaskConfigs.get(i);
      Preconditions.checkState(pinotTaskConfig.getTaskType().equals(taskType));
      helixTaskConfigs.add(pinotTaskConfig.toHelixTaskConfig(parentTaskName + TASK_NAME_SEPARATOR + i));
    }
    JobConfig.Builder jobBuilder = new JobConfig.Builder().setInstanceGroupTag(minionInstanceTag)
        .addTaskConfigs(helixTaskConfigs)
        .setNumConcurrentTasksPerInstance(Integer.MAX_VALUE);
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
