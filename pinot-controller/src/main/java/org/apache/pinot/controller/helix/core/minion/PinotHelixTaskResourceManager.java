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
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>PinotHelixTaskResourceManager</code> manages all the task resources in Pinot cluster.
 * In case you are wondering why methods that access taskDriver are synchronized, see comment in PR #1437
 */
public class PinotHelixTaskResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixTaskResourceManager.class);

  // Do not change this because Helix uses the same separator
  public static final String TASK_NAME_SEPARATOR = "_";

  private static final String TASK_QUEUE_PREFIX = "TaskQueue" + TASK_NAME_SEPARATOR;
  private static final String TASK_PREFIX = "Task" + TASK_NAME_SEPARATOR;

  private final TaskDriver _taskDriver;

  public PinotHelixTaskResourceManager(TaskDriver taskDriver) {
    _taskDriver = taskDriver;
  }

  /**
   * Get all task types.
   * @note: It reads all resource config back and check which are workflows and which are jobs, so it can take some time
   * if there are a lot of tasks.
   * @return Set of all task types
   */
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
  public synchronized void cleanUpTaskQueue(String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Cleaning up task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.cleanupQueue(helixJobQueueName);
  }

  /**
   * Stop the task queue for the given task type.
   *
   * @param taskType Task type
   */
  public synchronized void stopTaskQueue(String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Stopping task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.stop(helixJobQueueName);
  }

  /**
   * Resume the task queue for the given task type.
   *
   * @param taskType Task type
   */
  public synchronized void resumeTaskQueue(String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    LOGGER.info("Resuming task queue: {} for task type: {}", helixJobQueueName, taskType);
    _taskDriver.resume(helixJobQueueName);
  }

  /**
   * Delete the task queue for the given task type.
   *
   * @param taskType Task type
   * @param forceDelete Expert only option to force deleting the task queue without going through the Helix transitions.
   *                    CAUTION: if set true, workflow and all of its jobs' related ZNodes will be deleted immediately,
   *                    no matter whether there are jobs running or not.
   *                    Enabling this option can cause ZooKeeper delete failure as Helix might inadvertently try to
   *                    write the deleted ZNodes back to ZooKeeper. Also this option might corrupt Task Framework cache.
   */
  public synchronized void deleteTaskQueue(String taskType, boolean forceDelete) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    if (forceDelete) {
      LOGGER.warn("Force deleting task queue: {} for task type: {}", helixJobQueueName, taskType);
    } else {
      LOGGER.info("Deleting task queue: {} for task type: {}", helixJobQueueName, taskType);
    }
    _taskDriver.delete(helixJobQueueName, forceDelete);
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
  public synchronized TaskState getTaskQueueState(String taskType) {
    return _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getWorkflowState();
  }

  /**
   * Submit a list of child tasks with same task type to the Minion instances with the default tag.
   *
   * @param pinotTaskConfigs List of child task configs to be submitted
   * @param taskTimeoutMs Timeout in milliseconds for each task
   * @param numConcurrentTasksPerInstance Maximum number of concurrent tasks allowed per instance
   * @return Name of the submitted parent task
   */
  public synchronized String submitTask(List<PinotTaskConfig> pinotTaskConfigs, long taskTimeoutMs,
      int numConcurrentTasksPerInstance) {
    return submitTask(pinotTaskConfigs, Helix.UNTAGGED_MINION_INSTANCE, taskTimeoutMs, numConcurrentTasksPerInstance);
  }

  /**
   * Submit a list of child tasks with same task type to the Minion instances with the given tag.
   *
   * @param pinotTaskConfigs List of child task configs to be submitted
   * @param minionInstanceTag Tag of the Minion instances to submit the task to
   * @param taskTimeoutMs Timeout in milliseconds for each task
   * @param numConcurrentTasksPerInstance Maximum number of concurrent tasks allowed per instance
   * @return Name of the submitted parent task
   */
  public synchronized String submitTask(List<PinotTaskConfig> pinotTaskConfigs, String minionInstanceTag,
      long taskTimeoutMs, int numConcurrentTasksPerInstance) {
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
            .setTimeoutPerTask(taskTimeoutMs).setNumConcurrentTasksPerInstance(numConcurrentTasksPerInstance)
            .setIgnoreDependentJobFailure(true).setMaxAttemptsPerTask(1).setFailureThreshold(Integer.MAX_VALUE);
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
  public synchronized Set<String> getTasks(String taskType) {
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
  public synchronized Map<String, TaskState> getTaskStates(String taskType) {
    Map<String, TaskState> helixJobStates = new HashMap<>();
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType));

    if (workflowContext == null) {
      return helixJobStates;
    }
    helixJobStates = workflowContext.getJobStates();
    Map<String, TaskState> taskStates = new HashMap<>(helixJobStates.size());
    for (Map.Entry<String, TaskState> entry : helixJobStates.entrySet()) {
      taskStates.put(getPinotTaskName(entry.getKey()), entry.getValue());
    }
    return taskStates;
  }

  /**
   * This method returns a count of sub-tasks in various states, given the top-level task name.
   * @param parentTaskName (e.g. "Task_TestTask_1624403781879")
   * @return TaskCount object
   */
  public synchronized TaskCount getTaskCount(String parentTaskName) {
    TaskCount taskCount = new TaskCount();
    JobContext jobContext = _taskDriver.getJobContext(getHelixJobName(parentTaskName));

    if (jobContext == null) {
      return taskCount;
    }
    Set<Integer> partitionSet = jobContext.getPartitionSet();
    taskCount.addToTotal(partitionSet.size());
    for (int partition : partitionSet) {
      TaskPartitionState state = jobContext.getPartitionState(partition);
      // Helix returns state as null if the task is not enqueued anywhere yet
      if (state == null) {
        // task is not yet assigned to a participant
        taskCount.addToWaiting(1);
      } else if (state.equals(TaskPartitionState.INIT) || state.equals(TaskPartitionState.RUNNING)) {
        taskCount.addToRunning(1);
      } else if (state.equals(TaskPartitionState.TASK_ERROR)) {
        taskCount.addToError(1);
      }
    }
    return taskCount;
  }

  /**
   * Returns a set of Task names (in the form "Task_TestTask_1624403781879") that are in progress or not started yet.
   *
   * @param taskType
   * @return Set of task names
   */
  public synchronized Set<String> getTasksInProgress(String taskType) {
    Set<String> tasksInProgress = new HashSet<>();
    Map<String, TaskState> helixJobStates =
        _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getJobStates();

    for (Map.Entry<String, TaskState> entry : helixJobStates.entrySet()) {
      if (entry.getValue().equals(TaskState.NOT_STARTED) || entry.getValue().equals(TaskState.IN_PROGRESS)) {
        tasksInProgress.add(getPinotTaskName(entry.getKey()));
      }
    }
    return tasksInProgress;
  }

  /**
   * Get the task state for the given task name.
   *
   * @param taskName Task name
   * @return Task state
   */
  public synchronized TaskState getTaskState(String taskName) {
    String taskType = getTaskType(taskName);
    return _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType)).getJobState(getHelixJobName(taskName));
  }

  /**
   * Get the child task configs for the given task name.
   *
   * @param taskName Task name
   * @return List of child task configs
   */
  public synchronized List<PinotTaskConfig> getTaskConfigs(String taskName) {
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
  public static String getHelixJobQueueName(String taskType) {
    return TASK_QUEUE_PREFIX + taskType;
  }

  /**
   * Helper method to convert Pinot task name to Helix Job name with JobQueue prefix.
   * <p>E.g. Task_DummyTask_12345 -> TaskQueue_DummyTask_Task_DummyTask_12345
   *
   * @param pinotTaskName Pinot task name
   * @return helixJobName Helix Job name
   */
  private static String getHelixJobName(String pinotTaskName) {
    return getHelixJobQueueName(getTaskType(pinotTaskName)) + TASK_NAME_SEPARATOR + pinotTaskName;
  }

  /**
   * Helper method to convert Helix Job name with JobQueue prefix to Pinot task name.
   * <p>E.g. TaskQueue_DummyTask_Task_DummyTask_12345 -> Task_DummyTask_12345
   *
   * @param helixJobName Helix Job name
   * @return Pinot task name
   */
  private static String getPinotTaskName(String helixJobName) {
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
  private static String getTaskType(String name) {
    return name.split(TASK_NAME_SEPARATOR)[1];
  }

  public static class TaskCount {
    private int _waiting;   // Number of tasks waiting to be scheduled on minions
    private int _error;     // Number of tasks in error
    private int _running;   // Number of tasks currently running in minions
    private int _total;     // Total number of tasks in the batch

    public TaskCount() {
    }

    public void addToWaiting(int waiting) {
      _waiting += waiting;
    }

    public void addToRunning(int running) {
      _running += running;
    }

    public void addToTotal(int total) {
      _total += total;
    }

    public void addToError(int error) {
      _error += error;
    }

    public int getWaiting() {
      return _waiting;
    }

    public int getRunning() {
      return _running;
    }

    public int getTotal() {
      return _total;
    }

    public int getError() {
      return _error;
    }

    public void accumulate(TaskCount other) {
      addToWaiting(other.getWaiting());
      addToRunning(other.getRunning());
      addToError(other.getError());
      addToTotal(other.getTotal());
    }
  }
}
