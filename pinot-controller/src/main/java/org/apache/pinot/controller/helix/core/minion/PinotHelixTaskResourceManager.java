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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
import org.apache.pinot.common.utils.DateTimeUtils;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.TABLE_NAME;


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
    for (int partition : partitionSet) {
      TaskPartitionState state = jobContext.getPartitionState(partition);
      taskCount.addTaskState(state);
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
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType));
    if (workflowContext == null) {
      return tasksInProgress;
    }

    Map<String, TaskState> helixJobStates = workflowContext.getJobStates();

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
   * Helper method to return a map of task names to corresponding task state
   * where the task corresponds to the given Pinot table name. This is used to
   * check status of all tasks for a given table.
   * @param taskType Task Name
   * @param tableNameWithType table name with type to filter on
   * @return Map of filtered task name to corresponding state
   */
  public synchronized Map<String, TaskState> getTaskStatesByTable(String taskType, String tableNameWithType) {
    Map<String, TaskState> filteredTaskStateMap = new HashMap<>();
    Map<String, TaskState> taskStateMap = getTaskStates(taskType);

    for (Map.Entry<String, TaskState> taskState : taskStateMap.entrySet()) {
      String taskName = taskState.getKey();

      // Iterate through all task configs associated with this task name
      for (PinotTaskConfig taskConfig : getTaskConfigs(taskName)) {
        Map<String, String> pinotConfigs = taskConfig.getConfigs();

        // Filter task configs that matches this table name
        if (pinotConfigs != null) {
          String tableNameConfig = pinotConfigs.get(TABLE_NAME);
          if (tableNameConfig != null && tableNameConfig.equals(tableNameWithType)) {
            // Found a match ! Track state for this particular task in the final result map
            filteredTaskStateMap.put(taskName, taskStateMap.get(taskName));
            break;
          }
        }
      }
    }
    return filteredTaskStateMap;
  }

  /**
   * Fetch count of sub-tasks for each of the tasks for the given taskType.
   *
   * @param taskType      Pinot taskType / Helix JobQueue
   * @return Map of Pinot Task Name to TaskCount
   */
  public synchronized Map<String, TaskCount> getTaskCounts(String taskType) {
    Map<String, TaskCount> taskCounts = new TreeMap<>();
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType));
    if (workflowContext == null) {
      return taskCounts;
    }
    Map<String, TaskState> helixJobStates = workflowContext.getJobStates();
    for (String helixJobName : helixJobStates.keySet()) {
      String pinotTaskName = getPinotTaskName(helixJobName);
      taskCounts.put(pinotTaskName, getTaskCount(pinotTaskName));
    }
    return taskCounts;
  }

  /**
   * Given a taskType, helper method to debug all the HelixJobs for the taskType.
   * For each of the HelixJobs, collects status of the (sub)tasks in the taskbatch.
   *
   * @param taskType      Pinot taskType / Helix JobQueue
   * @param verbosity     By default, does not show details for completed tasks.
   *                      If verbosity > 0, shows details for all tasks.
   * @return Map of Pinot Task Name to TaskDebugInfo. TaskDebugInfo contains details for subtasks.
   */
  public synchronized Map<String, TaskDebugInfo> getTasksDebugInfo(String taskType, int verbosity) {
    Map<String, TaskDebugInfo> taskDebugInfos = new TreeMap<>();
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType));
    if (workflowContext == null) {
      return taskDebugInfos;
    }
    Map<String, TaskState> helixJobStates = workflowContext.getJobStates();
    for (String helixJobName : helixJobStates.keySet()) {
      taskDebugInfos.put(getPinotTaskName(helixJobName), getTaskDebugInfo(workflowContext, helixJobName, verbosity));
    }
    return taskDebugInfos;
  }

  /**
   * Given a taskName, collects status of the (sub)tasks in the taskName.
   *
   * @param taskName      Pinot taskName
   * @param verbosity     By default, does not show details for completed tasks.
   *                      If verbosity > 0, shows details for all tasks.
   * @return TaskDebugInfo contains details for subtasks in this task batch.
   */
  public synchronized TaskDebugInfo getTaskDebugInfo(String taskName, int verbosity) {
    String taskType = getTaskType(taskName);
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType));
    if (workflowContext == null) {
      return null;
    }
    String helixJobName = getHelixJobName(taskName);
    return getTaskDebugInfo(workflowContext, helixJobName, verbosity);
  }

  private synchronized TaskDebugInfo getTaskDebugInfo(WorkflowContext workflowContext, String helixJobName,
      int verbosity) {
    boolean showCompleted = verbosity > 0;
    TaskDebugInfo taskDebugInfo = new TaskDebugInfo();
    taskDebugInfo.setTaskState(workflowContext.getJobState(helixJobName));
    long jobStartTimeMs = workflowContext.getJobStartTime(helixJobName);
    if (jobStartTimeMs > 0) {
      taskDebugInfo.setStartTime(DateTimeUtils.epochToDefaultDateFormat(jobStartTimeMs));
    }
    JobContext jobContext = _taskDriver.getJobContext(helixJobName);
    if (jobContext != null) {
      JobConfig jobConfig = _taskDriver.getJobConfig(helixJobName);
      long jobExecutionStartTimeMs = jobContext.getExecutionStartTime();
      if (jobExecutionStartTimeMs > 0) {
        taskDebugInfo.setExecutionStartTime(DateTimeUtils.epochToDefaultDateFormat(jobExecutionStartTimeMs));
      }
      Set<Integer> partitionSet = jobContext.getPartitionSet();
      TaskCount subtaskCount = new TaskCount();
      for (int partition : partitionSet) {
        // First get the partition's state and update the subtaskCount
        TaskPartitionState partitionState = jobContext.getPartitionState(partition);
        subtaskCount.addTaskState(partitionState);
        // Skip details for COMPLETED tasks
        if (!showCompleted && partitionState == TaskPartitionState.COMPLETED) {
          continue;
        }
        SubtaskDebugInfo subtaskDebugInfo = new SubtaskDebugInfo();
        String taskIdForPartition = jobContext.getTaskIdForPartition(partition);
        subtaskDebugInfo.setTaskId(taskIdForPartition);
        subtaskDebugInfo.setState(partitionState);
        long subtaskStartTimeMs = jobContext.getPartitionStartTime(partition);
        if (subtaskStartTimeMs > 0) {
          subtaskDebugInfo.setStartTime(DateTimeUtils.epochToDefaultDateFormat(subtaskStartTimeMs));
        }
        long subtaskFinishTimeMs = jobContext.getPartitionFinishTime(partition);
        if (subtaskFinishTimeMs > 0) {
          subtaskDebugInfo.setFinishTime(DateTimeUtils.epochToDefaultDateFormat(subtaskFinishTimeMs));
        }
        subtaskDebugInfo.setParticipant(jobContext.getAssignedParticipant(partition));
        subtaskDebugInfo.setInfo(jobContext.getPartitionInfo(partition));
        TaskConfig helixTaskConfig = jobConfig.getTaskConfig(taskIdForPartition);
        if (helixTaskConfig != null) {
          PinotTaskConfig pinotTaskConfig = PinotTaskConfig.fromHelixTaskConfig(helixTaskConfig);
          subtaskDebugInfo.setTaskConfig(pinotTaskConfig);
        }
        taskDebugInfo.addSubtaskInfo(subtaskDebugInfo);
      }
      taskDebugInfo.setSubtaskCount(subtaskCount);
    }
    return taskDebugInfo;
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

  @JsonPropertyOrder({"taskState", "subtaskCount", "startTime", "executionStartTime", "subtaskInfos"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class TaskDebugInfo {
    private String _startTime;
    private String _executionStartTime;
    private TaskState _taskState;
    private TaskCount _subtaskCount;
    private List<SubtaskDebugInfo> _subtaskInfos;

    public TaskDebugInfo() {
    }

    public void setStartTime(String startTime) {
      _startTime = startTime;
    }

    public void setExecutionStartTime(String executionStartTime) {
      _executionStartTime = executionStartTime;
    }

    public void setTaskState(TaskState taskState) {
      _taskState = taskState;
    }

    public void setSubtaskCount(TaskCount subtaskCount) {
      _subtaskCount = subtaskCount;
    }

    public void addSubtaskInfo(SubtaskDebugInfo subtaskInfo) {
      if (_subtaskInfos == null) {
        _subtaskInfos = new ArrayList<>();
      }
      _subtaskInfos.add(subtaskInfo);
    }

    public String getStartTime() {
      return _startTime;
    }

    public String getExecutionStartTime() {
      return _executionStartTime;
    }

    public TaskState getTaskState() {
      return _taskState;
    }

    public TaskCount getSubtaskCount() {
      return _subtaskCount;
    }

    public List<SubtaskDebugInfo> getSubtaskInfos() {
      return _subtaskInfos;
    }
  }

  @JsonPropertyOrder({"taskId", "state", "startTime", "finishTime", "participant", "info", "taskConfig"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class SubtaskDebugInfo {
    private String _taskId;
    private TaskPartitionState _state;
    private String _startTime;
    private String _finishTime;
    private String _participant;
    private String _info;
    private PinotTaskConfig _taskConfig;

    public SubtaskDebugInfo() {
    }

    public void setTaskId(String taskId) {
      _taskId = taskId;
    }

    public void setState(TaskPartitionState state) {
      _state = state;
    }

    public void setStartTime(String startTime) {
      _startTime = startTime;
    }

    public void setFinishTime(String finishTime) {
      _finishTime = finishTime;
    }

    public void setParticipant(String participant) {
      _participant = participant;
    }

    public void setInfo(String info) {
      _info = info;
    }

    public void setTaskConfig(PinotTaskConfig taskConfig) {
      _taskConfig = taskConfig;
    }

    public String getTaskId() {
      return _taskId;
    }

    public TaskPartitionState getState() {
      return _state;
    }

    public String getStartTime() {
      return _startTime;
    }

    public String getFinishTime() {
      return _finishTime;
    }

    public String getParticipant() {
      return _participant;
    }

    public String getInfo() {
      return _info;
    }

    public PinotTaskConfig getTaskConfig() {
      return _taskConfig;
    }
  }

  @JsonPropertyOrder({"total", "completed", "running", "waiting", "error", "unknown"})
  public static class TaskCount {
    private int _waiting;   // Number of tasks waiting to be scheduled on minions
    private int _error;     // Number of tasks in error
    private int _running;   // Number of tasks currently running in minions
    private int _completed; // Number of tasks completed normally
    private int _unknown;   // Number of tasks with all other states
    private int _total;     // Total number of tasks in the batch

    public TaskCount() {
    }

    /* Update count based on state for each task running under a HelixJob/PinotTask */
    public void addTaskState(TaskPartitionState state) {
      _total++;
      // Helix returns state as null if the task is not enqueued anywhere yet
      if (state == null) {
        // task is not yet assigned to a participant
        _waiting++;
      } else {
        switch (state) {
          case INIT:
          case RUNNING:
            _running++;
            break;
          case TASK_ERROR:
            _error++;
            break;
          case COMPLETED:
            _completed++;
            break;
          default:
            _unknown++;
            break;
        }
      }
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

    public int getCompleted() {
      return _completed;
    }

    public int getUnknown() {
      return _unknown;
    }

    public void accumulate(TaskCount other) {
      _waiting += other.getWaiting();
      _running += other.getRunning();
      _error += other.getError();
      _completed += other.getCompleted();
      _unknown += other.getUnknown();
      _total += other.getTotal();
    }
  }
}
