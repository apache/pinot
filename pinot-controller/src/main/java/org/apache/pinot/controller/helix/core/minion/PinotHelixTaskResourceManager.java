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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBiMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;
import org.apache.pinot.common.utils.DateTimeUtils;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.JsonUtils;
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
  private static final String UNKNOWN_TABLE_NAME = "unknown";

  private final TaskDriver _taskDriver;
  private final PinotHelixResourceManager _helixResourceManager;
  private final long _taskExpireTimeMs;

  public PinotHelixTaskResourceManager(PinotHelixResourceManager helixResourceManager, TaskDriver taskDriver) {
    this(helixResourceManager, taskDriver, TimeUnit.HOURS.toMillis(24));
  }

  public PinotHelixTaskResourceManager(PinotHelixResourceManager helixResourceManager, TaskDriver taskDriver,
      long taskExpireTimeMs) {
    _helixResourceManager = helixResourceManager;
    _taskDriver = taskDriver;
    _taskExpireTimeMs = taskExpireTimeMs;
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
  public synchronized void ensureTaskQueueExists(String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    WorkflowConfig workflowConfig = _taskDriver.getWorkflowConfig(helixJobQueueName);
    if (workflowConfig == null) {
      // Task queue does not exist
      LOGGER.info("Creating task queue: {} for task type: {}", helixJobQueueName, taskType);

      // Set full parallelism
      // Don't allow overlap job assignment so that we can control number of concurrent tasks per instance
      JobQueue jobQueue = new JobQueue.Builder(helixJobQueueName).setWorkflowConfig(
          new WorkflowConfig.Builder().setParallelJobs(Integer.MAX_VALUE).build()).build();
      _taskDriver.createQueue(jobQueue);
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
   * Delete a single task from the task queue. The task queue should be
   * stopped before deleting the task, otherwise it fails with exception.
   *
   * @param taskName the task to delete from the queue.
   * @param forceDelete as said in helix comment, if set true, all job's related zk nodes will
   *                    be clean up from zookeeper even if its workflow information can not be found.
   */
  public synchronized void deleteTask(String taskName, boolean forceDelete) {
    String taskType = getTaskType(taskName);
    String helixJobQueueName = getHelixJobQueueName(taskType);
    if (forceDelete) {
      LOGGER.warn("Force deleting task: {} from queue: {} of task type: {}", taskName, helixJobQueueName, taskType);
    } else {
      LOGGER.info("Deleting task: {} from queue: {} of task type: {}", taskName, helixJobQueueName, taskType);
    }
    _taskDriver.deleteJob(helixJobQueueName, taskName, forceDelete);
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
  @Nullable
  public synchronized TaskState getTaskQueueState(String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    if (_taskDriver.getWorkflowConfig(helixJobQueueName) == null) {
      return null;
    }
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(helixJobQueueName);
    return workflowContext != null ? workflowContext.getWorkflowState() : TaskState.NOT_STARTED;
  }

  /**
   * Submit a list of child tasks with same task type to the Minion instances with the default tag.
   *
   * @param pinotTaskConfigs List of child task configs to be submitted
   * @param taskTimeoutMs Timeout in milliseconds for each task
   * @param numConcurrentTasksPerInstance Maximum number of concurrent tasks allowed per instance
   * @param maxAttemptsPerTask Maximum number of attempts per task
   * @return Name of the submitted parent task
   */
  public synchronized String submitTask(List<PinotTaskConfig> pinotTaskConfigs, long taskTimeoutMs,
      int numConcurrentTasksPerInstance, int maxAttemptsPerTask) {
    return submitTask(pinotTaskConfigs, Helix.UNTAGGED_MINION_INSTANCE, taskTimeoutMs, numConcurrentTasksPerInstance,
        maxAttemptsPerTask);
  }

  /**
   * Submit a list of child tasks with same task type to the Minion instances with the given tag.
   *
   * @param pinotTaskConfigs List of child task configs to be submitted
   * @param minionInstanceTag Tag of the Minion instances to submit the task to
   * @param taskTimeoutMs Timeout in milliseconds for each task
   * @param numConcurrentTasksPerInstance Maximum number of concurrent tasks allowed per instance
   * @param maxAttemptsPerTask Maximum number of attempts per task
   * @return Name of the submitted parent task
   */
  public synchronized String submitTask(List<PinotTaskConfig> pinotTaskConfigs, String minionInstanceTag,
      long taskTimeoutMs, int numConcurrentTasksPerInstance, int maxAttemptsPerTask) {
    int numChildTasks = pinotTaskConfigs.size();
    Preconditions.checkState(numChildTasks > 0);
    Preconditions.checkState(numConcurrentTasksPerInstance > 0);

    String taskType = pinotTaskConfigs.get(0).getTaskType();
    String parentTaskName = getParentTaskName(taskType, UUID.randomUUID() + "_" + System.currentTimeMillis());
    return submitTask(parentTaskName, pinotTaskConfigs, minionInstanceTag, taskTimeoutMs, numConcurrentTasksPerInstance,
        maxAttemptsPerTask);
  }

  /**
   * Submit a list of child tasks with same task type to the Minion instances with the given tag.
   *
   * @param parentTaskName Parent task name to be submitted
   * @param pinotTaskConfigs List of child task configs to be submitted
   * @param minionInstanceTag Tag of the Minion instances to submit the task to
   * @param taskTimeoutMs Timeout in milliseconds for each task
   * @param numConcurrentTasksPerInstance Maximum number of concurrent tasks allowed per instance
   * @param maxAttemptsPerTask Maximum number of attempts per task
   * @return Name of the submitted parent task
   */
  public synchronized String submitTask(String parentTaskName, List<PinotTaskConfig> pinotTaskConfigs,
      String minionInstanceTag, long taskTimeoutMs, int numConcurrentTasksPerInstance, int maxAttemptsPerTask) {
    int numChildTasks = pinotTaskConfigs.size();
    Preconditions.checkState(numChildTasks > 0);
    Preconditions.checkState(numConcurrentTasksPerInstance > 0);

    String taskType = pinotTaskConfigs.get(0).getTaskType();
    LOGGER.info(
        "Submitting parent task: {} of type: {} with {} child task configs: {} to Minion instances with tag: {}",
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
            .setIgnoreDependentJobFailure(true).setMaxAttemptsPerTask(maxAttemptsPerTask)
            .setFailureThreshold(Integer.MAX_VALUE).setExpiry(_taskExpireTimeMs);
    _taskDriver.enqueueJob(getHelixJobQueueName(taskType), parentTaskName, jobBuilder);

    return parentTaskName;
  }

  /**
   * Get all tasks for the given task type.
   *
   * @param taskType Task type
   * @return Set of task names
   */
  public synchronized Set<String> getTasks(String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    WorkflowConfig workflowConfig = _taskDriver.getWorkflowConfig(helixJobQueueName);
    Preconditions.checkArgument(workflowConfig != null, "Task queue: %s for task type: %s does not exist",
        helixJobQueueName, taskType);
    Set<String> helixJobs = workflowConfig.getJobDag().getAllNodes();
    Set<String> tasks = new HashSet<>(helixJobs.size());
    for (String helixJobName : helixJobs) {
      tasks.add(getPinotTaskName(helixJobName));
    }
    return tasks;
  }

  /**
   * Get all task states for the given task type.
   * NOTE: For tasks just submitted without the context created, count them as NOT_STARTED.
   *
   * @param taskType Task type
   * @return Map from task name to task state
   */
  public synchronized Map<String, TaskState> getTaskStates(String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    WorkflowConfig workflowConfig = _taskDriver.getWorkflowConfig(helixJobQueueName);
    Preconditions.checkArgument(workflowConfig != null, "Task queue: %s for task type: %s does not exist",
        helixJobQueueName, taskType);
    Set<String> helixJobs = workflowConfig.getJobDag().getAllNodes();
    if (helixJobs.isEmpty()) {
      return Collections.emptyMap();
    }
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType));
    if (workflowContext == null) {
      return helixJobs.stream()
          .collect(Collectors.toMap(PinotHelixTaskResourceManager::getPinotTaskName, ignored -> TaskState.NOT_STARTED));
    } else {
      Map<String, TaskState> helixJobStates = workflowContext.getJobStates();
      return helixJobs.stream().collect(Collectors.toMap(PinotHelixTaskResourceManager::getPinotTaskName,
          helixJobName -> helixJobStates.getOrDefault(helixJobName, TaskState.NOT_STARTED)));
    }
  }

  /**
   * This method returns a count of sub-tasks in various states, given the top-level task name.
   *
   * @param taskName in the form "Task_<taskType>_<uuid>_<timestamp>"
   * @return TaskCount object
   */
  public synchronized TaskCount getTaskCount(String taskName) {
    String helixJobName = getHelixJobName(taskName);
    JobConfig jobConfig = _taskDriver.getJobConfig(helixJobName);
    Preconditions.checkArgument(jobConfig != null, "Task: %s does not exist", taskName);
    Set<String> subtasks = jobConfig.getTaskConfigMap().keySet();
    TaskCount taskCount = new TaskCount();
    JobContext jobContext = _taskDriver.getJobContext(helixJobName);
    if (jobContext == null) {
      int numSubtasks = subtasks.size();
      for (int i = 0; i < numSubtasks; i++) {
        taskCount.addTaskState(null);
      }
      return taskCount;
    }
    Map<String, Integer> taskIdPartitionMap = jobContext.getTaskIdPartitionMap();
    for (String taskId : subtasks) {
      TaskPartitionState state = null;
      Integer partition = taskIdPartitionMap.get(taskId);
      if (partition != null) {
        state = jobContext.getPartitionState(partition);
      }
      taskCount.addTaskState(state);
    }
    return taskCount;
  }

  /**
   * This method returns a map of table name to count of sub-tasks in various states, given the top-level task name.
   *
   * @param taskName in the form "Task_<taskType>_<uuid>_<timestamp>"
   * @return a map of table name to {@link TaskCount}
   */
  public synchronized Map<String, TaskCount> getTableTaskCount(String taskName) {
    String helixJobName = getHelixJobName(taskName);
    JobConfig jobConfig = _taskDriver.getJobConfig(helixJobName);
    Preconditions.checkArgument(jobConfig != null, "Task: %s does not exist", taskName);
    Map<String, TaskConfig> taskConfigMap = jobConfig.getTaskConfigMap();
    Map<String, TaskCount> taskCountMap = new HashMap<>();
    JobContext jobContext = _taskDriver.getJobContext(helixJobName);
    if (jobContext == null) {
      for (TaskConfig taskConfig : taskConfigMap.values()) {
        String tableName = taskConfig.getConfigMap().getOrDefault(MinionConstants.TABLE_NAME_KEY, UNKNOWN_TABLE_NAME);
        taskCountMap.computeIfAbsent(tableName, k -> new TaskCount()).addTaskState(null);
      }
      return taskCountMap;
    }
    Map<String, Integer> taskIdPartitionMap = jobContext.getTaskIdPartitionMap();
    for (Map.Entry<String, TaskConfig> entry : taskConfigMap.entrySet()) {
      String taskId = entry.getKey();
      TaskPartitionState state = null;
      Integer partition = taskIdPartitionMap.get(taskId);
      if (partition != null) {
        state = jobContext.getPartitionState(partition);
      }
      TaskConfig taskConfig = entry.getValue();
      String tableName = taskConfig.getConfigMap().getOrDefault(MinionConstants.TABLE_NAME_KEY, UNKNOWN_TABLE_NAME);
      taskCountMap.computeIfAbsent(tableName, k -> new TaskCount()).addTaskState(state);
    }
    return taskCountMap;
  }

  /**
   * Returns a set of Task names (in the form "Task_<taskType>_<uuid>_<timestamp>") that are in progress or not started
   * yet.
   * NOTE: For tasks just submitted without the context created, count them as NOT_STARTED.
   *
   * @param taskType Task type
   * @return Set of task names
   */
  public synchronized Set<String> getTasksInProgress(String taskType) {
    WorkflowConfig workflowConfig = _taskDriver.getWorkflowConfig(getHelixJobQueueName(taskType));
    if (workflowConfig == null) {
      return Collections.emptySet();
    }
    Set<String> helixJobs = workflowConfig.getJobDag().getAllNodes();
    if (helixJobs.isEmpty()) {
      return Collections.emptySet();
    }
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType));
    if (workflowContext == null) {
      return helixJobs.stream().map(PinotHelixTaskResourceManager::getPinotTaskName).collect(Collectors.toSet());
    } else {
      Map<String, TaskState> helixJobStates = workflowContext.getJobStates();
      return helixJobs.stream().filter(helixJobName -> {
        TaskState taskState = helixJobStates.get(helixJobName);
        return taskState == null || taskState == TaskState.NOT_STARTED || taskState == TaskState.IN_PROGRESS;
      }).map(PinotHelixTaskResourceManager::getPinotTaskName).collect(Collectors.toSet());
    }
  }

  /**
   * Get the task state for the given task name.
   * NOTE: For tasks just submitted without the context created, count them as NOT_STARTED.
   *
   * @param taskName Task name
   * @return Task state
   */
  @Nullable
  public synchronized TaskState getTaskState(String taskName) {
    String helixJobName = getHelixJobName(taskName);
    JobConfig jobConfig = _taskDriver.getJobConfig(helixJobName);
    if (jobConfig == null) {
      return null;
    }
    String taskType = getTaskType(taskName);
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType));
    if (workflowContext != null) {
      TaskState jobState = workflowContext.getJobState(helixJobName);
      if (jobState != null) {
        return jobState;
      }
    }
    return TaskState.NOT_STARTED;
  }

  /**
   * Get states of all the sub tasks for a given task.
   *
   * @param taskName the task whose sub tasks to check
   * @return states of all the sub tasks
   */
  public synchronized Map<String, TaskPartitionState> getSubtaskStates(String taskName) {
    String helixJobName = getHelixJobName(taskName);
    JobConfig jobConfig = _taskDriver.getJobConfig(helixJobName);
    Preconditions.checkArgument(jobConfig != null, "Task: %s does not exist", taskName);
    Set<String> subtasks = jobConfig.getTaskConfigMap().keySet();
    Map<String, TaskPartitionState> subtaskStates = new HashMap<>();
    JobContext jobContext = _taskDriver.getJobContext(helixJobName);
    if (jobContext == null) {
      for (String taskId : subtasks) {
        subtaskStates.put(taskId, null);
      }
      return subtaskStates;
    }
    Map<String, Integer> taskIdPartitionMap = jobContext.getTaskIdPartitionMap();
    for (String taskId : subtasks) {
      TaskPartitionState state = null;
      Integer partition = taskIdPartitionMap.get(taskId);
      if (partition != null) {
        state = jobContext.getPartitionState(partition);
      }
      subtaskStates.put(taskId, state);
    }
    return subtaskStates;
  }

  /**
   * Get the child task configs for the given task name.
   *
   * @param taskName Task name
   * @return List of child task configs
   */
  public synchronized List<PinotTaskConfig> getSubtaskConfigs(String taskName) {
    Collection<TaskConfig> helixTaskConfigs =
        _taskDriver.getJobConfig(getHelixJobName(taskName)).getTaskConfigMap().values();
    List<PinotTaskConfig> taskConfigs = new ArrayList<>(helixTaskConfigs.size());
    for (TaskConfig helixTaskConfig : helixTaskConfigs) {
      taskConfigs.add(PinotTaskConfig.fromHelixTaskConfig(helixTaskConfig));
    }
    return taskConfigs;
  }

  /**
   * Get the task runtime config for the given task name. A task can have multiple subtasks, whose configs can be
   * retrieved via the getSubtaskConfigs() method instead.
   *
   * @param taskName Task name
   * @return Configs for the task returned as a Map.
   */
  public synchronized Map<String, String> getTaskRuntimeConfig(String taskName) {
    JobConfig jobConfig = _taskDriver.getJobConfig(getHelixJobName(taskName));
    HashMap<String, String> configs = new HashMap<>();
    configs.put("ConcurrentTasksPerWorker", String.valueOf(jobConfig.getNumConcurrentTasksPerInstance()));
    configs.put("TaskTimeoutMs", String.valueOf(jobConfig.getTimeoutPerTask()));
    configs.put("TaskExpireTimeMs", String.valueOf(jobConfig.getExpiry()));
    configs.put("MinionWorkerGroupTag", jobConfig.getInstanceGroupTag());
    return configs;
  }

  /**
   * Get configs of the specified sub task for a given task.
   *
   * @param taskName the task whose sub tasks to check
   * @param subtaskNames the sub tasks to check
   * @return the configs of the sub tasks
   */
  public synchronized Map<String, PinotTaskConfig> getSubtaskConfigs(String taskName, @Nullable String subtaskNames) {
    JobConfig jobConfig = _taskDriver.getJobConfig(getHelixJobName(taskName));
    if (jobConfig == null) {
      return Collections.emptyMap();
    }
    Map<String, TaskConfig> helixTaskConfigs = jobConfig.getTaskConfigMap();
    Map<String, PinotTaskConfig> taskConfigs = new HashMap<>(HashUtil.getHashMapCapacity(helixTaskConfigs.size()));
    if (StringUtils.isEmpty(subtaskNames)) {
      helixTaskConfigs.forEach((sub, cfg) -> taskConfigs.put(sub, PinotTaskConfig.fromHelixTaskConfig(cfg)));
      return taskConfigs;
    }
    for (String subtaskName : StringUtils.split(subtaskNames, ',')) {
      TaskConfig taskConfig = helixTaskConfigs.get(subtaskName);
      if (taskConfig != null) {
        taskConfigs.put(subtaskName, PinotTaskConfig.fromHelixTaskConfig(taskConfig));
      }
    }
    return taskConfigs;
  }

  public synchronized Map<String, Object> getSubtaskProgress(String taskName, @Nullable String subtaskNames,
      Executor executor, HttpClientConnectionManager connMgr, Map<String, String> workerEndpoints,
      Map<String, String> requestHeaders, int timeoutMs)
      throws Exception {
    return getSubtaskProgress(taskName, subtaskNames,
        new CompletionServiceHelper(executor, connMgr, HashBiMap.create(0)), workerEndpoints, requestHeaders,
        timeoutMs);
  }

  @VisibleForTesting
  Map<String, Object> getSubtaskProgress(String taskName, @Nullable String subtaskNames,
      CompletionServiceHelper completionServiceHelper, Map<String, String> workerEndpoints,
      Map<String, String> requestHeaders, int timeoutMs)
      throws Exception {
    String helixJobName = getHelixJobName(taskName);
    JobConfig jobConfig = _taskDriver.getJobConfig(helixJobName);
    Preconditions.checkArgument(jobConfig != null, "Task: %s does not exist", taskName);
    Set<String> subtasks = jobConfig.getTaskConfigMap().keySet();
    Set<String> selectedSubtasks;
    if (StringUtils.isNotEmpty(subtaskNames)) {
      selectedSubtasks = new HashSet<>(Arrays.asList(StringUtils.split(subtaskNames, ',')));
      selectedSubtasks.retainAll(subtasks);
    } else {
      selectedSubtasks = subtasks;
    }
    if (selectedSubtasks.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Object> subtaskProgressMap = new HashMap<>();
    JobContext jobContext = _taskDriver.getJobContext(helixJobName);
    if (jobContext == null) {
      for (String taskId : selectedSubtasks) {
        subtaskProgressMap.put(taskId, "No worker has run this subtask");
      }
      return subtaskProgressMap;
    }
    // The worker running the subtask and task state as tracked by helix.
    Map<String, Pair<String, TaskPartitionState>> subtaskWorkerAndStateMap = new HashMap<>();
    Map<String, Set<String>> workerSubtasksMap = new HashMap<>();
    Map<String, Integer> taskIdPartitionMap = jobContext.getTaskIdPartitionMap();
    for (String taskId : selectedSubtasks) {
      String worker = null;
      TaskPartitionState state = null;
      Integer partition = taskIdPartitionMap.get(taskId);
      if (partition != null) {
        worker = jobContext.getAssignedParticipant(partition);
        state = jobContext.getPartitionState(partition);
      }
      subtaskWorkerAndStateMap.put(taskId, Pair.of(worker, state));
      if (worker != null) {
        workerSubtasksMap.computeIfAbsent(worker, k -> new HashSet<>()).add(taskId);
      }
    }
    LOGGER.debug("Found subtasks on workers: {}", workerSubtasksMap);
    List<String> workerUrls = new ArrayList<>();
    workerSubtasksMap.forEach((workerId, subtasksOnWorker) -> workerUrls.add(
        String.format("%s/tasks/subtask/progress?subtaskNames=%s", workerEndpoints.get(workerId),
            StringUtils.join(subtasksOnWorker, CommonConstants.Minion.TASK_LIST_SEPARATOR))));
    LOGGER.debug("Getting task progress with workerUrls: {}", workerUrls);
    // Scatter and gather progress from multiple workers.
    if (!workerUrls.isEmpty()) {
      CompletionServiceHelper.CompletionServiceResponse serviceResponse =
          completionServiceHelper.doMultiGetRequest(workerUrls, null, true, requestHeaders, timeoutMs);
      for (Map.Entry<String, String> entry : serviceResponse._httpResponses.entrySet()) {
        String worker = entry.getKey();
        String resp = entry.getValue();
        LOGGER.debug("Got resp: {} from worker: {}", resp, worker);
        if (StringUtils.isNotEmpty(resp)) {
          subtaskProgressMap.putAll(JsonUtils.stringToObject(resp, Map.class));
        }
      }
      if (serviceResponse._failedResponseCount > 0) {
        // Instead of aborting, subtasks without worker side progress return the task status tracked by Helix.
        // The detailed worker failure response is logged as error by CompletionServiceResponse for debugging.
        LOGGER.warn("There were {} workers failed to report task progress. Got partial progress info: {}",
            serviceResponse._failedResponseCount, subtaskProgressMap);
      }
    }
    // Check if any subtask missed their progress from the worker.
    // And simply check all subtasks if no subtasks are specified.
    for (String taskId : selectedSubtasks) {
      if (subtaskProgressMap.containsKey(taskId)) {
        continue;
      }
      // Return the task progress status tracked by Helix.
      Pair<String, TaskPartitionState> workerAndState = subtaskWorkerAndStateMap.get(taskId);
      if (workerAndState.getLeft() == null) {
        subtaskProgressMap.put(taskId, "No worker has run this subtask");
      } else {
        subtaskProgressMap.put(taskId,
            String.format("No status from worker: %s. Got status: %s from Helix", workerAndState.getLeft(),
                workerAndState.getRight()));
      }
    }
    return subtaskProgressMap;
  }

  /**
   * Gets progress of all subtasks with specified state tracked by given minion workers in memory
   * @param subtaskState a specified subtask state, valid values are in org.apache.pinot.minion.event.MinionTaskState
   * @param executor an {@link Executor} used to run logic on
   * @param connMgr a {@link HttpClientConnectionManager} used to manage http connections
   * @param selectedMinionWorkerEndpoints a map of worker id to http endpoint for minions to get subtask progress from
   * @param requestHeaders http headers used to send requests to minion workers
   * @param timeoutMs timeout (in millisecond) for requests sent to minion workers
   * @return a map of minion worker id to subtask progress
   */
  public synchronized Map<String, Object> getSubtaskOnWorkerProgress(String subtaskState, Executor executor,
      HttpClientConnectionManager connMgr, Map<String, String> selectedMinionWorkerEndpoints,
      Map<String, String> requestHeaders, int timeoutMs)
      throws JsonProcessingException {
    return getSubtaskOnWorkerProgress(subtaskState, new CompletionServiceHelper(executor, connMgr, HashBiMap.create(0)),
        selectedMinionWorkerEndpoints, requestHeaders, timeoutMs);
  }

  @VisibleForTesting
  Map<String, Object> getSubtaskOnWorkerProgress(String subtaskState, CompletionServiceHelper completionServiceHelper,
      Map<String, String> selectedMinionWorkerEndpoints, Map<String, String> requestHeaders, int timeoutMs)
      throws JsonProcessingException {
    Map<String, Object> minionWorkerIdSubtaskProgressMap = new HashMap<>();
    if (selectedMinionWorkerEndpoints.isEmpty()) {
      return minionWorkerIdSubtaskProgressMap;
    }
    Map<String, String> minionWorkerUrlToWorkerIdMap = selectedMinionWorkerEndpoints.entrySet().stream().collect(
        Collectors.toMap(
            entry -> String.format("%s/tasks/subtask/state/progress?subTaskState=%s", entry.getValue(), subtaskState),
            Map.Entry::getKey));
    List<String> workerUrls = new ArrayList<>(minionWorkerUrlToWorkerIdMap.keySet());
    LOGGER.debug("Getting task progress with workerUrls: {}", workerUrls);
    // Scatter and gather progress from multiple workers.
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(workerUrls, null, true, requestHeaders, timeoutMs);
    for (Map.Entry<String, String> entry : serviceResponse._httpResponses.entrySet()) {
      String worker = entry.getKey();
      String resp = entry.getValue();
      LOGGER.debug("Got resp: {} from worker: {}", resp, worker);
      minionWorkerIdSubtaskProgressMap.put(minionWorkerUrlToWorkerIdMap.get(worker),
          JsonUtils.stringToObject(resp, Map.class));
    }
    if (serviceResponse._failedResponseCount > 0) {
      // Instead of aborting, subtasks without worker side progress return the task status tracked by Helix.
      // The detailed worker failure response is logged as error by CompletionServiceResponse for debugging.
      LOGGER.warn("There were {} workers failed to report task progress. Got partial progress info: {}",
          serviceResponse._failedResponseCount, minionWorkerIdSubtaskProgressMap);
    }
    return minionWorkerIdSubtaskProgressMap;
  }

  /**
   * Helper method to return a map of task names to corresponding task state
   * where the task corresponds to the given Pinot table name. This is used to
   * check status of all tasks for a given table.
   * @param taskType Pinot taskType / Helix JobQueue
   * @param tableNameWithType table name with type to filter on
   * @return Map of filtered task name to corresponding state
   */
  public synchronized Map<String, TaskState> getTaskStatesByTable(String taskType, String tableNameWithType) {
    Map<String, TaskState> filteredTaskStateMap = new HashMap<>();
    Map<String, TaskState> taskStateMap = getTaskStates(taskType);

    for (Map.Entry<String, TaskState> taskState : taskStateMap.entrySet()) {
      String taskName = taskState.getKey();

      // Iterate through all task configs associated with this task name
      for (PinotTaskConfig taskConfig : getSubtaskConfigs(taskName)) {
        // Filter task configs that matches this table name
        String tableNameConfig = taskConfig.getTableName();
        if (tableNameConfig != null && tableNameConfig.equals(tableNameWithType)) {
          // Found a match ! Track state for this particular task in the final result map
          filteredTaskStateMap.put(taskName, taskStateMap.get(taskName));
          break;
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
    Set<String> tasks = getTasks(taskType);
    Map<String, TaskCount> taskCounts = new TreeMap<>();
    for (String taskName : tasks) {
      taskCounts.put(taskName, getTaskCount(taskName));
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
   * Given a taskType and a tableNameWithType, helper method to debug all the HelixJobs for
   * the taskType and tableNameWithType. For each of the HelixJobs, collects status of
   * the (sub)tasks in the taskbatch.
   *
   * @param taskType Pinot taskType / Helix JobQueue
   * @param tableNameWithType Table name with type to filter on
   * @param verbosity By default, does not show details for completed tasks.
   *                  If verbosity > 0, shows details for all tasks.
   * @return Map of Pinot Task Name to TaskDebugInfo. TaskDebugInfo contains details for subtasks.
   */
  public synchronized Map<String, TaskDebugInfo> getTasksDebugInfoByTable(String taskType, String tableNameWithType,
      int verbosity) {
    Map<String, TaskDebugInfo> taskDebugInfos = new TreeMap<>();
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType));
    if (workflowContext == null) {
      return taskDebugInfos;
    }

    Map<String, TaskState> helixJobStates = workflowContext.getJobStates();
    for (String helixJobName : helixJobStates.keySet()) {
      String pinotTaskName = getPinotTaskName(helixJobName);

      // Iterate through all task configs associated with this task name
      for (PinotTaskConfig taskConfig : getSubtaskConfigs(pinotTaskName)) {
        Map<String, String> pinotConfigs = taskConfig.getConfigs();

        // Filter task configs that matches this table name
        if (pinotConfigs != null) {
          String tableNameConfig = pinotConfigs.get(TABLE_NAME);
          if (tableNameConfig != null && tableNameConfig.equals(tableNameWithType)) {
            // Found a match. Add task debug info to the result
            // TODO: If a task consists of subtasks belonging to the current table and other tables at the same time,
            //  we will collect debug info of them all. We may want to filter out debug info that does not belong
            //  to the given table.
            taskDebugInfos.put(pinotTaskName, getTaskDebugInfo(workflowContext, helixJobName, verbosity));
            break;
          }
        }
      }
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
      long jobFinishTimeMs = jobContext.getFinishTime();
      if (jobFinishTimeMs > 0) {
        taskDebugInfo.setFinishTime(DateTimeUtils.epochToDefaultDateFormat(jobFinishTimeMs));
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
  public static String getHelixJobName(String pinotTaskName) {
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

  public String getParentTaskName(String taskType, String taskName) {
    return TASK_PREFIX + taskType + TASK_NAME_SEPARATOR + taskName;
  }

  public String getTaskMetadataByTable(String taskType, String tableNameWithType)
      throws JsonProcessingException {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixResourceManager.getPropertyStore();
    ZNRecord raw = MinionTaskMetadataUtils.fetchTaskMetadata(propertyStore, taskType, tableNameWithType);
    if (raw == null) {
      return JsonUtils.objectToString(JsonUtils.newObjectNode());
    }
    return JsonUtils.objectToString(raw);
  }

  public void deleteTaskMetadataByTable(String taskType, String tableNameWithType) {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixResourceManager.getPropertyStore();
    MinionTaskMetadataUtils.deleteTaskMetadata(propertyStore, taskType, tableNameWithType);
  }

  /**
   * Gets the last update time (in ms) of all minion task metadata.
   * @return a map storing the last update time (in ms) of all minion task metadata: (tableNameWithType -> taskType
   *         -> last update time in ms)
   */
  public Map<String, Map<String, Long>> getTaskMetadataLastUpdateTimeMs() {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixResourceManager.getPropertyStore();
    return MinionTaskMetadataUtils.getAllTaskMetadataLastUpdateTimeMs(propertyStore);
  }

  @JsonPropertyOrder({"taskState", "subtaskCount", "startTime", "executionStartTime", "finishTime", "subtaskInfos"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class TaskDebugInfo {
    // Time at which the task (which may have multiple subtasks) got created.
    private String _startTime;
    // Time at which the first subtask of the task got scheduled.
    private String _executionStartTime;
    // Time at which the task has ended.
    private String _finishTime;
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

    public void setFinishTime(String finishTime) {
      _finishTime = finishTime;
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

    public String getFinishTime() {
      return _finishTime;
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
