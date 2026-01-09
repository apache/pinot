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
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.helix.model.InstanceConfig;
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
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;
import org.apache.pinot.common.utils.DateTimeUtils;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.controller.api.resources.MinionStatusResponse;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
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
  private static final String UNKNOWN_TENANT_NAME = "unknown";

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

    // Get task name prefix and suffix from the first task config.
    String taskNamePrefix = pinotTaskConfigs.get(0).getConfigs()
        .getOrDefault(BatchConfigProperties.TASK_NAME_PREFIX_KEY, UUID.randomUUID().toString());
    String taskNameSuffix =
        pinotTaskConfigs.get(0).getConfigs().getOrDefault(BatchConfigProperties.TASK_NAME_SUFFIX_KEY, "");

    String parentTaskName =
        getParentTaskName(taskType, taskNamePrefix + "_" + System.currentTimeMillis() + taskNameSuffix);
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
   * @return Set of task names. Null for invalid task type.
   */
  @Nullable
  public synchronized Set<String> getTasks(String taskType) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    WorkflowConfig workflowConfig = _taskDriver.getWorkflowConfig(helixJobQueueName);
    if (workflowConfig == null) {
      return null;
    }
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
   * It also collects waiting times for subtasks with null state and running times for subtasks in RUNNING state.
   *
   * @param taskName in the form "Task_<taskType>_<uuid>_<timestamp>"
   * @return a map of table name to {@link TaskStatusSummary}
   */
  public synchronized Map<String, TaskStatusSummary> getTableTaskStatusSummary(String taskName) {
    String helixJobName = getHelixJobName(taskName);
    JobConfig jobConfig = _taskDriver.getJobConfig(helixJobName);
    Preconditions.checkArgument(jobConfig != null, "Task: %s does not exist", taskName);
    Map<String, TaskConfig> taskConfigMap = jobConfig.getTaskConfigMap();
    Map<String, TaskStatusSummary> taskStatusSummaryMap = new HashMap<>();
    JobContext jobContext = _taskDriver.getJobContext(helixJobName);
    if (jobContext == null) {
      // No job context available, only populate TaskCount without timing info
      for (TaskConfig taskConfig : taskConfigMap.values()) {
        String tableName = taskConfig.getConfigMap().getOrDefault(MinionConstants.TABLE_NAME_KEY, UNKNOWN_TABLE_NAME);
        TaskStatusSummary taskStatusSummary = taskStatusSummaryMap.computeIfAbsent(tableName,
            k -> new TaskStatusSummary());
        taskStatusSummary.getTaskCount().addTaskState(null);
      }
      return taskStatusSummaryMap;
    }
    long jobStartTime = jobContext.getStartTime();
    long currentTime = System.currentTimeMillis();
    Map<String, Integer> taskIdPartitionMap = jobContext.getTaskIdPartitionMap();
    for (Map.Entry<String, TaskConfig> entry : taskConfigMap.entrySet()) {
      String taskId = entry.getKey();
      TaskPartitionState state = null;
      long executionStartTime = 0;
      Integer partition = taskIdPartitionMap.get(taskId);
      if (partition != null) {
        state = jobContext.getPartitionState(partition);
        executionStartTime = jobContext.getPartitionStartTime(partition);
      }
      TaskConfig taskConfig = entry.getValue();
      String tableName = taskConfig.getConfigMap().getOrDefault(MinionConstants.TABLE_NAME_KEY, UNKNOWN_TABLE_NAME);

      TaskStatusSummary taskStatusSummary = taskStatusSummaryMap.computeIfAbsent(tableName,
          k -> new TaskStatusSummary());
      taskStatusSummary.getTaskCount().addTaskState(state);

      // Calculate waiting time for subtasks with null state
      if (state == null && jobStartTime > 0) {
        long waitingTimeMillis = (currentTime - jobStartTime);
        taskStatusSummary.getSubtaskWaitingTimes().put(taskId, waitingTimeMillis);
      }

      // Calculate running time for RUNNING subtasks
      if (state == TaskPartitionState.RUNNING && executionStartTime > 0) {
        long runningTimeMillis = (currentTime - executionStartTime);
        taskStatusSummary.getSubtaskRunningTimes().put(taskId, runningTimeMillis);
      }
    }
    return taskStatusSummaryMap;
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
    return getTasksByStatus(taskType, 0).getInProgressTasks();
  }

  /**
   * Returns tasks organized by status (in-progress and recent).
   * NOTE: For tasks just submitted without the context created, count them as NOT_STARTED.
   * This method combines in-progress tasks and recent tasks in a single Helix call to avoid duplicate calls.
   *
   * @param taskType Task type
   * @param afterTimestampMs If > 0, also include tasks that started after this timestamp (in milliseconds).
   *                         This is used to detect short-lived tasks that started and completed between cycles.
   * @return TasksByStatus containing in-progress tasks and recent tasks that started after the timestamp
   */
  public synchronized TasksByStatus getTasksByStatus(String taskType, long afterTimestampMs) {
    String helixJobQueueName = getHelixJobQueueName(taskType);
    WorkflowConfig workflowConfig = _taskDriver.getWorkflowConfig(helixJobQueueName);
    if (workflowConfig == null) {
      return new TasksByStatus();
    }
    Set<String> helixJobs = workflowConfig.getJobDag().getAllNodes();
    if (helixJobs.isEmpty()) {
      return new TasksByStatus();
    }
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(helixJobQueueName);
    if (workflowContext == null) {
      // If no context, return all jobs as in-progress (backward compatible behavior)
      Set<String> allTasks = helixJobs.stream()
          .map(PinotHelixTaskResourceManager::getPinotTaskName)
          .collect(Collectors.toSet());
      // If timestamp is specified, we can't filter by start time without context, so return all as in-progress
      TasksByStatus result = new TasksByStatus();
      result.setInProgressTasks(allTasks);
      return result;
    }

    Map<String, TaskState> helixJobStates = workflowContext.getJobStates();
    Map<String, Long> jobStartTimes = afterTimestampMs > 0 ? workflowContext.getJobStartTimes() : null;
    Set<String> inProgressTasks = new HashSet<>();
    Set<String> recentTasks = new HashSet<>();
    TasksByStatus result = new TasksByStatus();

    for (String helixJobName : helixJobs) {
      String pinotTaskName = getPinotTaskName(helixJobName);
      TaskState taskState = helixJobStates.get(helixJobName);

      // Include if in-progress
      if (taskState == null || taskState == TaskState.NOT_STARTED || taskState == TaskState.IN_PROGRESS) {
        inProgressTasks.add(pinotTaskName);
        continue;
      }

      if (afterTimestampMs > 0) {
        // If not in-progress but timestamp is specified, check if task started after timestamp
        Long jobStartTime = jobStartTimes.get(helixJobName);
        if (jobStartTime != null && jobStartTime > afterTimestampMs) {
          recentTasks.add(pinotTaskName);
        }
      }
    }
    result.setInProgressTasks(inProgressTasks);
    result.setRecentTasks(recentTasks);
    return result;
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
    JobConfig jobConfig = _taskDriver.getJobConfig(getHelixJobName(taskName));
    if (jobConfig == null) {
      return List.of();
    }
    Collection<TaskConfig> helixTaskConfigs = jobConfig.getTaskConfigMap().values();
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
    Preconditions.checkArgument(jobConfig != null, "Task: %s does not exist", taskName);
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
    if (tasks == null) {
      return Collections.emptyMap();
    }
    Map<String, TaskCount> taskCounts = new TreeMap<>();
    for (String taskName : tasks) {
      taskCounts.put(taskName, getTaskCount(taskName));
    }
    return taskCounts;
  }

  /**
   * Fetch count of sub-tasks for each of the tasks for the given taskType, filtered by state.
   *
   * @param taskType      Pinot taskType / Helix JobQueue
   * @param state         State(s) to filter by. Can be single state or comma-separated multiple states
   *                      (waiting, running, error, completed, dropped, timedOut, aborted, unknown, total)
   * @return Map of Pinot Task Name to TaskCount containing only tasks that have > 0 count for any of the
   *         specified states
   */
  public synchronized Map<String, TaskCount> getTaskCounts(String taskType, String state) {
    return getTaskCounts(taskType, state, null);
  }

  /**
   * Fetch count of sub-tasks for each of the tasks for the given taskType, filtered by state and/or table.
   *
   * @param taskType           Pinot taskType / Helix JobQueue
   * @param state              State(s) to filter by. Can be single state or comma-separated multiple states
   *                           (NOT_STARTED, IN_PROGRESS, STOPPED, STOPPING, FAILED, COMPLETED, ABORTED, TIMED_OUT,
   *                           TIMING_OUT, FAILING). Can be null to skip state filtering.
   * @param tableNameWithType  Table name with type to filter by. Only tasks that have subtasks for this table
   *                           will be returned. Can be null to skip table filtering.
   * @return Map of Pinot Task Name to TaskCount containing only tasks that match the specified filters
   */
  public synchronized Map<String, TaskCount> getTaskCounts(String taskType, @Nullable String state,
      @Nullable String tableNameWithType) {
    Set<String> tasks = getTasks(taskType);
    if (tasks == null) {
      return Collections.emptyMap();
    }

    // Parse and validate comma-separated states if provided
    Set<TaskState> requestedStates = null;
    if (StringUtils.isNotEmpty(state)) {
      String[] stateArray = state.trim().split(",");
      requestedStates = new HashSet<>();
      for (String s : stateArray) {
        String normalizedState = s.trim().toUpperCase();
        // Validate each state upfront
        TaskState taskState = validateAndParseTaskState(normalizedState);
        requestedStates.add(taskState);
      }
    }

    // Get all task states if we need to filter by state
    Map<String, TaskState> taskStates = null;
    if (requestedStates != null) {
      taskStates = getTaskStates(taskType);
    }

    Map<String, TaskCount> taskCounts = new TreeMap<>();
    for (String taskName : tasks) {
      // Apply state filtering first (less expensive)
      if (requestedStates != null) {
        TaskState currentTaskState = taskStates.get(taskName);
        if (currentTaskState == null || !requestedStates.contains(currentTaskState)) {
          continue;
        }
      }

      // Apply table filtering next (also less expensive than getting task count)
      if (StringUtils.isNotEmpty(tableNameWithType) && !hasTasksForTable(taskName, tableNameWithType)) {
        continue;
      }

      // Only collect TaskCount after passing all filters (expensive operation)
      TaskCount taskCount = getTaskCount(taskName);
      taskCounts.put(taskName, taskCount);
    }
    return taskCounts;
  }

  /**
   * Validates and parses a task state string into TaskState enum.
   *
   * @param state State string to validate (should be uppercase)
   * @throws IllegalArgumentException if the state is invalid
   * @return TaskState enum value
   */
  private TaskState validateAndParseTaskState(String state) {
    try {
      return TaskState.valueOf(state);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid state: " + state + ". Valid states are: "
          + Arrays.toString(TaskState.values()));
    }
  }

  /**
   * Helper method to check if a task has any subtasks for the specified table.
   *
   * @param taskName          Task name to check
   * @param tableNameWithType Table name with type to check for
   * @return true if the task has subtasks for the specified table
   */
  private boolean hasTasksForTable(String taskName, String tableNameWithType) {
    try {
      // Get all subtask configs for this task
      List<PinotTaskConfig> subtaskConfigs = getSubtaskConfigs(taskName);

      // Check if any subtask is for the specified table
      for (PinotTaskConfig taskConfig : subtaskConfigs) {
        String taskTableName = taskConfig.getTableName();
        if (taskTableName != null && taskTableName.equals(tableNameWithType)) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      // If we can't get the subtask configs, assume no match
      LOGGER.warn("Failed to get subtask configs for task: {}", taskName, e);
      return false;
    }
  }

  /**
   * Get the server tenant name for a given table by looking up its configuration.
   * Returns "unknown" if the table or tenant cannot be determined.
   *
   * @param tableName Table name with type (e.g., "myTable_OFFLINE")
   * @return Server tenant name or "unknown"
   */
  private String getTenantForTable(String tableName) {
    if (tableName == null || UNKNOWN_TABLE_NAME.equals(tableName)) {
      return UNKNOWN_TENANT_NAME;
    }

    try {
      TableConfig tableConfig = _helixResourceManager.getTableConfig(tableName);
      if (tableConfig != null && tableConfig.getTenantConfig() != null) {
        String serverTenant = tableConfig.getTenantConfig().getServer();
        return serverTenant != null ? serverTenant : UNKNOWN_TENANT_NAME;
      }
      return UNKNOWN_TENANT_NAME;
    } catch (Exception e) {
      LOGGER.warn("Failed to determine tenant for table: {}", tableName, e);
      return UNKNOWN_TENANT_NAME;
    }
  }

  /**
   * Get a summary of all tasks across all task types, grouped by tenant.
   *
   * <p>Only includes tasks with RUNNING or WAITING subtasks. Completed, failed, or aborted tasks are excluded.
   * Tasks are first resolved to their table, then grouped by the table's server tenant.
   *
   * @param tenantFilter Optional tenant name to filter results. If null, returns all tenants.
   * @return TaskSummaryResponse containing aggregated task counts grouped by tenant
   */
  public synchronized TaskSummaryResponse getTasksSummary(@Nullable String tenantFilter) {
    TaskSummaryResponse response = new TaskSummaryResponse();
    Set<String> taskTypes = getTaskTypes();

    if (taskTypes == null || taskTypes.isEmpty()) {
      return response;
    }

    // Map: tenant -> taskType -> aggregated TaskCount
    Map<String, Map<String, TaskCount>> tenantToTaskTypeCounts = new TreeMap<>();
    int totalRunning = 0;
    int totalWaiting = 0;

    for (String taskType : taskTypes) {
      Map<String, TaskCount> taskCounts = getTaskCounts(taskType);
      if (taskCounts == null || taskCounts.isEmpty()) {
        continue;
      }

      // For each parent task, only fetch table breakdown if it has active tasks
      for (Map.Entry<String, TaskCount> entry : taskCounts.entrySet()) {
        String taskName = entry.getKey();
        TaskCount totalTaskCount = entry.getValue();

        // Skip if this parent task has no running/waiting tasks
        if (totalTaskCount.getRunning() == 0 && totalTaskCount.getWaiting() == 0) {
          continue;
        }

        // Get the table name from the first subtask
        // Note: All subtasks in a parent task belong to the same table
        List<PinotTaskConfig> subtaskConfigs = getSubtaskConfigs(taskName);
        if (subtaskConfigs.isEmpty()) {
          continue;
        }

        PinotTaskConfig firstSubtaskConfig = subtaskConfigs.get(0);
        Map<String, String> configs = firstSubtaskConfig.getConfigs();
        String tableName = (configs != null)
            ? configs.getOrDefault(MinionConstants.TABLE_NAME_KEY, UNKNOWN_TABLE_NAME)
            : UNKNOWN_TABLE_NAME;

        if (UNKNOWN_TABLE_NAME.equals(tableName)) {
          continue;
        }

        // Get tenant for this table
        String tenant = getTenantForTable(tableName);

        // Apply tenant filter if specified
        if (tenantFilter != null && !tenantFilter.equals(tenant)) {
          continue;
        }

        // Accumulate counts for this tenant and task type
        tenantToTaskTypeCounts
            .computeIfAbsent(tenant, k -> new TreeMap<>())
            .computeIfAbsent(taskType, k -> new TaskCount())
            .accumulate(totalTaskCount);
      }
    }

    // Build tenant breakdown from aggregated data
    List<TenantTaskBreakdown> tenantBreakdowns = new ArrayList<>();
    for (Map.Entry<String, Map<String, TaskCount>> tenantEntry : tenantToTaskTypeCounts.entrySet()) {
      String tenant = tenantEntry.getKey();
      Map<String, TaskCount> taskTypeCounts = tenantEntry.getValue();

      int tenantRunning = 0;
      int tenantWaiting = 0;
      List<TaskTypeBreakdown> taskTypeBreakdowns = new ArrayList<>();

      for (Map.Entry<String, TaskCount> taskTypeEntry : taskTypeCounts.entrySet()) {
        String taskType = taskTypeEntry.getKey();
        TaskCount aggregatedCount = taskTypeEntry.getValue();

        int running = aggregatedCount.getRunning();
        int waiting = aggregatedCount.getWaiting();

        // Only include task types that have running or waiting tasks
        if (running > 0 || waiting > 0) {
          tenantRunning += running;
          tenantWaiting += waiting;
          taskTypeBreakdowns.add(new TaskTypeBreakdown(taskType, running, waiting));
        }
      }

      // Only include tenants that have active tasks
      if (tenantRunning > 0 || tenantWaiting > 0) {
        totalRunning += tenantRunning;
        totalWaiting += tenantWaiting;
        tenantBreakdowns.add(new TenantTaskBreakdown(tenant, tenantRunning, tenantWaiting, taskTypeBreakdowns));
      }
    }

    response.setTotalRunningTasks(totalRunning);
    response.setTotalWaitingTasks(totalWaiting);
    response.setTaskBreakdown(tenantBreakdowns);

    return response;
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
      taskDebugInfos.put(getPinotTaskName(helixJobName),
          getTaskDebugInfo(workflowContext, helixJobName, null, verbosity));
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
            // Found a match. Add task debug info for this table to the result
            taskDebugInfos.put(pinotTaskName,
                getTaskDebugInfo(workflowContext, helixJobName, tableNameWithType, verbosity));
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
    return getTaskDebugInfo(taskName, null, verbosity);
  }

  /**
   * Given a taskName and table name collects status of the (sub)tasks in the taskName for the table.
   *
   * @param taskName          Pinot taskName
   * @param tableNameWithType table name for which subtask status to fetch
   * @param verbosity         By default, does not show details for completed tasks.
   *                          If verbosity > 0, shows details for all tasks.
   * @return TaskDebugInfo contains details for subtasks in this task batch.
   */
  public synchronized TaskDebugInfo getTaskDebugInfo(String taskName, @Nullable String tableNameWithType,
      int verbosity) {
    String taskType = getTaskType(taskName);
    WorkflowContext workflowContext = _taskDriver.getWorkflowContext(getHelixJobQueueName(taskType));
    if (workflowContext == null) {
      return null;
    }
    String helixJobName = getHelixJobName(taskName);
    return getTaskDebugInfo(workflowContext, helixJobName, tableNameWithType, verbosity);
  }

  private synchronized TaskDebugInfo getTaskDebugInfo(WorkflowContext workflowContext, String helixJobName,
      @Nullable String tableNameWithType, int verbosity) {
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
      String triggeredBy = jobConfig.getTaskConfigMap().values().stream().findFirst()
          .map(TaskConfig::getConfigMap)
          .map(taskConfigs -> taskConfigs.get(MinionConstants.TRIGGERED_BY))
          .orElse("");
      taskDebugInfo.setTriggeredBy(triggeredBy);
      Set<Integer> partitionSet = jobContext.getPartitionSet();
      TaskCount subtaskCount = new TaskCount();
      for (int partition : partitionSet) {
        // First get the partition's state and update the subtaskCount
        String taskIdForPartition = jobContext.getTaskIdForPartition(partition);
        TaskPartitionState partitionState = jobContext.getPartitionState(partition);
        TaskConfig helixTaskConfig = jobConfig.getTaskConfig(taskIdForPartition);
        PinotTaskConfig pinotTaskConfig = null;
        if (helixTaskConfig != null) {
          pinotTaskConfig = PinotTaskConfig.fromHelixTaskConfig(helixTaskConfig);
          if ((tableNameWithType != null) && (!tableNameWithType.equals(pinotTaskConfig.getTableName()))) {
            // Filter task configs that match this table name
            continue;
          }
        }
        subtaskCount.addTaskState(partitionState);
        // Skip details for COMPLETED tasks
        if (!showCompleted && partitionState == TaskPartitionState.COMPLETED) {
          continue;
        }
        SubtaskDebugInfo subtaskDebugInfo = new SubtaskDebugInfo();
        subtaskDebugInfo.setTaskId(taskIdForPartition);
        subtaskDebugInfo.setState(partitionState);
        subtaskDebugInfo.setTriggeredBy(triggeredBy);
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
        subtaskDebugInfo.setTaskConfig(pinotTaskConfig);
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
  public static String getPinotTaskName(String helixJobName) {
    return helixJobName.substring(TASK_QUEUE_PREFIX.length() + getTaskType(helixJobName).length() + 1);
  }

  /**
   * Helper method to extract task type from Pinot task name, Helix JobQueue name or Helix Job name.
   * <p>E.g. Task_DummyTask_12345 -> DummyTask (from Pinot task name)
   * <p>E.g. TaskQueue_DummyTask -> DummyTask (from Helix JobQueue name)
   * <p>E.g. TaskQueue_DummyTask_Task_DummyTask_12345 -> DummyTask (from Helix Job name)
   *
   * @param name Pinot task name, Helix JobQueue name, or Helix Job name
   * @return Task type
   */
  public static String getTaskType(String name) {
    String[] parts = name.split(TASK_NAME_SEPARATOR);
    if (parts.length < 2) {
      throw new IllegalArgumentException(String.format("Invalid task name : %s. Missing separator %s",
          name, TASK_NAME_SEPARATOR));
    }
    return parts[1];
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

  /**
   * Gets the status of all minion instances, including their task counts and drain state.
   *
   * @param statusFilter Optional filter by status ("ONLINE", "OFFLINE", or "DRAINED"). If null, returns all minions.
   * @param includeTaskCounts Whether to include running task counts For each minion. Default false.
   * @return MinionStatusResponse containing status information for minion instances
   */
  public MinionStatusResponse getMinionStatus(String statusFilter, boolean includeTaskCounts) {
    // Validate status filter
    if (statusFilter != null && !statusFilter.isEmpty()) {
      if (!"ONLINE".equalsIgnoreCase(statusFilter)
          && !"OFFLINE".equalsIgnoreCase(statusFilter)
          && !"DRAINED".equalsIgnoreCase(statusFilter)) {
        throw new IllegalArgumentException("Invalid status filter. Must be 'ONLINE', 'OFFLINE', or 'DRAINED'");
      }
    }

    // Get all instances and filter for minions, then sort them
    List<String> allInstances = _helixResourceManager.getAllInstances();
    List<String> minionInstances = allInstances.stream()
        .filter(InstanceTypeUtils::isMinion)
        .sorted()
        .collect(Collectors.toList());

    // Get running task counts per minion only if requested (can be expensive)
    Map<String, Integer> runningTaskCounts = new HashMap<>();
    if (includeTaskCounts) {
      try {
        runningTaskCounts = getRunningTaskCountsPerMinion();
      } catch (Exception e) {
        LOGGER.warn("Failed to get running task counts from task resource manager", e);
        // Continue with an empty map - task counts will be 0
      }
    }

    // Build status list for each minion
    List<MinionStatusResponse.MinionStatus> minionStatusList = new ArrayList<>();
    for (String minionInstanceId : minionInstances) {
      InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
      if (instanceConfig == null) {
        continue;
      }

      // Determine minion status: OFFLINE (disabled in Helix), DRAINED, or ONLINE
      boolean isEnabled = instanceConfig.getInstanceEnabled();
      List<String> tags = instanceConfig.getTags();
      boolean isDrained = tags != null && tags.contains(Helix.DRAINED_MINION_INSTANCE);

      String status;
      if (!isEnabled) {
        status = "OFFLINE";
      } else if (isDrained) {
        status = "DRAINED";
      } else {
        status = "ONLINE";
      }

      // Apply status filter if specified
      if (statusFilter != null && !statusFilter.isEmpty() && !status.equalsIgnoreCase(statusFilter)) {
        continue;
      }

      // Get host and port
      String host = instanceConfig.getHostName();
      int port = Integer.parseInt(instanceConfig.getPort());

      // Get the running task count for this minion
      int runningTaskCount = runningTaskCounts.getOrDefault(minionInstanceId, 0);

      MinionStatusResponse.MinionStatus minionStatus =
          new MinionStatusResponse.MinionStatus(minionInstanceId, host, port, runningTaskCount, status);
      minionStatusList.add(minionStatus);
    }

    return new MinionStatusResponse(minionStatusList.size(), minionStatusList);
  }

  /**
   * Gets the count of running tasks for each minion instance.
   * This method iterates through all task workflows and jobs to count tasks in RUNNING state
   * assigned to each minion.
   *
   * @return Map of minion instance ID to running task count
   */
  public synchronized Map<String, Integer> getRunningTaskCountsPerMinion() {
    Map<String, Integer> runningTaskCounts = new HashMap<>();

    // Get all workflows (task queues)
    Set<String> workflows = _taskDriver.getWorkflows().keySet();

    for (String workflow : workflows) {
      WorkflowContext workflowContext = _taskDriver.getWorkflowContext(workflow);
      if (workflowContext == null) {
        continue;
      }

      // Get all jobs in this workflow
      Set<String> jobs = workflowContext.getJobStates().keySet();
      for (String job : jobs) {
        JobContext jobContext = _taskDriver.getJobContext(job);
        if (jobContext == null) {
          continue;
        }

        // Iterate through all partitions (subtasks) in this job
        Set<Integer> partitions = jobContext.getPartitionSet();
        for (Integer partition : partitions) {
          TaskPartitionState state = jobContext.getPartitionState(partition);
          // Count INIT and RUNNING tasks (INIT means assigned but not yet started)
          if (state == TaskPartitionState.RUNNING || state == TaskPartitionState.INIT) {
            String assignedParticipant = jobContext.getAssignedParticipant(partition);
            if (assignedParticipant != null) {
              runningTaskCounts.put(assignedParticipant,
                  runningTaskCounts.getOrDefault(assignedParticipant, 0) + 1);
            }
          }
        }
      }
    }

    return runningTaskCounts;
  }

  @JsonPropertyOrder({
      "taskState", "subtaskCount", "startTime", "executionStartTime", "finishTime", "triggeredBy",
      "subtaskInfos"
  })
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
    private String _triggeredBy;
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

    public String getTriggeredBy() {
      return _triggeredBy;
    }

    public TaskDebugInfo setTriggeredBy(String triggeredBy) {
      _triggeredBy = triggeredBy;
      return this;
    }

    public TaskCount getSubtaskCount() {
      return _subtaskCount;
    }

    public List<SubtaskDebugInfo> getSubtaskInfos() {
      return _subtaskInfos;
    }
  }

  @JsonPropertyOrder({"taskId", "state", "startTime", "finishTime", "participant", "info", "triggeredBy", "taskConfig"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class SubtaskDebugInfo {
    private String _taskId;
    private TaskPartitionState _state;
    private String _startTime;
    private String _finishTime;
    private String _participant;
    private String _info;
    private String _triggeredBy;
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

    public String getTriggeredBy() {
      return _triggeredBy;
    }

    public SubtaskDebugInfo setTriggeredBy(String triggeredBy) {
      _triggeredBy = triggeredBy;
      return this;
    }

    public PinotTaskConfig getTaskConfig() {
      return _taskConfig;
    }
  }

  /**
   * Result class that organizes tasks by status.
   * Holds sets of tasks directly, making it extensible for future use cases.
   */
  public static class TasksByStatus {
    private Set<String> _inProgressTasks = Collections.emptySet();
    private Set<String> _recentTasks = Collections.emptySet(); // Tasks that started after timestamp (any state)

    public Set<String> getInProgressTasks() {
      return _inProgressTasks;
    }

    public void setInProgressTasks(Set<String> inProgressTasks) {
      _inProgressTasks = inProgressTasks;
    }

    public Set<String> getRecentTasks() {
      return _recentTasks;
    }

    public void setRecentTasks(Set<String> recentTasks) {
      _recentTasks = recentTasks;
    }
  }

  @JsonPropertyOrder({"total", "completed", "running", "waiting", "error", "unknown", "dropped", "timedOut", "aborted"})
  public static class TaskCount {
    private int _waiting;   // Number of tasks waiting to be scheduled on minions
    private int _error;     // Number of tasks in error
    private int _running;   // Number of tasks currently running in minions
    private int _completed; // Number of tasks completed normally
    private int _unknown;   // Number of tasks with all other states
    private int _total;     // Total number of tasks in the batch
    private int _dropped;   // Total number of tasks dropped
    // (Task can be dropped due to no available assigned instance, etc.)
    private int _timedOut;  // Total number of tasks timed out
    private int _aborted;   // Total number of tasks aborted

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
          case DROPPED:
            _dropped++;
            break;
          case TIMED_OUT:
            _timedOut++;
            break;
          case TASK_ABORTED:
            _aborted++;
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

    public int getDropped() {
      return _dropped;
    }

    public int getTimedOut() {
      return _timedOut;
    }

    public int getAborted() {
      return _aborted;
    }

    public void accumulate(TaskCount other) {
      _waiting += other.getWaiting();
      _running += other.getRunning();
      _error += other.getError();
      _completed += other.getCompleted();
      _unknown += other.getUnknown();
      _total += other.getTotal();
      _dropped += other.getDropped();
      _timedOut += other.getTimedOut();
      _aborted += other.getAborted();
    }
  }

  public static class TaskStatusSummary {
    private TaskCount _taskCount = new TaskCount();
    private Map<String, Long> _subtaskWaitingTimes = new HashMap<>(); // subtask ID -> waiting time in milliseconds
    private Map<String, Long> _subtaskRunningTimes = new HashMap<>(); // subtask ID -> running time in milliseconds

    public TaskCount getTaskCount() {
      return _taskCount;
    }

    public void setTaskCount(TaskCount taskCount) {
      _taskCount = taskCount;
    }

    public Map<String, Long> getSubtaskWaitingTimes() {
      return _subtaskWaitingTimes;
    }

    public void setSubtaskWaitingTimes(Map<String, Long> subtaskWaitingTimes) {
      _subtaskWaitingTimes = subtaskWaitingTimes;
    }

    public Map<String, Long> getSubtaskRunningTimes() {
      return _subtaskRunningTimes;
    }

    public void setSubtaskRunningTimes(Map<String, Long> subtaskRunningTimes) {
      _subtaskRunningTimes = subtaskRunningTimes;
    }
  }

  /**
   * Response model for the {@code GET /tasks/summary} endpoint.
   *
   * <p>Provides summary information about tasks currently managed by the Pinot cluster, grouped by tenant.
   * Only tasks with RUNNING or WAITING subtasks are included; completed, failed, or aborted tasks are excluded.
   *
   * <p>Fields:
   * <ul>
   *   <li>{@code totalRunningTasks}: Total tasks in RUNNING or INIT state across all tenants</li>
   *   <li>{@code totalWaitingTasks}: Total tasks in WAITING state (not yet assigned to a worker)</li>
   *   <li>{@code taskBreakdown}: Task counts grouped by tenant and task type. Tasks with unknown tenant
   *       configuration appear under tenant name "unknown"</li>
   * </ul>
   *
   * @see TenantTaskBreakdown
   * @see TaskTypeBreakdown
   */
  @JsonPropertyOrder({"totalRunningTasks", "totalWaitingTasks", "taskBreakdown"})
  public static class TaskSummaryResponse {
    private int _totalRunningTasks;
    private int _totalWaitingTasks;
    private List<TenantTaskBreakdown> _taskBreakdown;

    public TaskSummaryResponse() {
      _totalRunningTasks = 0;
      _totalWaitingTasks = 0;
      _taskBreakdown = new ArrayList<>();
    }

    public int getTotalRunningTasks() {
      return _totalRunningTasks;
    }

    public void setTotalRunningTasks(int totalRunningTasks) {
      _totalRunningTasks = totalRunningTasks;
    }

    public int getTotalWaitingTasks() {
      return _totalWaitingTasks;
    }

    public void setTotalWaitingTasks(int totalWaitingTasks) {
      _totalWaitingTasks = totalWaitingTasks;
    }

    public List<TenantTaskBreakdown> getTaskBreakdown() {
      return _taskBreakdown;
    }

    public void setTaskBreakdown(List<TenantTaskBreakdown> taskBreakdown) {
      _taskBreakdown = taskBreakdown;
    }
  }

  /**
   * Tenant-level breakdown of task counts for the {@code /tasks/summary} API response.
   *
   * <p>Fields:
   * <ul>
   *   <li>{@code tenant}: Server tenant name from table configuration (or "unknown" if not configured)</li>
   *   <li>{@code runningTasks}: Total tasks in RUNNING or INIT state for this tenant</li>
   *   <li>{@code waitingTasks}: Total tasks waiting to be assigned for this tenant</li>
   *   <li>{@code taskTypeBreakdown}: Running/waiting counts per task type for this tenant</li>
   * </ul>
   *
   * @see TaskSummaryResponse
   * @see TaskTypeBreakdown
   */
  @JsonPropertyOrder({"tenant", "runningTasks", "waitingTasks", "taskTypeBreakdown"})
  public static class TenantTaskBreakdown {
    private String _tenant;
    private int _runningTasks;
    private int _waitingTasks;
    private List<TaskTypeBreakdown> _taskTypeBreakdown;

    public TenantTaskBreakdown() {
      _taskTypeBreakdown = new ArrayList<>();
    }

    public TenantTaskBreakdown(String tenant, int runningTasks, int waitingTasks,
        List<TaskTypeBreakdown> taskTypeBreakdown) {
      _tenant = tenant;
      _runningTasks = runningTasks;
      _waitingTasks = waitingTasks;
      _taskTypeBreakdown = taskTypeBreakdown;
    }

    public String getTenant() {
      return _tenant;
    }

    public void setTenant(String tenant) {
      _tenant = tenant;
    }

    public int getRunningTasks() {
      return _runningTasks;
    }

    public void setRunningTasks(int runningTasks) {
      _runningTasks = runningTasks;
    }

    public int getWaitingTasks() {
      return _waitingTasks;
    }

    public void setWaitingTasks(int waitingTasks) {
      _waitingTasks = waitingTasks;
    }

    public List<TaskTypeBreakdown> getTaskTypeBreakdown() {
      return _taskTypeBreakdown;
    }

    public void setTaskTypeBreakdown(List<TaskTypeBreakdown> taskTypeBreakdown) {
      _taskTypeBreakdown = taskTypeBreakdown;
    }
  }

  /**
   * Task type breakdown of task counts for the {@code /tasks/summary} API response.
   *
   * <p>Fields:
   * <ul>
   *   <li>{@code taskType}: Task type name (e.g., "SegmentGenerationAndPushTask", "MergeRollupTask")</li>
   *   <li>{@code runningCount}: Tasks in RUNNING state</li>
   *   <li>{@code waitingCount}: Tasks waiting to be scheduled</li>
   * </ul>
   *
   * @see TaskSummaryResponse
   * @see TenantTaskBreakdown
   */
  @JsonPropertyOrder({"taskType", "runningCount", "waitingCount"})
  public static class TaskTypeBreakdown {
    private String _taskType;
    private int _runningCount;
    private int _waitingCount;

    public TaskTypeBreakdown() {
    }

    public TaskTypeBreakdown(String taskType, int runningCount, int waitingCount) {
      _taskType = taskType;
      _runningCount = runningCount;
      _waitingCount = waitingCount;
    }

    public String getTaskType() {
      return _taskType;
    }

    public void setTaskType(String taskType) {
      _taskType = taskType;
    }

    public int getRunningCount() {
      return _runningCount;
    }

    public void setRunningCount(int runningCount) {
      _runningCount = runningCount;
    }

    public int getWaitingCount() {
      return _waitingCount;
    }

    public void setWaitingCount(int waitingCount) {
      _waitingCount = waitingCount;
    }
  }
}
