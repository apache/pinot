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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.helix.AccessOption;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.minion.TaskGeneratorMostRecentRunInfo;
import org.apache.pinot.common.minion.TaskManagerStatusCache;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.exception.NoTaskScheduledException;
import org.apache.pinot.controller.api.exception.TaskAlreadyExistsException;
import org.apache.pinot.controller.api.exception.UnknownTaskTypeException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorRegistry;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>PinotTaskManager</code> is the component inside Pinot Controller to periodically check the Pinot
 * cluster status and schedule new tasks.
 * <p><code>PinotTaskManager</code> is also responsible for checking the health status on each type of tasks, detect and
 * fix issues accordingly.
 */
public class PinotTaskManager extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTaskManager.class);

  public final static String PINOT_TASK_MANAGER_KEY = "PinotTaskManager";
  public final static String SKIP_LATE_CRON_SCHEDULE = "SkipLateCronSchedule";
  public final static String MAX_CRON_SCHEDULE_DELAY_IN_SECONDS = "MaxCronScheduleDelayInSeconds";
  public final static String LEAD_CONTROLLER_MANAGER_KEY = "LeadControllerManager";
  public final static String SCHEDULE_KEY = "schedule";

  private static final String TABLE_CONFIG_PARENT_PATH = "/CONFIGS/TABLE";
  private static final String TABLE_CONFIG_PATH_PREFIX = "/CONFIGS/TABLE/";
  private static final String TASK_QUEUE_PATH_PATTERN = "/TaskRebalancer/TaskQueue_%s/Context";

  private final PinotHelixTaskResourceManager _helixTaskResourceManager;
  private final ClusterInfoAccessor _clusterInfoAccessor;
  private final TaskGeneratorRegistry _taskGeneratorRegistry;

  // For cron-based scheduling
  private final Scheduler _scheduler;
  private final boolean _skipLateCronSchedule;
  private final int _maxCronScheduleDelayInSeconds;
  private final Map<String, Map<String, String>> _tableTaskTypeToCronExpressionMap = new ConcurrentHashMap<>();
  private final Map<String, TableTaskSchedulerUpdater> _tableTaskSchedulerUpdaterMap = new ConcurrentHashMap<>();

  // For metrics
  private final Map<String, TaskTypeMetricsUpdater> _taskTypeMetricsUpdaterMap = new ConcurrentHashMap<>();
  private final Map<TaskState, Integer> _taskStateToCountMap = new ConcurrentHashMap<>();

  private final ZkTableConfigChangeListener _zkTableConfigChangeListener = new ZkTableConfigChangeListener();

  private final TaskManagerStatusCache<TaskGeneratorMostRecentRunInfo> _taskManagerStatusCache;

  public PinotTaskManager(PinotHelixTaskResourceManager helixTaskResourceManager,
      PinotHelixResourceManager helixResourceManager, LeadControllerManager leadControllerManager,
      ControllerConf controllerConf, ControllerMetrics controllerMetrics,
      TaskManagerStatusCache<TaskGeneratorMostRecentRunInfo> taskManagerStatusCache, Executor executor,
      MultiThreadedHttpConnectionManager connectionManager) {
    super("PinotTaskManager", controllerConf.getTaskManagerFrequencyInSeconds(),
        controllerConf.getPinotTaskManagerInitialDelaySeconds(), helixResourceManager, leadControllerManager,
        controllerMetrics);
    _helixTaskResourceManager = helixTaskResourceManager;
    _taskManagerStatusCache = taskManagerStatusCache;
    _clusterInfoAccessor =
        new ClusterInfoAccessor(helixResourceManager, helixTaskResourceManager, controllerConf, controllerMetrics,
            leadControllerManager, executor, connectionManager);
    _taskGeneratorRegistry = new TaskGeneratorRegistry(_clusterInfoAccessor);
    _skipLateCronSchedule = controllerConf.isSkipLateCronSchedule();
    _maxCronScheduleDelayInSeconds = controllerConf.getMaxCronScheduleDelayInSeconds();
    if (controllerConf.isPinotTaskManagerSchedulerEnabled()) {
      try {
        _scheduler = new StdSchedulerFactory().getScheduler();
        _scheduler.start();
        synchronized (_zkTableConfigChangeListener) {
          // Subscribe child changes before reading the data to avoid missing changes
          LOGGER.info("Check and subscribe to tables change under PropertyStore path: {}", TABLE_CONFIG_PARENT_PATH);
          _pinotHelixResourceManager.getPropertyStore()
              .subscribeChildChanges(TABLE_CONFIG_PARENT_PATH, _zkTableConfigChangeListener);
          List<String> tables = _pinotHelixResourceManager.getPropertyStore()
              .getChildNames(TABLE_CONFIG_PARENT_PATH, AccessOption.PERSISTENT);
          if (CollectionUtils.isNotEmpty(tables)) {
            checkTableConfigChanges(tables);
          }
        }
      } catch (SchedulerException e) {
        throw new RuntimeException("Caught exception while setting up the scheduler", e);
      }
    } else {
      _scheduler = null;
    }
  }

  public Map<String, String> createTask(String taskType, String tableName, @Nullable String taskName,
      Map<String, String> taskConfigs)
      throws Exception {
    if (taskName == null) {
      taskName = tableName + "_" + UUID.randomUUID();
      LOGGER.info("Task name is missing, auto-generate one: {}", taskName);
    }
    String minionInstanceTag =
        taskConfigs.getOrDefault("minionInstanceTag", CommonConstants.Helix.UNTAGGED_MINION_INSTANCE);
    _helixTaskResourceManager.ensureTaskQueueExists(taskType);
    addTaskTypeMetricsUpdaterIfNeeded(taskType);
    String parentTaskName = _helixTaskResourceManager.getParentTaskName(taskType, taskName);
    TaskState taskState = _helixTaskResourceManager.getTaskState(parentTaskName);
    if (taskState != null) {
      throw new TaskAlreadyExistsException(
          "Task [" + taskName + "] of type [" + taskType + "] is already created. Current state is " + taskState);
    }
    List<String> tableNameWithTypes = new ArrayList<>();
    if (TableNameBuilder.getTableTypeFromTableName(tableName) == null) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      if (_pinotHelixResourceManager.hasOfflineTable(offlineTableName)) {
        tableNameWithTypes.add(offlineTableName);
      }
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      if (_pinotHelixResourceManager.hasRealtimeTable(realtimeTableName)) {
        tableNameWithTypes.add(realtimeTableName);
      }
    } else {
      if (_pinotHelixResourceManager.hasTable(tableName)) {
        tableNameWithTypes.add(tableName);
      }
    }
    if (tableNameWithTypes.isEmpty()) {
      throw new TableNotFoundException("'tableName' " + tableName + " is not found");
    }

    PinotTaskGenerator taskGenerator = _taskGeneratorRegistry.getTaskGenerator(taskType);
    // Generate each type of tasks
    if (taskGenerator == null) {
      throw new UnknownTaskTypeException(
          "Task type: " + taskType + " is not registered, cannot enable it for table: " + tableName);
    }
    // responseMap holds the table to task name mapping.
    Map<String, String> responseMap = new HashMap<>();
    for (String tableNameWithType : tableNameWithTypes) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      LOGGER.info("Trying to create tasks of type: {}, table: {}", taskType, tableNameWithType);
      List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(tableConfig, taskConfigs);
      if (pinotTaskConfigs.isEmpty()) {
        LOGGER.warn("No ad-hoc task generated for task type: {}", taskType);
        continue;
      }
      LOGGER.info("Submitting ad-hoc task for task type: {} with task configs: {}", taskType, pinotTaskConfigs);
      _controllerMetrics.addMeteredTableValue(taskType, ControllerMeter.NUMBER_ADHOC_TASKS_SUBMITTED, 1);
      responseMap.put(tableNameWithType,
          _helixTaskResourceManager.submitTask(parentTaskName, pinotTaskConfigs, minionInstanceTag,
              taskGenerator.getTaskTimeoutMs(), taskGenerator.getNumConcurrentTasksPerInstance(),
              taskGenerator.getMaxAttemptsPerTask()));
    }
    if (responseMap.isEmpty()) {
      throw new NoTaskScheduledException("No task scheduled for 'tableName': " + tableName);
    }
    return responseMap;
  }

  private class ZkTableConfigChangeListener implements IZkChildListener {

    @Override
    public synchronized void handleChildChange(String path, List<String> tableNamesWithType) {
      checkTableConfigChanges(tableNamesWithType);
    }
  }

  private void checkTableConfigChanges(List<String> tableNamesWithType) {
    LOGGER.info("Checking task config changes in table configs");
    // NOTE: we avoided calling _leadControllerManager::isLeaderForTable here to skip tables the current
    // controller is not leader for. Because _leadControllerManager updates its leadership states based
    // on a ZK event, and that ZK event may come later than this method call, making current controller
    // think it's not lead for certain tables, when it should be if the leadership states are fully updated.
    if (_tableTaskSchedulerUpdaterMap.isEmpty()) {
      // Initial setup
      for (String tableNameWithType : tableNamesWithType) {
        subscribeTableConfigChanges(tableNameWithType);
      }
    } else {
      Set<String> existingTables = new HashSet<>(_tableTaskSchedulerUpdaterMap.keySet());
      Set<String> tablesToAdd = new HashSet<>();
      for (String tableNameWithType : tableNamesWithType) {
        if (!existingTables.remove(tableNameWithType)) {
          tablesToAdd.add(tableNameWithType);
        }
      }
      for (String tableNameWithType : tablesToAdd) {
        subscribeTableConfigChanges(tableNameWithType);
      }
      if (!existingTables.isEmpty()) {
        LOGGER.info("Found tables to clean up cron task scheduler: {}", existingTables);
        for (String tableNameWithType : existingTables) {
          cleanUpCronTaskSchedulerForTable(tableNameWithType);
        }
      }
    }
  }

  private String getPropertyStorePathForTable(String tableWithType) {
    return TABLE_CONFIG_PATH_PREFIX + tableWithType;
  }

  private String getPropertyStorePathForTaskQueue(String taskType) {
    return String.format(TASK_QUEUE_PATH_PATTERN, taskType);
  }

  public synchronized void cleanUpCronTaskSchedulerForTable(String tableWithType) {
    LOGGER.info("Cleaning up task in scheduler for table {}", tableWithType);
    TableTaskSchedulerUpdater tableTaskSchedulerUpdater = _tableTaskSchedulerUpdaterMap.get(tableWithType);
    if (tableTaskSchedulerUpdater != null) {
      _pinotHelixResourceManager.getPropertyStore()
          .unsubscribeDataChanges(getPropertyStorePathForTable(tableWithType), tableTaskSchedulerUpdater);
    }
    removeAllTasksFromCronExpressions(tableWithType);
    _tableTaskSchedulerUpdaterMap.remove(tableWithType);
  }

  private synchronized void removeAllTasksFromCronExpressions(String tableWithType) {
    Set<JobKey> jobKeys;
    try {
      jobKeys = _scheduler.getJobKeys(GroupMatcher.anyJobGroup());
    } catch (SchedulerException e) {
      LOGGER.error("Got exception when fetching all jobKeys", e);
      return;
    }
    for (JobKey jobKey : jobKeys) {
      if (jobKey.getName().equals(tableWithType)) {
        try {
          _scheduler.deleteJob(jobKey);
          _controllerMetrics.addValueToTableGauge(getCronJobName(tableWithType, jobKey.getGroup()),
              ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED, -1L);
        } catch (SchedulerException e) {
          LOGGER.error("Got exception when deleting the scheduled job - {}", jobKey, e);
        }
      }
    }
    _tableTaskTypeToCronExpressionMap.remove(tableWithType);
  }

  public static String getCronJobName(String tableWithType, String taskType) {
    return String.format("%s.%s", tableWithType, taskType);
  }

  public synchronized void subscribeTableConfigChanges(String tableWithType) {
    if (_tableTaskSchedulerUpdaterMap.containsKey(tableWithType)) {
      return;
    }
    TableTaskSchedulerUpdater tableTaskSchedulerUpdater = new TableTaskSchedulerUpdater(tableWithType, this);
    _pinotHelixResourceManager.getPropertyStore()
        .subscribeDataChanges(getPropertyStorePathForTable(tableWithType), tableTaskSchedulerUpdater);
    _tableTaskSchedulerUpdaterMap.put(tableWithType, tableTaskSchedulerUpdater);
    try {
      updateCronTaskScheduler(tableWithType);
    } catch (Exception e) {
      LOGGER.error("Failed to create cron task in scheduler for table: {}", tableWithType, e);
    }
  }

  public synchronized void updateCronTaskScheduler(String tableWithType) {
    LOGGER.info("Trying to update task schedule for table: {}", tableWithType);
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableWithType);
    if (tableConfig == null) {
      LOGGER.info("tableConfig is null, trying to remove all the tasks for table {} if any", tableWithType);
      removeAllTasksFromCronExpressions(tableWithType);
      return;
    }
    TableTaskConfig taskConfig = tableConfig.getTaskConfig();
    if (taskConfig == null) {
      LOGGER.info("taskConfig is null, trying to remove all the tasks for table {} if any", tableWithType);
      removeAllTasksFromCronExpressions(tableWithType);
      return;
    }
    Map<String, Map<String, String>> taskTypeConfigsMap = taskConfig.getTaskTypeConfigsMap();
    if (taskTypeConfigsMap == null) {
      LOGGER.info("taskTypeConfigsMap is null, trying to remove all the tasks for table {} if any", tableWithType);
      removeAllTasksFromCronExpressions(tableWithType);
      return;
    }
    Map<String, String> taskToCronExpressionMap = getTaskToCronExpressionMap(taskTypeConfigsMap);
    LOGGER.info("Got taskToCronExpressionMap {} ", taskToCronExpressionMap);
    updateCronTaskScheduler(tableWithType, taskToCronExpressionMap);
  }

  private void updateCronTaskScheduler(String tableWithType, Map<String, String> taskToCronExpressionMap) {
    Map<String, String> existingScheduledTasks = _tableTaskTypeToCronExpressionMap.get(tableWithType);
    if (existingScheduledTasks != null && !existingScheduledTasks.isEmpty()) {

      // Loop over existing tasks to identify tasks to be removed or updated
      for (Map.Entry<String, String> entry : existingScheduledTasks.entrySet()) {
        String existingTaskType = entry.getKey();
        String newCronExpression = taskToCronExpressionMap.get(existingTaskType);

        if (newCronExpression == null) {
          // Task should be removed
          try {
            _scheduler.deleteJob(JobKey.jobKey(tableWithType, existingTaskType));
            _controllerMetrics.addValueToTableGauge(getCronJobName(tableWithType, existingTaskType),
                ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED, -1L);
          } catch (SchedulerException e) {
            LOGGER.error("Failed to delete scheduled job for table {}, task type {}", tableWithType, existingTaskType,
                e);
          }
          continue;
        }

        if (!entry.getValue().equalsIgnoreCase(newCronExpression)) {
          // Update existing task with new cron expr
          try {
            TriggerKey triggerKey = TriggerKey.triggerKey(tableWithType, existingTaskType);
            Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
                .withSchedule(CronScheduleBuilder.cronSchedule(newCronExpression)).build();
            _scheduler.rescheduleJob(triggerKey, trigger);
          } catch (SchedulerException e) {
            LOGGER.error("Failed to update scheduled job for table {}, task type {}", tableWithType, existingTaskType,
                e);
          }
        }
      }

      // Loop over new tasks to identify tasks to be added
      for (Map.Entry<String, String> entry : taskToCronExpressionMap.entrySet()) {
        String newTaskType = entry.getKey();
        if (!existingScheduledTasks.containsKey(newTaskType)) {
          String newCronExpression = entry.getValue();
          try {
            scheduleJob(tableWithType, newTaskType, newCronExpression);
          } catch (SchedulerException e) {
            LOGGER.error("Failed to schedule cron task for table {}, task {}, cron expr {}", tableWithType, newTaskType,
                newCronExpression, e);
          }
        }
      }
    } else {
      for (String taskType : taskToCronExpressionMap.keySet()) {
        // Schedule new job
        String cronExpr = taskToCronExpressionMap.get(taskType);
        try {
          scheduleJob(tableWithType, taskType, cronExpr);
        } catch (SchedulerException e) {
          LOGGER.error("Failed to schedule cron task for table {}, task {}, cron expr {}", tableWithType, taskType,
              cronExpr, e);
        }
      }
    }
    _tableTaskTypeToCronExpressionMap.put(tableWithType, taskToCronExpressionMap);
  }

  private void scheduleJob(String tableWithType, String taskType, String cronExprStr)
      throws SchedulerException {
    boolean exists = false;
    try {
      exists = _scheduler.checkExists(JobKey.jobKey(tableWithType, taskType));
    } catch (SchedulerException e) {
      LOGGER.error("Failed to check job existence for job key - table: {}, task: {} ", tableWithType, taskType, e);
    }
    if (!exists) {
      LOGGER.info("Trying to schedule a job with cron expression: {} for table {}, task type: {}", cronExprStr,
          tableWithType, taskType);
      Trigger trigger = TriggerBuilder.newTrigger().withIdentity(TriggerKey.triggerKey(tableWithType, taskType))
          .withSchedule(CronScheduleBuilder.cronSchedule(cronExprStr)).build();
      JobDataMap jobDataMap = new JobDataMap();
      jobDataMap.put(PINOT_TASK_MANAGER_KEY, this);
      jobDataMap.put(LEAD_CONTROLLER_MANAGER_KEY, _leadControllerManager);
      jobDataMap.put(SKIP_LATE_CRON_SCHEDULE, _skipLateCronSchedule);
      jobDataMap.put(MAX_CRON_SCHEDULE_DELAY_IN_SECONDS, _maxCronScheduleDelayInSeconds);
      JobDetail jobDetail =
          JobBuilder.newJob(CronJobScheduleJob.class).withIdentity(tableWithType, taskType).setJobData(jobDataMap)
              .build();
      try {
        _scheduler.scheduleJob(jobDetail, trigger);
        _controllerMetrics.addValueToTableGauge(getCronJobName(tableWithType, taskType),
            ControllerGauge.CRON_SCHEDULER_JOB_SCHEDULED, 1L);
      } catch (Exception e) {
        LOGGER.error("Failed to parse Cron expression - " + cronExprStr, e);
        throw e;
      }
      Date nextRuntime = trigger.getNextFireTime();
      LOGGER.info("Scheduled task for table: {}, task type: {}, next runtime: {}", tableWithType, taskType,
          nextRuntime);
    }
  }

  private Map<String, String> getTaskToCronExpressionMap(Map<String, Map<String, String>> taskTypeConfigsMap) {
    Map<String, String> taskToCronExpressionMap = new HashMap<>();
    for (String taskType : taskTypeConfigsMap.keySet()) {
      Map<String, String> taskTypeConfig = taskTypeConfigsMap.get(taskType);
      if (taskTypeConfig == null || !taskTypeConfig.containsKey(SCHEDULE_KEY)) {
        continue;
      }
      String cronExprStr = taskTypeConfig.get(SCHEDULE_KEY);
      if (cronExprStr == null) {
        continue;
      }
      taskToCronExpressionMap.put(taskType, cronExprStr);
    }
    return taskToCronExpressionMap;
  }

  /**
   * Returns the cluster info accessor.
   * <p>Cluster info accessor can be used to initialize the task generator.
   */
  public ClusterInfoAccessor getClusterInfoAccessor() {
    return _clusterInfoAccessor;
  }

  /**
   * Returns the task generator registry.
   */
  public TaskGeneratorRegistry getTaskGeneratorRegistry() {
    return _taskGeneratorRegistry;
  }

  /**
   * Registers a task generator.
   * <p>This method can be used to plug in custom task generators.
   */
  public void registerTaskGenerator(PinotTaskGenerator taskGenerator) {
    _taskGeneratorRegistry.registerTaskGenerator(taskGenerator);
  }

  /**
   * Public API to schedule tasks (all task types) for all tables. It might be called from the non-leader controller.
   * Returns a map from the task type to the task scheduled.
   */
  public synchronized Map<String, String> scheduleTasks() {
    return scheduleTasks(_pinotHelixResourceManager.getAllTables(), false);
  }

  /**
   * Helper method to schedule tasks (all task types) for the given tables that have the tasks enabled. Returns a map
   * from the task type to the task scheduled.
   */
  private synchronized Map<String, String> scheduleTasks(List<String> tableNamesWithType, boolean isLeader) {
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.NUMBER_TIMES_SCHEDULE_TASKS_CALLED, 1L);

    // Scan all table configs to get the tables with tasks enabled
    Map<String, List<TableConfig>> enabledTableConfigMap = new HashMap<>();
    for (String tableNameWithType : tableNamesWithType) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (tableConfig != null && tableConfig.getTaskConfig() != null) {
        Set<String> enabledTaskTypes = tableConfig.getTaskConfig().getTaskTypeConfigsMap().keySet();
        for (String enabledTaskType : enabledTaskTypes) {
          enabledTableConfigMap.computeIfAbsent(enabledTaskType, k -> new ArrayList<>()).add(tableConfig);
        }
      }
    }

    // Generate each type of tasks
    Map<String, String> tasksScheduled = new HashMap<>();
    for (Map.Entry<String, List<TableConfig>> entry : enabledTableConfigMap.entrySet()) {
      String taskType = entry.getKey();
      List<TableConfig> enabledTableConfigs = entry.getValue();
      PinotTaskGenerator taskGenerator = _taskGeneratorRegistry.getTaskGenerator(taskType);
      if (taskGenerator != null) {
        _helixTaskResourceManager.ensureTaskQueueExists(taskType);
        addTaskTypeMetricsUpdaterIfNeeded(taskType);
        tasksScheduled.put(taskType, scheduleTask(taskGenerator, enabledTableConfigs, isLeader));
      } else {
        List<String> enabledTables = new ArrayList<>(enabledTableConfigs.size());
        for (TableConfig enabledTableConfig : enabledTableConfigs) {
          enabledTables.add(enabledTableConfig.getTableName());
        }
        LOGGER.warn("Task type: {} is not registered, cannot enable it for tables: {}", taskType, enabledTables);
        tasksScheduled.put(taskType, null);
      }
    }

    return tasksScheduled;
  }

  /**
   * Helper method to schedule task with the given task generator for the given tables that have the task enabled.
   * Returns the task name, or {@code null} if no task is scheduled.
   */
  @Nullable
  private String scheduleTask(PinotTaskGenerator taskGenerator, List<TableConfig> enabledTableConfigs,
      boolean isLeader) {
    LOGGER.info("Trying to schedule task type: {}, isLeader: {}", taskGenerator.getTaskType(), isLeader);
    List<PinotTaskConfig> pinotTaskConfigs;
    try {
      /* TODO taskGenerator may skip generating tasks for some of the tables being passed to it.
        In that case, we should not be storing success timestamps for those table. Same with exceptions that should
        only be associated with the table for which it was raised and not every eligible table. We can have the
        generateTasks() return a list of TaskGeneratorMostRecentRunInfo for each table
       */
      pinotTaskConfigs = taskGenerator.generateTasks(enabledTableConfigs);
      long successRunTimestamp = System.currentTimeMillis();
      for (TableConfig tableConfig : enabledTableConfigs) {
        _taskManagerStatusCache.saveTaskGeneratorInfo(tableConfig.getTableName(), taskGenerator.getTaskType(),
            taskGeneratorMostRecentRunInfo -> taskGeneratorMostRecentRunInfo.addSuccessRunTs(successRunTimestamp));
        // before the first task schedule, the follow two gauge metrics will be empty
        // TODO: find a better way to report task generation information
        _controllerMetrics.setOrUpdateTableGauge(tableConfig.getTableName(), taskGenerator.getTaskType(),
            ControllerGauge.TIME_MS_SINCE_LAST_SUCCESSFUL_MINION_TASK_GENERATION,
            () -> System.currentTimeMillis() - successRunTimestamp);
        _controllerMetrics.setOrUpdateTableGauge(tableConfig.getTableName(), taskGenerator.getTaskType(),
            ControllerGauge.LAST_MINION_TASK_GENERATION_ENCOUNTERS_ERROR, 0L);
      }
    } catch (Exception e) {
      StringWriter errors = new StringWriter();
      try (PrintWriter pw = new PrintWriter(errors)) {
        e.printStackTrace(pw);
      }
      long successRunTimestamp = System.currentTimeMillis();
      for (TableConfig tableConfig : enabledTableConfigs) {
        _taskManagerStatusCache.saveTaskGeneratorInfo(tableConfig.getTableName(), taskGenerator.getTaskType(),
            taskGeneratorMostRecentRunInfo -> taskGeneratorMostRecentRunInfo.addErrorRunMessage(successRunTimestamp,
                errors.toString()));
        // before the first task schedule, the follow gauge metric will be empty
        // TODO: find a better way to report task generation information
        _controllerMetrics.setOrUpdateTableGauge(tableConfig.getTableName(), taskGenerator.getTaskType(),
            ControllerGauge.LAST_MINION_TASK_GENERATION_ENCOUNTERS_ERROR, 1L);
      }
      throw e;
    }
    if (!isLeader) {
      taskGenerator.nonLeaderCleanUp();
    }
    String taskType = taskGenerator.getTaskType();
    int numTasks = pinotTaskConfigs.size();
    if (numTasks > 0) {
      LOGGER.info("Submitting {} tasks for task type: {} with task configs: {}", numTasks, taskType, pinotTaskConfigs);
      _controllerMetrics.addMeteredTableValue(taskType, ControllerMeter.NUMBER_TASKS_SUBMITTED, numTasks);
      return _helixTaskResourceManager.submitTask(pinotTaskConfigs, taskGenerator.getTaskTimeoutMs(),
          taskGenerator.getNumConcurrentTasksPerInstance(), taskGenerator.getMaxAttemptsPerTask());
    }
    LOGGER.info("No task to schedule for task type: {}", taskType);
    return null;
  }

  /**
   * Public API to schedule tasks (all task types) for the given table. It might be called from the non-leader
   * controller. Returns a map from the task type to the task scheduled.
   */
  public synchronized Map<String, String> scheduleTasks(String tableNameWithType) {
    return scheduleTasks(Collections.singletonList(tableNameWithType), false);
  }

  /**
   * Public API to schedule task for the given task type. It might be called from the non-leader controller. Returns the
   * task name, or {@code null} if no task is scheduled.
   */
  @Nullable
  public synchronized String scheduleTask(String taskType) {
    PinotTaskGenerator taskGenerator = _taskGeneratorRegistry.getTaskGenerator(taskType);
    Preconditions.checkState(taskGenerator != null, "Task type: %s is not registered", taskType);

    // Scan all table configs to get the tables with task enabled
    List<TableConfig> enabledTableConfigs = new ArrayList<>();
    for (String tableNameWithType : _pinotHelixResourceManager.getAllTables()) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (tableConfig != null && tableConfig.getTaskConfig() != null && tableConfig.getTaskConfig()
          .isTaskTypeEnabled(taskType)) {
        enabledTableConfigs.add(tableConfig);
      }
    }

    _helixTaskResourceManager.ensureTaskQueueExists(taskType);
    addTaskTypeMetricsUpdaterIfNeeded(taskType);
    return scheduleTask(taskGenerator, enabledTableConfigs, false);
  }

  /**
   * Public API to schedule task for the given task type on the given table. It might be called from the non-leader
   * controller. Returns the task name, or {@code null} if no task is scheduled.
   */
  @Nullable
  public synchronized String scheduleTask(String taskType, String tableNameWithType) {
    PinotTaskGenerator taskGenerator = _taskGeneratorRegistry.getTaskGenerator(taskType);
    Preconditions.checkState(taskGenerator != null, "Task type: %s is not registered", taskType);

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);

    Preconditions.checkState(
        tableConfig.getTaskConfig() != null && tableConfig.getTaskConfig().isTaskTypeEnabled(taskType),
        "Table: %s does not have task type: %s enabled", tableNameWithType, taskType);

    _helixTaskResourceManager.ensureTaskQueueExists(taskType);
    addTaskTypeMetricsUpdaterIfNeeded(taskType);
    return scheduleTask(taskGenerator, Collections.singletonList(tableConfig), false);
  }

  @Override
  protected void processTables(List<String> tableNamesWithType, Properties taskProperties) {
    scheduleTasks(tableNamesWithType, true);
  }

  @Override
  public void cleanUpTask() {
    LOGGER.info("Cleaning up all task generators");
    for (String taskType : _taskGeneratorRegistry.getAllTaskTypes()) {
      _taskGeneratorRegistry.getTaskGenerator(taskType).nonLeaderCleanUp();
    }
  }

  @Nullable
  public Scheduler getScheduler() {
    return _scheduler;
  }

  public synchronized void reportMetrics(String taskType) {
    // Reset all counters to 0
    for (Map.Entry<TaskState, Integer> entry : _taskStateToCountMap.entrySet()) {
      entry.setValue(0);
    }
    if (_helixTaskResourceManager.getTaskTypes().contains(taskType)) {
      Map<String, TaskState> taskStates = _helixTaskResourceManager.getTaskStates(taskType);
      for (TaskState taskState : taskStates.values()) {
        _taskStateToCountMap.merge(taskState, 1, Integer::sum);
      }
    }
    for (Map.Entry<TaskState, Integer> taskStateEntry : _taskStateToCountMap.entrySet()) {
      _controllerMetrics.setValueOfTableGauge(String.format("%s.%s", taskType, taskStateEntry.getKey()),
          ControllerGauge.TASK_STATUS, taskStateEntry.getValue());
    }
  }

  private synchronized void addTaskTypeMetricsUpdaterIfNeeded(String taskType) {
    if (!_taskTypeMetricsUpdaterMap.containsKey(taskType)) {
      TaskTypeMetricsUpdater taskTypeMetricsUpdater = new TaskTypeMetricsUpdater(taskType, this);
      _pinotHelixResourceManager.getPropertyStore()
          .subscribeDataChanges(getPropertyStorePathForTaskQueue(taskType), taskTypeMetricsUpdater);
      _taskTypeMetricsUpdaterMap.put(taskType, taskTypeMetricsUpdater);
    }
  }
}
