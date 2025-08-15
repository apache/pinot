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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


@Test(groups = "stateless")
public class PinotTaskManagerStatelessTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "myTable";
  public static final String TABLE_NAME_WITH_TYPE = "myTable_OFFLINE";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final long TIMEOUT_IN_MS = 10_000L;
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTaskManagerStatelessTest.class);

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
  }

  @Test
  public void testDefaultPinotTaskManagerNoScheduler()
      throws Exception {
    startController();
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    assertNull(taskManager.getScheduler());
    stopController();
  }

  @Test
  public void testSkipLateCronSchedule()
      throws Exception {
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.TASK_MANAGER_SKIP_LATE_CRON_SCHEDULE, "true");
    properties.put(ControllerConf.ControllerPeriodicTasksConf.TASK_MANAGER_MAX_CRON_SCHEDULE_DELAY_IN_SECONDS, "10");
    startController(properties);
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
    addFakeMinionInstancesToAutoJoinHelixCluster(1);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("myMap", FieldSpec.DataType.STRING)
        .addSingleValueDimension("myMapStr", FieldSpec.DataType.STRING)
        .addSingleValueDimension("complexMapStr", FieldSpec.DataType.STRING).build();
    addSchema(schema);
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    Scheduler scheduler = taskManager.getScheduler();
    assertNotNull(scheduler);

    // Add Table with one task.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 * * ? * * *")))).build();
    waitForEVToDisappear(tableConfig.getTableName());
    addTableConfig(tableConfig, "TASK");
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 1 && jgn.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE),
        "JobGroupNames should have SegmentGenerationAndPushTask only");
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */10 * ? * * *", true, 10);

    dropOfflineTable(RAW_TABLE_NAME);
    waitForJobGroupNames(_controllerStarter.getTaskManager(), List::isEmpty, "JobGroupNames should be empty");
    stopFakeInstances();
    stopController();
  }

  @Test
  public void testCreateTaskWithMaxNumSubTasksLimit()
      throws Exception {
    setupTest();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    waitForEVToDisappear(tableConfig.getTableName());
    addTableConfig(tableConfig, "TASK");

    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    PinotHelixTaskResourceManager taskResourceManager = _controllerStarter.getHelixTaskResourceManager();

    // Register task generator that generates more tasks than the limit allows
    String taskType = "TestTaskType";
    int maxSubTasks = 3;
    int generatedTasks = 5;

    taskManager.registerTaskGenerator(createFakeTaskGenerator(taskType, maxSubTasks, generatedTasks));

    taskResourceManager.ensureTaskQueueExists(taskType);

    // Test adhoc task creation. This should throw a RuntimeException
    // if the number of subtasks exceeds the maxSubTasks limit.
    try {
      taskManager.createTask(taskType, RAW_TABLE_NAME, null, new HashMap<>());
      fail("Expected RuntimeException due to exceeding maxSubTasks limit");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("greater than the maximum number of tasks"));
    }

    // Verify that increasing max subtasks limit allows task creation
    taskType = "TestTaskType2";
    maxSubTasks = 5;
    generatedTasks = 5;
    taskManager.registerTaskGenerator(createFakeTaskGenerator(taskType, maxSubTasks, generatedTasks));
    Map<String, String> result = taskManager.createTask(taskType, RAW_TABLE_NAME, null, new HashMap<>());

    // Verify task was created but limited to maxSubTasks
    assertNotNull(result);
    assertEquals(result.size(), 1);
    String taskName = result.get(TABLE_NAME_WITH_TYPE);
    assertNotNull(taskName);

    // Verify the number of subtasks is limited to maxSubTasks
    List<PinotTaskConfig> subtaskConfigs = taskResourceManager.getSubtaskConfigs(taskName);
    assertEquals(subtaskConfigs.size(), maxSubTasks,
        "Expected " + maxSubTasks + " subtasks but got " + subtaskConfigs.size());

    dropOfflineTable(RAW_TABLE_NAME);
    stopFakeInstances();
    stopController();
  }

  @Test
  public void testScheduleTaskWithMaxNumSubTasksLimit()
      throws Exception {
    setupTest();

    String taskType = "TestScheduleTaskType";

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of(taskType, new HashMap<>()))).build();
    waitForEVToDisappear(tableConfig.getTableName());
    addTableConfig(tableConfig, "TASK");

    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    PinotHelixTaskResourceManager taskResourceManager = _controllerStarter.getHelixTaskResourceManager();

    // Register task generator that generates more tasks than the limit allows
    int maxSubTasks = 2;
    int generatedTasks = 7;

    taskManager.registerTaskGenerator(createFakeTaskGenerator(taskType, maxSubTasks, generatedTasks));

    taskResourceManager.ensureTaskQueueExists(taskType);

    // Test scheduled task creation - should limit to maxSubTasks
    TaskSchedulingContext context = new TaskSchedulingContext()
        .setTablesToSchedule(Collections.singleton(TABLE_NAME_WITH_TYPE))
        .setTasksToSchedule(Collections.singleton(taskType));

    Map<String, TaskSchedulingInfo> result = taskManager.scheduleTasks(context);

    // Verify task was scheduled but limited to maxSubTasks
    assertNotNull(result);
    assertTrue(result.containsKey(taskType));
    TaskSchedulingInfo schedulingInfo = result.get(taskType);
    assertNotNull(schedulingInfo.getScheduledTaskNames());
    assertEquals(schedulingInfo.getScheduledTaskNames().size(), 1);

    String taskName = schedulingInfo.getScheduledTaskNames().get(0);

    // Verify the number of subtasks is limited to maxSubTasks
    List<PinotTaskConfig> subtaskConfigs = taskResourceManager.getSubtaskConfigs(taskName);
    assertEquals(subtaskConfigs.size(), maxSubTasks,
        "Expected " + maxSubTasks + " subtasks but got " + subtaskConfigs.size());

    // Verify that setting triggered by in context to adhoc should result in task schedule failure
    context.setTriggeredBy(CommonConstants.TaskTriggers.ADHOC_TRIGGER.name());
    try {
      taskManager.scheduleTasks(context);
      fail("Expected RuntimeException due to exceeding maxSubTasks limit");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("greater than the maximum number of tasks"));
    }

    dropOfflineTable(RAW_TABLE_NAME);
    stopFakeInstances();
    stopController();
  }

  @Test
  public void testMaxNumSubTasksWithMultipleTables()
      throws Exception {
    setupTest();
    // Create two schemas and tables
    String rawTableName1 = "testTable1";
    String rawTableName2 = "testTable2";
    String tableNameWithType1 = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName1);
    String tableNameWithType2 = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName2);

    Schema schema1 = new Schema.SchemaBuilder().setSchemaName(rawTableName1)
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING).build();
    Schema schema2 = new Schema.SchemaBuilder().setSchemaName(rawTableName2)
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING).build();
    addSchema(schema1);
    addSchema(schema2);

    String taskType = "TestMultiTableTaskType";

    TableConfig tableConfig1 = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName1).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of(taskType, new HashMap<>()))).build();
    TableConfig tableConfig2 = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName2).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of(taskType, new HashMap<>()))).build();
    waitForEVToDisappear(tableConfig1.getTableName());
    waitForEVToDisappear(tableConfig2.getTableName());
    addTableConfig(tableConfig1, "TASK");
    addTableConfig(tableConfig2, "TASK");

    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    PinotHelixTaskResourceManager taskResourceManager = _controllerStarter.getHelixTaskResourceManager();

    // Register task generator that generates tasks for each table

    int maxSubTasks = 4;
    int tasksPerTable = 6;

    taskManager.registerTaskGenerator(createFakeTaskGenerator(taskType, maxSubTasks, tasksPerTable));

    taskResourceManager.ensureTaskQueueExists(taskType);

    // Test scheduled task creation for both tables - should limit total to maxSubTasks across all tables
    TaskSchedulingContext context = new TaskSchedulingContext()
        .setTablesToSchedule(Set.of(tableNameWithType1, tableNameWithType2))
        .setTasksToSchedule(Collections.singleton(taskType));

    Map<String, TaskSchedulingInfo> result = taskManager.scheduleTasks(context);

    // Verify task was scheduled but limited to maxSubTasks total
    assertNotNull(result);
    assertTrue(result.containsKey(taskType));
    TaskSchedulingInfo schedulingInfo = result.get(taskType);
    assertNotNull(schedulingInfo.getScheduledTaskNames());
    assertEquals(schedulingInfo.getScheduledTaskNames().size(), 1);

    String taskName = schedulingInfo.getScheduledTaskNames().get(0);

    // Verify the total number of subtasks is limited to maxSubTasks across all tables
    List<PinotTaskConfig> subtaskConfigs = taskResourceManager.getSubtaskConfigs(taskName);
    assertEquals(subtaskConfigs.size(), maxSubTasks,
        "Expected " + maxSubTasks + " total subtasks across all tables but got " + subtaskConfigs.size());

    dropOfflineTable(rawTableName1);
    dropOfflineTable(rawTableName2);
    stopFakeInstances();
    stopController();
  }

  private void testValidateTaskGeneration(Function<PinotTaskManager, Void> validateFunction)
      throws Exception {
    setupTest();
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    Scheduler scheduler = taskManager.getScheduler();
    assertNotNull(scheduler);

    String segmentGenerationAndPushTask = MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE;

    // Add Table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of(segmentGenerationAndPushTask, ImmutableMap.of("schedule", "0 */10 * ? * * *")))).build();
    waitForEVToDisappear(tableConfig.getTableName());
    addTableConfig(tableConfig, "TASK");
    waitForJobGroupNames(taskManager, jgn -> jgn.size() == 1 && jgn.contains(segmentGenerationAndPushTask),
        "JobGroupNames should have SegmentGenerationAndPushTask only");
    validateJob(segmentGenerationAndPushTask, "0 */10 * ? * * *");

    // Ensure task queue exists
    PinotHelixTaskResourceManager taskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    taskResourceManager.ensureTaskQueueExists(segmentGenerationAndPushTask);

    // Register the task generator
    taskManager.registerTaskGenerator(new BaseTaskGenerator() {
      @Override
      public String getTaskType() {
        return segmentGenerationAndPushTask;
      }

      @Override
      public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
        // test validates that this method never gets called as the task queue is in stopped state
        return List.of(new PinotTaskConfig(segmentGenerationAndPushTask, new HashMap<>()));
      }
    });

    // Stop the task queue
    taskResourceManager.stopTaskQueue(segmentGenerationAndPushTask);

    // Assert the task queue state
    TestUtils.waitForCondition(aVoid -> {
      TaskState taskQueueState = taskResourceManager.getTaskQueueState(segmentGenerationAndPushTask);
      return TaskState.STOPPED.equals(taskQueueState);
    }, TIMEOUT_IN_MS, "task queue state was not in STOPPED state within ten seconds.");

    // Exercise the test
    validateFunction.apply(taskManager);

    // Drop table
    dropOfflineTable(RAW_TABLE_NAME);
    waitForJobGroupNames(_controllerStarter.getTaskManager(), List::isEmpty, "JobGroupNames should be empty");

    stopFakeInstances();
    stopController();
  }

  @Test
  public void testPinotTaskManagerScheduleTaskWithStoppedTaskQueue()
      throws Exception {
    testValidateTaskGeneration(taskManager -> {
      String taskName = "SegmentGenerationAndPushTask";
      TaskSchedulingContext context = new TaskSchedulingContext()
          .setTablesToSchedule(Collections.singleton(TABLE_NAME_WITH_TYPE))
          .setTasksToSchedule(Collections.singleton(taskName));
      // Validate schedule tasks for table when task queue is in stopped state
      TaskSchedulingInfo info = taskManager.scheduleTasks(context).get(taskName);
      assertNotNull(info);
      assertNull(info.getScheduledTaskNames());
      assertFalse(info.getSchedulingErrors().isEmpty());
      return null;
    });
  }

  @Test
  public void testPinotTaskManagerCreateTaskWithStoppedTaskQueue()
      throws Exception {
    testValidateTaskGeneration(taskManager -> {
      Map<String, String> taskMap;
      try {
        // Validate task creation for table when task queue is in stopped state
        taskMap = taskManager.createTask("SegmentGenerationAndPushTask", "myTable", "myTaskName", new HashMap<>());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      assertNotNull(taskMap);
      assertEquals(taskMap.size(), 0);
      return null;
    });
  }

  @Test
  public void testPinotTaskManagerSchedulerWithUpdate()
      throws Exception {
    setupTest();
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    Scheduler scheduler = taskManager.getScheduler();
    assertNotNull(scheduler);

    // 1. Add Table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */10 * ? * * *")))).build();
    waitForEVToDisappear(tableConfig.getTableName());
    addTableConfig(tableConfig, "TASK");
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 1 && jgn.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE),
        "JobGroupNames should have SegmentGenerationAndPushTask only");
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */10 * ? * * *");

    // 2. Update table to new schedule
    tableConfig.setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */20 * ? * * *"))));
    updateTableConfig(tableConfig, "TASK");
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 1 && jgn.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE),
        "JobGroupNames should have SegmentGenerationAndPushTask only");
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */20 * ? * * *");

    // 3. Update table to new task and schedule
    tableConfig.setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */30 * ? * * *"),
            "MergeRollupTask", ImmutableMap.of("schedule", "0 */10 * ? * * *"))));
    updateTableConfig(tableConfig, "TASK");
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 2 && jgn.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE) && jgn.contains(
            MinionConstants.MergeRollupTask.TASK_TYPE),
        "JobGroupNames should have SegmentGenerationAndPushTask and MergeRollupTask");
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */30 * ? * * *");
    validateJob(MinionConstants.MergeRollupTask.TASK_TYPE, "0 */10 * ? * * *");

    // 4. Remove one task from the table
    tableConfig.setTaskConfig(
        new TableTaskConfig(ImmutableMap.of("MergeRollupTask", ImmutableMap.of("schedule", "0 */10 * ? * * *"))));
    updateTableConfig(tableConfig, "TASK");
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 1 && jgn.contains(MinionConstants.MergeRollupTask.TASK_TYPE),
        "JobGroupNames should have MergeRollupTask only");
    validateJob(MinionConstants.MergeRollupTask.TASK_TYPE, "0 */10 * ? * * *");

    // 4. Drop table
    dropOfflineTable(RAW_TABLE_NAME);
    waitForJobGroupNames(_controllerStarter.getTaskManager(), List::isEmpty, "JobGroupNames should be empty");

    stopFakeInstances();
    stopController();
  }

  @Test
  public void testPinotTaskManagerSchedulerWithRestart()
      throws Exception {
    setupTest();
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    Scheduler scheduler = taskManager.getScheduler();
    assertNotNull(scheduler);

    // Add Table with one task.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */10 * ? * * *")))).build();
    waitForEVToDisappear(tableConfig.getTableName());
    addTableConfig(tableConfig, "TASK");
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 1 && jgn.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE),
        "JobGroupNames should have SegmentGenerationAndPushTask only");
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */10 * ? * * *");

    // Restart controller.
    restartController();
    TestUtils.waitForCondition((aVoid) -> {
      try {
        long tableSize = getTableSize(OFFLINE_TABLE_NAME);
        return tableSize == TableSizeReader.DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
      } catch (Exception e) {
        return false;
      }
    }, 5000L, "Failed to restart controller");

    // Update table to add a new task
    tableConfig.setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */10 * ? * * *"),
            "MergeRollupTask", ImmutableMap.of("schedule", "0 */20 * ? * * *"))));
    updateTableConfig(tableConfig, "TASK");

    // Task is put into table config.
    TableConfig tableConfigAfterRestart =
        _controllerStarter.getHelixResourceManager().getTableConfig(OFFLINE_TABLE_NAME);
    Map<String, String> taskCfgs =
        tableConfigAfterRestart.getTaskConfig().getConfigsForTaskType(MinionConstants.MergeRollupTask.TASK_TYPE);
    assertTrue(taskCfgs.containsKey("schedule"));

    // The new MergeRollup task wouldn't be scheduled if not eagerly checking table configs
    // after setting up subscriber on ChildChanges zk event when controller gets restarted.
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 2 && jgn.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE) && jgn.contains(
            MinionConstants.MergeRollupTask.TASK_TYPE),
        "JobGroupNames should have SegmentGenerationAndPushTask and MergeRollupTask");

    dropOfflineTable(RAW_TABLE_NAME);
    waitForJobGroupNames(_controllerStarter.getTaskManager(), List::isEmpty, "JobGroupNames should be empty");

    stopFakeInstances();
    stopController();
  }

  private void waitForJobGroupNames(PinotTaskManager taskManager, Predicate<List<String>> predicate,
      String errorMessage) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        Scheduler scheduler = taskManager.getScheduler();
        List<String> jobGroupNames = scheduler.getJobGroupNames();
        return predicate.test(jobGroupNames);
      } catch (SchedulerException e) {
        throw new RuntimeException(e);
      }
    }, TIMEOUT_IN_MS, errorMessage);
  }

  private void validateJob(String taskType, String cronExpression)
      throws Exception {
    validateJob(taskType, cronExpression, false, 600);
  }

  private void validateJob(String taskType, String cronExpression, boolean skipLateCronSchedule, int maxDelayInSeconds)
      throws Exception {
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    Scheduler scheduler = taskManager.getScheduler();
    assert scheduler != null;
    Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.groupEquals(taskType));
    assertEquals(jobKeys.size(), 1);
    JobKey jobKey = jobKeys.iterator().next();
    JobDetail jobDetail = scheduler.getJobDetail(jobKey);
    assertEquals(jobDetail.getJobClass(), CronJobScheduleJob.class);
    assertEquals(jobDetail.getKey().getName(), OFFLINE_TABLE_NAME);
    assertEquals(jobDetail.getKey().getGroup(), taskType);
    assertSame(jobDetail.getJobDataMap().get("PinotTaskManager"), taskManager);
    assertSame(jobDetail.getJobDataMap().get("LeadControllerManager"), _controllerStarter.getLeadControllerManager());
    assertEquals(jobDetail.getJobDataMap().get("SkipLateCronSchedule"), skipLateCronSchedule);
    assertEquals(jobDetail.getJobDataMap().get("MaxCronScheduleDelayInSeconds"), maxDelayInSeconds);
    // jobDetail and jobTrigger are not added atomically by the scheduler,
    // the jobDetail is added to an internal map firstly, and jobTrigger
    // is added to another internal map afterwards, so we check for the existence
    // of jobTrigger with some waits to be more defensive.
    TestUtils.waitForCondition(aVoid -> {
      try {
        return scheduler.getTriggersOfJob(jobKey).size() == 1;
      } catch (SchedulerException e) {
        throw new RuntimeException(e);
      }
    }, TIMEOUT_IN_MS, "JobDetail exiting but missing JobTrigger");

    // There is no guarantee that previous changes have been applied, therefore we need to
    // retry the check for a bit
    TestUtils.waitForCondition(aVoid -> {
      try {
        List<? extends Trigger> triggersOfJob = scheduler.getTriggersOfJob(jobKey);
        Trigger trigger = triggersOfJob.iterator().next();
        assertTrue(trigger instanceof CronTrigger);
        assertEquals(((CronTrigger) trigger).getCronExpression(), cronExpression);
      } catch (SchedulerException ex) {
        throw new RuntimeException(ex);
      } catch (AssertionError assertionError) {
        LOGGER.warn("Unexpected cron expression. Hasn't been replicated yet?", assertionError);
      }
      return true;
    }, TIMEOUT_IN_MS, 500L, "Cron expression didn't change to " + cronExpression);
  }

  private void addTableConfig(TableConfig tableConfig, String validationTypesToSkip)
      throws IOException {
    String createTableUriStr =
        String.format(_controllerRequestURLBuilder.forTableCreate() + "?validationTypesToSkip=%s",
            validationTypesToSkip);
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPostRequest(new URI(createTableUriStr), tableConfig.toJsonString(),
              Collections.emptyMap()));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private void updateTableConfig(TableConfig tableConfig, String validationTypesToSkip)
      throws IOException {
    String updateTableUriStr = String.format(
        _controllerRequestURLBuilder.forUpdateTableConfig(tableConfig.getTableName()) + "?validationTypesToSkip=%s",
        validationTypesToSkip);
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPutRequest(new URI(updateTableUriStr), tableConfig.toJsonString(),
              Collections.emptyMap()));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private BaseTaskGenerator createFakeTaskGenerator(String taskType, int maxSubTasks, int generatedTasks) {
    return new BaseTaskGenerator() {
      @Override
      public String getTaskType() {
        return taskType;
      }

      @Override
      public int getMaxAllowedSubTasksPerTask() {
        return maxSubTasks;
      }

      @Override
      public long getTaskTimeoutMs() {
        return 10000; // 10 seconds
      }

      @Override
      public int getNumConcurrentTasksPerInstance() {
        return 5;
      }

      @Override
      public int getMaxAttemptsPerTask() {
        return 5;
      }

      @Override
      public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
        List<PinotTaskConfig> configs = new ArrayList<>();
        for (TableConfig tableConfig : tableConfigs) {
          for (int i = 0; i < generatedTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put("taskId", tableConfig.getTableName() + "_task_" + i);
            configs.add(new PinotTaskConfig(taskType, config));
          }
        }
        return configs;
      }

      @Override
      public List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs) {
        return generateTasks(List.of(tableConfig));
      }
    };
  }

  private void setupTest()
      throws Exception {
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    startController(properties);
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
    addFakeMinionInstancesToAutoJoinHelixCluster(1);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("myMap", FieldSpec.DataType.STRING)
        .addSingleValueDimension("myMapStr", FieldSpec.DataType.STRING)
        .addSingleValueDimension("complexMapStr", FieldSpec.DataType.STRING).build();
    addSchema(schema);
  }

  @AfterClass
  public void tearDown() {
    stopZk();
  }
}
