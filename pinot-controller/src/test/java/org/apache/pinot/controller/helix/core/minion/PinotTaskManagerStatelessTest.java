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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
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
    addTableConfig(tableConfig);
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
  public void testPinotTaskManagerSchedulerWithUpdate()
      throws Exception {
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    startController(properties);
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("myMap", FieldSpec.DataType.STRING)
        .addSingleValueDimension("myMapStr", FieldSpec.DataType.STRING)
        .addSingleValueDimension("complexMapStr", FieldSpec.DataType.STRING).build();
    addSchema(schema);
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    Scheduler scheduler = taskManager.getScheduler();
    assertNotNull(scheduler);

    // 1. Add Table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */10 * ? * * *")))).build();
    waitForEVToDisappear(tableConfig.getTableName());
    addTableConfig(tableConfig);
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 1 && jgn.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE),
        "JobGroupNames should have SegmentGenerationAndPushTask only");
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */10 * ? * * *");

    // 2. Update table to new schedule
    tableConfig.setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */20 * ? * * *"))));
    updateTableConfig(tableConfig);
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 1 && jgn.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE),
        "JobGroupNames should have SegmentGenerationAndPushTask only");
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */20 * ? * * *");

    // 3. Update table to new task and schedule
    tableConfig.setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */30 * ? * * *"),
            "MergeRollupTask", ImmutableMap.of("schedule", "0 */10 * ? * * *"))));
    updateTableConfig(tableConfig);
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 2 && jgn.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE) && jgn
            .contains(MinionConstants.MergeRollupTask.TASK_TYPE),
        "JobGroupNames should have SegmentGenerationAndPushTask and MergeRollupTask");
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */30 * ? * * *");
    validateJob(MinionConstants.MergeRollupTask.TASK_TYPE, "0 */10 * ? * * *");

    // 4. Remove one task from the table
    tableConfig.setTaskConfig(
        new TableTaskConfig(ImmutableMap.of("MergeRollupTask", ImmutableMap.of("schedule", "0 */10 * ? * * *"))));
    updateTableConfig(tableConfig);
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
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    startController(properties);
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
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
            ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */10 * ? * * *")))).build();
    waitForEVToDisappear(tableConfig.getTableName());
    addTableConfig(tableConfig);
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 1 && jgn.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE),
        "JobGroupNames should have SegmentGenerationAndPushTask only");
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */10 * ? * * *");

    // Restart controller
    stopController();
    startController(properties);
    // wait for controller to start correctly.
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
    updateTableConfig(tableConfig);

    // Task is put into table config.
    TableConfig tableConfigAfterRestart =
        _controllerStarter.getHelixResourceManager().getTableConfig(OFFLINE_TABLE_NAME);
    Map<String, String> taskCfgs =
        tableConfigAfterRestart.getTaskConfig().getConfigsForTaskType(MinionConstants.MergeRollupTask.TASK_TYPE);
    assertTrue(taskCfgs.containsKey("schedule"));

    // The new MergeRollup task wouldn't be scheduled if not eagerly checking table configs
    // after setting up subscriber on ChildChanges zk event when controller gets restarted.
    waitForJobGroupNames(_controllerStarter.getTaskManager(),
        jgn -> jgn.size() == 2 && jgn.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE) && jgn
            .contains(MinionConstants.MergeRollupTask.TASK_TYPE),
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

  @AfterClass
  public void tearDown() {
    stopZk();
  }
}
