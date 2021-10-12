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
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.impl.matchers.GroupMatcher;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PinotTaskManagerStatelessTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "myTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);

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
    addTableConfig(tableConfig);
    Thread.sleep(2000);
    List<String> jobGroupNames = scheduler.getJobGroupNames();
    assertEquals(jobGroupNames, Lists.newArrayList(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE));
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */10 * ? * * *");

    // 2. Update table to new schedule
    tableConfig.setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */20 * ? * * *"))));
    updateTableConfig(tableConfig);
    Thread.sleep(2000);
    jobGroupNames = scheduler.getJobGroupNames();
    assertEquals(jobGroupNames, Lists.newArrayList(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE));
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */20 * ? * * *");

    // 3. Update table to new task and schedule
    tableConfig.setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */30 * ? * * *"),
            "MergeRollupTask", ImmutableMap.of("schedule", "0 */10 * ? * * *"))));
    updateTableConfig(tableConfig);
    Thread.sleep(2000);
    jobGroupNames = scheduler.getJobGroupNames();
    assertEquals(jobGroupNames.size(), 2);
    assertTrue(jobGroupNames.contains(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE));
    assertTrue(jobGroupNames.contains(MinionConstants.MergeRollupTask.TASK_TYPE));
    validateJob(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, "0 */30 * ? * * *");
    validateJob(MinionConstants.MergeRollupTask.TASK_TYPE, "0 */10 * ? * * *");

    // 4. Remove one task from the table
    tableConfig.setTaskConfig(
        new TableTaskConfig(ImmutableMap.of("MergeRollupTask", ImmutableMap.of("schedule", "0 */10 * ? * * *"))));
    updateTableConfig(tableConfig);
    Thread.sleep(2000);
    jobGroupNames = scheduler.getJobGroupNames();
    assertEquals(jobGroupNames, Lists.newArrayList(MinionConstants.MergeRollupTask.TASK_TYPE));
    validateJob(MinionConstants.MergeRollupTask.TASK_TYPE, "0 */10 * ? * * *");

    // 4. Drop table
    dropOfflineTable(RAW_TABLE_NAME);
    jobGroupNames = scheduler.getJobGroupNames();
    assertTrue(jobGroupNames.isEmpty());
    stopController();
  }

  private void validateJob(String taskType, String cronExpression)
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
    List<? extends Trigger> triggersOfJob = scheduler.getTriggersOfJob(jobKey);
    assertEquals(triggersOfJob.size(), 1);
    Trigger trigger = triggersOfJob.iterator().next();
    assertTrue(trigger instanceof CronTrigger);
    assertEquals(((CronTrigger) trigger).getCronExpression(), cronExpression);
  }

  @AfterClass
  public void tearDown() {
    stopZk();
  }
}
