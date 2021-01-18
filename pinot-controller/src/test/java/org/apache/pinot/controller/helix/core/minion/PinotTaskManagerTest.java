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
import java.util.Set;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.impl.matchers.GroupMatcher;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotTaskManagerTest extends ControllerTest {

  private static final String RAW_TABLE_NAME = "myTable";

  @BeforeClass
  public void setup()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);

    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("myMap", FieldSpec.DataType.STRING)
        .addSingleValueDimension("myMapStr", FieldSpec.DataType.STRING)
        .addSingleValueDimension("complexMapStr", FieldSpec.DataType.STRING).build();
    addSchema(schema);
  }

  @Test
  public void testPinotTaskManagerSchedulerWithUpdate()
      throws Exception {
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    Scheduler scheduler = taskManager.getScheduler();
    Assert.assertNotNull(scheduler);

    // 1. Add Table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */10 * ? * * *")))).build();
    addTableConfig(tableConfig);
    Thread.sleep(2000);
    List<String> jobGroupNames = scheduler.getJobGroupNames();
    Assert.assertEquals(jobGroupNames.size(), 1);
    for (String group : jobGroupNames) {
      Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.groupEquals(group));
      for (JobKey jobKey : jobKeys) {
        JobDetail jobDetail = scheduler.getJobDetail(jobKey);
        Assert.assertEquals(jobDetail.getJobClass(), CronJobScheduleJob.class);
        Assert.assertEquals(jobDetail.getKey().getName(), RAW_TABLE_NAME + "_OFFLINE");
        Assert.assertEquals(jobDetail.getKey().getGroup(), MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE);
        Assert.assertEquals(jobDetail.getJobDataMap().get("PinotTaskManager"), taskManager);
        Assert.assertEquals(jobDetail.getJobDataMap().get("LeadControllerManager"),
            _controllerStarter.getLeadControllerManager());
        List<? extends Trigger> triggersOfJob = scheduler.getTriggersOfJob(jobKey);
        Assert.assertEquals(triggersOfJob.size(), 1);
        for (Trigger trigger : triggersOfJob) {
          Assert.assertTrue(trigger instanceof CronTrigger);
          Assert.assertEquals(((CronTrigger) trigger).getCronExpression(), "0 */10 * ? * * *");
        }
      }
    }

    // 2. Update table to new schedule
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("SegmentGenerationAndPushTask", ImmutableMap.of("schedule", "0 */20 * ? * * *")))).build();
    updateTableConfig(tableConfig);
    Thread.sleep(2000);
    jobGroupNames = scheduler.getJobGroupNames();
    Assert.assertEquals(jobGroupNames.size(), 1);
    for (String group : jobGroupNames) {
      Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.groupEquals(group));
      for (JobKey jobKey : jobKeys) {
        JobDetail jobDetail = scheduler.getJobDetail(jobKey);
        Assert.assertEquals(jobDetail.getJobClass(), CronJobScheduleJob.class);
        Assert.assertEquals(jobDetail.getKey().getName(), RAW_TABLE_NAME + "_OFFLINE");
        Assert.assertEquals(jobDetail.getKey().getGroup(), MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE);
        Assert.assertEquals(jobDetail.getJobDataMap().get("PinotTaskManager"), taskManager);
        Assert.assertEquals(jobDetail.getJobDataMap().get("LeadControllerManager"),
            _controllerStarter.getLeadControllerManager());
        List<? extends Trigger> triggersOfJob = scheduler.getTriggersOfJob(jobKey);
        Assert.assertEquals(triggersOfJob.size(), 1);
        for (Trigger trigger : triggersOfJob) {
          Assert.assertTrue(trigger instanceof CronTrigger);
          Assert.assertEquals(((CronTrigger) trigger).getCronExpression(), "0 */20 * ? * * *");
        }
      }
    }
    // 3. Drop table
    dropOfflineTable(RAW_TABLE_NAME);
    jobGroupNames = scheduler.getJobGroupNames();
    Assert.assertTrue(jobGroupNames.isEmpty());
  }

  @AfterClass
  public void teardown() {
    stopController();
    stopZk();
  }
}