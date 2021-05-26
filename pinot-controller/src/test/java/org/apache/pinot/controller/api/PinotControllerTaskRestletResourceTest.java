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
package org.apache.pinot.controller.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.periodictask.PeriodicTaskInfo;
import org.apache.pinot.core.periodictask.PeriodicTaskState;
import org.apache.pinot.core.periodictask.TaskExecutionResult;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.core.common.ObjectSerDeUtils.ObjectType.List;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/**
 * Tests to verify Trigger API endpoint functionality.
 */
public class PinotControllerTaskRestletResourceTest extends ControllerTest {
  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
  }

  @Test
  public void testTriggerSchedule() throws Exception {
    String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.forTriggerTasksGet());
    List<PeriodicTaskInfo> taskInfos = JsonUtils.stringToObject(jsonOutputStr, new TypeReference<List<PeriodicTaskInfo>>() {});
    for (PeriodicTaskInfo taskInfo : taskInfos) {
      String execResult = sendPutRequest(_controllerRequestURLBuilder.forTriggerTaskSchedule(taskInfo.getTaskName()));
      TaskExecutionResult result = JsonUtils.stringToObject(execResult, new TypeReference<TaskExecutionResult>() {});
      assertEquals(result.getStatus(), TaskExecutionResult.Status.IN_PROGRESS);
    }

    String execResult = sendPutRequest(_controllerRequestURLBuilder.forTriggerTaskSchedule("dummyTask"));
    TaskExecutionResult result = JsonUtils.stringToObject(execResult, new TypeReference<TaskExecutionResult>() {});
    assertEquals(result.getStatus(), TaskExecutionResult.Status.NO_OP);
  }

  @Test
  public void testTriggerTaskState() throws Exception {
    String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.forTriggerTasksGet());
    List<PeriodicTaskInfo> taskInfos = JsonUtils.stringToObject(jsonOutputStr, new TypeReference<List<PeriodicTaskInfo>>() {});
    for (PeriodicTaskInfo taskInfo : taskInfos) {
      String execResult = sendPutRequest(_controllerRequestURLBuilder.forTriggerTaskSchedule(taskInfo.getTaskName()));
      Thread.sleep(1000);
      /* There is an initial delay period before the task gets transitioned to RUNNING, if the user queries the
       * state right away, the state of the task would be INIT */
      String taskState = sendGetRequest(_controllerRequestURLBuilder.forTriggerTaskStateGet(taskInfo.getTaskName()));
      PeriodicTaskState state = JsonUtils.stringToObject(taskState, new TypeReference<PeriodicTaskState>() {});
      assertEquals(state, PeriodicTaskState.INIT);
    }

    String taskState = sendGetRequest(_controllerRequestURLBuilder.forTriggerTaskStateGet("dummyTask"));
    assertEquals(0, taskState.length()); // Expect an empty response for invalid taskName
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
