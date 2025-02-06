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
package org.apache.pinot.integration.tests;

import java.util.Map;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingContext;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingInfo;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class MinionTaskTestUtils {
  private MinionTaskTestUtils() {
  }

  public static void assertNoTaskSchedule(TaskSchedulingContext context, PinotTaskManager taskManager) {
    Map<String, TaskSchedulingInfo> infoMap = taskManager.scheduleTasks(context);
    infoMap.forEach((key, value) -> assertNoTaskSchedule(value));
  }

  public static void assertNoTaskSchedule(TaskSchedulingInfo info) {
    assertNotNull(info.getScheduledTaskNames());
    assertTrue(info.getScheduledTaskNames().isEmpty());
    assertNoTaskErrors(info);
  }

  public static void assertNoTaskErrors(TaskSchedulingInfo info) {
    assertNotNull(info.getGenerationErrors());
    assertTrue(info.getGenerationErrors().isEmpty());
    assertNotNull(info.getSchedulingErrors());
    assertTrue(info.getSchedulingErrors().isEmpty());
  }
}
