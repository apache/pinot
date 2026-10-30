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
package org.apache.pinot.tools.admin.command;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Verifies cleanup is safe before execution and preserves reusable command configuration.
public class AdminCommandCleanupTest {
  @Test
  public void testDeleteSchemaCleanupIsRepeatableBeforeExecution() {
    DeleteSchemaCommand command = new DeleteSchemaCommand().setSchemaName("events");
    String configuredCommand = command.toString();
    command.cleanup();
    command.cleanup();
    assertEquals(command.getName(), "DeleteSchema");
    assertEquals(command.toString(), configuredCommand);
    assertEquals(command.setSchemaName("updated").toString(), configuredCommand.replace("events", "updated"));
  }

  @Test
  public void testStartBrokerCleanupIsRepeatableBeforeStartup() {
    StartBrokerCommand command = new StartBrokerCommand().setClusterName("cleanup-test").setPort(12345).setGrpcPort(-1);
    String configuredCommand = command.toString();
    command.cleanup();
    command.cleanup();
    assertEquals(command.getClusterName(), "cleanup-test");
    assertEquals(command.getBrokerPort(), 12345);
    assertEquals(command.getBrokerGrpcPort(), -1);
    assertEquals(command.toString(), configuredCommand);
  }
}
