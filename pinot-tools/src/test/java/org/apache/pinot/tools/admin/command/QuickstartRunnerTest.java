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

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.mockito.MockedConstruction;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;


/// Verifies quickstart process ownership during shutdown. Tests use thread-scoped construction mocks and private
/// temporary directories so they never scan real PID files, kill processes, or change global system properties.
public class QuickstartRunnerTest {
  @DataProvider
  public Object[][] forkModes() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "forkModes")
  public void testExternalZookeeperSkipsGlobalPidScanning(boolean forkServers)
      throws Exception {
    Path directory = Files.createTempDirectory("quickstart-runner-test");
    try (MockedConstruction<StopProcessCommand> stoppers =
        mockConstruction(StopProcessCommand.class, withSettings().defaultAnswer(RETURNS_SELF))) {
      QuickstartRunner runner = newRunner(directory, "external-zookeeper:2181", forkServers);
      QuickstartServerProcess ownedServer = mock(QuickstartServerProcess.class);
      if (forkServers) {
        addOwnedServer(runner, ownedServer);
      }
      runner.stop();
      runner.stop();

      assertEquals(stoppers.constructed().size(), 1);
      StopProcessCommand stopper = stoppers.constructed().getFirst();
      verify(stopper).execute();
      // Selecting any role enables a global PID scan and can stop processes belonging to another local cluster.
      verifyNoMoreInteractions(stopper);
      if (forkServers) {
        verify(ownedServer).stop();
      }
      verifyNoMoreInteractions(ownedServer);
    } finally {
      FileUtils.deleteDirectory(directory.toFile());
    }
  }

  @Test(dataProvider = "forkModes")
  public void testEmbeddedZookeeperKeepsLegacyCleanup(boolean forkServers)
      throws Exception {
    Path directory = Files.createTempDirectory("quickstart-runner-test");
    try (MockedConstruction<StopProcessCommand> stoppers =
        mockConstruction(StopProcessCommand.class, withSettings().defaultAnswer(RETURNS_SELF))) {
      newRunner(directory, null, forkServers).stop();

      assertEquals(stoppers.constructed().size(), 1);
      StopProcessCommand stopper = stoppers.constructed().getFirst();
      verify(stopper).stopController();
      verify(stopper).stopBroker();
      verify(stopper).stopZookeeper();
      if (!forkServers) {
        verify(stopper).stopServer();
      }
      verify(stopper).execute();
      verifyNoMoreInteractions(stopper);
    } finally {
      FileUtils.deleteDirectory(directory.toFile());
    }
  }

  private static QuickstartRunner newRunner(Path directory, String externalZookeeper, boolean forkServers)
      throws Exception {
    return new QuickstartRunner(List.of(), 0, 0, 0, 0, directory.toFile(), true, null, Map.of(), externalZookeeper,
        false, Map.of()).setForkServers(forkServers);
  }

  @SuppressWarnings("unchecked")
  private static void addOwnedServer(QuickstartRunner runner, QuickstartServerProcess server)
      throws Exception {
    // Register an owned process without starting a real cluster; this field is declared with the same element type.
    Field field = QuickstartRunner.class.getDeclaredField("_serverProcesses");
    field.setAccessible(true);
    ((List<QuickstartServerProcess>) field.get(runner)).add(server);
  }
}
