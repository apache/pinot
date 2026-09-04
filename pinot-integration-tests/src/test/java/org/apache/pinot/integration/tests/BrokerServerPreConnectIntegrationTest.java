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

import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.broker.requesthandler.ServerPreConnector;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


/// Integration test for the broker startup server pre-connect feature
/// (`pinot.broker.startup.preconnect.enabled`). Brings up a real ZK + controller + server + broker with
/// an offline table, then verifies end-to-end that:
///
///  1. The production pre-connect path (`RoutingManager` -> `QueryRouter` -> `ServerChannels` -> a live
///     server) opens one channel per (server, table type).
///  2. The readiness gate genuinely holds and then releases -- exercised directly against
///     [ServerPreConnector] with a slow connector, because the broker's own gate is fast enough that
///     asserting on its terminal status could not distinguish a working gate from one stuck open.
///
/// The server is started **before** the broker so that routing, and therefore the set of channels to
/// pre-connect, is non-empty by the time the broker's pre-connect thread runs. With the broker first
/// there is nothing to connect and the feature would appear to pass while doing nothing.
public class BrokerServerPreConnectIntegrationTest extends BaseClusterIntegrationTest {
  private static final long PRECONNECT_TIMEOUT_MS = 30_000L;

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_STARTUP_PRECONNECT_ENABLED, true);
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_STARTUP_PRECONNECT_TIMEOUT_MS,
        PRECONNECT_TIMEOUT_MS);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startController();
    startServer();
    startBroker();

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Build and upload segments so the broker has a live server to route to -- and therefore a real
    // channel to pre-connect against.
    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());
    stopBroker();
    stopServer();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void preConnectEnabledBrokerReachesGoodServiceStatus() {
    String instanceId = _brokerStarters.get(0).getInstanceId();
    TestUtils.waitForCondition(aVoid -> ServiceStatus.getServiceStatus(instanceId) == ServiceStatus.Status.GOOD,
        PRECONNECT_TIMEOUT_MS, "Broker with pre-connect enabled never reported GOOD service status");
  }

  @Test
  public void preConnectOpensChannelsToEveryLiveServer() {
    // Connecting an already-open channel is a no-op that still counts as connected, so the expected
    // count holds regardless of any earlier lazy connects from setUp's queries.
    int expectedChannels = _serverStarters.size() * TableType.values().length;
    int connected = _brokerStarters.get(0).getBrokerRequestHandler()
        .preConnectServers(System.currentTimeMillis() + PRECONNECT_TIMEOUT_MS);
    Assert.assertEquals(connected, expectedChannels,
        "Pre-connect should open a channel to every routable server for both table types");
  }

  /// The readiness gate is the highest-risk part of the feature, and the broker's own pre-connect
  /// finishes in milliseconds here, so a terminal-status assertion cannot tell a working gate from one
  /// that never engaged. Drive [ServerPreConnector] directly with a connector slower than the budget and
  /// assert both halves of the contract: the wait is bounded, and it ends.
  @Test
  public void preConnectBoundsTheGateWhenServersAreSlow() {
    List<ServerInstance> servers = List.of(mock(ServerInstance.class));
    long budgetMs = 500L;
    long startMs = System.currentTimeMillis();
    int connected = new ServerPreConnector(() -> servers, (server, tableType, timeoutMs) -> {
      try {
        Thread.sleep(30_000L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return false;
    }).preConnect(startMs + budgetMs);
    long elapsedMs = System.currentTimeMillis() - startMs;

    Assert.assertEquals(connected, 0);
    Assert.assertTrue(elapsedMs < 10_000L,
        "Pre-connect held the readiness gate for " + elapsedMs + " ms, well past its " + budgetMs + " ms budget");
  }
}
