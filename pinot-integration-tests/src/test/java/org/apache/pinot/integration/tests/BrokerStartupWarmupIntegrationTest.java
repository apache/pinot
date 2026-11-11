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
import org.apache.pinot.broker.requesthandler.BrokerWarmupConfig;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/// Integration test for the broker startup data-plane warmup feature
/// (`pinot.broker.startup.warmup.*`). Brings up a real ZK + controller + server + broker with an offline
/// table, then verifies two things end-to-end:
///
///  1. With warmup enabled the readiness gate opens: the broker's [ServiceStatus] reaches `GOOD`.
///     Readiness is held at `STARTING` until warmup completes, and the broker's query path serves
///     regardless of [ServiceStatus], so a gate that never released would not fail `setUp` -- asserting
///     `GOOD` is what actually proves the gate released after warmup.
///  2. The production warmup path (`RoutingManager` set-cover -> [QueryRouter] probe -> reduce) reaches
///     its depth floor and returns `true` within the budget against a live server.
public class BrokerStartupWarmupIntegrationTest extends BaseClusterIntegrationTest {
  private static final long WARMUP_BUDGET_MS = 30_000L;
  // Small floor so the integration test warms and opens readiness quickly (the depth floor is only large
  // in production, to drive JIT to its top tier).
  private static final int WARMUP_MIN_ITERATIONS = 10;

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_ENABLED, true);
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_BUDGET_MS, WARMUP_BUDGET_MS);
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_MIN_ITERATIONS,
        WARMUP_MIN_ITERATIONS);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startController();
    startBroker();
    startServer();

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Build and upload segments so the broker has a live server to route probe queries to.
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
  public void warmupEnabledBrokerReachesGoodServiceStatus() {
    String instanceId = _brokerStarters.get(0).getInstanceId();
    TestUtils.waitForCondition(aVoid -> ServiceStatus.getServiceStatus(instanceId) == ServiceStatus.Status.GOOD,
        WARMUP_BUDGET_MS, "Broker with warmup enabled never reported GOOD service status");
    // Once GOOD, the warmup gate has released: its description is no longer the "warming up" text. (The
    // composite ServiceStatus description concatenates every callback's "<name>:<desc>;", so it is never
    // literally "None"; the meaningful assertion is that the warming description is gone.) The full
    // STARTING -> "Warming up broker data plane" -> GOOD transition is covered deterministically in
    // BrokerWarmupGateTest, since a healthy cluster warms faster than this poll can observe.
    Assert.assertFalse(ServiceStatus.getStatusDescription(instanceId).contains("Warming up broker data plane"),
        "Once GOOD, the warmup gate must no longer report the warming-up description");
  }

  @Test
  public void warmUpReachesFloorAgainstLiveServer() {
    BrokerWarmupConfig config = new BrokerWarmupConfig(true, WARMUP_BUDGET_MS, WARMUP_MIN_ITERATIONS, 1);
    boolean reachedFloor = _brokerStarters.get(0).getBrokerRequestHandler()
        .warmUp(config, System.currentTimeMillis() + WARMUP_BUDGET_MS);
    Assert.assertTrue(reachedFloor,
        "Warmup should reach its probe-count floor against a live server within the budget");
  }

  @Test
  public void warmUpReachesFloorWithConcurrentProbes() {
    // Concurrency 3: probes fire in parallel on a 3-thread pool, exercising the concurrent scatter/gather
    // path the serial arms do not.
    BrokerWarmupConfig config = new BrokerWarmupConfig(true, WARMUP_BUDGET_MS, WARMUP_MIN_ITERATIONS, 3);
    boolean reachedFloor = _brokerStarters.get(0).getBrokerRequestHandler()
        .warmUp(config, System.currentTimeMillis() + WARMUP_BUDGET_MS);
    Assert.assertTrue(reachedFloor, "Concurrent warmup should reach its probe-count floor within the budget");
  }

  @Test
  public void warmUpReturnsFalseWhenBudgetExpiresBeforeFloor() {
    // An unreachable floor with a short budget must exit on the budget (returning false) and must not hang.
    long shortBudgetMs = 2_000L;
    BrokerWarmupConfig config = new BrokerWarmupConfig(true, shortBudgetMs, 100_000_000, 1);
    long start = System.currentTimeMillis();
    boolean reachedFloor = _brokerStarters.get(0).getBrokerRequestHandler()
        .warmUp(config, start + shortBudgetMs);
    long elapsed = System.currentTimeMillis() - start;
    Assert.assertFalse(reachedFloor, "Warmup must return false when the budget expires before the floor");
    // Tight bound: with the budget a hard ceiling (each Future.get is bounded by the remaining budget and
    // stragglers are cancelled), warmUp returns within a small epsilon of the budget, not merely "eventually".
    Assert.assertTrue(elapsed < shortBudgetMs + 2_000L,
        "Warmup must return within ~budget, not hang (elapsed " + elapsed + " ms, budget " + shortBudgetMs + ")");
  }
}
