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
package org.apache.pinot.query.runtime.queries;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// A server can terminate a query before some of its stages run, for example when the resource accountant kills the
/// query to protect the heap. These stages must still send the termination error to their receivers. Otherwise the
/// broker gets no error and waits for data until the query times out.
///
/// Like the query server, each server runs all the stages of a query in one [QueryExecutionContext], and the test
/// terminates the context of one server.
public class QueryTerminatedBeforeStageStartsTest extends QueryRunnerTestBase {
  private static final long QUERY_TIMEOUT_MS = 30_000L;
  private static final String QUERY_OPTIONS = "SET timeoutMs = " + QUERY_TIMEOUT_MS + "; ";
  private static final String GROUP_BY_QUERY = QUERY_OPTIONS + "SELECT col1, COUNT(*) FROM a GROUP BY col1";
  /// The leaf stage of `a` runs a pipeline breaker, which collects the keys from `b` before the stage starts.
  private static final String DYNAMIC_BROADCAST_QUERY = QUERY_OPTIONS
      + "SELECT /*+ joinOptions(join_strategy='dynamic_broadcast') */ col1 FROM a WHERE a.col1 IN "
      + "(SELECT b.col2 FROM b WHERE b.col3 < 10) AND a.col3 > 0";
  private static final String KILL_MESSAGE = "OOM killed on SERVER: test";
  /// The broker must report the kill long before the query times out.
  private static final long MAX_REPORT_TIME_MS = 10_000L;

  private QueryServerEnclosure _killedServer;
  private final Map<QueryServerEnclosure, QueryExecutionContext> _executionContexts = new ConcurrentHashMap<>();
  private volatile boolean _killBeforeScheduling;

  @BeforeClass
  public void setUp()
      throws Exception {
    MockInstanceDataManagerFactory factory1 = new MockInstanceDataManagerFactory("server1");
    factory1.registerTable(QueryRunnerTest.SCHEMA_BUILDER.setSchemaName("a").build(), "a_REALTIME");
    factory1.registerTable(QueryRunnerTest.SCHEMA_BUILDER.setSchemaName("b").build(), "b_REALTIME");
    factory1.addSegment("a_REALTIME", QueryRunnerTest.buildRows("a_REALTIME"));
    factory1.addSegment("b_REALTIME", QueryRunnerTest.buildRows("b_REALTIME"));
    MockInstanceDataManagerFactory factory2 = new MockInstanceDataManagerFactory("server2");
    factory2.registerTable(QueryRunnerTest.SCHEMA_BUILDER.setSchemaName("a").build(), "a_REALTIME");
    factory2.addSegment("a_REALTIME", QueryRunnerTest.buildRows("a_REALTIME"));

    _reducerHostname = "localhost";
    _reducerPort = QueryTestUtils.getAvailablePort();
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    reducerConfig.put(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _reducerPort);
    _mailboxService =
        new MailboxService(_reducerHostname, _reducerPort, InstanceType.BROKER, new PinotConfiguration(reducerConfig));
    _mailboxService.start();

    // The gated executor lets a test act after the op chains are scheduled but before they start.
    Map<String, Object> serverConfig =
        Map.of(Server.MULTISTAGE_EXECUTOR_CONFIG_PREFIX + ".type", GatedExecutorServicePlugin.ID);
    QueryServerEnclosure server1 = new QueryServerEnclosure(factory1, serverConfig);
    server1.start();
    QueryServerEnclosure server2 = new QueryServerEnclosure(factory2, serverConfig);
    server2.start();
    int port1 = server1.getPort();
    int port2 = server2.getPort();
    _servers.put(new QueryServerInstance("Server_localhost_" + port1, "localhost", port1, port1), server1);
    _servers.put(new QueryServerInstance("Server_localhost_" + port2, "localhost", port2, port2), server2);
    _killedServer = server1;

    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerPort, port1, port2,
        factory1.getRegisteredSchemaMap(), factory1.buildTableSegmentNameMap(), factory2.buildTableSegmentNameMap(),
        null);
  }

  @AfterClass
  public void tearDown() {
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

  @BeforeMethod
  public void resetQueryState() {
    _executionContexts.clear();
    _killBeforeScheduling = false;
  }

  @AfterMethod
  public void openGate() {
    GatedExecutorServicePlugin.openGate();
  }

  @DataProvider(name = "queries")
  public Object[][] queries() {
    return new Object[][]{{GROUP_BY_QUERY}, {DYNAMIC_BROADCAST_QUERY}};
  }

  /// The server kills the query after the broker dispatched it, but before the server scheduled any of its stages.
  @Test(dataProvider = "queries")
  public void testKillBeforeStagesAreScheduled(String query) {
    _killBeforeScheduling = true;
    long startTimeMs = System.currentTimeMillis();
    QueryDispatcher.QueryResult result = queryRunner(query, false);
    assertKillReported(result, startTimeMs);
  }

  /// The server kills the query after it scheduled the op chains of its stages, but before they started.
  @Test
  public void testKillAfterStagesAreScheduledBeforeTheyStart()
      throws Exception {
    int numWorkers = 0;
    for (DispatchablePlanFragment stagePlan : planQuery(GROUP_BY_QUERY).getQueryPlan().getQueryStagesWithoutRoot()) {
      numWorkers += stagePlan.getWorkerMetadataList().size();
    }
    int numOpChains = numWorkers;
    GatedExecutorServicePlugin.closeGate();
    CompletableFuture<QueryDispatcher.QueryResult> resultFuture =
        CompletableFuture.supplyAsync(() -> queryRunner(GROUP_BY_QUERY, false));
    TestUtils.waitForCondition(aVoid -> GatedExecutorServicePlugin.getNumParkedTasks() == numOpChains, 10_000L,
        "Op chains were not scheduled");
    long killTimeMs = System.currentTimeMillis();
    _executionContexts.get(_killedServer).terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, KILL_MESSAGE);
    GatedExecutorServicePlugin.openGate();
    assertKillReported(resultFuture.get(2 * QUERY_TIMEOUT_MS, TimeUnit.MILLISECONDS), killTimeMs);
  }

  private static void assertKillReported(QueryDispatcher.QueryResult result, long fromTimeMs) {
    long elapsedMs = System.currentTimeMillis() - fromTimeMs;
    QueryProcessingException processingException = result.getProcessingException();
    assertNotNull(processingException, "Expected the query to fail");
    assertEquals(processingException.getErrorCode(), QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.getId(),
        "Unexpected error after " + elapsedMs + "ms: " + processingException.getMessage());
    assertTrue(processingException.getMessage().contains(KILL_MESSAGE), processingException.getMessage());
    assertTrue(elapsedMs < MAX_REPORT_TIME_MS, "The kill was reported after " + elapsedMs + "ms");
  }

  /// Like the query server, each server runs all the stages of the query in one execution context.
  @Override
  protected QueryExecutionContext getExecutionContext(QueryServerEnclosure server,
      Map<String, String> requestMetadataMap) {
    return _executionContexts.computeIfAbsent(server, key -> {
      QueryExecutionContext context = QueryExecutionContext.forMseServerRequest(requestMetadataMap, "serverId");
      if (_killBeforeScheduling && key == _killedServer) {
        context.terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, KILL_MESSAGE);
      }
      return context;
    });
  }
}
