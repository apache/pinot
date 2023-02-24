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
package org.apache.pinot.query.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.runtime.QueryRunner;
import org.testng.annotations.Test;


public class QueryDispatcherTest extends QueryTestSet {
  private static final Random RANDOM_REQUEST_ID_GEN = new Random();
  private static final int QUERY_SERVER_COUNT = 2;
  private static final ExecutorService WORKER_EXECUTOR_SERVICE = Executors.newFixedThreadPool(
      ResourceManager.DEFAULT_QUERY_WORKER_THREADS, new NamedThreadFactory("QueryDispatcherTest_Worker"));
  private static final ExecutorService RUNNER_EXECUTOR_SERVICE = Executors.newFixedThreadPool(
      ResourceManager.DEFAULT_QUERY_RUNNER_THREADS, new NamedThreadFactory("QueryDispatcherTest_Runner"));

  private final Map<Integer, QueryServer> _queryServerMap = new HashMap<>();
  private final Map<Integer, QueryRunner> _queryRunnerMap = new HashMap<>();

  private QueryEnvironment _queryEnvironment;
  private QueryDispatcher.DispatchClient _dispatchClient;

  @Test
  public void addTests() {
  }

  /**
  @BeforeClass
  public void setUp()
      throws Exception {

    for (int i = 0; i < QUERY_SERVER_COUNT; i++) {
      int availablePort = QueryTestUtils.getAvailablePort();
      QueryRunner queryRunner = Mockito.mock(QueryRunner.class);;
      Mockito.when(queryRunner.getQueryWorkerExecutorService()).thenReturn(WORKER_EXECUTOR_SERVICE);
      Mockito.when(queryRunner.getQueryRunnerExecutorService()).thenReturn(RUNNER_EXECUTOR_SERVICE);
      QueryServer queryServer = new QueryServer(availablePort, queryRunner);
      queryServer.start();
      _queryServerMap.put(availablePort, queryServer);
      _queryRunnerMap.put(availablePort, queryRunner);
    }

    List<Integer> portList = Lists.newArrayList(_queryServerMap.keySet());

    // reducer port doesn't matter, we are testing the worker instance not GRPC.
    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(1, portList.get(0), portList.get(1),
        QueryEnvironmentTestBase.TABLE_SCHEMAS, QueryEnvironmentTestBase.SERVER1_SEGMENTS,
        QueryEnvironmentTestBase.SERVER2_SEGMENTS);
  }

  @AfterClass
  public void tearDown() {
    for (QueryServer worker : _queryServerMap.values()) {
      worker.shutdown();
    }
  }

  @BeforeMethod
  public void beforeMethod() {
    _dispatchClient = Mockito.mock(QueryDispatcher.DispatchClient.class);
    QueryDispatcher.AsyncResponse successAsyncResponse =
        new QueryDispatcher.AsyncResponse(Mockito.mock(VirtualServer.class), 1,
            Worker.QueryResponse.getDefaultInstance(), null);
    Mockito.doAnswer(invocationOnMock -> {
      Consumer<QueryDispatcher.AsyncResponse> callback = invocationOnMock.getArgument(4);
      callback.accept(successAsyncResponse);
      return null;
    }).when(_dispatchClient).submit(Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test(dataProvider = "testSql")
  public void testQueryDispatcherCanSendCorrectPayload(String sql)
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    QueryDispatcher dispatcher = new QueryDispatcher();
    dispatcher = Mockito.spy(dispatcher);
    Mockito.doReturn(_dispatchClient).when(dispatcher).getOrCreateDispatchClient(Mockito.any(), Mockito.anyInt());
    int reducerStageId = dispatcher.submit(RANDOM_REQUEST_ID_GEN.nextLong(), queryPlan, 10_000L, new HashMap<>());
    Assert.assertTrue(PlannerUtils.isRootStage(reducerStageId));
    dispatcher.shutdown();
  }

  @Test
  public void testQueryDispatcherCanReturnErrorsFromDispatch() {
    QueryPlan queryPlan = Mockito.mock(QueryPlan.class);
    ServerInstance serverInstance = Mockito.mock(ServerInstance.class);
    VirtualServer virtualServer1 = new VirtualServer(serverInstance, 1);
    VirtualServer virtualServer2 = new VirtualServer(serverInstance, 2);
    VirtualServer virtualServer3 = new VirtualServer(serverInstance, 3);

    Map<Integer, StageMetadata> stageMetadataMap = new HashMap<>();
    StageMetadata stageMetadata = new StageMetadata();
    stageMetadataMap.put(1, stageMetadata);
    stageMetadata.setServerInstances(ImmutableList.of(virtualServer1, virtualServer2, virtualServer3));

    Mockito.doAnswer(invocationOnMock -> {
      Consumer<QueryDispatcher.AsyncResponse> callback = invocationOnMock.getArgument(4);
      callback.accept(new QueryDispatcher.AsyncResponse(
          virtualServer2, 1, Worker.QueryResponse.getDefaultInstance(), new RuntimeException("foo")));
      return null;
    }).when(_dispatchClient).submit(
        Mockito.any(), Mockito.anyInt(), Mockito.same(virtualServer2), Mockito.any(), Mockito.any());
    QueryDispatcher dispatcher = new QueryDispatcher();
    dispatcher = Mockito.spy(dispatcher);
    Mockito.doReturn(_dispatchClient).when(dispatcher).getOrCreateDispatchClient(Mockito.any(), Mockito.anyInt());
    try {
      dispatcher.submit(1, queryPlan, 10_000L, new HashMap<>());
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      System.out.println(e.getMessage());
      // Assert.assertTrue(e.getMessage().contains("foo"));
    }
    dispatcher.shutdown();
  }

  @Test
  public void testQueryDispatcherWillThrowOnTimeout() {
  } */
}
