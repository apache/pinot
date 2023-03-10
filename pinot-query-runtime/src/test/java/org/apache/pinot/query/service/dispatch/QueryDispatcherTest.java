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
package org.apache.pinot.query.service.dispatch;

import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.service.QueryServer;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


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
  private DispatchClient _dispatchClient;

  @BeforeClass
  public void setUp()
      throws Exception {

    for (int i = 0; i < QUERY_SERVER_COUNT; i++) {
      int availablePort = QueryTestUtils.getAvailablePort();
      QueryRunner queryRunner = Mockito.mock(QueryRunner.class);;
      Mockito.when(queryRunner.getQueryWorkerExecutorService()).thenReturn(WORKER_EXECUTOR_SERVICE);
      Mockito.when(queryRunner.getQueryRunnerExecutorService()).thenReturn(RUNNER_EXECUTOR_SERVICE);
      QueryServer queryServer = new QueryServer(availablePort, queryRunner);
      queryServer = Mockito.spy(queryServer);
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

  @Test(dataProvider = "testSql")
  public void testQueryDispatcherCanSendCorrectPayload(String sql)
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    QueryDispatcher dispatcher = new QueryDispatcher();
    int reducerStageId = dispatcher.submit(RANDOM_REQUEST_ID_GEN.nextLong(), queryPlan, 10_000L, new HashMap<>());
    Assert.assertTrue(PlannerUtils.isRootStage(reducerStageId));
    dispatcher.shutdown();
  }

  @Test
  public void testQueryDispatcherThrowsWhenQueryServerThrows()
      throws Exception {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    Mockito.doThrow(new RuntimeException("foo")).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    QueryDispatcher dispatcher = new QueryDispatcher();
    try {
      dispatcher.submit(RANDOM_REQUEST_ID_GEN.nextLong(), queryPlan, 10_000L, new HashMap<>());
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error dispatching query"));
    }
    dispatcher.shutdown();
  }

  @Test
  public void testQueryDispatcherCancelWhenQueryServerCallsOnError()
      throws Exception {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        StreamObserver<Worker.QueryResponse> observer = invocationOnMock.getArgument(1);
        observer.onError(new RuntimeException("foo"));
        return null;
      }
    }).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    QueryDispatcher dispatcher = new QueryDispatcher();
    long requestId = RANDOM_REQUEST_ID_GEN.nextLong();
    try {
      dispatcher.submitAndReduce(requestId, queryPlan, null, 10_000L, new HashMap<>(), null);
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error executing query"));
    }
    // wait just a little, until the cancel is being called.
    Thread.sleep(50);
    for (QueryServer queryServer : _queryServerMap.values()) {
      Mockito.verify(queryServer, Mockito.times(1)).cancel(Mockito.argThat(a -> a.getRequestId() == requestId),
          Mockito.any());
    }
    dispatcher.shutdown();
  }

  @Test
  public void testQueryDispatcherCancelWhenQueryReducerThrowsError()
      throws Exception {
    String sql = "SELECT * FROM a";
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    QueryDispatcher dispatcher = new QueryDispatcher();
    long requestId = RANDOM_REQUEST_ID_GEN.nextLong();
    try {
      // will throw b/c mailboxService is null
      dispatcher.submitAndReduce(requestId, queryPlan, null, 10_000L, new HashMap<>(), null);
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error executing query"));
    }
    // wait just a little, until the cancel is being called.
    Thread.sleep(50);
    for (QueryServer queryServer : _queryServerMap.values()) {
      Mockito.verify(queryServer, Mockito.times(1)).cancel(Mockito.argThat(a -> a.getRequestId() == requestId),
          Mockito.any());
    }
    dispatcher.shutdown();
  }

  @Test
  public void testQueryDispatcherThrowsWhenQueryServerCallsOnError()
      throws Exception {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        StreamObserver<Worker.QueryResponse> observer = invocationOnMock.getArgument(1);
        observer.onError(new RuntimeException("foo"));
        return null;
      }
    }).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    QueryDispatcher dispatcher = new QueryDispatcher();
    try {
      dispatcher.submit(RANDOM_REQUEST_ID_GEN.nextLong(), queryPlan, 10_000L, new HashMap<>());
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error dispatching query"));
    }
    dispatcher.shutdown();
  }

  @Test
  public void testQueryDispatcherThrowsWhenQueryServerTimesOut() {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    CountDownLatch neverClosingLatch = new CountDownLatch(1);
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        neverClosingLatch.await(5, TimeUnit.SECONDS);
        StreamObserver<Worker.QueryResponse> observer = invocationOnMock.getArgument(1);
        observer.onCompleted();
        return null;
      }
    }).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    QueryDispatcher dispatcher = new QueryDispatcher();
    try {
      dispatcher.submit(RANDOM_REQUEST_ID_GEN.nextLong(), queryPlan, 1_000, new HashMap<>());
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Timed out waiting for response")
          || e.getMessage().contains("Error dispatching query"));
    }
    neverClosingLatch.countDown();
    dispatcher.shutdown();
  }

  @Test
  public void testQueryDispatcherThrowsWhenDeadlinePreExpiredAndAsyncResponseNotPolled() {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    QueryDispatcher dispatcher = new QueryDispatcher();
    try {
      dispatcher.submit(RANDOM_REQUEST_ID_GEN.nextLong(), queryPlan, -10_000, new HashMap<>());
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Timed out waiting"));
    }
    dispatcher.shutdown();
  }
}
