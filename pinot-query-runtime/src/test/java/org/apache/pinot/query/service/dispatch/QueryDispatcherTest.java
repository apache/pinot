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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.service.server.QueryServer;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class QueryDispatcherTest extends QueryTestSet {
  private static final AtomicLong REQUEST_ID_GEN = new AtomicLong();
  private static final int QUERY_SERVER_COUNT = 2;

  private final Map<Integer, QueryServer> _queryServerMap = new HashMap<>();

  private QueryEnvironment _queryEnvironment;
  private QueryDispatcher _queryDispatcher;

  @BeforeClass
  public void setUp()
      throws Exception {
    for (int i = 0; i < QUERY_SERVER_COUNT; i++) {
      int availablePort = QueryTestUtils.getAvailablePort();
      QueryRunner queryRunner = Mockito.mock(QueryRunner.class);
      QueryServer queryServer = Mockito.spy(new QueryServer(availablePort, queryRunner));
      queryServer.start();
      _queryServerMap.put(availablePort, queryServer);
    }
    List<Integer> portList = new ArrayList<>(_queryServerMap.keySet());

    // reducer port doesn't matter, we are testing the worker instance not GRPC.
    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(1, portList.get(0), portList.get(1),
        QueryEnvironmentTestBase.TABLE_SCHEMAS, QueryEnvironmentTestBase.SERVER1_SEGMENTS,
        QueryEnvironmentTestBase.SERVER2_SEGMENTS, null);
    _queryDispatcher = new QueryDispatcher(Mockito.mock(MailboxService.class));
  }

  @AfterClass
  public void tearDown() {
    _queryDispatcher.shutdown();
    for (QueryServer worker : _queryServerMap.values()) {
      worker.shutdown();
    }
  }

  @Test(dataProvider = "testSql")
  public void testQueryDispatcherCanSendCorrectPayload(String sql)
      throws Exception {
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    _queryDispatcher.submit(REQUEST_ID_GEN.getAndIncrement(), dispatchableSubPlan, 10_000L, Collections.emptyMap());
  }

  @Test
  public void testQueryDispatcherThrowsWhenQueryServerThrows() {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    Mockito.doThrow(new RuntimeException("foo")).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try {
      _queryDispatcher.submit(REQUEST_ID_GEN.getAndIncrement(), dispatchableSubPlan, 10_000L, Collections.emptyMap());
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error dispatching query"));
    }
    Mockito.reset(failingQueryServer);
  }

  @Test
  public void testQueryDispatcherCancelWhenQueryServerCallsOnError()
      throws Exception {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    Mockito.doAnswer(invocationOnMock -> {
      StreamObserver<Worker.QueryResponse> observer = invocationOnMock.getArgument(1);
      observer.onError(new RuntimeException("foo"));
      return null;
    }).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    long requestId = REQUEST_ID_GEN.getAndIncrement();
    RequestContext context = new DefaultRequestContext();
    context.setRequestId(requestId);
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try {
      _queryDispatcher.submitAndReduce(context, dispatchableSubPlan, 10_000L, Collections.emptyMap(), null);
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error dispatching query"));
    }
    // wait just a little, until the cancel is being called.
    Thread.sleep(50);
    for (QueryServer queryServer : _queryServerMap.values()) {
      Mockito.verify(queryServer, Mockito.times(1))
          .cancel(Mockito.argThat(a -> a.getRequestId() == requestId), Mockito.any());
    }
    Mockito.reset(failingQueryServer);
  }

  @Test
  public void testQueryDispatcherCancelWhenQueryReducerThrowsError()
      throws Exception {
    String sql = "SELECT * FROM a";
    long requestId = REQUEST_ID_GEN.getAndIncrement();
    RequestContext context = new DefaultRequestContext();
    context.setRequestId(requestId);
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try {
      // will throw b/c mailboxService is mocked
      _queryDispatcher.submitAndReduce(context, dispatchableSubPlan, 10_000L, Collections.emptyMap(), null);
      Assert.fail("Method call above should have failed");
    } catch (NullPointerException e) {
      // Expected
    }
    // wait just a little, until the cancel is being called.
    Thread.sleep(50);
    for (QueryServer queryServer : _queryServerMap.values()) {
      Mockito.verify(queryServer, Mockito.times(1))
          .cancel(Mockito.argThat(a -> a.getRequestId() == requestId), Mockito.any());
    }
  }

  @Test
  public void testQueryDispatcherThrowsWhenQueryServerCallsOnError() {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    Mockito.doAnswer(invocationOnMock -> {
      StreamObserver<Worker.QueryResponse> observer = invocationOnMock.getArgument(1);
      observer.onError(new RuntimeException("foo"));
      return null;
    }).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try {
      _queryDispatcher.submit(REQUEST_ID_GEN.getAndIncrement(), dispatchableSubPlan, 10_000L, Collections.emptyMap());
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error dispatching query"));
    }
    Mockito.reset(failingQueryServer);
  }

  @Test
  public void testQueryDispatcherThrowsWhenQueryServerTimesOut() {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    QueryServer failingQueryServer = _queryServerMap.values().iterator().next();
    CountDownLatch neverClosingLatch = new CountDownLatch(1);
    Mockito.doAnswer(invocationOnMock -> {
      neverClosingLatch.await();
      StreamObserver<Worker.QueryResponse> observer = invocationOnMock.getArgument(1);
      observer.onCompleted();
      return null;
    }).when(failingQueryServer).submit(Mockito.any(), Mockito.any());
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    try {
      _queryDispatcher.submit(REQUEST_ID_GEN.getAndIncrement(), dispatchableSubPlan, 200L, Collections.emptyMap());
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      String message = e.getMessage();
      Assert.assertTrue(
          message.contains("Timed out waiting for response") || message.contains("Error dispatching query"));
    }
    neverClosingLatch.countDown();
    Mockito.reset(failingQueryServer);
  }

  @Test(expectedExceptions = TimeoutException.class)
  public void testQueryDispatcherThrowsWhenDeadlinePreExpiredAndAsyncResponseNotPolled()
      throws Exception {
    String sql = "SELECT * FROM a WHERE col1 = 'foo'";
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);
    _queryDispatcher.submit(REQUEST_ID_GEN.getAndIncrement(), dispatchableSubPlan, 0L, Collections.emptyMap());
  }
}
