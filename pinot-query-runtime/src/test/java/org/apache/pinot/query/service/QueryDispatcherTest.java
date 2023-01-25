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

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class QueryDispatcherTest extends QueryTestSet {
  private static final Random RANDOM_REQUEST_ID_GEN = new Random();
  private static final int QUERY_SERVER_COUNT = 2;
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(
      ResourceManager.DEFAULT_QUERY_WORKER_THREADS, new NamedThreadFactory("QueryDispatcherTestExecutorService"));

  private final Map<Integer, QueryServer> _queryServerMap = new HashMap<>();
  private final Map<Integer, QueryRunner> _queryRunnerMap = new HashMap<>();

  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp()
      throws Exception {

    for (int i = 0; i < QUERY_SERVER_COUNT; i++) {
      int availablePort = QueryTestUtils.getAvailablePort();
      QueryRunner queryRunner = Mockito.mock(QueryRunner.class);;
      Mockito.when(queryRunner.getExecutorService()).thenReturn(EXECUTOR_SERVICE);
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

  @Test(dataProvider = "testSql")
  public void testQueryDispatcherCanSendCorrectPayload(String sql)
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    QueryDispatcher dispatcher = new QueryDispatcher();
    int reducerStageId = dispatcher.submit(RANDOM_REQUEST_ID_GEN.nextLong(), queryPlan, 10_000L, new HashMap<>());
    Assert.assertTrue(PlannerUtils.isRootStage(reducerStageId));
    dispatcher.shutdown();
  }
}
