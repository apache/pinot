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
package org.apache.pinot.query.service.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.planner.DispatchablePlanFragment;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class QueryServerTest extends QueryTestSet {
  private static final Random RANDOM_REQUEST_ID_GEN = new Random();
  private static final int QUERY_SERVER_COUNT = 2;
  private static final String KEY_OF_SERVER_INSTANCE_HOST = "pinot.query.runner.server.hostname";
  private static final String KEY_OF_SERVER_INSTANCE_PORT = "pinot.query.runner.server.port";
  private static final ExecutorService LEAF_WORKER_EXECUTOR_SERVICE =
      Executors.newFixedThreadPool(ResourceManager.DEFAULT_QUERY_WORKER_THREADS,
          new NamedThreadFactory("QueryDispatcherTest_LeafWorker"));
  private static final ExecutorService INTERM_WORKER_EXECUTOR_SERVICE =
      Executors.newCachedThreadPool(new NamedThreadFactory("QueryDispatcherTest_IntermWorker"));

  private final Map<Integer, QueryServer> _queryServerMap = new HashMap<>();
  private final Map<Integer, QueryRunner> _queryRunnerMap = new HashMap<>();

  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp()
      throws Exception {

    for (int i = 0; i < QUERY_SERVER_COUNT; i++) {
      int availablePort = QueryTestUtils.getAvailablePort();
      QueryRunner queryRunner = Mockito.mock(QueryRunner.class);
      Mockito.when(queryRunner.getQueryWorkerLeafExecutorService()).thenReturn(LEAF_WORKER_EXECUTOR_SERVICE);
      Mockito.when(queryRunner.getQueryWorkerIntermExecutorService()).thenReturn(INTERM_WORKER_EXECUTOR_SERVICE);
      QueryServer queryServer = new QueryServer(availablePort, queryRunner);
      queryServer.start();
      _queryServerMap.put(availablePort, queryServer);
      _queryRunnerMap.put(availablePort, queryRunner);
    }

    List<Integer> portList = Lists.newArrayList(_queryServerMap.keySet());

    // reducer port doesn't matter, we are testing the worker instance not GRPC.
    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(1, portList.get(0), portList.get(1),
        QueryEnvironmentTestBase.TABLE_SCHEMAS, QueryEnvironmentTestBase.SERVER1_SEGMENTS,
        QueryEnvironmentTestBase.SERVER2_SEGMENTS, null);
  }

  @AfterClass
  public void tearDown() {
    for (QueryServer worker : _queryServerMap.values()) {
      worker.shutdown();
    }
  }

  @Test
  public void testException() {
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery("SELECT * FROM a");
    // only get one worker request out.
    Worker.QueryRequest queryRequest = getQueryRequest(dispatchableSubPlan, 1);
    QueryRunner mockRunner =
        _queryRunnerMap.get(Integer.parseInt(queryRequest.getMetadataOrThrow(KEY_OF_SERVER_INSTANCE_PORT)));
    Mockito.doThrow(new RuntimeException("foo")).when(mockRunner).processQuery(Mockito.any(), Mockito.anyMap());
    // submit the request for testing.
    Worker.QueryResponse resp = submitRequest(queryRequest);
    // reset the mock runner before assert.
    Mockito.reset(mockRunner);
    // should contain error message pattern
    String errorMessage = resp.getMetadataMap().get(QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_ERROR);
    Assert.assertTrue(errorMessage.contains("foo"));
  }

  @Test(dataProvider = "testSql")
  public void testWorkerAcceptsWorkerRequestCorrect(String sql)
      throws Exception {
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(sql);

    for (int stageId = 0; stageId < dispatchableSubPlan.getQueryStageList().size(); stageId++) {
      if (stageId > 0) { // we do not test reduce stage.
        // only get one worker request out.
        Worker.QueryRequest queryRequest = getQueryRequest(dispatchableSubPlan, stageId);

        // submit the request for testing.
        Worker.QueryResponse resp = submitRequest(queryRequest);
        Assert.assertNotNull(resp.getMetadataMap().get(QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_OK));

        DispatchablePlanFragment dispatchablePlanFragment = dispatchableSubPlan.getQueryStageList().get(stageId);

        StageMetadata stageMetadata = new StageMetadata.Builder()
            .setWorkerMetadataList(dispatchablePlanFragment.getWorkerMetadataList())
            .addCustomProperties(dispatchablePlanFragment.getCustomProperties()).build();

        // ensure mock query runner received correctly deserialized payload.
        QueryRunner mockRunner =
            _queryRunnerMap.get(Integer.parseInt(queryRequest.getMetadataOrThrow(KEY_OF_SERVER_INSTANCE_PORT)));
        String requestIdStr = queryRequest.getMetadataOrThrow(QueryConfig.KEY_OF_BROKER_REQUEST_ID);

        // since submitRequest is async, we need to wait for the mockRunner to receive the query payload.
        int finalStageId = stageId;
        TestUtils.waitForCondition(aVoid -> {
          try {
            Mockito.verify(mockRunner).processQuery(Mockito.argThat(distributedStagePlan -> {
              PlanNode planNode =
                  dispatchableSubPlan.getQueryStageList().get(finalStageId).getPlanFragment().getFragmentRoot();
              return isStageNodesEqual(planNode, distributedStagePlan.getStageRoot()) && isStageMetadataEqual(
                  stageMetadata, distributedStagePlan.getStageMetadata());
            }), Mockito.argThat(requestMetadataMap -> requestIdStr.equals(
                requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_ID))));
            return true;
          } catch (Throwable t) {
            return false;
          }
        }, 10000L, "Error verifying mock QueryRunner intercepted query payload!");

        // reset the mock runner.
        Mockito.reset(mockRunner);
      }
    }
  }

  private boolean isStageMetadataEqual(StageMetadata expected, StageMetadata actual) {
    if (!EqualityUtils.isEqual(StageMetadata.getTableName(expected),
        StageMetadata.getTableName(actual))) {
      return false;
    }
    TimeBoundaryInfo expectedTimeBoundaryInfo = StageMetadata.getTimeBoundary(expected);
    TimeBoundaryInfo actualTimeBoundaryInfo = StageMetadata.getTimeBoundary(actual);
    if (expectedTimeBoundaryInfo == null && actualTimeBoundaryInfo != null
        || expectedTimeBoundaryInfo != null && actualTimeBoundaryInfo == null) {
      return false;
    }
    if (expectedTimeBoundaryInfo != null && actualTimeBoundaryInfo != null && (
        !EqualityUtils.isEqual(expectedTimeBoundaryInfo.getTimeColumn(), actualTimeBoundaryInfo.getTimeColumn())
            || !EqualityUtils.isEqual(expectedTimeBoundaryInfo.getTimeValue(),
            actualTimeBoundaryInfo.getTimeValue()))) {
      return false;
    }
    List<WorkerMetadata> expectedWorkerMetadataList = expected.getWorkerMetadataList();
    List<WorkerMetadata> actualWorkerMetadataList = actual.getWorkerMetadataList();
    if (expectedWorkerMetadataList.size() != actualWorkerMetadataList.size()) {
      return false;
    }
    for (int i = 0; i < expectedWorkerMetadataList.size(); i++) {
      if (!isWorkerMetadataEqual(expectedWorkerMetadataList.get(i), actualWorkerMetadataList.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isWorkerMetadataEqual(WorkerMetadata expected, WorkerMetadata actual) {
    if (!expected.getVirtualServerAddress().hostname().equals(actual.getVirtualServerAddress().hostname())
        || expected.getVirtualServerAddress().port() != actual.getVirtualServerAddress().port()
        || expected.getVirtualServerAddress().workerId() != actual.getVirtualServerAddress().workerId()) {
      return false;
    }
    return EqualityUtils.isEqual(WorkerMetadata.getTableSegmentsMap(expected),
        WorkerMetadata.getTableSegmentsMap(actual));
  }

  private static boolean isStageNodesEqual(PlanNode left, PlanNode right) {
    // This only checks the stage tree structure is correct. because the input/stageId fields are not
    // part of the generic proto ser/de; which is tested in query planner.
    if (left.getPlanFragmentId() != right.getPlanFragmentId() || left.getClass() != right.getClass()
        || left.getInputs().size() != right.getInputs().size()) {
      return false;
    }
    left.getInputs().sort(Comparator.comparingInt(PlanNode::getPlanFragmentId));
    right.getInputs().sort(Comparator.comparingInt(PlanNode::getPlanFragmentId));
    for (int i = 0; i < left.getInputs().size(); i++) {
      if (!isStageNodesEqual(left.getInputs().get(i), right.getInputs().get(i))) {
        return false;
      }
    }
    return true;
  }

  private Worker.QueryResponse submitRequest(Worker.QueryRequest queryRequest) {
    String host = queryRequest.getMetadataMap().get(KEY_OF_SERVER_INSTANCE_HOST);
    int port = Integer.parseInt(queryRequest.getMetadataMap().get(KEY_OF_SERVER_INSTANCE_PORT));
    long timeoutMs = Long.parseLong(queryRequest.getMetadataMap().get(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS));
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    PinotQueryWorkerGrpc.PinotQueryWorkerBlockingStub stub = PinotQueryWorkerGrpc.newBlockingStub(channel);
    Worker.QueryResponse resp = stub.withDeadline(Deadline.after(timeoutMs, TimeUnit.MILLISECONDS))
        .submit(queryRequest);
    channel.shutdown();
    return resp;
  }

  private Worker.QueryRequest getQueryRequest(DispatchableSubPlan dispatchableSubPlan, int stageId) {
    Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdMap =
        dispatchableSubPlan.getQueryStageList().get(stageId).getServerInstanceToWorkerIdMap();
    // this particular test set requires the request to have a single QueryServerInstance to dispatch to
    // as it is not testing the multi-tenancy dispatch (which is in the QueryDispatcherTest)
    QueryServerInstance serverInstance = serverInstanceToWorkerIdMap.keySet().iterator().next();
    int workerId = serverInstanceToWorkerIdMap.get(serverInstance).get(0);

    return Worker.QueryRequest.newBuilder().addStagePlan(
            QueryPlanSerDeUtils.serialize(dispatchableSubPlan, stageId, serverInstance, ImmutableList.of(workerId)))
        // the default configurations that must exist.
        .putMetadata(QueryConfig.KEY_OF_BROKER_REQUEST_ID, String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()))
        .putMetadata(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS,
            String.valueOf(CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS))
        // extra configurations we want to test also parsed out correctly.
        .putMetadata(KEY_OF_SERVER_INSTANCE_HOST, serverInstance.getHostname())
        .putMetadata(KEY_OF_SERVER_INSTANCE_PORT, String.valueOf(serverInstance.getQueryServicePort())).build();
  }
}
