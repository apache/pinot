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

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.StageNodeSerDeUtils;
import org.apache.pinot.query.routing.QueryPlanSerDeUtils;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertTrue;


public class QueryServerTest extends QueryTestSet {
  private static final Random RANDOM_REQUEST_ID_GEN = new Random();
  private static final int QUERY_SERVER_COUNT = 2;
  private static final String KEY_OF_SERVER_INSTANCE_HOST = "pinot.query.runner.server.hostname";
  private static final String KEY_OF_SERVER_INSTANCE_PORT = "pinot.query.runner.server.port";

  private final Map<Integer, QueryServer> _queryServerMap = new HashMap<>();
  private final Map<Integer, QueryRunner> _queryRunnerMap = new HashMap<>();

  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp()
      throws Exception {
    for (int i = 0; i < QUERY_SERVER_COUNT; i++) {
      int availablePort = QueryTestUtils.getAvailablePort();
      QueryRunner queryRunner = mock(QueryRunner.class);
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
  public void testException()
      throws Exception {
    DispatchableSubPlan queryPlan = _queryEnvironment.planQuery("SELECT * FROM a");
    // only get one worker request out.
    Worker.QueryRequest queryRequest = getQueryRequest(queryPlan, 1);
    Map<String, String> requestMetadata = QueryPlanSerDeUtils.fromProtoProperties(queryRequest.getMetadata());
    QueryRunner mockRunner = _queryRunnerMap.get(Integer.parseInt(requestMetadata.get(KEY_OF_SERVER_INSTANCE_PORT)));
    doThrow(new RuntimeException("foo")).when(mockRunner).processQuery(any(), any(), any());
    // submit the request for testing.
    Worker.QueryResponse resp = submitRequest(queryRequest, requestMetadata);
    // reset the mock runner before assert.
    reset(mockRunner);
    // should contain error message pattern
    String errorMessage = resp.getMetadataMap().get(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR);
    assertTrue(errorMessage.contains("foo"));
  }

  @Test(dataProvider = "testSql")
  public void testWorkerAcceptsWorkerRequestCorrect(String sql)
      throws Exception {
    DispatchableSubPlan queryPlan = _queryEnvironment.planQuery(sql);
    List<DispatchablePlanFragment> stagePlans = queryPlan.getQueryStageList();
    int numStages = stagePlans.size();
    // Ignore reduce stage (stage 0)
    for (int stageId = 1; stageId < numStages; stageId++) {
      // only get one worker request out.
      Worker.QueryRequest queryRequest = getQueryRequest(queryPlan, stageId);
      Map<String, String> requestMetadata = QueryPlanSerDeUtils.fromProtoProperties(queryRequest.getMetadata());

      // submit the request for testing.
      Worker.QueryResponse resp = submitRequest(queryRequest, requestMetadata);
      assertTrue(resp.getMetadataMap().containsKey(CommonConstants.Query.Response.ServerResponseStatus.STATUS_OK));

      DispatchablePlanFragment dispatchableStagePlan = stagePlans.get(stageId);
      List<WorkerMetadata> workerMetadataList = dispatchableStagePlan.getWorkerMetadataList();
      StageMetadata stageMetadata =
          new StageMetadata(stageId, workerMetadataList, dispatchableStagePlan.getCustomProperties());

      // ensure mock query runner received correctly deserialized payload.
      QueryRunner mockRunner = _queryRunnerMap.get(Integer.parseInt(requestMetadata.get(KEY_OF_SERVER_INSTANCE_PORT)));
      String requestId = requestMetadata.get(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID);

      // since submitRequest is async, we need to wait for the mockRunner to receive the query payload.
      TestUtils.waitForCondition(aVoid -> {
        try {
          verify(mockRunner, times(workerMetadataList.size())).processQuery(any(), argThat(stagePlan -> {
            PlanNode planNode = dispatchableStagePlan.getPlanFragment().getFragmentRoot();
            return isStageNodesEqual(planNode, stagePlan.getRootNode()) && isStageMetadataEqual(stageMetadata,
                stagePlan.getStageMetadata());
          }), argThat(requestMetadataMap -> requestId.equals(
              requestMetadataMap.get(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID))));
          return true;
        } catch (Throwable t) {
          return false;
        }
      }, 10000L, "Error verifying mock QueryRunner intercepted query payload!");

      // reset the mock runner.
      reset(mockRunner);
    }
  }

  private boolean isStageMetadataEqual(StageMetadata expected, StageMetadata actual) {
    if (!Objects.equals(expected.getTableName(), actual.getTableName())) {
      return false;
    }
    TimeBoundaryInfo expectedTimeBoundaryInfo = expected.getTimeBoundary();
    TimeBoundaryInfo actualTimeBoundaryInfo = actual.getTimeBoundary();
    if (expectedTimeBoundaryInfo != null || actualTimeBoundaryInfo != null) {
      if (expectedTimeBoundaryInfo == null || actualTimeBoundaryInfo == null) {
        return false;
      }
      if (!expectedTimeBoundaryInfo.getTimeColumn().equals(actualTimeBoundaryInfo.getTimeColumn())
          || !expectedTimeBoundaryInfo.getTimeValue().equals(actualTimeBoundaryInfo.getTimeValue())) {
        return false;
      }
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
    return expected.getWorkerId() == actual.getWorkerId() && EqualityUtils.isEqual(expected.getTableSegmentsMap(),
        actual.getTableSegmentsMap());
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

  private Worker.QueryResponse submitRequest(Worker.QueryRequest queryRequest, Map<String, String> requestMetadata) {
    String host = requestMetadata.get(KEY_OF_SERVER_INSTANCE_HOST);
    int port = Integer.parseInt(requestMetadata.get(KEY_OF_SERVER_INSTANCE_PORT));
    long timeoutMs = Long.parseLong(requestMetadata.get(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS));
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    PinotQueryWorkerGrpc.PinotQueryWorkerBlockingStub stub = PinotQueryWorkerGrpc.newBlockingStub(channel);
    Worker.QueryResponse resp =
        stub.withDeadline(Deadline.after(timeoutMs, TimeUnit.MILLISECONDS)).submit(queryRequest);
    channel.shutdown();
    return resp;
  }

  private Worker.QueryRequest getQueryRequest(DispatchableSubPlan queryPlan, int stageId) {
    DispatchablePlanFragment stagePlan = queryPlan.getQueryStageList().get(stageId);
    Plan.StageNode rootNode =
        StageNodeSerDeUtils.serializeStageNode((AbstractPlanNode) stagePlan.getPlanFragment().getFragmentRoot());
    List<Worker.WorkerMetadata> workerMetadataList =
        QueryPlanSerDeUtils.toProtoWorkerMetadataList(stagePlan.getWorkerMetadataList());
    ByteString customProperty = QueryPlanSerDeUtils.toProtoProperties(stagePlan.getCustomProperties());

    // this particular test set requires the request to have a single QueryServerInstance to dispatch to
    // as it is not testing the multi-tenancy dispatch (which is in the QueryDispatcherTest)
    QueryServerInstance serverInstance = stagePlan.getServerInstanceToWorkerIdMap().keySet().iterator().next();
    Worker.StageMetadata stageMetadata =
        Worker.StageMetadata.newBuilder().setStageId(stageId).addAllWorkerMetadata(workerMetadataList)
            .setCustomProperty(customProperty).build();
    Worker.StagePlan protoStagePlan =
        Worker.StagePlan.newBuilder().setRootNode(rootNode.toByteString()).setStageMetadata(stageMetadata).build();

    Map<String, String> requestMetadata = new HashMap<>();
    // the default configurations that must exist.
    requestMetadata.put(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID,
        String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));
    requestMetadata.put(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS,
        String.valueOf(CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS));
    // extra configurations we want to test also parsed out correctly.
    requestMetadata.put(KEY_OF_SERVER_INSTANCE_HOST, serverInstance.getHostname());
    requestMetadata.put(KEY_OF_SERVER_INSTANCE_PORT, Integer.toString(serverInstance.getQueryServicePort()));

    return Worker.QueryRequest.newBuilder().addStagePlan(protoStagePlan)
        .setMetadata(QueryPlanSerDeUtils.toProtoProperties(requestMetadata)).build();
  }
}
