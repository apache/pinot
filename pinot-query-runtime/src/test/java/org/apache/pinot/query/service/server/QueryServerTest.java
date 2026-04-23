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
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.apache.pinot.query.routing.QueryPlanSerDeUtils;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.LoggerConstants;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.util.TestUtils;
import org.slf4j.MDC;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
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
    ServerMetrics.deregister();
    ServerMetrics.register(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
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
    ServerMetrics.deregister();
  }

  @AfterMethod
  public void tearDownMethod() {
    MDC.clear();
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
    assertTrue(errorMessage.contains("foo"), "Error message should contain 'foo' but it is: " + errorMessage);
  }

  @Test
  public void testMseQueriesMetricIncrementedOnSuccessfulSubmit()
      throws Exception {
    long mseBefore = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count();
    long grpcBefore = ServerMetrics.get().getMeteredValue(ServerMeter.GRPC_QUERIES).count();
    DispatchableSubPlan queryPlan = _queryEnvironment.planQuery("SELECT * FROM a");
    Worker.QueryRequest queryRequest = getQueryRequest(queryPlan, 1);
    Map<String, String> requestMetadata = QueryPlanSerDeUtils.fromProtoProperties(queryRequest.getMetadata());
    Worker.QueryResponse resp = submitRequest(queryRequest, requestMetadata);
    assertTrue(resp.getMetadataMap().containsKey(CommonConstants.Query.Response.ServerResponseStatus.STATUS_OK));
    TestUtils.waitForCondition(
        aVoid -> ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count() == mseBefore + 1,
        5000L, "MSE_QUERIES was not incremented");
    assertEquals(ServerMetrics.get().getMeteredValue(ServerMeter.GRPC_QUERIES).count(), grpcBefore,
        "GRPC_QUERIES should NOT be incremented by the MSE path");
  }

  @Test
  public void testMseQueriesMetricIncrementedOnFailedSubmit()
      throws Exception {
    long mseBefore = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count();
    DispatchableSubPlan queryPlan = _queryEnvironment.planQuery("SELECT * FROM a");
    Worker.QueryRequest queryRequest = getQueryRequest(queryPlan, 1);
    Map<String, String> requestMetadata = QueryPlanSerDeUtils.fromProtoProperties(queryRequest.getMetadata());
    QueryRunner mockRunner = _queryRunnerMap.get(Integer.parseInt(requestMetadata.get(KEY_OF_SERVER_INSTANCE_PORT)));
    doThrow(new RuntimeException("foo")).when(mockRunner).processQuery(any(), any(), any());

    Worker.QueryResponse resp = submitRequest(queryRequest, requestMetadata);
    reset(mockRunner);

    String errorMessage = resp.getMetadataMap().get(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR);
    assertTrue(errorMessage.contains("foo"), "Error message should contain 'foo' but it is: " + errorMessage);
    TestUtils.waitForCondition(
        aVoid -> ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count() == mseBefore + 1,
        5000L, "MSE_QUERIES was not incremented");
  }

  @Test
  public void testMseQueriesMetricIncrementedBeforeMetadataDeserialization() {
    long mseBefore = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count();
    QueryServer queryServer = _queryServerMap.values().iterator().next();
    Worker.QueryResponse[] responseHolder = new Worker.QueryResponse[1];

    queryServer.submit(Worker.QueryRequest.newBuilder().setMetadata(ByteString.copyFromUtf8("invalid")).build(),
        new StreamObserver<>() {
          @Override
          public void onNext(Worker.QueryResponse value) {
            responseHolder[0] = value;
          }

          @Override
          public void onError(Throwable t) {
          }

          @Override
          public void onCompleted() {
          }
        });

    assertEquals(ServerMetrics.get().getMeteredValue(ServerMeter.MSE_QUERIES).count(), mseBefore + 1,
        "MSE_QUERIES should be incremented before metadata deserialization");
    assertTrue(responseHolder[0].getMetadataMap()
            .containsKey(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR),
        "Malformed metadata should still return an error response");
  }

  @Test(dataProvider = "testSql")
  public void testWorkerAcceptsWorkerRequestCorrect(String sql)
      throws Exception {
    DispatchableSubPlan queryPlan = _queryEnvironment.planQuery(sql);
    Set<DispatchablePlanFragment> stagePlans = queryPlan.getQueryStagesWithoutRoot();
    // Ignore reduce stage (stage 0)
    for (DispatchablePlanFragment stagePlan : stagePlans) {
      int stageId = stagePlan.getPlanFragment().getFragmentId();
      // only get one worker request out.
      Worker.QueryRequest queryRequest = getQueryRequest(queryPlan, stageId);
      Map<String, String> requestMetadata = QueryPlanSerDeUtils.fromProtoProperties(queryRequest.getMetadata());

      // submit the request for testing.
      Worker.QueryResponse resp = submitRequest(queryRequest, requestMetadata);
      assertTrue(resp.getMetadataMap().containsKey(CommonConstants.Query.Response.ServerResponseStatus.STATUS_OK));

      List<WorkerMetadata> workerMetadataList = stagePlan.getWorkerMetadataList();
      StageMetadata stageMetadata = new StageMetadata(stageId, workerMetadataList, stagePlan.getCustomProperties());

      // ensure mock query runner received correctly deserialized payload.
      QueryRunner mockRunner = _queryRunnerMap.get(Integer.parseInt(requestMetadata.get(KEY_OF_SERVER_INSTANCE_PORT)));
      String requestId = requestMetadata.get(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID);

      // since submitRequest is async, we need to wait for the mockRunner to receive the query payload.
      TestUtils.waitForCondition(aVoid -> {
        try {
          verify(mockRunner, times(workerMetadataList.size())).processQuery(any(), argThat(stagePlanArg -> {
            PlanNode planNode = stagePlan.getPlanFragment().getFragmentRoot();
            return planNode.equals(stagePlanArg.getRootNode()) && isStageMetadataEqual(stageMetadata,
                stagePlanArg.getStageMetadata());
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
    DispatchablePlanFragment stagePlan = queryPlan.getQueryStageMap().get(stageId);
    Plan.PlanNode rootNode = PlanNodeSerializer.process(stagePlan.getPlanFragment().getFragmentRoot());
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

  @Test
  public void testMdcRegistersUpstreamDownstreamStageIds() {
    QueryExecutionContext executionContext = QueryExecutionContext.forMseTest();
    QueryThreadContext.MseWorkerInfo workerInfo =
        new QueryThreadContext.MseWorkerInfo(1, 0, Set.of(2, 3), Set.of(0));

    try (QueryThreadContext ignored = QueryThreadContext.open(executionContext, workerInfo,
        ThreadAccountantUtils.getNoOpAccountant())) {
      assertEquals(MDC.get(LoggerConstants.UPSTREAM_STAGE_IDS_KEY.getKey()), "2,3");
      assertEquals(MDC.get(LoggerConstants.DOWNSTREAM_STAGE_IDS_KEY.getKey()), "0");
      assertEquals(MDC.get(LoggerConstants.STAGE_ID_KEY.getKey()), "1");
      assertEquals(MDC.get(LoggerConstants.WORKER_ID_KEY.getKey()), "0");
    }

    assertNull(MDC.get(LoggerConstants.UPSTREAM_STAGE_IDS_KEY.getKey()));
    assertNull(MDC.get(LoggerConstants.DOWNSTREAM_STAGE_IDS_KEY.getKey()));
    assertNull(MDC.get(LoggerConstants.STAGE_ID_KEY.getKey()));
    assertNull(MDC.get(LoggerConstants.WORKER_ID_KEY.getKey()));
  }

  @Test
  public void testMdcSkipsEmptyStageIds() {
    QueryExecutionContext executionContext = QueryExecutionContext.forMseTest();
    QueryThreadContext.MseWorkerInfo workerInfo =
        new QueryThreadContext.MseWorkerInfo(2, 0);

    try (QueryThreadContext ignored = QueryThreadContext.open(executionContext, workerInfo,
        ThreadAccountantUtils.getNoOpAccountant())) {
      assertNull(MDC.get(LoggerConstants.UPSTREAM_STAGE_IDS_KEY.getKey()));
      assertNull(MDC.get(LoggerConstants.DOWNSTREAM_STAGE_IDS_KEY.getKey()));
    }
  }

  private static final DataSchema DUMMY_SCHEMA =
      new DataSchema(new String[]{"col"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

  private static MailboxReceiveNode receiveNode(int stageId, int senderStageId) {
    return new MailboxReceiveNode(stageId, DUMMY_SCHEMA, senderStageId,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        null, null, false, false, null);
  }

  private static MailboxSendNode sendNode(int stageId, List<PlanNode> inputs, int receiverStageId) {
    return new MailboxSendNode(stageId, DUMMY_SCHEMA, inputs, receiverStageId,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        null, false, null, false, "murmur");
  }

  private static MailboxSendNode sendNode(int stageId, List<PlanNode> inputs, List<Integer> receiverStageIds) {
    return new MailboxSendNode(stageId, DUMMY_SCHEMA, inputs, receiverStageIds,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        null, false, null, false, "murmur");
  }

  @Test
  public void testCollectUpstreamStageIdsSingleReceive() {
    // SendNode(stage=1) → ReceiveNode(sender=2)
    MailboxReceiveNode receive = receiveNode(1, 2);
    MailboxSendNode root = sendNode(1, List.of(receive), 0);
    assertEquals(QueryServer.collectUpstreamStageIds(root), Set.of(2));
  }

  @Test
  public void testCollectUpstreamStageIdsMultipleReceives() {
    // SendNode(stage=1) → [ReceiveNode(sender=2), ReceiveNode(sender=3)]
    MailboxReceiveNode receive1 = receiveNode(1, 2);
    MailboxReceiveNode receive2 = receiveNode(1, 3);
    MailboxSendNode root = sendNode(1, List.of(receive1, receive2), 0);
    assertEquals(QueryServer.collectUpstreamStageIds(root), Set.of(2, 3));
  }

  @Test
  public void testCollectUpstreamStageIdsNestedReceive() {
    // SendNode(stage=1) → SendNode(stage=1, inner) → ReceiveNode(sender=3)
    MailboxReceiveNode receive = receiveNode(1, 3);
    MailboxSendNode inner = sendNode(1, List.of(receive), 0);
    MailboxSendNode root = sendNode(1, List.of(inner), 0);
    assertEquals(QueryServer.collectUpstreamStageIds(root), Set.of(3));
  }

  @Test
  public void testCollectUpstreamStageIdsNoReceives() {
    MailboxSendNode root = sendNode(1, List.of(), 0);
    assertEquals(QueryServer.collectUpstreamStageIds(root), Set.of());
  }

  @Test
  public void testCollectUpstreamStageIdsReceiveAtRoot() {
    MailboxReceiveNode root = receiveNode(1, 5);
    assertEquals(QueryServer.collectUpstreamStageIds(root), Set.of(5));
  }

  @Test
  public void testCollectDownstreamStageIdsSingleReceiver() {
    MailboxSendNode root = sendNode(1, List.of(), 0);
    assertEquals(QueryServer.collectDownstreamStageIds(root), Set.of(0));
  }

  @Test
  public void testCollectDownstreamStageIdsMultipleReceivers() {
    MailboxSendNode root = sendNode(1, List.of(), List.of(0, 4));
    assertEquals(QueryServer.collectDownstreamStageIds(root), Set.of(0, 4));
  }

  @Test
  public void testCollectDownstreamStageIdsNonSendNode() {
    MailboxReceiveNode root = receiveNode(1, 2);
    assertEquals(QueryServer.collectDownstreamStageIds(root), Set.of());
  }
}
