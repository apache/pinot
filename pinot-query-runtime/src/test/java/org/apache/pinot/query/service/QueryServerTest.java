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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.routing.WorkerInstance;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;


public class QueryServerTest extends QueryTestSet {
  private static final Random RANDOM_REQUEST_ID_GEN = new Random();
  private static final int QUERY_SERVER_COUNT = 2;
  private static final String KEY_OF_SERVER_INSTANCE_HOST = "pinot.query.runner.server.hostname";
  private static final String KEY_OF_SERVER_INSTANCE_PORT = "pinot.query.runner.server.port";

  private final Map<Integer, QueryServer> _queryServerMap = new HashMap<>();
  private final Map<Integer, ServerInstance> _queryServerInstanceMap = new HashMap<>();
  private final Map<Integer, QueryRunner> _queryRunnerMap = new HashMap<>();

  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp()
      throws Exception {

    for (int i = 0; i < QUERY_SERVER_COUNT; i++) {
      int availablePort = QueryTestUtils.getAvailablePort();
      QueryRunner queryRunner = Mockito.mock(QueryRunner.class);
      QueryServer queryServer = new QueryServer(availablePort, queryRunner);
      queryServer.start();
      _queryServerMap.put(availablePort, queryServer);
      _queryRunnerMap.put(availablePort, queryRunner);
      // this only test the QueryServer functionality so the server port can be the same as the mailbox port.
      // this is only use for test identifier purpose.
      _queryServerInstanceMap.put(availablePort, new WorkerInstance("localhost", availablePort, availablePort,
          availablePort, availablePort));
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

  @SuppressWarnings("unchecked")
  @Test(dataProvider = "testSql")
  public void testWorkerAcceptsWorkerRequestCorrect(String sql)
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);

    for (int stageId : queryPlan.getStageMetadataMap().keySet()) {
      if (stageId > 0) { // we do not test reduce stage.
        // only get one worker request out.
        Worker.QueryRequest queryRequest = getQueryRequest(queryPlan, stageId);

        // submit the request for testing.
        submitRequest(queryRequest);

        StageMetadata stageMetadata = queryPlan.getStageMetadataMap().get(stageId);

        // ensure mock query runner received correctly deserialized payload.
        QueryRunner mockRunner = _queryRunnerMap.get(
            Integer.parseInt(queryRequest.getMetadataOrThrow(KEY_OF_SERVER_INSTANCE_PORT)));
        String requestIdStr = queryRequest.getMetadataOrThrow(QueryConfig.KEY_OF_BROKER_REQUEST_ID);

        // since submitRequest is async, we need to wait for the mockRunner to receive the query payload.
        TestUtils.waitForCondition(aVoid -> {
          try {
            Mockito.verify(mockRunner).processQuery(Mockito.argThat(distributedStagePlan -> {
              StageNode stageNode = queryPlan.getQueryStageMap().get(stageId);
              return isStageNodesEqual(stageNode, distributedStagePlan.getStageRoot())
                  && isMetadataMapsEqual(stageMetadata, distributedStagePlan.getMetadataMap().get(stageId));
            }), any(OpChainSchedulerService.class), Mockito.argThat(requestMetadataMap ->
                requestIdStr.equals(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_ID))));
            return true;
          } catch (Throwable t) {
            return false;
          }
        }, 10000L, "Error verifying mock QueryRunner intercepted query payload!");
      }
    }
  }

  private static boolean isMetadataMapsEqual(StageMetadata left, StageMetadata right) {
    return left.getServerInstances().equals(right.getServerInstances())
        && left.getServerInstanceToSegmentsMap().equals(right.getServerInstanceToSegmentsMap())
        && left.getScannedTables().equals(right.getScannedTables());
  }

  private static boolean isStageNodesEqual(StageNode left, StageNode right) {
    // This only checks the stage tree structure is correct. because the input/stageId fields are not
    // part of the generic proto ser/de; which is tested in query planner.
    if (left.getStageId() != right.getStageId() || left.getClass() != right.getClass()
        || left.getInputs().size() != right.getInputs().size()) {
      return false;
    }
    left.getInputs().sort(Comparator.comparingInt(StageNode::getStageId));
    right.getInputs().sort(Comparator.comparingInt(StageNode::getStageId));
    for (int i = 0; i < left.getInputs().size(); i++) {
      if (!isStageNodesEqual(left.getInputs().get(i), right.getInputs().get(i))) {
        return false;
      }
    }
    return true;
  }

  private void submitRequest(Worker.QueryRequest queryRequest) {
    String host = queryRequest.getMetadataMap().get(KEY_OF_SERVER_INSTANCE_HOST);
    int port = Integer.parseInt(queryRequest.getMetadataMap().get(KEY_OF_SERVER_INSTANCE_PORT));
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    PinotQueryWorkerGrpc.PinotQueryWorkerBlockingStub stub = PinotQueryWorkerGrpc.newBlockingStub(channel);
    Worker.QueryResponse resp = stub.submit(queryRequest);
    // TODO: validate meaningful return value
    Assert.assertNotNull(resp.getMetadataMap().get(QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_OK));
    channel.shutdown();
  }

  private Worker.QueryRequest getQueryRequest(QueryPlan queryPlan, int stageId) {
    ServerInstance serverInstance = queryPlan.getStageMetadataMap().get(stageId).getServerInstances().get(0);

    return Worker.QueryRequest.newBuilder().setStagePlan(QueryPlanSerDeUtils.serialize(
            QueryDispatcher.constructDistributedStagePlan(queryPlan, stageId, serverInstance)))
        // the default configurations that must exist.
        .putMetadata(QueryConfig.KEY_OF_BROKER_REQUEST_ID, String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()))
        .putMetadata(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS,
            String.valueOf(CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS))
        // extra configurations we want to test also parsed out correctly.
        .putMetadata(KEY_OF_SERVER_INSTANCE_HOST, serverInstance.getHostname())
        .putMetadata(KEY_OF_SERVER_INSTANCE_PORT, String.valueOf(serverInstance.getPort())).build();
  }
}
