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
import io.grpc.ManagedChannelBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestUtils;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.routing.WorkerInstance;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class QueryServerTest {
  private static final int QUERY_SERVER_COUNT = 2;
  private Map<Integer, QueryServer> _queryServerMap = new HashMap<>();
  private Map<Integer, ServerInstance> _queryServerInstanceMap = new HashMap<>();

  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp()
      throws Exception {

    for (int i = 0; i < QUERY_SERVER_COUNT; i++) {
      int availablePort = QueryEnvironmentTestUtils.getAvailablePort();
      QueryServer queryServer = new QueryServer(availablePort, Mockito.mock(QueryRunner.class));
      queryServer.start();
      _queryServerMap.put(availablePort, queryServer);
      // this only test the QueryServer functionality so the server port can be the same as the mailbox port.
      // this is only use for test identifier purpose.
      _queryServerInstanceMap.put(availablePort, new WorkerInstance("localhost", availablePort, availablePort));
    }

    List<Integer> portList = Lists.newArrayList(_queryServerMap.keySet());

    // reducer port doesn't matter, we are testing the worker instance not GRPC.
    _queryEnvironment = QueryEnvironmentTestUtils.getQueryEnvironment(1, portList.get(0), portList.get(1));
  }

  @AfterClass
  public void tearDown() {
    for (QueryServer worker : _queryServerMap.values()) {
      worker.shutdown();
    }
  }

  @Test
  public void testWorkerAcceptsWorkerRequestCorrect()
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.planQuery("SELECT * FROM a JOIN b ON a.col1 = b.col2");

    String singleServerStage = QueryEnvironmentTestUtils.getTestStageByServerCount(queryPlan, 1);

    Worker.QueryRequest queryRequest = getQueryRequest(queryPlan, singleServerStage);

    // submit the request for testing.
    submitRequest(queryRequest);
  }

  private void submitRequest(Worker.QueryRequest queryRequest) {
    String host = queryRequest.getMetadataMap().get("SERVER_INSTANCE_HOST");
    int port = Integer.parseInt(queryRequest.getMetadataMap().get("SERVER_INSTANCE_PORT"));
    PinotQueryWorkerGrpc.PinotQueryWorkerBlockingStub stub =
        PinotQueryWorkerGrpc.newBlockingStub(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
    Worker.QueryResponse resp = stub.submit(queryRequest);
    // TODO: validate meaningful return value
    Assert.assertNotNull(resp.getMetadataMap().get("OK"));
  }

  private Worker.QueryRequest getQueryRequest(QueryPlan queryPlan, String stageId) {
    ServerInstance serverInstance = queryPlan.getStageMetadataMap().get(stageId).getServerInstances().get(0);

    return Worker.QueryRequest.newBuilder().setQueryPlan(QueryPlanSerDeUtils.serialize(
            QueryDispatcher.constructDistributedQueryPlan(queryPlan, stageId, serverInstance)))
        .putMetadata("SERVER_INSTANCE_HOST", serverInstance.getHostname())
        .putMetadata("SERVER_INSTANCE_PORT", String.valueOf(serverInstance.getPort())).build();
  }
}
