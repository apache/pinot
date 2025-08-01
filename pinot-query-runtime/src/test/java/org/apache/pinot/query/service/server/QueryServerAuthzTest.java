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
import io.grpc.Attributes;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.access.QueryAccessControl;
import org.apache.pinot.query.access.QueryAccessControlFactory;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.apache.pinot.query.routing.QueryPlanSerDeUtils;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertSame;


public class QueryServerAuthzTest {
  private static final String AUTH_HEADER = "authorization";
  private static final String SECRET = "my-shared-secret";
  private static final Metadata.Key<String> AUTH_METADATA_KEY = Metadata.Key.of(AUTH_HEADER, ASCII_STRING_MARSHALLER);
  private static class DenyAllAccessControlFactory extends QueryAccessControlFactory {
    public static final QueryAccessControl DENY_ALL_ACCESS = new QueryAccessControl() {
      @Override
      public boolean hasAccess(Attributes attributes, Metadata metadata) {
        String authorization = metadata.get(AUTH_METADATA_KEY);
        return SECRET.equals(authorization);
      }
    };

    public QueryAccessControl create() {
      return DENY_ALL_ACCESS;
    }
  }
  private static final Random RANDOM_REQUEST_ID_GEN = new Random();
  private static final int QUERY_SERVER_COUNT = 2;
  private static final String KEY_OF_SERVER_INSTANCE_HOST = "pinot.query.runner.server.hostname";
  private static final String KEY_OF_SERVER_INSTANCE_PORT = "pinot.query.runner.server.port";

  private final Map<Integer, QueryServer> _queryServerMap = new HashMap<>();

  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp()
      throws Exception {
    for (int i = 0; i < QUERY_SERVER_COUNT; i++) {
      int availablePort = QueryTestUtils.getAvailablePort();
      QueryRunner queryRunner = mock(QueryRunner.class);
      QueryServer queryServer = new QueryServer(availablePort, queryRunner, null, new DenyAllAccessControlFactory());
      queryServer.start();
      _queryServerMap.put(availablePort, queryServer);
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
  public void testAccessDeniedError()
      throws Exception {
    DispatchableSubPlan queryPlan = _queryEnvironment.planQuery("SELECT * FROM a");
    Worker.QueryRequest queryRequest = getQueryRequest(queryPlan, 1);
    Map<String, String> requestMetadata = QueryPlanSerDeUtils.fromProtoProperties(queryRequest.getMetadata());
    try {
      submitRequest(queryRequest, requestMetadata, "the-wrong-password");
    } catch (StatusRuntimeException ex) {
      assertSame(ex.getStatus().getCode(), Status.PERMISSION_DENIED.getCode(),
          "Expected permission denied from DenyAllQueryAccessControl");
      return;
    }
    throw new RuntimeException("Expected DenyAllQueryAccessControl to raise a StatusRuntimeException");
  }

  @Test
  public void testAccessAllowed()
      throws Exception {
    DispatchableSubPlan queryPlan = _queryEnvironment.planQuery("SELECT * FROM a");
    Worker.QueryRequest queryRequest = getQueryRequest(queryPlan, 1);
    Map<String, String> requestMetadata = QueryPlanSerDeUtils.fromProtoProperties(queryRequest.getMetadata());
    submitRequest(queryRequest, requestMetadata, SECRET);
  }

  private Worker.QueryResponse submitRequest(Worker.QueryRequest queryRequest, Map<String, String> requestMetadata,
      String secretKey) {
    String host = requestMetadata.get(KEY_OF_SERVER_INSTANCE_HOST);
    int port = Integer.parseInt(requestMetadata.get(KEY_OF_SERVER_INSTANCE_PORT));
    long timeoutMs = Long.parseLong(requestMetadata.get(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS));
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    Metadata headers = new Metadata();
    headers.put(AUTH_METADATA_KEY, secretKey);
    PinotQueryWorkerGrpc.PinotQueryWorkerBlockingStub stub = PinotQueryWorkerGrpc
        .newBlockingStub(channel)
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
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
}
