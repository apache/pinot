package org.apache.pinot.query.runtime;

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
import org.apache.pinot.query.dispatch.QueryDispatcher;
import org.apache.pinot.query.planner.QueryPlan;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class QueryWorkerTest {
  private int QUERY_WORKER_COUNT = 2;
  private Map<Integer, QueryWorker> _queryWorkerMap = new HashMap<>();
  private Map<Integer, ServerInstance> _queryWorkerInstanceMap = new HashMap<>();

  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp()
      throws Exception {

    for (int i = 0; i < QUERY_WORKER_COUNT; i++) {
      int availablePort = QueryEnvironmentTestUtils.getAvailablePort();
      QueryWorker workerService = new QueryWorker(availablePort, Mockito.mock(QueryRunner.class));
      workerService.start();
      _queryWorkerMap.put(availablePort, workerService);
      _queryWorkerInstanceMap.put(availablePort, QueryEnvironmentTestUtils.getServerInstance(availablePort));
    }

    List<Integer> portList = Lists.newArrayList(_queryWorkerMap.keySet());

    // reducer port doesn't matter, we are testing the worker instance not GRPC.
    _queryEnvironment = QueryEnvironmentTestUtils.getQueryEnvironment(1, portList.get(0), portList.get(1));
  }

  @AfterClass
  public void tearDown() {
    for (QueryWorker worker : _queryWorkerMap.values()) {
      worker.shutdown();
    }
  }

  @Test
  public void testWorkerAcceptsWorkerRequestCorrect()
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.sqlQuery("SELECT * FROM a JOIN b ON a.c1 = b.c2");

    String singleServerStage = QueryEnvironmentTestUtils.getTestStageByServerCount(queryPlan, 1);

    Worker.QueryRequest queryRequest = getQueryRequest(queryPlan, singleServerStage);

    // submit the request for testing.
    submitRequest(queryRequest);
  }

  private void submitRequest(Worker.QueryRequest queryRequest) {
    String host = queryRequest.getMetadataMap().get("SERVER_INSTANCE_HOST");
    int port = Integer.parseInt(queryRequest.getMetadataMap().get("SERVER_INSTANCE_PORT"));
    PinotQueryWorkerGrpc.PinotQueryWorkerBlockingStub stub = PinotQueryWorkerGrpc.newBlockingStub(
        ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
    Worker.QueryResponse resp = stub.submit(queryRequest);
    // TODO: validate meaningful return value
    Assert.assertNotNull(resp.getMetadataMap().get("OK"));
  }

  private Worker.QueryRequest getQueryRequest(QueryPlan queryPlan, String stageId) {
    ServerInstance serverInstance = queryPlan.getStageMetadataMap().get(stageId).getServerInstances().get(0);

    return Worker.QueryRequest.newBuilder()
        .setSerializedQueryPlan(QueryDispatcher.constructSerializedStageQueryRequest(queryPlan, stageId, serverInstance))
        .putMetadata("SERVER_INSTANCE_HOST", serverInstance.getHostname())
        .putMetadata("SERVER_INSTANCE_PORT", String.valueOf(serverInstance.getGrpcPort()))
        .build();
  }
}
