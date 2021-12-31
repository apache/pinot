package org.apache.pinot.query.runtime;

import io.grpc.ManagedChannelBuilder;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.dispatch.QueryDispatcher;
import org.apache.pinot.query.planner.QueryPlan;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QueryWorkerTest extends QueryRuntimeTestBase {

  @Test
  public void testWorkerAcceptsWorkerRequestCorrect()
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.sqlQuery("SELECT * FROM a JOIN b ON a.c1 = b.c2");

    String singleServerStage = getStage(queryPlan, 1);

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
