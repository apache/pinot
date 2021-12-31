package org.apache.pinot.query.dispatch;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannelBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dispatch a query to different workers.
 */
public class QueryDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryDispatcher.class);

  private final Map<String, DispatchClient> _dispatchClientMap = new ConcurrentHashMap<>();
  private final GrpcQueryClient.Config _config;

  public QueryDispatcher(GrpcQueryClient.Config config) {
    _config = config;
  }

  public void submit(QueryPlan queryPlan) throws Exception {
    for (Map.Entry<String, StageMetadata> stage : queryPlan.getStageMetadataMap().entrySet()) {
      String stageId = stage.getKey();
      List<ServerInstance> serverInstances = stage.getValue().getServerInstances();
      for (ServerInstance serverInstance : serverInstances) {
        String host = serverInstance.getHostname();
        int port = serverInstance.getGrpcPort();
        DispatchClient client = getOrCreateDispatchClient(host, port);
        Worker.QueryResponse response = client.submit(Worker.QueryRequest.newBuilder()
            .setSerializedQueryPlan(constructSerializedStageQueryRequest(queryPlan, stageId, serverInstance)).build());
        if (response.containsMetadata("ERROR")) {
          throw new RuntimeException(String.format("Unable to execute query plan at stage %s on server %s: ERROR: %s",
              stageId, serverInstance, response));
        }
      }
    }
  }

  // construct a stage plan based on queryPlan and the particular stage, and the instance of the server;
  public static ByteString constructSerializedStageQueryRequest(QueryPlan queryPlan, String stageId,
      ServerInstance serverInstance) {
    WorkerQueryRequest workerQueryRequest = constructStageQueryRequest(queryPlan, stageId, serverInstance);
    try (ByteArrayOutputStream bs = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(bs)) {
      os.writeObject(workerQueryRequest);
      return ByteString.copyFrom(bs.toByteArray());
    } catch (IOException e) {
      LOGGER.error("Error when creating stage plan!");
      throw new RuntimeException(e);
    }
  }

  public static WorkerQueryRequest constructStageQueryRequest(QueryPlan queryPlan, String stageId,
      ServerInstance serverInstance) {
    return new WorkerQueryRequest(stageId, serverInstance, queryPlan.getQueryStageMap().get(stageId),
            queryPlan.getStageMetadataMap());
  }

  private DispatchClient getOrCreateDispatchClient(String host, int port) {
    String key = String.format("%s_%d", host, port);
    return _dispatchClientMap.computeIfAbsent(key, k -> new DispatchClient(host, port, _config));
  }

  public static class DispatchClient {
    private final PinotQueryWorkerGrpc.PinotQueryWorkerBlockingStub _blockingStub;
    public DispatchClient(String host, int port, GrpcQueryClient.Config config) {
      ManagedChannelBuilder managedChannelBuilder = ManagedChannelBuilder
          .forAddress(host, port)
          .usePlaintext();
      _blockingStub = PinotQueryWorkerGrpc.newBlockingStub(managedChannelBuilder.build());
    }
    public Worker.QueryResponse submit(Worker.QueryRequest request) {
      return _blockingStub.submit(request);
    }
  }
}
