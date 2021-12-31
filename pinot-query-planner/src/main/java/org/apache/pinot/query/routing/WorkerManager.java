package org.apache.pinot.query.routing;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.StageMetadata;


/**
 * Manages stage to worker assignment.
 *
 * Currently it is implemented by wrapping routing manager from Pinot Broker.
 */
public class WorkerManager {
  private final String _hostName;
  private int _port;
  private final RoutingManager _routingManager;

  public WorkerManager(String hostName, int port, RoutingManager routingManager) {
    _hostName = hostName;
    _port = port;
    _routingManager = routingManager;
  }

  public void assignWorkerToStage(String stageId, StageMetadata stageMetadata) {
    List<String> scannedTables = stageMetadata.getScannedTables();
    if (scannedTables.size() == 1) { // table scan stage, need to attach server as well as segment info.
      RoutingTable routingTable = _routingManager.getRoutingTable(scannedTables.get(0));
      Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
      stageMetadata.setServerInstances(new ArrayList<>(serverInstanceToSegmentsMap.keySet()));
      stageMetadata.setServerInstanceToSegmentsMap(new HashMap<>(serverInstanceToSegmentsMap));
    } else if (stageId.equalsIgnoreCase("ROOT")) {
      stageMetadata.setServerInstances(Lists.newArrayList(new ReducerInstance(_hostName, _port)));
    } else {
      stageMetadata.setServerInstances(new ArrayList<>(_routingManager.getEnabledServerInstanceMap().values()));
    }
  }

  /**
   * Special instance that accepts final stage result.
   */
  public static class ReducerInstance extends ServerInstance {
    private final int _grpcPort;

    public ReducerInstance(String hostName, int port) {
      super(hostName, port);
      _grpcPort = port;
    }

    @Override
    public int getGrpcPort() {
      return _grpcPort;
    }
  }
}
