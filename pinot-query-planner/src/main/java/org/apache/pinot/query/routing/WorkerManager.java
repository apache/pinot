package org.apache.pinot.query.routing;

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
  private final RoutingManager _routingManager;

  public WorkerManager(RoutingManager routingManager) {
    _routingManager = routingManager;
  }

  public void assignWorkerToStage(StageMetadata stageMetadata) {
    List<String> scannedTables = stageMetadata.getScannedTables();
    if (scannedTables.size() == 1) { // table scan stage, need to attach server as well as segment info.
      RoutingTable routingTable = _routingManager.getRoutingTable(scannedTables.get(0));
      Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
      stageMetadata.setServerInstances(new ArrayList<>(serverInstanceToSegmentsMap.keySet()));
      stageMetadata.setServerInstanceToSegmentsMap(new HashMap<>(serverInstanceToSegmentsMap));
    } else {
      stageMetadata.setServerInstances(new ArrayList<>(_routingManager.getEnabledServerInstanceMap().values()));
    }
  }
}
