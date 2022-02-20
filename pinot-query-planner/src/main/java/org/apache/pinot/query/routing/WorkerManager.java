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
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;


/**
 * Manages stage to worker assignment.
 *
 * Currently it is implemented by wrapping routing manager from Pinot Broker.
 */
public class WorkerManager {
  private static final CalciteSqlCompiler CALCITE_SQL_COMPILER = new CalciteSqlCompiler();

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
      RoutingTable routingTable = getRoutingTable(scannedTables.get(0));
      Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
      stageMetadata.setServerInstances(new ArrayList<>(serverInstanceToSegmentsMap.keySet()));
      stageMetadata.setServerInstanceToSegmentsMap(new HashMap<>(serverInstanceToSegmentsMap));
    } else if (stageId.equalsIgnoreCase("ROOT")) {
      stageMetadata.setServerInstances(Lists.newArrayList(new WorkerInstance(_hostName, _port)));
    } else {
      stageMetadata.setServerInstances(new ArrayList<>(_routingManager.getEnabledServerInstanceMap().values()));
    }
  }

  private RoutingTable getRoutingTable(String tableName) {
    return _routingManager.getRoutingTable(
        CALCITE_SQL_COMPILER.compileToBrokerRequest(String.format("SELECT * FROM %s", tableName)));
  }
}
