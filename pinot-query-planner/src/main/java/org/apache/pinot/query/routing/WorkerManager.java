package org.apache.pinot.query.routing;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.routing.RouteManager;
import org.apache.pinot.core.routing.RouteTable;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Manages stage to worker assignment.
 *
 * Currently it is implemented by wrapping routing manager from Pinot Broker.
 */
public class WorkerManager {

  private final String _hostName;
  private int _port;
  private final RouteManager _routingManager;

  public WorkerManager(String hostName, int port, RouteManager routingManager) {
    _hostName = hostName;
    _port = port;
    _routingManager = routingManager;
  }

  public void assignWorkerToStage(String stageId, StageMetadata stageMetadata) {
    List<String> scannedTables = stageMetadata.getScannedTables();
    if (scannedTables.size() == 1) { // table scan stage, need to attach server as well as segment info.
      RouteTable routingTable = getRoutingTable(scannedTables.get(0));
      Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
      stageMetadata.setServerInstances(new ArrayList<>(serverInstanceToSegmentsMap.keySet()));
      stageMetadata.setServerInstanceToSegmentsMap(new HashMap<>(serverInstanceToSegmentsMap));
    } else if (stageId.equalsIgnoreCase("ROOT")) {
      stageMetadata.setServerInstances(Lists.newArrayList(new WorkerInstance(_hostName, _port)));
    } else {
      stageMetadata.setServerInstances(new ArrayList<>(_routingManager.getEnabledServerInstanceMap().values()));
    }
  }

  private RouteTable getRoutingTable(String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    // TODO: support both offline and realtime, now we hard code offline table.
    String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName);
    return _routingManager.getRoutingTable(tableNameWithType);
  }
}
