package org.apache.pinot.query.planner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.nodes.StageNode;
import org.apache.pinot.query.planner.nodes.TableScanNode;


public class StageMetadata implements Serializable {
  private List<String> _scannedTables;

  // used for assigning server/worker nodes.
  private List<ServerInstance> _serverInstances;

  // used for table scan stage.
  private Map<ServerInstance, List<String>> _serverInstanceToSegmentsMap;

  public StageMetadata() {
    _scannedTables = new ArrayList<>();
    _serverInstanceToSegmentsMap = new HashMap<>();
  }

  public void attach(StageNode stageNode) {
    if (stageNode instanceof TableScanNode) {
      _scannedTables.add(((TableScanNode) stageNode).getTableName().get(0));
    }
  }

  public List<String> getScannedTables() {
    return _scannedTables;
  }

  // -----------------------------------------------
  // attached physical plan context.
  // -----------------------------------------------

  public Map<ServerInstance, List<String>> getServerInstanceToSegmentsMap() {
    return _serverInstanceToSegmentsMap;
  }

  public void setServerInstanceToSegmentsMap(Map<ServerInstance, List<String>> serverInstanceToSegmentsMap) {
    this._serverInstanceToSegmentsMap = serverInstanceToSegmentsMap;
  }

  public List<ServerInstance> getServerInstances() {
    return _serverInstances;
  }

  public void setServerInstances(List<ServerInstance> serverInstances) {
    _serverInstances = serverInstances;
  }
}
