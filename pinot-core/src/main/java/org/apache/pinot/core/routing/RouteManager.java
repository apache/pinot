package org.apache.pinot.core.routing;

import java.util.Map;
import org.apache.pinot.core.transport.ServerInstance;


public interface RouteManager {
  Map<String, ServerInstance> getEnabledServerInstanceMap();

  RouteTable getRoutingTable(String tableName);
}
