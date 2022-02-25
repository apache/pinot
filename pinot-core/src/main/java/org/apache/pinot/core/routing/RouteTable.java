package org.apache.pinot.core.routing;

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.transport.ServerInstance;


public interface RouteTable {

  Map<ServerInstance, List<String>> getServerInstanceToSegmentsMap();
}
