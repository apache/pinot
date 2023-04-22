package org.apache.pinot.query.routing;

import java.util.List;
import java.util.Map;


public class WorkerMetadata {
  private VirtualServerAddress _virtualServerAddress;

  // used for table scan stage - we use ServerInstance instead of VirtualServer
  // here because all virtual servers that share a server instance will have the
  // same segments on them
  private List<Map<String, List<String>>> _segmentsList;
}
