package org.apache.pinot.query.context;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.routing.QueryServerInstance;


public class PhysicalPlannerContext {
  private final Supplier<Integer> _nodeIdGenerator = new Supplier<>() {
    private int _id = 0;

    @Override
    public Integer get() {
      return _id++;
    }
  };
  /**
   * This is hacky. We should have a centralized place to get this. This map currently is populated during table scan
   * worker assignment
   */
  private final Map<String, QueryServerInstance> _instanceIdToQueryServerInstance = new HashMap<>();
  @Nullable
  private final RoutingManager _routingManager;
  private final String _hostName;
  private final int _port;
  private final long _requestId;

  /**
   * Used by controller when it needs to extract table names from the query.
   * TODO: Controller should only rely on SQL parser to extract table names.
   */
  public PhysicalPlannerContext() {
    _routingManager = null;
    _hostName = "";
    _port = 0;
    _requestId = 0;
  }

  public PhysicalPlannerContext(RoutingManager routingManager, String hostName, int port, long requestId) {
    _routingManager = routingManager;
    _hostName = hostName;
    _port = port;
    _requestId = requestId;
  }

  public Supplier<Integer> getNodeIdGenerator() {
    return _nodeIdGenerator;
  }

  public Map<String, QueryServerInstance> getInstanceIdToQueryServerInstance() {
    return _instanceIdToQueryServerInstance;
  }

  @Nullable
  public RoutingManager getRoutingManager() {
    return _routingManager;
  }

  public String getHostName() {
    return _hostName;
  }

  public int getPort() {
    return _port;
  }

  public long getRequestId() {
    return _requestId;
  }
}
