package com.linkedin.pinot.routing;

import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import com.linkedin.pinot.transport.config.ResourceRoutingConfig;
import com.linkedin.pinot.transport.config.RoutingTableConfig;


public class CfgBasedRouting implements RoutingTable {

  private RoutingTableConfig _cfg;

  public CfgBasedRouting() {
  }

  public void init(RoutingTableConfig cfg) {
    _cfg = cfg;
  }

  @Override
  public Map<SegmentIdSet, List<ServerInstance>> findServers(RoutingTableLookupRequest request) {

    ResourceRoutingConfig cfg = _cfg.getResourceRoutingCfg().get(request.getResourceName());

    if (null == cfg)
      throw new RuntimeException("Unable to find routing setting for resource :" + request.getResourceName());

    return cfg.buildRequestRoutingMap();
  }

  @Override
  public void start() {
    // Nothing to be done here
  }

  @Override
  public void shutdown() {
    // Nothing to be done here
  }
}
