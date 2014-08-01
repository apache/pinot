package com.linkedin.pinot.server.conf;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.pinot.transport.config.RoutingTableConfig;
import com.linkedin.pinot.transport.config.ConnectionPoolConfig;

public class BrokerConf {

  public static enum RoutingMode
  {
    CONFIG,
    HELIX
  };
  
  
  public static final String ROUTING_MODE_KEY = "routingMode";
  public static final String CFG_BASED_ROUTING = "routing";
  public static final String HELIX_CONFIG = "helix";
  public static final String CONNECTION_POOL_CONFIG = "connPool";
  
  // TODO: Revisit defaults
  private static final String DEFAULT_ROUTING_MODE = "CONFIG";
  
  private RoutingMode _routingMode;
  private RoutingTableConfig _cfgBasedRouting;
  private ConnectionPoolConfig _connPool;
  
  public BrokerConf()
  {
    _routingMode = RoutingMode.valueOf(DEFAULT_ROUTING_MODE);
    _cfgBasedRouting = new RoutingTableConfig();
    _connPool = new ConnectionPoolConfig();
  }
  
  public void init(Configuration cfg) throws ConfigurationException 
  {
    if (cfg.containsKey(ROUTING_MODE_KEY))
      _routingMode = RoutingMode.valueOf(cfg.getString(ROUTING_MODE_KEY));
    
    if ( (_routingMode == RoutingMode.CONFIG) && (cfg.containsKey(CFG_BASED_ROUTING)) )
    {
      _cfgBasedRouting.init(cfg.subset(CFG_BASED_ROUTING));
    }
    
    if ( cfg.containsKey(CONNECTION_POOL_CONFIG))
    {
      _connPool.init(cfg.subset(CONNECTION_POOL_CONFIG));
    }
  }

  public RoutingMode getRoutingMode() {
    return _routingMode;
  }

  public RoutingTableConfig getCfgBasedRouting() {
    return _cfgBasedRouting;
  }

  public ConnectionPoolConfig getConnPool() {
    return _connPool;
  }
}
