/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.conf;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.pinot.transport.config.ConnectionPoolConfig;
import com.linkedin.pinot.transport.config.RoutingTableConfig;


public class TransportClientConf {

  public static enum RoutingMode {
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

  public TransportClientConf() {
    _routingMode = RoutingMode.valueOf(DEFAULT_ROUTING_MODE);
    _cfgBasedRouting = new RoutingTableConfig();
    _connPool = new ConnectionPoolConfig();
  }

  public void init(Configuration cfg) throws ConfigurationException {
    if (cfg.containsKey(ROUTING_MODE_KEY)) {
      _routingMode = RoutingMode.valueOf(cfg.getString(ROUTING_MODE_KEY));
    }
    if ((_routingMode == RoutingMode.CONFIG)) {
      _cfgBasedRouting.init(cfg.subset(CFG_BASED_ROUTING));
    }

    Configuration connPoolCfg = cfg.subset(CONNECTION_POOL_CONFIG);
    if (connPoolCfg != null) {
      _connPool.init(connPoolCfg);
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
