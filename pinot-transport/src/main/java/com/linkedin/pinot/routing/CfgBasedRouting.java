/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.routing;

import java.util.Map;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import com.linkedin.pinot.transport.config.ResourceRoutingConfig;
import com.linkedin.pinot.transport.config.RoutingTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CfgBasedRouting implements RoutingTable {
  private static final Logger logger = LoggerFactory.getLogger(CfgBasedRouting.class);

  private RoutingTableConfig _cfg;

  public CfgBasedRouting() {
  }

  public void init(RoutingTableConfig cfg) {
    _cfg = cfg;
  }

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {

    ResourceRoutingConfig cfg = _cfg.getResourceRoutingCfg().get(request.getResourceName());

    if (null == cfg) {
      logger.warn("Unable to find routing setting for resource :" + request.getResourceName());
      return null;
    }

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
