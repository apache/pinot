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
package com.linkedin.pinot.broker.routing;

import java.util.Map;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import com.linkedin.pinot.transport.config.PerTableRoutingConfig;
import com.linkedin.pinot.transport.config.RoutingTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CfgBasedRouting implements RoutingTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CfgBasedRouting.class);

  private RoutingTableConfig _cfg;

  public CfgBasedRouting() {
  }

  public void init(RoutingTableConfig cfg) {
    _cfg = cfg;
  }

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {

    PerTableRoutingConfig cfg = _cfg.getPerTableRoutingCfg().get(request.getTableName());

    if (null == cfg) {
      LOGGER.warn("Unable to find routing setting for table :" + request.getTableName());
      return null;
    }

    return cfg.buildRequestRoutingMap();
  }

  @Override
  public boolean routingTableExists(String tableName) {
    Map<ServerInstance, SegmentIdSet> routingTableEntry = findServers(new RoutingTableLookupRequest(tableName, null, null));
    return routingTableEntry != null && !routingTableEntry.isEmpty();
  }

  @Override
  public void start() {
    // Nothing to be done here
  }

  @Override
  public void shutdown() {
    // Nothing to be done here
  }

  @Override
  public String dumpSnapshot(String tableName) throws Exception {
    return null;
  }
}
