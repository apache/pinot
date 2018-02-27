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

package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Routing table builder for large offline clusters (over 20-30 servers) that avoids having each request go to every server.
 */
public class LargeClusterRoutingTableBuilder extends GeneratorBasedRoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(LargeClusterRoutingTableBuilder.class);

  /** Number of servers to hit for each query (this is a soft limit, not a hard limit) */
  private int _targetNumServersPerQuery = 20;

  @Override
  protected RoutingTableGenerator buildRoutingTableGenerator() {
    return new LargeClusterOfflineRoutingTableGenerator();
  }

  private class LargeClusterOfflineRoutingTableGenerator extends BaseRoutingTableGenerator {
    private Map<String, List<String>> _segmentToServersMap = new HashMap<>();

    public LargeClusterOfflineRoutingTableGenerator() {
      super(_targetNumServersPerQuery);
    }

    public void init(ExternalView externalView, List<InstanceConfig> instanceConfigList) {
      RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigList);
      for (String segmentName : externalView.getPartitionSet()) {
        List<String> serversForSegment = new ArrayList<>();
        for (Map.Entry<String, String> entry : externalView.getStateMap(segmentName).entrySet()) {
          String serverName = entry.getKey();
          if (entry.getValue().equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)
              && !instancePruner.isInactive(serverName)) {
            serversForSegment.add(serverName);
          }
        }
        if (!serversForSegment.isEmpty()) {
          _segmentToServersMap.put(segmentName, serversForSegment);
        }
      }
    }

    @Override
    protected Map<String, List<String>> getSegmentToServersMap() {
      return _segmentToServersMap;
    }
  }

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    // TODO jfim This is a broker-level configuration for now, until we refactor the configuration of the routing table to allow per-table routing settings
    if (configuration.containsKey("offlineTargetServerCountPerQuery")) {
      final String targetServerCountPerQuery = configuration.getString("offlineTargetServerCountPerQuery");
      try {
        _targetNumServersPerQuery = Integer.parseInt(targetServerCountPerQuery);
        LOGGER.info("Using offline target server count of {}", _targetNumServersPerQuery);
      } catch (Exception e) {
        LOGGER.warn(
            "Could not get the offline target server count per query from configuration value {}, keeping default value {}",
            targetServerCountPerQuery, _targetNumServersPerQuery, e);
      }
    } else {
      LOGGER.info("Using default value for offline target server count of {}", _targetNumServersPerQuery);
    }
  }
}
