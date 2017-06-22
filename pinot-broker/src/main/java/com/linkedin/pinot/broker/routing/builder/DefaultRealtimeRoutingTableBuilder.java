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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.broker.routing.ServerToSegmentSetMap;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.transport.common.SegmentIdSet;

/**
 * Create a given number of routing tables based on random selections from ExternalView.
 */
public class DefaultRealtimeRoutingTableBuilder extends AbstractRoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRealtimeRoutingTableBuilder.class);
  private RoutingTableBuilder _realtimeHLCRoutingTableBuilder;
  private RoutingTableBuilder _realtimeLLCRoutingTableBuilder;
  boolean _hasLLC;
  boolean _hasHLC;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _realtimeHLCRoutingTableBuilder = new KafkaHighLevelConsumerBasedRoutingTableBuilder();
    _realtimeLLCRoutingTableBuilder = new KafkaLowLevelConsumerRoutingTableBuilder();
    _realtimeHLCRoutingTableBuilder.init(configuration, tableConfig, propertyStore);
    _realtimeLLCRoutingTableBuilder.init(configuration, tableConfig, propertyStore);
  }

  @Override
  public void computeRoutingTableFromExternalView(String tableName, ExternalView externalView, List<InstanceConfig> instanceConfigList) {
    Set<String> segments = externalView.getPartitionSet();
    for (String segment : segments) {
      if (SegmentName.isHighLevelConsumerSegmentName(segment)) {
        _hasHLC = true;
      }
      if (SegmentName.isLowLevelConsumerSegmentName(segment)) {
        _hasLLC = true;
      }
    }
    if (_hasHLC) {
      _realtimeHLCRoutingTableBuilder.computeRoutingTableFromExternalView(tableName, externalView, instanceConfigList);
    }
    if (_hasLLC) {
      _realtimeLLCRoutingTableBuilder.computeRoutingTableFromExternalView(tableName, externalView, instanceConfigList);
    }
  }

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {
    boolean forceLLC = false;
    boolean forceHLC = false;
    for (String routingOption : request.getRoutingOptions()) {
      if (routingOption.equalsIgnoreCase("FORCE_HLC")) {
        forceHLC = true;
      }

      if (routingOption.equalsIgnoreCase("FORCE_LLC")) {
        forceLLC = true;
      }
    }
    if (forceHLC && forceLLC) {
      throw new RuntimeException("Trying to force routing to both HLC and LLC at the same time");
    }

    Map<ServerInstance, SegmentIdSet> serverToSegmentSetMaps;
    if (forceLLC) {
      serverToSegmentSetMaps = _realtimeLLCRoutingTableBuilder.findServers(request);
    } else if (forceHLC) {
      serverToSegmentSetMaps = _realtimeHLCRoutingTableBuilder.findServers(request);
    } else {
      if (_hasLLC) {
        serverToSegmentSetMaps = _realtimeLLCRoutingTableBuilder.findServers(request);
      } else if (_hasHLC) {
        serverToSegmentSetMaps = _realtimeHLCRoutingTableBuilder.findServers(request);
      } else {
        serverToSegmentSetMaps = Collections.emptyMap();
      }
    }

    return serverToSegmentSetMaps;
  }

  @Override
  public List<ServerToSegmentSetMap> getRoutingTables() {
    if (_hasLLC) {
      return _realtimeLLCRoutingTableBuilder.getRoutingTables();
    } else if(_hasHLC){
      return _realtimeHLCRoutingTableBuilder.getRoutingTables();
    } else {
      return Collections.emptyList();
    }
  }
}
