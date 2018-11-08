/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.utils.SegmentName;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Create a given number of routing tables based on random selections from ExternalView.
 */
public class DefaultRealtimeRoutingTableBuilder implements RoutingTableBuilder {
  private RoutingTableBuilder _realtimeHLCRoutingTableBuilder;
  private RoutingTableBuilder _realtimeLLCRoutingTableBuilder;
  private boolean _hasHLC;
  private boolean _hasLLC;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore, BrokerMetrics brokerMetrics) {
    _realtimeHLCRoutingTableBuilder = new HighLevelConsumerBasedRoutingTableBuilder();
    _realtimeLLCRoutingTableBuilder = new LowLevelConsumerRoutingTableBuilder();
    _realtimeHLCRoutingTableBuilder.init(configuration, tableConfig, propertyStore, brokerMetrics);
    _realtimeLLCRoutingTableBuilder.init(configuration, tableConfig, propertyStore, brokerMetrics);
  }

  @Override
  public void computeOnExternalViewChange(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    Set<String> segmentSet = externalView.getPartitionSet();
    for (String segmentName : segmentSet) {
      if (SegmentName.isHighLevelConsumerSegmentName(segmentName)) {
        _hasHLC = true;
      }
      if (SegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        _hasLLC = true;
      }
    }
    if (_hasHLC) {
      _realtimeHLCRoutingTableBuilder.computeOnExternalViewChange(tableName, externalView, instanceConfigs);
    }
    if (_hasLLC) {
      _realtimeLLCRoutingTableBuilder.computeOnExternalViewChange(tableName, externalView, instanceConfigs);
    }
  }

  @Override
  public Map<String, List<String>> getRoutingTable(RoutingTableLookupRequest request) {
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

    if (forceLLC) {
      return _realtimeLLCRoutingTableBuilder.getRoutingTable(request);
    } else if (forceHLC) {
      return _realtimeHLCRoutingTableBuilder.getRoutingTable(request);
    } else {
      if (_hasLLC) {
        return _realtimeLLCRoutingTableBuilder.getRoutingTable(request);
      } else if (_hasHLC) {
        return _realtimeHLCRoutingTableBuilder.getRoutingTable(request);
      } else {
        return Collections.emptyMap();
      }
    }
  }

  @Override
  public List<Map<String, List<String>>> getRoutingTables() {
    if (_hasLLC) {
      return _realtimeLLCRoutingTableBuilder.getRoutingTables();
    } else if (_hasHLC) {
      return _realtimeHLCRoutingTableBuilder.getRoutingTables();
    } else {
      return Collections.emptyList();
    }
  }
}
