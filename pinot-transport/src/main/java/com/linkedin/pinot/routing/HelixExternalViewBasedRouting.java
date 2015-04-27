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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;
import com.linkedin.pinot.routing.builder.KafkaHighLevelConsumerBasedRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RandomRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.transport.common.SegmentIdSet;


/**
 * HelixExternalViewBasedRouting will maintain the routing table for assigned data resource.
 *
 * @author xiafu
 *
 */
public class HelixExternalViewBasedRouting implements RoutingTable {

  private final Logger LOGGER = Logger.getLogger(HelixExternalViewBasedRouting.class);
  private final Set<String> _dataResourceSet = new HashSet<String>();
  private final RoutingTableBuilder _defaultOfflineRoutingTableBuilder;
  private final RoutingTableBuilder _defaultRealtimeRoutingTableBuilder;
  private final Map<String, RoutingTableBuilder> _routingTableBuilderMap;

  private final Map<String, List<ServerToSegmentSetMap>> _brokerRoutingTable =
      new ConcurrentHashMap<String, List<ServerToSegmentSetMap>>();
  private final Map<String, Long> _routingTableModifiedTimeStampMap = new HashMap<String, Long>();
  private final Random _random = new Random(System.currentTimeMillis());
  private final HelixExternalViewBasedTimeBoundaryService _timeBoundaryService;

  @Deprecated
  public HelixExternalViewBasedRouting(RoutingTableBuilder defaultOfflineRoutingTableBuilder,
      Map<String, RoutingTableBuilder> routingTableBuilderMap, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _timeBoundaryService = new HelixExternalViewBasedTimeBoundaryService(propertyStore);
    if (defaultOfflineRoutingTableBuilder != null) {
      _defaultOfflineRoutingTableBuilder = defaultOfflineRoutingTableBuilder;
    } else {
      _defaultOfflineRoutingTableBuilder = new RandomRoutingTableBuilder();
    }
    _defaultRealtimeRoutingTableBuilder = new KafkaHighLevelConsumerBasedRoutingTableBuilder();
    if (routingTableBuilderMap != null) {
      _routingTableBuilderMap = routingTableBuilderMap;
    } else {
      _routingTableBuilderMap = new HashMap<String, RoutingTableBuilder>();
    }
  }

  public HelixExternalViewBasedRouting(RoutingTableBuilder defaultOfflineRoutingTableBuilder,
      RoutingTableBuilder defaultRealtimeRoutingTableBuilder,
      Map<String, RoutingTableBuilder> routingTableBuilderMap, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _timeBoundaryService = new HelixExternalViewBasedTimeBoundaryService(propertyStore);
    if (defaultOfflineRoutingTableBuilder != null) {
      _defaultOfflineRoutingTableBuilder = defaultOfflineRoutingTableBuilder;
    } else {
      _defaultOfflineRoutingTableBuilder = new RandomRoutingTableBuilder();
    }
    if (defaultRealtimeRoutingTableBuilder != null) {
      _defaultRealtimeRoutingTableBuilder = defaultRealtimeRoutingTableBuilder;
    } else {
      _defaultRealtimeRoutingTableBuilder = new RandomRoutingTableBuilder();
    }
    if (routingTableBuilderMap != null) {
      _routingTableBuilderMap = routingTableBuilderMap;
    } else {
      _routingTableBuilderMap = new HashMap<String, RoutingTableBuilder>();
    }
  }

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {
    String resourceName = request.getResourceName();
    if ((_brokerRoutingTable == null) || (!_brokerRoutingTable.containsKey(resourceName))) {
      return null;
    }
    List<ServerToSegmentSetMap> serverToSegmentSetMaps = _brokerRoutingTable.get(resourceName);
    return serverToSegmentSetMaps.get(_random.nextInt(serverToSegmentSetMaps.size())).getRouting();
  }

  @Override
  public void start() {
    LOGGER.info("Start HelixExternalViewBasedRouting!");

  }

  @Override
  public void shutdown() {
    LOGGER.info("Shutdown HelixExternalViewBasedRouting!");
  }

  public synchronized void markDataResourceOnline(String resourceName, ExternalView externalView) {
    if (externalView == null) {
      return;
    }
    if (_routingTableModifiedTimeStampMap.containsKey(resourceName)) {
      long recentModifiedTimeStamp = _routingTableModifiedTimeStampMap.get(resourceName);
      LOGGER.info("ExternalView modified timestamp for resource: " + resourceName + " is " + externalView.getRecord().getModifiedTime());
      LOGGER.info("Recent updated timestamp for for resource: " + resourceName + " is " + recentModifiedTimeStamp);
      if (externalView.getRecord().getModifiedTime() <= recentModifiedTimeStamp) {
        LOGGER.info("No change on routing table version, do nothing for resource: " + resourceName);
        return;
      }
    }
    _routingTableModifiedTimeStampMap.put(resourceName, externalView.getRecord().getModifiedTime());
    if (!_dataResourceSet.contains(resourceName)) {
      LOGGER.info("Adding a new data resource to broker : " + resourceName);
      _dataResourceSet.add(resourceName);
    }
    RoutingTableBuilder routingTableBuilder = null;
    ResourceType resourceType = BrokerRequestUtils.getResourceTypeFromResourceName(resourceName);
    if (resourceType != null) {
      switch (resourceType) {
        case REALTIME:
          routingTableBuilder = _defaultRealtimeRoutingTableBuilder;
          break;
        case OFFLINE:
          routingTableBuilder = _defaultOfflineRoutingTableBuilder;
          break;
        default:
          routingTableBuilder = _defaultOfflineRoutingTableBuilder;
          break;
      }
    } else {
      routingTableBuilder = _defaultOfflineRoutingTableBuilder;
    }
    if (_routingTableBuilderMap.containsKey(resourceName) && (_routingTableBuilderMap.get(resourceName) != null)) {
      routingTableBuilder = _routingTableBuilderMap.get(resourceName);
    }
    LOGGER.info("Trying to compute routing table for resource : " + resourceName + ",by : " + routingTableBuilder);
    try {
      List<ServerToSegmentSetMap> serverToSegmentSetMap =
          routingTableBuilder.computeRoutingTableFromExternalView(resourceName, externalView);

      _brokerRoutingTable.put(resourceName, serverToSegmentSetMap);
    } catch (Exception e) {
      LOGGER.error("Failed to compute/update the routing table" + e.getCause());
    }
    try {
      LOGGER.info("Trying to compute time boundary service for resource : " + resourceName);
      _timeBoundaryService.updateTimeBoundaryService(externalView);
    } catch (Exception e) {
      LOGGER.error("Failed to update the TimeBoundaryService : " + e.getCause());
    }

  }

  public synchronized void markDataResourceOffline(String resourceName) {
    LOGGER.info("Trying to remove data resource from broker : " + resourceName);
    if (_dataResourceSet.contains(resourceName)) {
      _dataResourceSet.remove(resourceName);
      _brokerRoutingTable.remove(resourceName);
      _routingTableModifiedTimeStampMap.remove(resourceName);
      _timeBoundaryService.remove(resourceName);
    }
  }

  public boolean contains(String resourceName) {
    return _dataResourceSet.contains(resourceName);
  }

  public Set<String> getDataResourceSet() {
    return _dataResourceSet;
  }

  public Map<String, List<ServerToSegmentSetMap>> getBrokerRoutingTable() {
    return _brokerRoutingTable;
  }

  public TimeBoundaryService getTimeBoundaryService() {
    return _timeBoundaryService;
  }

}
