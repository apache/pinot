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

import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.response.ServerInstance;
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
  private final RoutingTableBuilder _defaultRoutingTableBuilder;
  private final Map<String, RoutingTableBuilder> _routingTableBuilderMap;

  private final Map<String, List<ServerToSegmentSetMap>> _brokerRoutingTable =
      new HashMap<String, List<ServerToSegmentSetMap>>();
  private final Map<String, Long> _routingTableModifiedTimeStampMap = new HashMap<String, Long>();
  private final Random _random = new Random(System.currentTimeMillis());
  private final HelixExternalViewBasedTimeBoundaryService _timeBoundaryService;

  public HelixExternalViewBasedRouting(RoutingTableBuilder defaultRoutingTableBuilder,
      Map<String, RoutingTableBuilder> routingTableBuilderMap, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _timeBoundaryService = new HelixExternalViewBasedTimeBoundaryService(propertyStore);
    if (defaultRoutingTableBuilder != null) {
      _defaultRoutingTableBuilder = defaultRoutingTableBuilder;
    } else {
      _defaultRoutingTableBuilder = new RandomRoutingTableBuilder();
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
    RoutingTableBuilder routingTableBuilder = _defaultRoutingTableBuilder;
    if (_routingTableBuilderMap.containsKey(resourceName) && (_routingTableBuilderMap.get(resourceName) != null)) {
      routingTableBuilder = _routingTableBuilderMap.get(resourceName);
    }
    LOGGER.info("Trying to compute routing table for resource : " + resourceName + ",by : " + routingTableBuilder);
    List<ServerToSegmentSetMap> serverToSegmentSetMap =
        routingTableBuilder.computeRoutingTableFromExternalView(resourceName, externalView);
    _brokerRoutingTable.put(resourceName, serverToSegmentSetMap);
    _timeBoundaryService.updateTimeBoundaryService(externalView);

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
