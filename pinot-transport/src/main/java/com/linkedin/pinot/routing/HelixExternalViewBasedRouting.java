package com.linkedin.pinot.routing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.helix.model.ExternalView;
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
  private final Random _random = new Random(System.currentTimeMillis());

  public HelixExternalViewBasedRouting(RoutingTableBuilder defaultRoutingTableBuilder,
      Map<String, RoutingTableBuilder> routingTableBuilderMap) {
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

  }

  public synchronized void markDataResourceOffline(String resourceName) {
    LOGGER.info("Trying to remove data resource from broker : " + resourceName);
    if (_dataResourceSet.contains(resourceName)) {
      _dataResourceSet.remove(resourceName);
      _brokerRoutingTable.remove(resourceName);
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

}
