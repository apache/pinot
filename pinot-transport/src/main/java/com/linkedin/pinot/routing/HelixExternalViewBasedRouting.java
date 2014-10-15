package com.linkedin.pinot.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.helix.model.ExternalView;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentId;
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
  private final int PORT = 8882;

  private final Map<String, List<ServerToSegmentSetMap>> _brokerRoutingTable =
      new HashMap<String, List<ServerToSegmentSetMap>>();

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {
    String resourceName = request.getResourceName();
    List<ServerToSegmentSetMap> serverToSegmentSetMaps = _brokerRoutingTable.get(resourceName);
    return serverToSegmentSetMaps.get(new Random(System.currentTimeMillis()).nextInt(serverToSegmentSetMaps.size()))
        .getRouting();
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
    LOGGER.info("Trying to add data resource to broker : " + resourceName);
    if (!_dataResourceSet.contains(resourceName)) {
      _dataResourceSet.add(resourceName);
    }

    List<ServerToSegmentSetMap> serverToSegmentSetMap = computeRoutingTableFromExternalView(resourceName, externalView);
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

  private List<ServerToSegmentSetMap> computeRoutingTableFromExternalView(String resourceName, ExternalView externalView) {
    // TODO(xiafu) : Has to implement a better solution.
    // Currently just compute 1 routing table.
    List<ServerToSegmentSetMap> serverToSegmentSetMaps =
        new ArrayList<HelixExternalViewBasedRouting.ServerToSegmentSetMap>();
    Map<String, Set<String>> serverToSegmentSetMap = new HashMap<String, Set<String>>();

    Set<String> segmentSet = externalView.getPartitionSet();
    for (String segment : segmentSet) {
      Map<String, String> instanceToStateMap = externalView.getStateMap(segment);
      for (String instance : instanceToStateMap.keySet()) {
        if (instanceToStateMap.get(instance).equals("ONLINE")) {
          if (serverToSegmentSetMap.containsKey(instance)) {
            serverToSegmentSetMap.get(instance).add(segment);
          } else {
            Set<String> instanceSegmentSet = new HashSet<String>();
            instanceSegmentSet.add(segment);
            serverToSegmentSetMap.put(instance, instanceSegmentSet);
          }
          break;
        }
      }
    }
    serverToSegmentSetMaps.add(new ServerToSegmentSetMap(serverToSegmentSetMap));
    return serverToSegmentSetMaps;
  }

  public Set<String> getDataResourceSet() {
    return _dataResourceSet;
  }

  public Map<String, List<ServerToSegmentSetMap>> getBrokerRoutingTable() {
    return _brokerRoutingTable;
  }

  public class ServerToSegmentSetMap {
    private Map<String, Set<String>> _serverToSegmentSetMap;
    private Map<ServerInstance, SegmentIdSet> _routingTable;

    ServerToSegmentSetMap(Map<String, Set<String>> serverToSegmentSetMap) {
      _serverToSegmentSetMap = serverToSegmentSetMap;
      _routingTable = new HashMap<ServerInstance, SegmentIdSet>();
      for (Entry<String, Set<String>> entry : _serverToSegmentSetMap.entrySet()) {
        ServerInstance serverInstance = new ServerInstance(entry.getKey().split("dataServer_")[1], PORT);
        SegmentIdSet segmentIdSet = new SegmentIdSet();
        for (String segmentId : entry.getValue()) {
          segmentIdSet.addSegment(new SegmentId(segmentId));
        }
        _routingTable.put(serverInstance, segmentIdSet);
      }
    }

    public Set<String> getServerSet() {
      return _serverToSegmentSetMap.keySet();
    }

    public Set<String> getSegmentSet(String server) {
      return _serverToSegmentSetMap.get(server);
    }

    public Map<ServerInstance, SegmentIdSet> getRouting() {
      return _routingTable;
    }
  }

}
