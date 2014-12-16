package com.linkedin.pinot.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.transport.common.SegmentId;
import com.linkedin.pinot.transport.common.SegmentIdSet;


/**
 * Data structure to hold a whole mapping from ServerInstances to all the queryable segments.
 * 
 * @author xiafu
 *
 */
public class ServerToSegmentSetMap {
  private Map<String, Set<String>> _serverToSegmentSetMap;
  private Map<ServerInstance, SegmentIdSet> _routingTable;

  public static final String NAME_PORT_DELIMITER = "_";

  public ServerToSegmentSetMap(Map<String, Set<String>> serverToSegmentSetMap) {
    _serverToSegmentSetMap = serverToSegmentSetMap;
    _routingTable = new HashMap<ServerInstance, SegmentIdSet>();
    for (Entry<String, Set<String>> entry : _serverToSegmentSetMap.entrySet()) {
      String namePortStr = entry.getKey().split(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)[1];
      String hostName = namePortStr.split(NAME_PORT_DELIMITER)[0];
      int port;
      try {
        port = Integer.parseInt(namePortStr.split(NAME_PORT_DELIMITER)[1]);
      } catch (Exception e) {
        port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
      }

      ServerInstance serverInstance = new ServerInstance(hostName, port);
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
