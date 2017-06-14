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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.transport.common.SegmentId;
import com.linkedin.pinot.transport.common.SegmentIdSet;


/**
 * Data structure to hold a whole mapping from ServerInstances to all the queryable segments.
 *
 *
 */
public class ServerToSegmentSetMap {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Logger logger = LoggerFactory.getLogger(ServerToSegmentSetMap.class);

  private Map<String, Set<String>> _serverToSegmentSetMap;
  private Map<ServerInstance, SegmentIdSet> _routingTable;

  public static final String NAME_PORT_DELIMITER = "_";

  public ServerToSegmentSetMap(Map<String, Set<String>> serverToSegmentSetMap) {
    _serverToSegmentSetMap = serverToSegmentSetMap;
    _routingTable = new HashMap<ServerInstance, SegmentIdSet>();
    for (Entry<String, Set<String>> entry : _serverToSegmentSetMap.entrySet()) {
      String instanceName = entry.getKey();
      ServerInstance serverInstance = ServerInstance.forInstanceName(instanceName);
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

  @Override
  public String toString() {
    try {
      JSONObject ret = new JSONObject();
      for (ServerInstance i : _routingTable.keySet()) {
        JSONArray serverInstanceSegmentList = new JSONArray();
        List<String> sortedSegmentIds = new ArrayList<>();

        for (SegmentId segmentId : _routingTable.get(i).getSegments()) {
          sortedSegmentIds.add(segmentId.getSegmentId());
        }

        Collections.sort(sortedSegmentIds);

        for (String sortedSegmentId : sortedSegmentIds) {
          serverInstanceSegmentList.put(sortedSegmentId);
        }

        ret.put(i.toString(), serverInstanceSegmentList);
      }
      return ret.toString();
    } catch (Exception e) {
      logger.error("error toString()", e);
      return "routing table : [ " + _routingTable + " ] ";
    }
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    ServerToSegmentSetMap other = (ServerToSegmentSetMap) o;

    return
        EqualityUtils.isEqual(_serverToSegmentSetMap, other._serverToSegmentSetMap) &&
        EqualityUtils.isEqual(_routingTable, other._routingTable);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_serverToSegmentSetMap);
    result = EqualityUtils.hashCodeOf(result, _routingTable);
    return result;
  }
}
