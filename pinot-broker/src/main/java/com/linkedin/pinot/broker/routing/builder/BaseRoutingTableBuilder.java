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

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;


/**
 * Base routing table builder class to share common methods between routing table builders.
 */
public abstract class BaseRoutingTableBuilder implements RoutingTableBuilder {
  protected final Random _random = new Random();
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseRoutingTableBuilder.class);

  // Set variable as volatile so all threads can get the up-to-date routing tables
  private volatile List<Map<String, List<String>>> _routingTables;


  private volatile List<Map<String, List<String>>> _lastRoutingTables;
  private volatile Map<String, List<String>> _lastSegment2ServerMap;


  protected static String getServerWithLeastSegmentsAssigned(List<String> servers,
      Map<String, List<String>> routingTable) {
    Collections.shuffle(servers);

    String selectedServer = null;
    int minNumSegmentsAssigned = Integer.MAX_VALUE;
    for (String server : servers) {
      List<String> segments = routingTable.get(server);
      if (segments == null) {
        routingTable.put(server, new ArrayList<String>());
        return server;
      } else {
        int numSegmentsAssigned = segments.size();
        if (numSegmentsAssigned < minNumSegmentsAssigned) {
          minNumSegmentsAssigned = numSegmentsAssigned;
          selectedServer = server;
        }
      }
    }
    return selectedServer;
  }

  protected void setRoutingTables(List<Map<String, List<String>>> routingTables) {
    _routingTables = routingTables;
  }

  @Override
  public Map<String, List<String>> getRoutingTable(RoutingTableLookupRequest request) {
    //LOGGER.info("routing table size: {}",_routingTables.size());
    int randIndex = _random.nextInt(_routingTables.size());
    //LOGGER.info("routing table {} key set: {}",randIndex,_routingTables.get(randIndex).keySet().toString());
    //LOGGER.info("routing table {} value set: {}",randIndex,_routingTables.get(randIndex).values().toString());
    return _routingTables.get(randIndex);
  }

  @Override
  public List<Map<String, List<String>>> getRoutingTables() {
    return _routingTables;
  }


  protected void setLastRoutingTable(List<Map<String, List<String>>> lastRoutingTables)
  {
    _lastRoutingTables = lastRoutingTables;
  }

  protected List<Map<String, List<String>>> getLastRoutingTable()
  {
    return _lastRoutingTables;
  }

  protected void setLastSegment2ServerMap(Map<String, List<String>> lastSegment2ServerMap)
  {
    _lastSegment2ServerMap = lastSegment2ServerMap;
  }

  protected Map<String, List<String>> getLastSegment2ServerMap()
  {
    return _lastSegment2ServerMap;
  }
}
