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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;


/**
 * Base routing table builder class to share common methods between routing table builders.
 */
public abstract class BaseRoutingTableBuilder implements RoutingTableBuilder {
  protected final Random _random = new Random();

  // Set variable as volatile so all threads can get the up-to-date routing tables
  private volatile List<Map<String, List<String>>> _routingTables;

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
    return _routingTables.get(_random.nextInt(_routingTables.size()));
  }

  @Override
  public List<Map<String, List<String>>> getRoutingTables() {
    return _routingTables;
  }
}
