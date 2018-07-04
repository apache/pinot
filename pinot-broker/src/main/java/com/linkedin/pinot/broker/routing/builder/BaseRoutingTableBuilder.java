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
  public static final String PINOT_BROKER_RESOURCES = "pinot-broker/src/main/resources/";
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseRoutingTableBuilder.class);

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
    String blackListFile;
    List<String> blackList = new ArrayList<String>();
    //String pinotHome = System.getenv("PINOT_HOME");
    String pinotHome = "/home/sajavadi/pinot/";
    blackListFile = pinotHome + PINOT_BROKER_RESOURCES + "blacklist.config";
    try
    {
      InputStream inStream = new FileInputStream(blackListFile);
      Scanner scanner = new Scanner(inStream);
      while(scanner.hasNextLine()){
        blackList.add(scanner.nextLine());
      }
      scanner.close();
    }
    catch (Exception e)
    {
      LOGGER.error(e.getMessage());
    }

    //LOGGER.info("blackListFile: {}",blackListFile);
    //LOGGER.info("routing table size: {}",_routingTables.size());
    int randIndex = _random.nextInt(_routingTables.size());
    //LOGGER.info("routing table {} key set: {}",randIndex,_routingTables.get(randIndex).keySet().toString());
    //LOGGER.info("routing table {} value set: {}",randIndex,_routingTables.get(randIndex).values().toString());
    Map<String, List<String>> routingTable = _routingTables.get(randIndex);
    if(!blackList.isEmpty())
    {
      String server = blackList.get(0);
      List<String> serverSegs = routingTable.get(server);
      routingTable.remove(server);
      while(!serverSegs.isEmpty())
      {
        for (String key : routingTable.keySet()) {
          if(!serverSegs.isEmpty())
          {
           routingTable.get(key).add(serverSegs.get(0));
           serverSegs.remove(0);
          }
        }
      }

    }
    return routingTable;
  }

  @Override
  public List<Map<String, List<String>>> getRoutingTables() {
    return _routingTables;
  }
}
