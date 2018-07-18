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

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Assign balanced number of segments to each server.
 */
public class BalancedRandomRoutingTableBuilder extends BaseRoutingTableBuilder {
  private static final int DEFAULT_NUM_ROUTING_TABLES = 10;
  private static final String NUM_ROUTING_TABLES_KEY = "numOfRoutingTables";

  private int _numRoutingTables = DEFAULT_NUM_ROUTING_TABLES;
  private IAwareLBControllerDaemon iAwareLBControllerDaemon;

  /*
  private  volatile String tableName;
  private  volatile ExternalView externalView;
  private  volatile List<InstanceConfig> instanceConfigs;
  */

  private List<Map<String, List<String>>> _lastRoutingTables;
  private Map<String, List<String>> _lastSegment2ServerMap;

  //private boolean isIAwareLBDaemonStarted = false;


  //use last routing table only


  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _numRoutingTables = configuration.getInt(NUM_ROUTING_TABLES_KEY, DEFAULT_NUM_ROUTING_TABLES);
    _lastRoutingTables = new ArrayList<>();
    _lastSegment2ServerMap = new HashMap<String, List<String>>();

    //if(!isIAwareLBDaemonStarted)
    //{
    //  isIAwareLBDaemonStarted = true;
      iAwareLBControllerDaemon = new IAwareLBControllerDaemon(this);
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
      ScheduledFuture future = executor.scheduleWithFixedDelay(iAwareLBControllerDaemon, 0, 1000, TimeUnit.MILLISECONDS);

      /*
      Thread daemon = new Thread(iAwareLBControllerDaemon);
      daemon.start();
      */
   // }

  }

  @Override
  public void computeRoutingTableFromExternalView(String tableName, ExternalView externalView, List<InstanceConfig> instanceConfigs) {

    _lastSegment2ServerMap.clear();

    List<Map<String, List<String>>> routingTables = new ArrayList<>(_numRoutingTables);
    for (int i = 0; i < _numRoutingTables; i++) {
      routingTables.add(new HashMap<String, List<String>>());
    }

    RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigs);
    for (String segmentName : externalView.getPartitionSet()) {
      // List of servers that are active and are serving the segment
      List<String> servers = new ArrayList<>();
      for (Map.Entry<String, String> entry : externalView.getStateMap(segmentName).entrySet()) {
        String serverName = entry.getKey();        if (entry.getValue().equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)
            && !instancePruner.isInactive(serverName)) {
          servers.add(serverName);
        }
      }

      int numServers = servers.size();
      if (numServers != 0) {
        for (Map<String, List<String>> routingTable : routingTables) {
          // Assign the segment to the server with least segments assigned
          routingTable.get(getServerWithLeastSegmentsAssigned(servers, routingTable)).add(segmentName);
        }
        _lastSegment2ServerMap.put(segmentName,servers);
      }
    }

    //iAwareLBControllerDaemon.setRoutingTables(routingTables);
    _lastRoutingTables.clear();

    for(int i=0;i<routingTables.size();i++)
    {
      _lastRoutingTables.add(new HashMap<String, List<String>>());
      for(String key:routingTables.get(i).keySet())
      {
        List<String> segList = new ArrayList <>();
        for(String segName:routingTables.get(i).get(key))
        {
          segList.add(segName);
        }
        _lastRoutingTables.get(i).put(key,segList);
      }

    }

    iAwareLBControllerDaemon.setRoutingTableBuilder(this);

    routingTables = WorkerWeightDeployer.applyWorkerWeights(routingTables, _lastSegment2ServerMap);
    setRoutingTables(routingTables);


    /*
    iAwareLBControllerDaemon.setTableName(tableName);
    iAwareLBControllerDaemon.setExternalView(externalView);
    iAwareLBControllerDaemon.setInstanceConfigs(instanceConfigs);
    */

    /*
    this.tableName = tableName;
    this.externalView = externalView;
    this.instanceConfigs = instanceConfigs;
    */

  }

  public void computeRoutingTableFromLastUpdate()
  {
    List<Map<String, List<String>>> routingTables = new ArrayList<>(_numRoutingTables);
    for (int i = 0; i < _numRoutingTables; i++) {
      routingTables.add(new HashMap<String, List<String>>());
    }
    routingTables = WorkerWeightDeployer.applyWorkerWeights(_lastRoutingTables, _lastSegment2ServerMap);
    setRoutingTables(routingTables);
  }

  public  static int getDefaultNumRoutingTables()
  {
    return DEFAULT_NUM_ROUTING_TABLES;
  }

  /*
  public String getTableName ()
  {
    return  tableName;
  }
  public ExternalView getExternalView ()
  {
    return  externalView;
  }
  public List<InstanceConfig> getInstanceConfigs ()
  {
    return  instanceConfigs;
  }
  */
}
