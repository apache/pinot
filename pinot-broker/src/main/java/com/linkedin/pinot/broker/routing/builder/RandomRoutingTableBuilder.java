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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import com.linkedin.pinot.broker.routing.ServerToSegmentSetMap;
import com.linkedin.pinot.common.config.TableConfig;


/**
 * Create a given number of routing tables based on random selections from ExternalView.
 */
public class RandomRoutingTableBuilder extends AbstractRoutingTableBuilder {

  private int _numberOfRoutingTables;

  public RandomRoutingTableBuilder() {
    this._numberOfRoutingTables = 10;
  }

  public RandomRoutingTableBuilder(int numberOfRoutingTables) {
    this._numberOfRoutingTables = numberOfRoutingTables;
  }

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _numberOfRoutingTables = configuration.getInt("numOfRoutingTables", 10);
  }

  @Override
  public synchronized void computeRoutingTableFromExternalView(String tableName,
      ExternalView externalView, List<InstanceConfig> instanceConfigList) {

    RoutingTableInstancePruner pruner = new RoutingTableInstancePruner(instanceConfigList);

    List<Map<String, Set<String>>> routingTables = new ArrayList<Map<String, Set<String>>>();
    for (int i = 0; i < _numberOfRoutingTables; ++i) {
      routingTables.add(new HashMap<String, Set<String>>());
    }

    String[] segmentSet = externalView.getPartitionSet().toArray(new String[0]);
    for (String segment : segmentSet) {
      Map<String, String> instanceToStateMap = externalView.getStateMap(segment);
      for (String instance : instanceToStateMap.keySet().toArray(new String[0])) {
        if (!instanceToStateMap.get(instance).equals("ONLINE") || pruner.isInactive(instance)) {
          instanceToStateMap.remove(instance);
        }
      }
      if (instanceToStateMap.size() > 0) {
        String[] instances = instanceToStateMap.keySet().toArray(new String[0]);
        Random randomSeed = new Random(System.currentTimeMillis());
        for (int i = 0; i < _numberOfRoutingTables; ++i) {
          String instance = instances[randomSeed.nextInt(instances.length)];
          if (routingTables.get(i).containsKey(instance)) {
            routingTables.get(i).get(instance).add(segment);
          } else {
            Set<String> instanceSegmentSet = new HashSet<String>();
            instanceSegmentSet.add(segment);
            routingTables.get(i).put(instance, instanceSegmentSet);
          }
        }
      }
    }

    List<ServerToSegmentSetMap> resultRoutingTableList = new ArrayList<ServerToSegmentSetMap>();
    for (int i = 0; i < _numberOfRoutingTables; ++i) {
      resultRoutingTableList.add(new ServerToSegmentSetMap(routingTables.get(i)));
    }
    setRoutingTables(resultRoutingTableList);
  }
}
