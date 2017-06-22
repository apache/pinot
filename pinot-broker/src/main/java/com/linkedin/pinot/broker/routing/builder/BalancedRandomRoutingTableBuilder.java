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
import java.util.Collections;
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

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.broker.routing.ServerToSegmentSetMap;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentIdSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Create a given number of routing tables based on random selections from ExternalView.
 */
public class BalancedRandomRoutingTableBuilder extends AbstractRoutingTableBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(BalancedRandomRoutingTableBuilder.class);

  private int _numberOfRoutingTables;

  public BalancedRandomRoutingTableBuilder() {
    this._numberOfRoutingTables = 10;
  }

  public BalancedRandomRoutingTableBuilder(int numberOfRoutingTables) {
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
      Map<String, String> instanceToStateMap = new HashMap<>(externalView.getStateMap(segment));
      for (String instance : instanceToStateMap.keySet().toArray(new String[0])) {
        if (!instanceToStateMap.get(instance).equals("ONLINE") || pruner.isInactive(instance)) {
          LOGGER.info("Removing offline/inactive instance '{}' from routing table computation", instance);
          instanceToStateMap.remove(instance);
        }
      }
      if (instanceToStateMap.size() > 0) {
        List<String> instanceList = new ArrayList<String>(instanceToStateMap.keySet());
        for (int i = 0; i < _numberOfRoutingTables; ++i) {
          Collections.shuffle(instanceList);
          String[] instances = instanceList.toArray(new String[instanceList.size()]);

          int minInstances = Integer.MAX_VALUE;
          int minIdx = -1;
          int base = 2;
          for (int k = 0; k < instances.length; ++k) {
            int sizeOfCurrentInstance = 0;
            if (routingTables.get(i).containsKey(instances[k])) {
              sizeOfCurrentInstance = routingTables.get(i).get(instances[k]).size();
            }
            if (sizeOfCurrentInstance < minInstances) {
              minInstances = sizeOfCurrentInstance;
              minIdx = k;
              base = 2;
            }
            if (sizeOfCurrentInstance == minInstances && (System.currentTimeMillis() % base == 0)) {
              minIdx = k;
              base = 2;
            } else {
              base++;
            }
          }
          if (routingTables.get(i).containsKey(instances[minIdx])) {
            routingTables.get(i).get(instances[minIdx]).add(segment);
          } else {
            Set<String> instanceSegmentSet = new HashSet<String>();
            instanceSegmentSet.add(segment);
            routingTables.get(i).put(instances[minIdx], instanceSegmentSet);
          }
        }
      }
    }

    List<ServerToSegmentSetMap> resultRoutingTableList = new ArrayList<ServerToSegmentSetMap>();
    for (int i = 0; i < _numberOfRoutingTables; ++i) {
      resultRoutingTableList.add(new ServerToSegmentSetMap(routingTables.get(i)));
    }
    setRoutingTables(resultRoutingTableList);
    setIsEmpty(externalView.getPartitionSet().isEmpty());
  }
}
