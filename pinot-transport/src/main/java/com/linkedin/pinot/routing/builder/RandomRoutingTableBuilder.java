package com.linkedin.pinot.routing.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.model.ExternalView;

import com.linkedin.pinot.routing.ServerToSegmentSetMap;


/**
 * Create a given number of routing tables based on random selections from ExternalView.
 * 
 * @author xiafu
 *
 */
public class RandomRoutingTableBuilder implements RoutingTableBuilder {

  private int _numberOfRoutingTables;

  public RandomRoutingTableBuilder() {
    this._numberOfRoutingTables = 10;
  }

  public RandomRoutingTableBuilder(int numberOfRoutingTables) {
    this._numberOfRoutingTables = numberOfRoutingTables;
  }

  @Override
  public void init(Configuration configuration) {
    _numberOfRoutingTables = configuration.getInt("numOfRoutingTables", 10);
  }

  @Override
  public List<ServerToSegmentSetMap> computeRoutingTableFromExternalView(String resourceName, ExternalView externalView) {

    List<Map<String, Set<String>>> routingTables = new ArrayList<Map<String, Set<String>>>();
    for (int i = 0; i < _numberOfRoutingTables; ++i) {
      routingTables.add(new HashMap<String, Set<String>>());
    }

    Set<String> segmentSet = externalView.getPartitionSet();
    for (String segment : segmentSet) {
      Map<String, String> instanceToStateMap = externalView.getStateMap(segment);
      for (String instance : instanceToStateMap.keySet()) {
        if (!instanceToStateMap.get(instance).equals("ONLINE")) {
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
    return resultRoutingTableList;

  }

}
