package com.linkedin.pinot.tools;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ZNRecord;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.testng.collections.Lists;


public class PinotRebalancer {
  private ZKHelixAdmin helixAdmin;
  private String clusterName;

  public PinotRebalancer(String zkAddress, String clusterName) {
    this.clusterName = clusterName;
    helixAdmin = new ZKHelixAdmin(zkAddress);
  }

  public void rebalanceTable(String tableName, String tenantName) {
    IdealState currentIdealState = helixAdmin.getResourceIdealState(clusterName, tableName);
    List<String> partitions = Lists.newArrayList(currentIdealState.getPartitionSet());
    LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
    states.put("OFFLINE", 0);
    states.put("ONLINE", 1);
    Map<String, Map<String, String>> mapFields = currentIdealState.getRecord().getMapFields();
    Set<String> currentHosts = new HashSet<>();
    for (String segment : mapFields.keySet()) {
      currentHosts.addAll(mapFields.get(segment).keySet());
    }
    AutoRebalanceStrategy rebalanceStrategy = new AutoRebalanceStrategy(tableName, partitions, states);
    List<String> instancesInClusterWithTag = helixAdmin.getInstancesInClusterWithTag(clusterName, tenantName);
    System.out.println("Old Nodes:" + currentHosts);
    System.out.println("New Nodes:" + instancesInClusterWithTag);
    Map<String, Map<String, String>> currentMapping = currentIdealState.getRecord().getMapFields();
    ZNRecord newMapping = rebalanceStrategy.computePartitionAssignment(instancesInClusterWithTag, currentMapping,
        instancesInClusterWithTag);
    System.out.println("previous mapping:" + currentIdealState);
    System.out.println("new mapping:" + newMapping);
    IdealState updatedIdealState = new IdealState(currentIdealState.getRecord());
    updatedIdealState.getRecord().setMapFields(newMapping.getMapFields());
    helixAdmin.setResourceIdealState(clusterName, tableName, updatedIdealState);
  }

  /**
   * USAGE PinotRebalancer <zkAddress> <clusterName> <tableName>
   * @param args
   */
  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("USAGE PinotRebalancer <zkAddress> <clusterName> <tableName> <tenantName>");
      System.exit(1);
    }
    String zkAddress = args[0];
    String clusterName = args[1];
    String tableName = args[2];
    String tenantName = args[2];
    PinotRebalancer rebalancer = new PinotRebalancer(zkAddress, clusterName);
    rebalancer.rebalanceTable(tableName, tenantName);
  }
}
