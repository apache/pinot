package com.linkedin.pinot.tools;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.zookeeper.data.Stat;
import org.testng.collections.Lists;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.Tenant.TenantBuilder;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.common.utils.ZkUtils;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.core.common.Constants;


public class PinotSegmentRebalancer {
  private ZKHelixAdmin helixAdmin;
  private String clusterName;
  private ZkHelixPropertyStore<ZNRecord> propertyStore;
  private ObjectMapper objectMapper;

  public PinotSegmentRebalancer(String zkAddress, String clusterName) {
    this.clusterName = clusterName;
    helixAdmin = new ZKHelixAdmin(zkAddress);
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    String path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);
    propertyStore = new ZkHelixPropertyStore<>(zkAddress, serializer, path);
    objectMapper = new ObjectMapper();
  }

  /**
   * return true if IdealState = ExternalView
   * @return
   */
  public int isStable(String tableName) {
    IdealState idealState = helixAdmin.getResourceIdealState(clusterName, tableName);
    ExternalView externalView = helixAdmin.getResourceExternalView(clusterName, tableName);
    Map<String, Map<String, String>> mapFieldsIS = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> mapFieldsEV = externalView.getRecord().getMapFields();
    int numDiff = 0;
    for (String segment : mapFieldsIS.keySet()) {
      Map<String, String> mapIS = mapFieldsIS.get(segment);
      Map<String, String> mapEV = mapFieldsEV.get(segment);

      for (String server : mapIS.keySet()) {
        String state = mapIS.get(server);
        if (mapEV == null || mapEV.get(server) == null || !mapEV.get(server).equals(state)) {
          System.out.println("Mismatch: segment" + segment + " server:" + server + " state:" + state);
          numDiff = numDiff + 1;
        }
      }
    }
    return numDiff;
  }

  /**
   * rebalances all tables for the tenant
   * @param tenantName
   */
  public void rebalanceTenantTables(String tenantName) throws Exception {
    String tableConfigPath = "/CONFIGS/TABLE";
    List<Stat> stats = new ArrayList<>();
    List<ZNRecord> tableConfigs = propertyStore.getChildren(tableConfigPath, stats, 0);
    String rawTenantName = tenantName.replaceAll("_OFFLINE", "").replace("_REALTIME", "");
    for (ZNRecord znRecord : tableConfigs) {
      AbstractTableConfig tableConfig = AbstractTableConfig.fromZnRecord(znRecord);
      if (tableConfig.getTenantConfig().getServer().equals(rawTenantName)) {
        System.out.println(tableConfig.getTableName() + ":" + tableConfig.getTenantConfig().getServer());
        rebalanceTable(tableConfig.getTableName(), tenantName);
      }
    }
  }

  /**
   * Rebalances a table
   * @param tableName
   * @throws Exception
   */
  public void rebalanceTable(String tableName) throws Exception {
    String tableConfigPath = "/CONFIGS/TABLE/" + tableName;
    Stat stat = new Stat();
    ZNRecord znRecord = propertyStore.get(tableConfigPath, stat, 0);
    AbstractTableConfig tableConfig = AbstractTableConfig.fromZnRecord(znRecord);
    String tenantName = tableConfig.getTenantConfig().getServer().replaceAll(TableType.OFFLINE.toString(), "")
        .replace(TableType.OFFLINE.toString(), "");
    rebalanceTable(tableName, tenantName);
  }

  /**
   * Rebalances a table within a tenant
   * @param tableName
   * @param tenantName
   * @throws Exception
   */
  public void rebalanceTable(String tableName, String tenantName) throws Exception {

    IdealState currentIdealState = helixAdmin.getResourceIdealState(clusterName, tableName);
    List<String> partitions = Lists.newArrayList(currentIdealState.getPartitionSet());
    LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
    states.put("OFFLINE", 0);
    states.put("ONLINE", Integer.parseInt(currentIdealState.getReplicas()));
    Map<String, Map<String, String>> mapFields = currentIdealState.getRecord().getMapFields();
    Set<String> currentHosts = new HashSet<>();
    for (String segment : mapFields.keySet()) {
      currentHosts.addAll(mapFields.get(segment).keySet());
    }
    AutoRebalanceStrategy rebalanceStrategy = new AutoRebalanceStrategy(tableName, partitions, states);

    List<String> instancesInClusterWithTag = helixAdmin.getInstancesInClusterWithTag(clusterName, tenantName);
    System.out.println("Current: Nodes:" + currentHosts);
    System.out.println("New Nodes:" + instancesInClusterWithTag);
    Map<String, Map<String, String>> currentMapping = currentIdealState.getRecord().getMapFields();
    ZNRecord newZnRecord = rebalanceStrategy.computePartitionAssignment(instancesInClusterWithTag, currentMapping,
        instancesInClusterWithTag);
    Map<String, Map<String, String>> newMapping = newZnRecord.getMapFields();
    System.out.println("Current segment Assignment:");
    printSegmentAssignment(currentMapping);
    System.out.println("Current segment Assignment:");
    printSegmentAssignment(newMapping);
    if (EqualityUtils.isEqual(newMapping, currentMapping)) {
      System.out.println("Skipping rebalancing for table:" + tableName + " since its already balanced");
    } else {
      IdealState updatedIdealState = new IdealState(currentIdealState.getRecord());
      updatedIdealState.getRecord().setMapFields(newMapping);
      System.out.println("Updating the idealstate for table:" + tableName);
      helixAdmin.setResourceIdealState(clusterName, tableName, updatedIdealState);
      int diff = Integer.MAX_VALUE;
      do {
        Thread.sleep(30000);
        diff = isStable(tableName);

        System.out.println(
            "Waiting for externalView to match idealstate for table:" + tableName + " Num segments difference");
      } while (diff > 0);
      System.out.println("Successfully rebalanced table:" + tableName);
    }
  }

  private void printSegmentAssignment(Map<String, Map<String, String>> mapping) throws Exception {
    StringWriter sw = new StringWriter();
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(sw, mapping);
    System.out.println(sw.toString());
    Map<String, List<String>> serverToSegmentMapping = new TreeMap<>();
    for (String segment : mapping.keySet()) {
      Map<String, String> serverToStateMap = mapping.get(segment);
      for (String server : serverToStateMap.keySet()) {
        if (!serverToSegmentMapping.containsKey(server)) {
          serverToSegmentMapping.put(server, new ArrayList<>());
        }
        serverToSegmentMapping.get(server).add(segment);
      }
    }
    DescriptiveStatistics stats = new DescriptiveStatistics();
    for (String server : serverToSegmentMapping.keySet()) {
      List<String> list = serverToSegmentMapping.get(server);
      System.out.println("server " + server + " has " + list.size() + " segments");
      stats.addValue(list.size());
    }
    System.out.println("Segment Distrbution stat");
    System.out.println(stats);
  }

  /**
   * USAGE PinotRebalancer --[rebalanceTable|rebalanceTenant] <zkAddress> <clusterName> <tableName>
   *    
   * @param args
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws Exception {

    String zkAddress;
    String clusterName;
    String tableName;
    String tenantName;
    if (args.length == 4) {
      zkAddress = args[0];
      clusterName = args[1];
      tableName = args[2];
      tenantName = args[3];
      PinotSegmentRebalancer rebalancer = new PinotSegmentRebalancer(zkAddress, clusterName);
      //rebalancer.rebalanceTable(tableName, tenantName);
      //System.out.println(rebalancer.isStable(tableName));
      rebalancer.rebalanceTenantTables(tenantName);
    } else {
      System.out.println(
          "USAGE PinotRebalancer [rebalanceTable|rebalanceTenant] <zkAddress> <clusterName> <tableName|tenantName>");
      System.exit(1);
    }
  }
}
