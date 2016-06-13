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
package com.linkedin.pinot.tools;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.EqualityUtils;


public class PinotSegmentRebalancer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRebalancer.class);
  static final String rebalanceTableCmd = "rebalanceTable";
  static final String rebalanceTenantCmd = "rebalanceTenant";

  private ZKHelixAdmin helixAdmin;
  private String clusterName;
  private ZkHelixPropertyStore<ZNRecord> propertyStore;
  private ObjectMapper objectMapper;
  private boolean dryRun = true;

  public PinotSegmentRebalancer(String zkAddress, String clusterName, boolean dryRun) {
    this.clusterName = clusterName;
    helixAdmin = new ZKHelixAdmin(zkAddress);
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    String path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);
    propertyStore = new ZkHelixPropertyStore<>(zkAddress, serializer, path);
    objectMapper = new ObjectMapper();
    this.dryRun = dryRun;
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
          LOGGER.info("Mismatch: segment" + segment + " server:" + server + " state:" + state);
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
    int nRebalances = 0;
    for (ZNRecord znRecord : tableConfigs) {
      AbstractTableConfig tableConfig = AbstractTableConfig.fromZnRecord(znRecord);
      if (tableConfig.getTenantConfig().getServer().equals(rawTenantName)) {
        LOGGER.info(tableConfig.getTableName() + ":" + tableConfig.getTenantConfig().getServer());
        nRebalances++;
        rebalanceTable(tableConfig.getTableName(), tenantName);
      }
    }
    if (nRebalances == 0) {
      LOGGER.info("No tables found for tenant " + tenantName);
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

    final TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (!tableType.equals(TableType.OFFLINE)) {
      // Rebalancing works for offline tables, not any other.
      LOGGER.warn("Don't know how to rebalance table " + tableName);
      return;
    }
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

    TableNameBuilder builder = new TableNameBuilder(tableType);
    List<String> instancesInClusterWithTag = helixAdmin.getInstancesInClusterWithTag(clusterName, builder.forTable(tenantName));
    LOGGER.info("Current: Nodes:" + currentHosts);
    LOGGER.info("New Nodes:" + instancesInClusterWithTag);
    Map<String, Map<String, String>> currentMapping = currentIdealState.getRecord().getMapFields();
    ZNRecord newZnRecord = rebalanceStrategy
        .computePartitionAssignment(instancesInClusterWithTag, currentMapping, instancesInClusterWithTag);
    Map<String, Map<String, String>> newMapping = newZnRecord.getMapFields();
    LOGGER.info("Current segment Assignment:");
    printSegmentAssignment(currentMapping);
    LOGGER.info("Final segment Assignment:");
    printSegmentAssignment(newMapping);
    if (!dryRun) {
      if (EqualityUtils.isEqual(newMapping, currentMapping)) {
        LOGGER.info("Skipping rebalancing for table:" + tableName + " since its already balanced");
      } else {
        IdealState updatedIdealState = new IdealState(currentIdealState.getRecord());
        updatedIdealState.getRecord().setMapFields(newMapping);
        LOGGER.info("Updating the idealstate for table:" + tableName);
        helixAdmin.setResourceIdealState(clusterName, tableName, updatedIdealState);
        int diff = Integer.MAX_VALUE;
        Thread.sleep(3000);
        do {
          diff = isStable(tableName);
          if (diff == 0) {
            break;
          } else {
            LOGGER.info(
                "Waiting for externalView to match idealstate for table:" + tableName + " Num segments difference:" + diff);
            Thread.sleep(30000);
          }
        } while (diff > 0);
        LOGGER.info("Successfully rebalanced table:" + tableName);
      }
    }
  }

  private void printSegmentAssignment(Map<String, Map<String, String>> mapping) throws Exception {
    StringWriter sw = new StringWriter();
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(sw, mapping);
    LOGGER.info(sw.toString());
    Map<String, List<String>> serverToSegmentMapping = new TreeMap<>();
    for (String segment : mapping.keySet()) {
      Map<String, String> serverToStateMap = mapping.get(segment);
      for (String server : serverToStateMap.keySet()) {
        if (!serverToSegmentMapping.containsKey(server)) {
          serverToSegmentMapping.put(server, new ArrayList<String>());
        }
        serverToSegmentMapping.get(server).add(segment);
      }
    }
    DescriptiveStatistics stats = new DescriptiveStatistics();
    for (String server : serverToSegmentMapping.keySet()) {
      List<String> list = serverToSegmentMapping.get(server);
      LOGGER.info("server " + server + " has " + list.size() + " segments");
      stats.addValue(list.size());
    }
    LOGGER.info("Segment Distrbution stat");
    LOGGER.info(stats.toString());
  }

  private static void usage() {
    System.out.println(
        "Usage: PinotRebalancer [" + rebalanceTableCmd + "|"  + rebalanceTenantCmd + "] <zkAddress> <clusterName> <tableName|tenantName>");
    System.out.println("Example: " + rebalanceTableCmd + " localhost:2181 PinotCluster myTable_OFFLINE");
    System.out.println("         " + rebalanceTenantCmd + " localhost:2181 PinotCluster beanCounter");
    System.exit(1);
  }
  public static void main(String[] args) throws Exception {

    final boolean dryRun = true;
    if (args.length != 4) {
      usage();
    }
    final String subCmd = args[0];
    final String zkAddress = args[1];
    final String clusterName = args[2];
    final String tableOrTenant = args[3];
    PinotSegmentRebalancer rebalancer = new PinotSegmentRebalancer(zkAddress, clusterName, dryRun);
    if (subCmd.equals(rebalanceTenantCmd)) {
      rebalancer.rebalanceTenantTables(tableOrTenant);
    } else if (subCmd.equals(rebalanceTableCmd)) {
      rebalancer.rebalanceTable(tableOrTenant);
    } else {
      usage();
    }
    if (dryRun) {
      System.out.println("That was a dryrun");
    }
  }
}
