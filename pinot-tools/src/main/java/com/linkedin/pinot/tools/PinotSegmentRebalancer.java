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

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotSegmentRebalancer extends PinotZKChanger {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRebalancer.class);
  static final String rebalanceTableCmd = "rebalanceTable";
  static final String rebalanceTenantCmd = "rebalanceTenant";

  private boolean dryRun = true;

  public PinotSegmentRebalancer(String zkAddress, String clusterName, boolean dryRun) {
    super(zkAddress, clusterName);
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
      TableConfig tableConfig;
      try {
        tableConfig = TableConfig.fromZnRecord(znRecord);
      } catch (Exception e) {
        LOGGER.warn("Failed to parse table configuration for ZnRecord id: {}. Skipping", znRecord.getId());
        continue;
      }
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
    TableConfig tableConfig = TableConfig.fromZnRecord(znRecord);
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
    int numReplicasInIdealState = Integer.parseInt(currentIdealState.getReplicas());
    final TableConfig offlineTableConfig = ZKMetadataProvider.getOfflineTableConfig(propertyStore, tableName);
    final int numReplicasInTableConfig = Integer.parseInt(offlineTableConfig.getValidationConfig().getReplication());

    final int targetNumReplicas = numReplicasInTableConfig;
    if (numReplicasInTableConfig < numReplicasInIdealState) {
      // AutoRebalanceStrategy,computePartitionAssignment works correctly if we increase the number of partitions,
      // but not if we decrease it. We need to use the PinotNumReplicaChanger to reduce the number of replicas.
      LOGGER.info("You first need to reduce the number of replicas from {} to {} for table {}. Use the ChangeNumReplicas command",
          numReplicasInIdealState, numReplicasInTableConfig, tableName);
      return;
    }

    states.put("OFFLINE", 0);
    states.put("ONLINE", targetNumReplicas);
    Map<String, Map<String, String>> mapFields = currentIdealState.getRecord().getMapFields();
    Set<String> currentHosts = new HashSet<>();
    for (String segment : mapFields.keySet()) {
      currentHosts.addAll(mapFields.get(segment).keySet());
    }
    AutoRebalanceStrategy rebalanceStrategy = new AutoRebalanceStrategy(tableName, partitions, states);

    String serverTenant = TableNameBuilder.forType(tableType).tableNameWithType(tenantName);
    List<String> instancesInClusterWithTag = helixAdmin.getInstancesInClusterWithTag(clusterName, serverTenant);
    List<String> enabledInstancesWithTag =
        HelixHelper.getEnabledInstancesWithTag(helixAdmin, clusterName, serverTenant);
    LOGGER.info("Current nodes: {}", currentHosts);
    LOGGER.info("New nodes: {}", instancesInClusterWithTag);
    LOGGER.info("Enabled nodes: {}", enabledInstancesWithTag);
    Map<String, Map<String, String>> currentMapping = currentIdealState.getRecord().getMapFields();
    ZNRecord newZnRecord = rebalanceStrategy
        .computePartitionAssignment(instancesInClusterWithTag, enabledInstancesWithTag, currentMapping, new ClusterDataCache());
    final Map<String, Map<String, String>> newMapping = newZnRecord.getMapFields();
    LOGGER.info("Current segment Assignment:");
    printSegmentAssignment(currentMapping);
    LOGGER.info("Final segment Assignment:");
    printSegmentAssignment(newMapping);
    if (!dryRun) {
      if (EqualityUtils.isEqual(newMapping, currentMapping)) {
        LOGGER.info("Skipping rebalancing for table:" + tableName + " since its already balanced");
      } else {
        HelixHelper.updateIdealState(helixManager, tableName,
            new com.google.common.base.Function<IdealState, IdealState>() {
              @Nullable
              @Override
              public IdealState apply(@Nullable IdealState idealState) {
                for (String segmentId : newMapping.keySet()) {
                  Map<String, String> instanceStateMap = newMapping.get(segmentId);

                  idealState.getInstanceStateMap(segmentId).clear();
                  for (String instanceId : instanceStateMap.keySet()) {
                    idealState.setPartitionState(segmentId, instanceId, instanceStateMap.get(instanceId));
                  }
                }
                return idealState;
              }
            }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
        waitForStable(tableName);
        LOGGER.info("Successfully rebalanced table:" + tableName);
      }
    }
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
