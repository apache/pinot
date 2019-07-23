/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.controller.helix.core.TableRebalancer;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSegmentStrategy;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSegmentStrategyFactory;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceUserConfigConstants;


/**
 * Helper class for pinot-admin tool's RebalanceTable command.
 * Serves as an entry point into {@link TableRebalancer} controller
 * code which does the rebalancing.
 */
public class PinotTableRebalancer extends PinotZKChanger {
  private final boolean _noDowntime;
  private final boolean _includeConsuming;
  private final boolean _dryRun;
  private final int _minReplicasToKeepUpForNoDowntime;
  private TableRebalancer.RebalancerStats _rebalancerStats;

  /**
   * Creates an instance of PinotTableRebalancer
   * @param zkAddress zookeeper address of the cluster
   * @param pinotClusterName pinot cluster name
   * @param dryRun true if rebalancer should run in dry run mode (just return final ideal state), false otherwise
   * @param noDowntime true if rebalancer should run without downtime, false otherwise
   * @param includeConsuming true if rebalancer should include consuming segments
   *                         while rebalancing, false otherwise
   * @param minReplicasToKeepUpForNoDowntime applicable for noDowntime approach
   */
  public PinotTableRebalancer(final String zkAddress, final String pinotClusterName, final boolean dryRun,
      final boolean noDowntime, final boolean includeConsuming, final int minReplicasToKeepUpForNoDowntime) {
    super(zkAddress, pinotClusterName);
    _noDowntime = noDowntime;
    _includeConsuming = includeConsuming;
    _dryRun = dryRun;
    _minReplicasToKeepUpForNoDowntime = minReplicasToKeepUpForNoDowntime;
  }

  public boolean rebalance(final String tableName, final String tableType)
      throws Exception {
    Preconditions.checkArgument(tableName != null && tableName.length() > 0, "Expecting a valid table name");
    Preconditions.checkArgument(
        tableType != null && (tableType.equalsIgnoreCase("OFFLINE") || tableType.equalsIgnoreCase("REALTIME")),
        "Expecting a valid table type (OFFLINE or REALTIME");

    final Configuration rebalanceUserConfig = new PropertiesConfiguration();
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.DRYRUN, _dryRun);
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.INCLUDE_CONSUMING, _includeConsuming);
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.DOWNTIME, !_noDowntime);

    if (_noDowntime) {
      rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME,
          _minReplicasToKeepUpForNoDowntime);
    }

    final TableType tabType = TableType.valueOf(tableType);
    final TableConfig tableConfig = getTableConfig(tableName, tabType);
    final TableRebalancer tableRebalancer = new TableRebalancer(helixManager, helixAdmin, clusterName);
    final RebalanceSegmentStrategyFactory rebalanceSegmentStrategyFactory =
        new RebalanceSegmentStrategyFactory(helixManager);
    final RebalanceSegmentStrategy rebalanceSegmentsStrategy =
        rebalanceSegmentStrategyFactory.getRebalanceSegmentsStrategy(tableConfig);
    final RebalanceResult rebalanceResult =
        tableRebalancer.rebalance(tableConfig, rebalanceSegmentsStrategy, rebalanceUserConfig);

    if (rebalanceResult.getStatus() == RebalanceResult.RebalanceStatus.FAILED) {
      System.out.println("Failed to rebalance table: " + tableName);
      return false;
    } else {
      System.out.println("Successfully rebalanced table: " + tableName);
      System.out.println("Ideal state");
      Map<String, Map<String, String>> idealState = rebalanceResult.getIdealStateMapping();
      for (Map.Entry<String, Map<String, String>> segments : idealState.entrySet()) {
        System.out.println("Segment: " + segments.getKey());
        final Map<String, String> segmentHostsMap = segments.getValue();
        for (Map.Entry<String, String> host : segmentHostsMap.entrySet()) {
          System.out.println(host.getKey() + ":" + host.getValue());
        }
      }
      _rebalancerStats = tableRebalancer.getRebalancerStats();
      return true;
    }
  }

  private TableConfig getTableConfig(final String tableName, final TableType tableType) {
    if (tableType == TableType.OFFLINE) {
      return ZKMetadataProvider.getOfflineTableConfig(propertyStore, tableName);
    } else {
      return ZKMetadataProvider.getRealtimeTableConfig(propertyStore, tableName);
    }
  }

  public TableRebalancer.RebalancerStats getRebalancerStats() {
    return _rebalancerStats;
  }
}
