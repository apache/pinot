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
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Helper class for pinot-admin tool's RebalanceTable command.
 */
public class PinotTableRebalancer extends PinotZKChanger {
  private final RebalanceConfig _rebalanceConfig = new RebalanceConfig();

  public PinotTableRebalancer(String zkAddress, String clusterName, boolean dryRun, boolean reassignInstances,
      boolean includeConsuming, boolean bootstrap, boolean downtime, int minReplicasToKeepUpForNoDowntime,
      boolean lowDiskMode, boolean bestEffort, long externalViewCheckIntervalInMs,
      long externalViewStabilizationTimeoutInMs) {
    super(zkAddress, clusterName);
    _rebalanceConfig.setDryRun(dryRun);
    _rebalanceConfig.setReassignInstances(reassignInstances);
    _rebalanceConfig.setIncludeConsuming(includeConsuming);
    _rebalanceConfig.setBootstrap(bootstrap);
    _rebalanceConfig.setDowntime(downtime);
    _rebalanceConfig.setMinAvailableReplicas(minReplicasToKeepUpForNoDowntime);
    _rebalanceConfig.setLowDiskMode(lowDiskMode);
    _rebalanceConfig.setBestEfforts(bestEffort);
    _rebalanceConfig.setExternalViewCheckIntervalInMs(externalViewCheckIntervalInMs);
    _rebalanceConfig.setExternalViewStabilizationTimeoutInMs(externalViewStabilizationTimeoutInMs);
  }

  public RebalanceResult rebalance(String tableNameWithType) {
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: " + tableNameWithType);
    return new TableRebalancer(_helixManager).rebalance(tableConfig, _rebalanceConfig,
        TableRebalancer.createUniqueRebalanceJobIdentifier());
  }
}
