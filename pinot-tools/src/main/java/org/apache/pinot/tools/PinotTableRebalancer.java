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
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;


/**
 * Helper class for pinot-admin tool's RebalanceTable command.
 */
public class PinotTableRebalancer extends PinotZKChanger {
  private final Configuration _rebalanceConfig = new BaseConfiguration();

  public PinotTableRebalancer(String zkAddress, String clusterName, boolean dryRun, boolean reassignInstances,
      boolean includeConsuming, boolean bootstrap, boolean downtime, int minReplicasToKeepUpForNoDowntime,
      boolean bestEffort, long externalViewCheckIntervalInMs, long externalViewStabilizationTimeoutInMs) {
    super(zkAddress, clusterName);
    _rebalanceConfig.addProperty(RebalanceConfigConstants.DRY_RUN, dryRun);
    _rebalanceConfig.addProperty(RebalanceConfigConstants.REASSIGN_INSTANCES, reassignInstances);
    _rebalanceConfig.addProperty(RebalanceConfigConstants.INCLUDE_CONSUMING, includeConsuming);
    _rebalanceConfig.addProperty(RebalanceConfigConstants.BOOTSTRAP, bootstrap);
    _rebalanceConfig.addProperty(RebalanceConfigConstants.DOWNTIME, downtime);
    _rebalanceConfig.addProperty(RebalanceConfigConstants.MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME,
        minReplicasToKeepUpForNoDowntime);
    _rebalanceConfig.addProperty(RebalanceConfigConstants.BEST_EFFORTS, bestEffort);
    _rebalanceConfig.addProperty(RebalanceConfigConstants.EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS,
        externalViewCheckIntervalInMs);
    _rebalanceConfig.addProperty(RebalanceConfigConstants.EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS,
        externalViewStabilizationTimeoutInMs);
    _rebalanceConfig.addProperty(RebalanceConfigConstants.JOB_ID,
        TableRebalancer.createUniqueRebalanceJobIdentifier());
  }

  public RebalanceResult rebalance(String tableNameWithType) {
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: " + tableNameWithType);
    return new TableRebalancer(_helixManager).rebalance(tableConfig, _rebalanceConfig);
  }
}
