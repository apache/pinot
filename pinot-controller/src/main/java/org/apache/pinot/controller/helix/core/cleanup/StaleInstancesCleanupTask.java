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
package org.apache.pinot.controller.helix.core.cleanup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.core.periodictask.BasePeriodicTask;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Automatically removes stale instances from the cluster to not spam Helix.
 * Stale instance is the instance not in use (not hosting any data or query) and has been in the offline status for more
 * than the stale instance retention time.
 */
public class StaleInstancesCleanupTask extends BasePeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(StaleInstancesCleanupTask.class);
  private final static String TASK_NAME = "StaleInstancesCleanupTask";

  protected final PinotHelixResourceManager _pinotHelixResourceManager;
  protected final LeadControllerManager _leadControllerManager;
  protected final ControllerMetrics _controllerMetrics;
  // This applies to both broker and server instances.
  private final long _staleInstancesCleanupTaskMinOfflineTimeBeforeDeletionInMilliseconds;

  public StaleInstancesCleanupTask(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    super(TASK_NAME, controllerConf.getStaleInstancesCleanupTaskFrequencyInSeconds(),
        controllerConf.getStaleInstanceCleanupTaskInitialDelaySeconds());
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _leadControllerManager = leadControllerManager;
    _controllerMetrics = controllerMetrics;
    _staleInstancesCleanupTaskMinOfflineTimeBeforeDeletionInMilliseconds =
        controllerConf.getStaleInstancesCleanupTaskInstancesRetentionInSeconds() * 1000L;
  }

  @Override
  protected void runTask(Properties periodicTaskProperties) {
    // Make it so that only one controller is responsible for cleaning up minion instances.
    if (!_leadControllerManager.isLeaderForTable(TASK_NAME)) {
      return;
    }

    List<String> offlineInstances = new ArrayList<>(_pinotHelixResourceManager.getAllInstances());
    offlineInstances.removeAll(_pinotHelixResourceManager.getOnlineInstanceList());

    Set<String> serverInstancesInUse = getServerInstancesInUse();
    Set<String> brokerInstancesInUse = getBrokerInstancesInUse();

    for (String offlineInstance : offlineInstances) {
      // Since ZNodes under "/LIVEINSTANCES" are ephemeral, if there is a ZK session expire (e.g. due to network issue),
      // the ZNode under "/LIVEINSTANCES" will be deleted. Thus, such race condition can happen when this task is
      // running.
      // In order to double confirm the live status of an instance, the field "LAST_OFFLINE_TIME" in ZNode under
      // "/INSTANCES/<instance_id>/HISTORY" needs to be checked. If the value is "-1", that means the instance is
      // ONLINE;
      // if the value is a timestamp, that means the instance starts to be OFFLINE since that time.
      if (InstanceTypeUtils.isMinion(offlineInstance)) {
        // Drop the minion instance if it has been offline for more than a period of this task.
        if (_pinotHelixResourceManager.isInstanceOfflineFor(offlineInstance,
            _staleInstancesCleanupTaskMinOfflineTimeBeforeDeletionInMilliseconds)) {
          LOGGER.info("Dropping minion instance: {}", offlineInstance);
          PinotResourceManagerResponse response = _pinotHelixResourceManager.dropInstance(offlineInstance);
          if (response.isSuccessful()) {
            _controllerMetrics.addValueToGlobalGauge(ControllerGauge.DROPPED_MINION_INSTANCES, 1);
          }
        }
        continue;
      }

      // Drop the broker instance if it has been offline for more than a period of this task.
      if (InstanceTypeUtils.isBroker(offlineInstance) && !brokerInstancesInUse.contains(offlineInstance)) {
        if (_pinotHelixResourceManager.isInstanceOfflineFor(offlineInstance,
            _staleInstancesCleanupTaskMinOfflineTimeBeforeDeletionInMilliseconds)) {
          LOGGER.info("Dropping broker instance: {}", offlineInstance);
          PinotResourceManagerResponse response = _pinotHelixResourceManager.dropInstance(offlineInstance);
          if (response.isSuccessful()) {
            _controllerMetrics.addValueToGlobalGauge(ControllerGauge.DROPPED_BROKER_INSTANCES, 1);
          }
        }
        continue;
      }

      // Drop the server instance if it has been offline for more than a period of this task.
      if (InstanceTypeUtils.isServer(offlineInstance) && !serverInstancesInUse.contains(offlineInstance)) {
        if (_pinotHelixResourceManager.isInstanceOfflineFor(offlineInstance,
            _staleInstancesCleanupTaskMinOfflineTimeBeforeDeletionInMilliseconds)) {
          LOGGER.info("Dropping server instance: {}", offlineInstance);
          PinotResourceManagerResponse response = _pinotHelixResourceManager.dropInstance(offlineInstance);
          if (response.isSuccessful()) {
            _controllerMetrics.addValueToGlobalGauge(ControllerGauge.DROPPED_SERVER_INSTANCES, 1);
          }
        }
      }
    }
  }

  private Set<String> getBrokerInstancesInUse() {
    Set<String> brokerInstancesInUse = new HashSet<>();
    final IdealState brokerResource = _pinotHelixResourceManager.getHelixAdmin()
        .getResourceIdealState(_pinotHelixResourceManager.getHelixClusterName(),
            CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    brokerResource.getPartitionSet()
        .forEach(table -> brokerInstancesInUse.addAll(brokerResource.getInstanceSet(table)));
    return brokerInstancesInUse;
  }

  private Set<String> getServerInstancesInUse() {
    Set<String> serverInstancesInUse = new HashSet<>();
    _pinotHelixResourceManager.getAllTables().forEach(tableName -> serverInstancesInUse.addAll(
        Optional.ofNullable(_pinotHelixResourceManager.getTableIdealState(tableName))
            .map(is -> is.getInstanceSet(tableName)).orElse(Collections.emptySet())));
    return serverInstancesInUse;
  }
}
