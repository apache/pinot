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
package org.apache.pinot.controller.helix.core.minion;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.core.periodictask.BasePeriodicTask;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A periodic task to clean up offline Minion instances to not spam Helix.
 */
public class MinionInstancesCleanupTask extends BasePeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionInstancesCleanupTask.class);
  private final static String TASK_NAME = "MinionInstancesCleanupTask";
  protected final PinotHelixResourceManager _pinotHelixResourceManager;
  protected final LeadControllerManager _leadControllerManager;
  protected final ControllerMetrics _controllerMetrics;
  private final long _minionInstanceCleanupTaskMinOfflineTimeBeforeDeletionInMilliseconds;

  public MinionInstancesCleanupTask(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    super(TASK_NAME, controllerConf.getMinionInstancesCleanupTaskFrequencyInSeconds(),
        controllerConf.getMinionInstancesCleanupTaskInitialDelaySeconds());
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _leadControllerManager = leadControllerManager;
    _controllerMetrics = controllerMetrics;
    _minionInstanceCleanupTaskMinOfflineTimeBeforeDeletionInMilliseconds =
        controllerConf.getMinionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds() * 1000L;
  }

  /**
   * @param periodicTaskParameters Currently not used, but can be used to specify how this task should be run.
   */
  @Override
  protected void runTask(@Nullable String periodicTaskParameters) {
    // Make it so that only one controller is responsible for cleaning up minion instances.
    if (!_leadControllerManager.isLeaderForTable(TASK_NAME)) {
      return;
    }

    List<String> offlineInstances = new ArrayList<>(_pinotHelixResourceManager.getAllInstances());
    offlineInstances.removeAll(_pinotHelixResourceManager.getOnlineInstanceList());
    for (String offlineInstance : offlineInstances) {
      // Since ZNodes under "/LIVEINSTANCES" are ephemeral, if there is a ZK session expire (e.g. due to network issue),
      // the ZNode under "/LIVEINSTANCES" will be deleted. Thus, such race condition can happen when this task is running.
      // In order to double confirm the live status of an instance, the field "LAST_OFFLINE_TIME" in ZNode under
      // "/INSTANCES/<instance_id>/HISTORY" needs to be checked. If the value is "-1", that means the instance is ONLINE;
      // if the value is a timestamp, that means the instance starts to be OFFLINE since that time.
      if (offlineInstance.startsWith(CommonConstants.Helix.PREFIX_OF_MINION_INSTANCE)) {
        // Drop the minion instance if it has been offline for more than a period of this task.
        if (_pinotHelixResourceManager
            .isInstanceOfflineFor(offlineInstance, _minionInstanceCleanupTaskMinOfflineTimeBeforeDeletionInMilliseconds)) {
          LOGGER.info("Dropping minion instance: {}", offlineInstance);
          PinotResourceManagerResponse response = _pinotHelixResourceManager.dropInstance(offlineInstance);
          if (response.isSuccessful()) {
            _controllerMetrics.addValueToGlobalGauge(ControllerGauge.DROPPED_MINION_INSTANCES, 1);
          }
        }
      }
    }
  }
}
