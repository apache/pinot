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
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.core.periodictask.BasePeriodicTask;


/**
 * A periodic task to clean up offline Minion instances to not spam Helix.
 */
public class MinionInstancesCleanupTask extends BasePeriodicTask {
  protected final PinotHelixResourceManager _pinotHelixResourceManager;
  protected final ControllerMetrics _controllerMetrics;

  public MinionInstancesCleanupTask(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf controllerConf,
      ControllerMetrics controllerMetrics) {
    super("MinionInstancesCleanupTask", controllerConf.getMinionInstancesCleanupTaskFrequencyInSeconds(),
        controllerConf.getMinionInstancesCleanupTaskInitialDelaySeconds());
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerMetrics = controllerMetrics;
  }

  @Override
  protected void runTask() {
    List<String> offlineInstances = new ArrayList<>(_pinotHelixResourceManager.getAllInstances());
    offlineInstances.removeAll(_pinotHelixResourceManager.getOnlineInstanceList());
    for (String offlineInstance : offlineInstances) {
      if (offlineInstance.startsWith(CommonConstants.Helix.PREFIX_OF_MINION_INSTANCE)) {
        PinotResourceManagerResponse response = _pinotHelixResourceManager.dropInstance(offlineInstance);
        if (response.isSuccessful()) {
          _controllerMetrics.addValueToGlobalGauge(ControllerGauge.DROPPED_MINION_INSTANCES, 1);
        }
      }
    }
  }
}
