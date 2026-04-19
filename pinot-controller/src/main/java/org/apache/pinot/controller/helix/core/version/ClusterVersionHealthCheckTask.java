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
package org.apache.pinot.controller.helix.core.version;

import java.util.Properties;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.core.periodictask.BasePeriodicTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic controller task that checks cluster version health and logs warnings when the
 * recommended rollout order ({@code CONTROLLER >= BROKER >= SERVER/MINION}) is violated.
 *
 * <p>This task is cluster-level (not table-level); a single leader controller runs it by
 * gating on {@link LeadControllerManager#isLeaderForTable(String)} with this task's own
 * name as the key. This guarantees the task fires even when the cluster has zero tables
 * or when the current controller is not leader of any existing table.
 *
 * <p>All findings are advisory (WARN-level log entries). No operation is blocked.
 */
public class ClusterVersionHealthCheckTask extends BasePeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterVersionHealthCheckTask.class);

  public static final String TASK_NAME = "ClusterVersionHealthCheckTask";

  private final LeadControllerManager _leadControllerManager;
  private final VersionCompatibilityService _versionCompatibilityService;

  public ClusterVersionHealthCheckTask(LeadControllerManager leadControllerManager, ControllerConf config,
      VersionCompatibilityService versionCompatibilityService) {
    super(TASK_NAME, config.getVersionHealthCheckFrequencyInSeconds(),
        ControllerConf.ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
    _leadControllerManager = leadControllerManager;
    _versionCompatibilityService = versionCompatibilityService;
  }

  @Override
  protected void runTask(Properties periodicTaskProperties) {
    if (!_leadControllerManager.isLeaderForTable(TASK_NAME)) {
      return;
    }
    try {
      CompatibilityCheckResult result = _versionCompatibilityService.checkRolloutOrderCompatibility();
      if (!result.isOk()) {
        LOGGER.warn("Cluster version health check detected rollout-order warnings: {}", result.getWarnings());
      } else {
        LOGGER.debug("Cluster version health check passed: {}", result.getMessage());
      }
    } catch (Exception e) {
      LOGGER.error("Unexpected error running cluster version health check", e);
    }
  }
}
