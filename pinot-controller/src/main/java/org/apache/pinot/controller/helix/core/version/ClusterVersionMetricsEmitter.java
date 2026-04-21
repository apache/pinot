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

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.core.periodictask.BasePeriodicTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic controller task that emits a per-(componentType, rawVersion) live-instance-count gauge
 * ({@link ControllerGauge#CLUSTER_INSTANCE_VERSION_COUNT}) so that mixed-version rollouts can be
 * observed from monitoring without scraping the {@code /cluster/versions} endpoint.
 *
 * <p><b>Leader-gated:</b> only the lead controller publishes, so each cluster emits exactly one
 * value per (componentType, rawVersion) key.
 *
 * <p><b>Override semantics:</b> the gauge is re-set on every run so a shrinking live-instance
 * count immediately reflects.  Labels present in the previous cycle but absent now (e.g. the last
 * instance at a given version was upgraded) are explicitly removed so the gauge does not linger
 * at a stale non-zero value.
 */
public class ClusterVersionMetricsEmitter extends BasePeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterVersionMetricsEmitter.class);

  public static final String TASK_NAME = "ClusterVersionMetricsEmitter";

  private final LeadControllerManager _leadControllerManager;
  private final VersionCompatibilityService _versionCompatibilityService;
  private final ControllerMetrics _controllerMetrics;

  // Labels emitted in the previous cycle; used to clear stale gauges when their (component,
  // version) combination disappears.
  private final Set<String> _previouslyEmittedSuffixes = new HashSet<>();

  public ClusterVersionMetricsEmitter(LeadControllerManager leadControllerManager, ControllerConf config,
      VersionCompatibilityService versionCompatibilityService, ControllerMetrics controllerMetrics) {
    super(TASK_NAME, config.getClusterVersionMetricsFrequencyInSeconds(),
        ControllerConf.ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
    _leadControllerManager = leadControllerManager;
    _versionCompatibilityService = versionCompatibilityService;
    _controllerMetrics = controllerMetrics;
  }

  @Override
  protected void runTask(Properties periodicTaskProperties) {
    if (!_leadControllerManager.isLeaderForTable(TASK_NAME)) {
      return;
    }
    try {
      ClusterVersionSummary summary = _versionCompatibilityService.getClusterVersionSummary();
      if (!summary.isDataAvailable()) {
        return;
      }
      Set<String> emittedThisCycle = new HashSet<>();
      for (ComponentVersionSummary comp : summary.getComponentSummaries().values()) {
        Map<String, Integer> versionCounts = comp.getVersionCounts();
        for (Map.Entry<String, Integer> e : versionCounts.entrySet()) {
          String suffix = comp.getComponentType() + "." + e.getKey();
          _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.CLUSTER_INSTANCE_VERSION_COUNT, suffix,
              e.getValue());
          emittedThisCycle.add(suffix);
        }
      }
      // Clear stale labels that were emitted before but not in this cycle.
      for (String stale : _previouslyEmittedSuffixes) {
        if (!emittedThisCycle.contains(stale)) {
          _controllerMetrics.removeGlobalGauge(stale, ControllerGauge.CLUSTER_INSTANCE_VERSION_COUNT);
        }
      }
      _previouslyEmittedSuffixes.clear();
      _previouslyEmittedSuffixes.addAll(emittedThisCycle);
    } catch (Exception e) {
      LOGGER.warn("Failed to emit cluster version metrics", e);
    }
  }
}
