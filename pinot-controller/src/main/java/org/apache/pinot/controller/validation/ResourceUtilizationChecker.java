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
package org.apache.pinot.controller.validation;

import com.google.common.collect.BiMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.core.periodictask.BasePeriodicTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is responsible for checking resource utilization for Pinot instances. To begin with, it checks
 * disk utilization for all server instances. The computed disk utilization is stored in the class
 * <code>org.apache.pinot.controller.validation.ResourceUtilizationInfo</code>.
 */
public class ResourceUtilizationChecker extends BasePeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUtilizationChecker.class);
  private final static String TASK_NAME = ResourceUtilizationChecker.class.getSimpleName();

  private final PoolingHttpClientConnectionManager _connectionManager;
  private final ControllerMetrics _controllerMetrics;
  private final DiskUtilizationChecker _diskUtilizationChecker;
  private final Executor _executor;
  private final PinotHelixResourceManager _helixResourceManager;

  public ResourceUtilizationChecker(ControllerConf config, PoolingHttpClientConnectionManager connectionManager,
      ControllerMetrics controllerMetrics, DiskUtilizationChecker diskUtilizationChecker, Executor executor,
      PinotHelixResourceManager pinotHelixResourceManager) {
    super(TASK_NAME, config.getResourceUtilizationCheckerFrequency(),
        config.getResourceUtilizationCheckerInitialDelay());
    _connectionManager = connectionManager;
    _controllerMetrics = controllerMetrics;
    _diskUtilizationChecker = diskUtilizationChecker;
    _executor = executor;
    _helixResourceManager = pinotHelixResourceManager;
  }

  @Override
  protected final void runTask(Properties periodicTaskProperties) {
    _controllerMetrics.addMeteredTableValue(_taskName, ControllerMeter.CONTROLLER_PERIODIC_TASK_RUN, 1L);
    Set<String> instances = new HashSet<>();
    try {
      Set<String> serverTenantNames = _helixResourceManager.getAllServerTenantNames();
      for (String serverTenantName : serverTenantNames) {
        Set<String> instancesForServerTenant = _helixResourceManager.getAllInstancesForServerTenant(serverTenantName);
        if (!instancesForServerTenant.isEmpty()) {
          instances.addAll(instancesForServerTenant);
        }
      }
      BiMap<String, String> instanceAdminEndpoints = _helixResourceManager.getDataInstanceAdminEndpoints(instances);
      BiMap<String, String> endpointsToInstances = instanceAdminEndpoints.inverse();
      CompletionServiceHelper completionServiceHelper =
          new CompletionServiceHelper(_executor, _connectionManager, endpointsToInstances);
      _diskUtilizationChecker.computeDiskUtilization(endpointsToInstances, completionServiceHelper);
    } catch (Exception e) {
      LOGGER.error("Caught exception while running task: {}", _taskName, e);
      _controllerMetrics.addMeteredTableValue(_taskName, ControllerMeter.CONTROLLER_PERIODIC_TASK_ERROR, 1L);
    }
  }
}
