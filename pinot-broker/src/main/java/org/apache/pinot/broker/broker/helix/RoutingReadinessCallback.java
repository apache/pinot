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
package org.apache.pinot.broker.broker.helix;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.spi.utils.CommonConstants.Helix;

import static java.util.Objects.requireNonNull;


/**
 * Service status callback that checks whether routing is ready for all tables assigned to this broker.
 * Returns STARTING if any assigned table is missing routing entries.
 * Returns GOOD once routing exists for all assigned tables or if no tables are assigned.
 */
public class RoutingReadinessCallback implements ServiceStatus.ServiceStatusCallback {

  private static final String STATUS_EXTERNAL_VIEW_NOT_FOUND = "Broker resource external view not found";

  private final HelixAdmin _helixAdmin;
  private final RoutingManager _routingManager;
  private final String _clusterName;
  private final String _instanceId;
  private volatile ServiceStatus.Status _serviceStatus = ServiceStatus.Status.STARTING;

  public RoutingReadinessCallback(HelixAdmin helixAdmin, RoutingManager routingManager,
      String clusterName, String instanceId) {
    _helixAdmin = requireNonNull(helixAdmin, "helixAdmin");
    _routingManager = requireNonNull(routingManager, "routingManager");
    _clusterName = requireNonNull(clusterName, "clusterName");
    _instanceId = requireNonNull(instanceId, "instanceId");
  }

  @Override
  public synchronized ServiceStatus.Status getServiceStatus() {
    // Return cached GOOD status to avoid re-checking
    if (_serviceStatus == ServiceStatus.Status.GOOD) {
      return ServiceStatus.Status.GOOD;
    }

    ExternalView brokerResourceExternalView =
        _helixAdmin.getResourceExternalView(_clusterName, Helix.BROKER_RESOURCE_INSTANCE);

    if (brokerResourceExternalView == null) {
      return ServiceStatus.Status.STARTING;
    }

    // Find all tables this broker instance is online for
    List<String> onlineTables = getOnlineTables(brokerResourceExternalView);

    // If no tables are online, consider it GOOD
    if (onlineTables.isEmpty()) {
      _serviceStatus = ServiceStatus.Status.GOOD;
      return _serviceStatus;
    }

    // Check routing exists for all online tables
    for (String tableName : onlineTables) {
      if (!_routingManager.routingExists(tableName)) {
        return ServiceStatus.Status.STARTING;
      }
    }

    _serviceStatus = ServiceStatus.Status.GOOD;
    return _serviceStatus;
  }

  @Override
  public synchronized String getStatusDescription() {
    if (_serviceStatus == ServiceStatus.Status.GOOD) {
      return ServiceStatus.STATUS_DESCRIPTION_NONE;
    }

    ExternalView brokerResourceExternalView =
        _helixAdmin.getResourceExternalView(_clusterName, Helix.BROKER_RESOURCE_INSTANCE);

    if (brokerResourceExternalView == null) {
      return STATUS_EXTERNAL_VIEW_NOT_FOUND;
    }

    List<String> onlineTables = getOnlineTables(brokerResourceExternalView);

    if (onlineTables.isEmpty()) {
      return ServiceStatus.STATUS_DESCRIPTION_NONE;
    }

    // Find tables missing routing
    List<String> missingRoutingTables = new ArrayList<>();
    for (String tableName : onlineTables) {
      if (!_routingManager.routingExists(tableName)) {
        missingRoutingTables.add(tableName);
      }
    }

    if (missingRoutingTables.isEmpty()) {
      return ServiceStatus.STATUS_DESCRIPTION_NONE;
    }

    return String.format("Waiting for routing to be ready for %d/%d tables: %s",
        missingRoutingTables.size(), onlineTables.size(), missingRoutingTables);
  }

  private List<String> getOnlineTables(ExternalView brokerResourceExternalView) {
    List<String> onlineTables = new ArrayList<>();
    for (String tableName : brokerResourceExternalView.getPartitionSet()) {
      Map<String, String> stateMap = brokerResourceExternalView.getStateMap(tableName);
      if (stateMap != null && stateMap.containsKey(_instanceId)) {
        onlineTables.add(tableName);
      }
    }
    return onlineTables;
  }
}
