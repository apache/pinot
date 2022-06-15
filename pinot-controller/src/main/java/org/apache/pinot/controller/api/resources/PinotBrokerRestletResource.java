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
package org.apache.pinot.controller.api.resources;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.services.PinotBrokerService;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/")
public class PinotBrokerRestletResource implements PinotBrokerService {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotBrokerRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Override
  public Map<String, Map<String, List<String>>> listBrokersMapping(String state) {
    Map<String, Map<String, List<String>>> resultMap = new HashMap<>();
    resultMap.put("tenants", getTenantsToBrokersMapping(state));
    resultMap.put("tables", getTablesToBrokersMapping(state));
    return resultMap;
  }

  @Override
  public Map<String, List<String>> getTenantsToBrokersMapping(String state) {
    Map<String, List<String>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllBrokerTenantNames().stream()
        .forEach(tenant -> resultMap.put(tenant, getBrokersForTenant(tenant, state)));
    return resultMap;
  }

  @Override
  public List<String> getBrokersForTenant(String tenantName, String state) {
    List<InstanceInfo> instanceInfoList = getBrokersForTenantV2(tenantName, state);
    List<String> tenantBrokers =
        instanceInfoList.stream().map(InstanceInfo::getInstanceName).collect(Collectors.toList());
    return tenantBrokers;
  }

  @Override
  public Map<String, List<String>> getTablesToBrokersMapping(String state) {
    Map<String, List<String>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllRawTables().stream()
        .forEach(table -> resultMap.put(table, getBrokersForTable(table, null, state)));
    return resultMap;
  }

  @Override
  public List<String> getBrokersForTable(String tableName, String tableTypeStr, String state) {
    List<InstanceInfo> instanceInfoList = getBrokersForTableV2(tableName, tableTypeStr, state);
    return instanceInfoList.stream().map(InstanceInfo::getInstanceName).collect(Collectors.toList());
  }

  @Override
  public Map<String, Map<String, List<InstanceInfo>>> listBrokersMappingV2(String state) {
    Map<String, Map<String, List<InstanceInfo>>> resultMap = new HashMap<>();
    resultMap.put("tenants", getTenantsToBrokersMappingV2(state));
    resultMap.put("tables", getTablesToBrokersMappingV2(state));
    return resultMap;
  }

  @Override
  public Map<String, List<InstanceInfo>> getTenantsToBrokersMappingV2(String state) {
    Map<String, List<InstanceInfo>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllBrokerTenantNames().stream()
        .forEach(tenant -> resultMap.put(tenant, getBrokersForTenantV2(tenant, state)));
    return resultMap;
  }

  @Override
  public List<InstanceInfo> getBrokersForTenantV2(String tenantName, String state) {
    if (!_pinotHelixResourceManager.getAllBrokerTenantNames().contains(tenantName)) {
      throw new ControllerApplicationException(LOGGER, String.format("Tenant '%s' not found.", tenantName),
          Response.Status.NOT_FOUND);
    }
    Set<InstanceConfig> tenantBrokers =
        new HashSet<>(_pinotHelixResourceManager.getAllInstancesConfigsForBrokerTenant(tenantName));
    Set<InstanceInfo> instanceInfoSet = tenantBrokers.stream()
        .map(x -> new InstanceInfo(x.getInstanceName(), x.getHostName(), Integer.parseInt(x.getPort())))
        .collect(Collectors.toSet());
    applyStateChanges(instanceInfoSet, state);
    return ImmutableList.copyOf(instanceInfoSet);
  }

  @Override
  public Map<String, List<InstanceInfo>> getTablesToBrokersMappingV2(String state) {
    Map<String, List<InstanceInfo>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllRawTables().stream()
        .forEach(table -> resultMap.put(table, getBrokersForTableV2(table, null, state)));
    return resultMap;
  }

  @Override
  public List<InstanceInfo> getBrokersForTableV2(String tableName, String tableTypeStr, String state) {
    try {
      List<String> tableNamesWithType = _pinotHelixResourceManager
          .getExistingTableNamesWithType(tableName, Constants.validateTableType(tableTypeStr));
      if (tableNamesWithType.isEmpty()) {
        throw new ControllerApplicationException(LOGGER, String.format("Table '%s' not found.", tableName),
            Response.Status.NOT_FOUND);
      }
      Set<InstanceConfig> tenantBrokers =
          new HashSet<>(_pinotHelixResourceManager.getBrokerInstancesConfigsFor(tableNamesWithType.get(0)));
      Set<InstanceInfo> instanceInfoSet = tenantBrokers.stream()
          .map(x -> new InstanceInfo(x.getInstanceName(), x.getHostName(), Integer.parseInt(x.getPort())))
          .collect(Collectors.toSet());
      applyStateChanges(instanceInfoSet, state);
      return ImmutableList.copyOf(instanceInfoSet);
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, String.format("Table '%s' not found.", tableName),
          Response.Status.NOT_FOUND);
    } catch (IllegalArgumentException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.FORBIDDEN);
    }
  }

  @Override
  public SuccessResponse toggleQueryRateLimiting(String brokerInstanceName, String state) {
    if (brokerInstanceName == null || !brokerInstanceName.startsWith("Broker_")) {
      throw new ControllerApplicationException(LOGGER,
          String.format("'%s' is not a valid broker instance name.", brokerInstanceName), Response.Status.BAD_REQUEST);
    }
    String stateInUpperCases = state.toUpperCase();
    validateQueryQuotaStateChange(stateInUpperCases);
    List<String> liveInstances = _pinotHelixResourceManager.getOnlineInstanceList();
    if (!liveInstances.contains(brokerInstanceName)) {
      throw new ControllerApplicationException(LOGGER, String.format("Instance '%s' not found.", brokerInstanceName),
          Response.Status.NOT_FOUND);
    }
    _pinotHelixResourceManager.toggleQueryQuotaStateForBroker(brokerInstanceName, stateInUpperCases);
    String msg = String
        .format("Set query rate limiting to: %s for all tables in broker: %s", stateInUpperCases, brokerInstanceName);
    LOGGER.info(msg);
    return new SuccessResponse(msg);
  }

  private void validateQueryQuotaStateChange(String state) {
    if (!"ENABLE".equals(state) && !"DISABLE".equals(state)) {
      throw new ControllerApplicationException(LOGGER, "Invalid query quota state: " + state,
          Response.Status.BAD_REQUEST);
    }
  }

  private void applyStateChanges(Set<InstanceInfo> brokers, String state) {
    if (state == null) {
      return;
    }

    List<String> onlineInstanceList = _pinotHelixResourceManager.getOnlineInstanceList();
    Set<InstanceInfo> onlineBrokers =
        brokers.stream().filter(x -> onlineInstanceList.contains(x.getInstanceName())).collect(Collectors.toSet());

    switch (state) {
      case CommonConstants.Helix.StateModel.BrokerResourceStateModel.ONLINE:
        brokers.retainAll(onlineBrokers);
        break;
      case CommonConstants.Helix.StateModel.BrokerResourceStateModel.OFFLINE:
        brokers.removeAll(onlineBrokers);
        break;
      default:
        break;
    }
  }
}
