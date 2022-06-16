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
package org.apache.pinot.controller.api.resources.services;

import com.google.common.collect.ImmutableList;
import io.swagger.annotations.ApiParam;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.resources.BrokersApiService;
import org.apache.pinot.controller.api.resources.Constants;
import org.apache.pinot.controller.api.resources.InstanceInfo;
import org.apache.pinot.controller.api.resources.NotFoundException;
import org.apache.pinot.controller.api.resources.SuccessResponse;
import org.apache.pinot.controller.api.resources.V2ApiService;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Service
public class V2ApiServiceImpl implements V2ApiService, BrokersApiService {
  public static final Logger LOGGER = LoggerFactory.getLogger(V2ApiServiceImpl.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  protected List<String> getBrokersForTable(String tableName, String tableTypeStr,
      String state) {
    List<InstanceInfo> instanceInfoList = getBrokersForTableV2(tableName, tableTypeStr, state);
    return instanceInfoList.stream().map(InstanceInfo::getInstanceName).collect(Collectors.toList());
  }

  @Override
  public Response getBrokersForTable(String tableName, String type, String state, SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok().entity(getBrokersForTable(tableName, type, state, securityContext)).build();
  }

  @Override
  public Response getBrokersForTenant(String tenantName, String state, SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok().entity(getBrokersForTenant(tenantName, state)).build();
  }

  protected List<String> getBrokersForTenant(String tenantName, String state) {
    List<InstanceInfo> instanceInfoList = getBrokersForTenantV2(tenantName, state);
    return instanceInfoList.stream().map(InstanceInfo::getInstanceName).collect(Collectors.toList());
  }

  protected Map<String, List<String>> getTablesToBrokersMapping(String state) {
    Map<String, List<String>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllRawTables()
        .forEach(table -> resultMap.put(table, getBrokersForTable(table, null, state)));
    return resultMap;
  }

  @Override
  public Response getTablesToBrokersMapping(String state, SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok().entity(getTablesToBrokersMapping(state)).build();
  }

  protected Map<String, List<String>> getTenantsToBrokersMapping(String state) {
    Map<String, List<String>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllBrokerTenantNames()
        .forEach(tenant -> resultMap.put(tenant, getBrokersForTenant(tenant, state)));
    return resultMap;
  }

  @Override
  public Response getTenantsToBrokersMapping(String state, SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok().entity(getTablesToBrokersMapping(state)).build();
  }

  @Override
  public Response listBrokersMapping(String state, SecurityContext securityContext)
      throws NotFoundException {
    Map<String, Map<String, List<String>>> resultMap = new HashMap<>();
    resultMap.put("tenants", getTenantsToBrokersMapping(state));
    resultMap.put("tables", getTablesToBrokersMapping(state));
    return Response.ok().entity(resultMap).build();
  }

  @Override
  public Response toggleQueryRateLimiting(String instanceName, @NotNull String state, SecurityContext securityContext)
      throws NotFoundException {
    if (instanceName == null || !instanceName.startsWith("Broker_")) {
      throw new ControllerApplicationException(LOGGER,
          String.format("'%s' is not a valid broker instance name.", instanceName), Response.Status.BAD_REQUEST);
    }
    String stateInUpperCases = state.toUpperCase();
    validateQueryQuotaStateChange(stateInUpperCases);
    List<String> liveInstances = _pinotHelixResourceManager.getOnlineInstanceList();
    if (!liveInstances.contains(instanceName)) {
      throw new ControllerApplicationException(LOGGER, String.format("Instance '%s' not found.", instanceName),
          Response.Status.NOT_FOUND);
    }
    _pinotHelixResourceManager.toggleQueryQuotaStateForBroker(instanceName, stateInUpperCases);
    String msg = String
        .format("Set query rate limiting to: %s for all tables in broker: %s", stateInUpperCases, instanceName);
    LOGGER.info(msg);
    return Response.ok().entity(new SuccessResponse(msg)).build();
  }

  private static void validateQueryQuotaStateChange(String state) {
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

  protected List<InstanceInfo> getBrokersForTenantV2(
      @ApiParam(value = "Name of the tenant", required = true) @PathParam("tenantName") String tenantName,
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state) {
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

  protected List<InstanceInfo> getBrokersForTableV2(String tableName, String tableTypeStr, String state) {
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

  protected Map<String, List<InstanceInfo>> getTablesToBrokersMappingV2(String state) {
    Map<String, List<InstanceInfo>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllRawTables()
        .forEach(table -> resultMap.put(table, getBrokersForTableV2(table, null, state)));
    return resultMap;
  }

  @Override
  public Response getBrokersForTableV2(String tableName, String type, String state, SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok().entity(getBrokersForTableV2(tableName, type, state)).build();
  }

  @Override
  public Response getBrokersForTenantV2(String tenantName, String state, SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok().entity(getBrokersForTenantV2(tenantName, state)).build();
  }

  @Override
  public Response getTablesToBrokersMappingV2(String state, SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok().entity(getTablesToBrokersMappingV2(state)).build();
  }

  protected Map<String, List<InstanceInfo>> getTenantsToBrokersMappingV2(String state) {
    Map<String, List<InstanceInfo>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllBrokerTenantNames()
        .forEach(tenant -> resultMap.put(tenant, getBrokersForTenantV2(tenant, state)));
    return resultMap;
  }

  @Override
  public Response getTenantsToBrokersMappingV2(String state, SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok().entity(getTenantsToBrokersMappingV2(state)).build();
  }

  @Override
  public Response listBrokersMappingV2(String state, SecurityContext securityContext)
      throws NotFoundException {
    Map<String, Map<String, List<InstanceInfo>>> resultMap = new HashMap<>();
    resultMap.put("tenants", getTenantsToBrokersMappingV2(state));
    resultMap.put("tables", getTablesToBrokersMappingV2(state));
    return Response.ok().entity(resultMap).build();
  }
}
