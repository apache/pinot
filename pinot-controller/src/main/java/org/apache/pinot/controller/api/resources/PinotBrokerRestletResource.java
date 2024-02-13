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
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.BROKER_TAG, authorizations = {@Authorization(value = CommonConstants.SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(
    apiKeyAuthDefinitions = @ApiKeyAuthDefinition(
        name = HttpHeaders.AUTHORIZATION,
        in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = CommonConstants.SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotBrokerRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotBrokerRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_BROKER)
  @ApiOperation(value = "List tenants and tables to brokers mappings",
      notes = "List tenants and tables to brokers mappings")
  public Map<String, Map<String, List<String>>> listBrokersMapping(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state, @Context HttpHeaders headers) {
    Map<String, Map<String, List<String>>> resultMap = new HashMap<>();
    resultMap.put("tenants", getTenantsToBrokersMapping(state));
    resultMap.put("tables", getTablesToBrokersMapping(state, headers));
    return resultMap;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tenants")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_BROKER)
  @ApiOperation(value = "List tenants to brokers mappings", notes = "List tenants to brokers mappings")
  public Map<String, List<String>> getTenantsToBrokersMapping(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state) {
    Map<String, List<String>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllBrokerTenantNames().stream()
        .forEach(tenant -> resultMap.put(tenant, getBrokersForTenant(tenant, state)));
    return resultMap;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tenants/{tenantName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_BROKER)
  @ApiOperation(value = "List brokers for a given tenant", notes = "List brokers for a given tenant")
  public List<String> getBrokersForTenant(
      @ApiParam(value = "Name of the tenant", required = true) @PathParam("tenantName") String tenantName,
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state) {
    List<InstanceInfo> instanceInfoList = getBrokersForTenantV2(tenantName, state);
    List<String> tenantBrokers =
        instanceInfoList.stream().map(InstanceInfo::getInstanceName).collect(Collectors.toList());
    return tenantBrokers;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tables")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_BROKER)
  @ApiOperation(value = "List tables to brokers mappings", notes = "List tables to brokers mappings")
  public Map<String, List<String>> getTablesToBrokersMapping(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state, @Context HttpHeaders headers) {
    Map<String, List<String>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllRawTables(headers.getHeaderString(CommonConstants.DATABASE)).stream()
        .forEach(table -> resultMap.put(table, getBrokersForTable(table, null, state, headers)));
    return resultMap;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tables/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_BROKER)
  @ApiOperation(value = "List brokers for a given table", notes = "List brokers for a given table")
  public List<String> getBrokersForTable(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state, @Context HttpHeaders headers) {
    List<InstanceInfo> instanceInfoList = getBrokersForTableV2(tableName, tableTypeStr, state, headers);
    return instanceInfoList.stream().map(InstanceInfo::getInstanceName).collect(Collectors.toList());
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/brokers")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_BROKER)
  @ApiOperation(value = "List tenants and tables to brokers mappings",
      notes = "List tenants and tables to brokers mappings")
  public Map<String, Map<String, List<InstanceInfo>>> listBrokersMappingV2(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state, @Context HttpHeaders headers) {
    Map<String, Map<String, List<InstanceInfo>>> resultMap = new HashMap<>();
    resultMap.put("tenants", getTenantsToBrokersMappingV2(state));
    resultMap.put("tables", getTablesToBrokersMappingV2(state, headers));
    return resultMap;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/brokers/tenants")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_BROKER)
  @ApiOperation(value = "List tenants to brokers mappings", notes = "List tenants to brokers mappings")
  public Map<String, List<InstanceInfo>> getTenantsToBrokersMappingV2(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state) {
    Map<String, List<InstanceInfo>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllBrokerTenantNames().stream()
        .forEach(tenant -> resultMap.put(tenant, getBrokersForTenantV2(tenant, state)));
    return resultMap;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/brokers/tenants/{tenantName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_BROKER)
  @ApiOperation(value = "List brokers for a given tenant", notes = "List brokers for a given tenant")
  public List<InstanceInfo> getBrokersForTenantV2(
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

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/brokers/tables")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_BROKER)
  @ApiOperation(value = "List tables to brokers mappings", notes = "List tables to brokers mappings")
  public Map<String, List<InstanceInfo>> getTablesToBrokersMappingV2(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state, @Context HttpHeaders headers) {
    Map<String, List<InstanceInfo>> resultMap = new HashMap<>();
    String databaseName = headers.getHeaderString(CommonConstants.DATABASE);
    _pinotHelixResourceManager.getAllRawTables(databaseName).stream()
        .forEach(table -> resultMap.put(table, getBrokersForTableV2(table, null, state, headers)));
    return resultMap;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/brokers/tables/{tableName}")
  @Authorize(targetType = TargetType.CLUSTER, paramName = "tableName", action = Actions.Table.GET_BROKER)
  @ApiOperation(value = "List brokers for a given table", notes = "List brokers for a given table")
  public List<InstanceInfo> getBrokersForTableV2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state, @Context HttpHeaders headers) {
    tableName = _pinotHelixResourceManager.getActualTableName(tableName,
        headers.getHeaderString(CommonConstants.DATABASE));
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

  @POST
  @Path("/brokers/instances/{instanceName}/qps")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_QPS)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Enable/disable the query rate limiting for a broker instance",
      notes = "Enable/disable the query rate limiting for a broker instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public SuccessResponse toggleQueryRateLimiting(
      @ApiParam(value = "Broker instance name", required = true, example = "Broker_my.broker.com_30000")
      @PathParam("instanceName") String brokerInstanceName,
      @ApiParam(value = "ENABLE|DISABLE", allowableValues = "ENABLE, DISABLE", required = true) @QueryParam("state")
      String state) {
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
