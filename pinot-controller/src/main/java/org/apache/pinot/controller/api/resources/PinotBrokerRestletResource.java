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

import com.fasterxml.jackson.databind.JsonNode;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
import org.apache.commons.io.IOUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.BROKER_TAG, authorizations = {
    @Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)
})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")
}))
@Path("/")
public class PinotBrokerRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotBrokerRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

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
    return _pinotHelixResourceManager.getAllRawTables(headers.getHeaderString(DATABASE)).stream()
        .collect(Collectors.toMap(table -> table, table -> getBrokersForTable(table, null, state, headers)));
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
        .map(x -> new InstanceInfo(x.getInstanceName(), x.getHostName(), Integer.parseInt(x.getPort()),
            Integer.parseInt(x.getPort())))
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
    return _pinotHelixResourceManager.getAllRawTables(headers.getHeaderString(DATABASE)).stream()
        .collect(Collectors.toMap(table -> table, table -> getBrokersForTableV2(table, null, state, headers)));
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
    tableName = DatabaseUtils.translateTableName(tableName, headers);
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
          .map(x -> new InstanceInfo(x.getInstanceName(), x.getHostName(), Integer.parseInt(x.getPort()),
              Integer.parseInt(x.getPort())))
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

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/timeseries/languages")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_BROKER)
  @ApiOperation(value = "Get timeseries languages from brokers", notes = "Get timeseries languages from brokers")
  public List<String> getBrokerTimeSeriesLanguages(@Context HttpHeaders headers) {
    try {
      List<String> brokerInstanceIds = _pinotHelixResourceManager.getAllBrokerInstances();
      if (brokerInstanceIds.isEmpty()) {
        throw new ControllerApplicationException(LOGGER, "No broker instances found.", Response.Status.NOT_FOUND);
      }

      InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(brokerInstanceIds.get(0));
      if (instanceConfig == null) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Instance config not found for broker instance: %s", brokerInstanceIds.get(0)),
            Response.Status.NOT_FOUND);
      }

      String url = String.format("%s://%s:%s/timeseries/languages",
          _controllerConf.getControllerBrokerProtocol(),
          instanceConfig.getHostName(),
          instanceConfig.getPort());

      String response = sendRequestRaw(url, "GET", "", null, Map.of());
      JsonNode responseNode = JsonUtils.stringToJsonNode(response);

      List<String> languages = new ArrayList<>();
      if (responseNode.isArray()) {
        responseNode.forEach(langNode -> {
          if (langNode.isTextual()) {
            languages.add(langNode.asText());
          }
        });
      }

      return languages;
    } catch (Exception e) {
      LOGGER.error("Error fetching timeseries languages from brokers", e);
      throw new ControllerApplicationException(LOGGER,
          "Error fetching timeseries languages from brokers: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
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

  private String sendRequestRaw(String urlString, String method, String body, JsonNode jsonNode,
      Map<String, String> headers) throws IOException {
    URL url = new URL(urlString);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setRequestProperty("Accept", "application/json");

    // Add headers
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      connection.setRequestProperty(entry.getKey(), entry.getValue());
    }

    // Write body if present
    if (jsonNode != null) {
      try (OutputStream os = connection.getOutputStream()) {
        byte[] input = JsonUtils.objectToBytes(jsonNode);
        os.write(input, 0, input.length);
      }
    }

    // Read response
    try (InputStream is = connection.getInputStream();
        InputStream errorStream = connection.getErrorStream()) {
      if (errorStream != null) {
        String error = IOUtils.toString(errorStream, StandardCharsets.UTF_8);
        LOGGER.error("Error response from broker: {}", error);
        throw new IOException("Error response from broker: " + error);
      }
      return IOUtils.toString(is, StandardCharsets.UTF_8);
    }
  }
}
