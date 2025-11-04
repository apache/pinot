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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
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
import org.apache.pinot.common.utils.config.QueryWorkloadConfigUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;

@Api(tags = Constants.QUERY_WORKLOAD_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key =
        SWAGGER_AUTHORIZATION_KEY, description =
        "The format of the key is  ```\"Basic <token>\" or \"Bearer "
            + "<token>\"```"), @ApiKeyAuthDefinition(name = CommonConstants.QUERY_WORKLOAD, in =
    ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = CommonConstants.QUERY_WORKLOAD, description =
    "Workload context passed through http header. If no context is provided 'default' workload "
        + "context will be considered.")
}))
@Path("/")
public class PinotQueryWorkloadRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotQueryWorkloadRestletResource.class);
  public static final String WORKLOAD = "workload";
  public static final String TABLE = "table";
  public static final String TENANT = "tenant";

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/queryWorkloadConfigs")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_QUERY_WORKLOAD_CONFIG)
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Get all query workload configs", notes = "Get all workload configs")
  public String getQueryWorkloadConfigs(@Context HttpHeaders httpHeaders) {
    try {
      LOGGER.info("Received request to get all queryWorkloadConfigs");
      List<QueryWorkloadConfig> queryWorkloadConfigs = _pinotHelixResourceManager.getAllQueryWorkloadConfigs();
      if (queryWorkloadConfigs.isEmpty()) {
        return JsonUtils.objectToString(Map.of());
      }
      String response = JsonUtils.objectToString(queryWorkloadConfigs);
      LOGGER.info("Successfully fetched all queryWorkloadConfigs");
      return response;
    } catch (Exception e) {
      String errorMessage = String.format("Error while getting all workload configs, error: %s", e);
      throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Retrieves the query workload configuration for the specified workload name.
   * <p>
   * This API returns the detailed configuration including node-specific settings,
   * enforcement profiles, propagation schemes, and cost splits.
   * </p>
   *
   * See {@link org.apache.pinot.spi.config.workload.PropagationScheme} and {@link EnforcementProfile} for more details
   * on the configuration definition and what each field means.
   * <p><strong>Example:</strong></p>
   * <pre>{@code
   * {
   *   "queryWorkloadName": "workload-foo1",
   *   "nodeConfigs": [
   *     {
   *       "nodeType": "brokerNode",
   *       "enforcementProfile": {
   *         "cpuCostNs": 500,
   *         "memoryCostBytes": 1000
   *       },
   *       "propagationScheme": {
   *         "propagationType": "TABLE",
   *         "propagationEntities": [
   *           {
   *             "entity": "airlineStats",
   *             "cpuCostNs": 250,
   *             "memoryCostBytes": 500
   *           },
   *           {
   *             "entity": "baseballStats",
   *             "cpuCostNs": 250,
   *             "memoryCostBytes": 500
   *           }
   *         ]
   *       }
   *     },
   *     {
   *       "nodeType": "serverNode",
   *       "enforcementProfile": {
   *         "cpuCostNs": 1500,
   *         "memoryCostBytes": 12000
   *       },
   *       "propagationScheme": {
   *         "propagationType": "TENANT",
   *         "propagationEntities": [
   *           {
   *             "entity": "DefaultTenant",
   *             "cpuCostNs": 1000,
   *             "memoryCostBytes": 8000
   *           },
   *           {
   *             "entity": "PremiumTenant",
   *             "cpuCostNs": 500,
   *             "memoryCostBytes": 4000
   *           }
   *         ]
   *       }
   *     }
   *   ]
   * }
   * }</pre>
   *
   * @param queryWorkloadName Name of the query workload
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/queryWorkloadConfigs/{queryWorkloadName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_QUERY_WORKLOAD_CONFIG)
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Get query workload config", notes = "Get workload configs for the workload name")
  public String getQueryWorkloadConfig(@PathParam("queryWorkloadName") String queryWorkloadName,
      @Context HttpHeaders httpHeaders) {
    try {
      LOGGER.info("Received request to get workload config for workload: {}", queryWorkloadName);
      QueryWorkloadConfig queryWorkloadConfig = _pinotHelixResourceManager.getQueryWorkloadConfig(queryWorkloadName);
      if (queryWorkloadConfig == null) {
        throw new ControllerApplicationException(LOGGER, "Workload config not found for workload: " + queryWorkloadName,
            Response.Status.NOT_FOUND, null);
      }
      String response = queryWorkloadConfig.toJsonString();
      LOGGER.info("Successfully fetched workload config for workload: {}", queryWorkloadName);
      return response;
    } catch (Exception e) {
      if (e instanceof ControllerApplicationException) {
        throw (ControllerApplicationException) e;
      } else {
        String errorMessage = String.format("Error while getting workload config for workload: %s, error: %s",
            queryWorkloadName, e);
        throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
  }


  /**
   * Retrieves all workload configurations associated with the specified instance.
   * <p>
   * This API returns a mapping of workload names to their instance-level cost
   * (CPU and memory) for the given Helix instance.
   * </p>
   *
   * See {@link InstanceCost} for more details on the instance cost definition and what each field means.
   *
   * <p><strong>Example:</strong></p>
   * <pre>{@code
   * GET /queryWorkloadConfigs/instance/Server_localhost_1234
   *
   * {
   *   "workload1": {
   *     "cpuCostNs": 100,
   *     "memoryCostBytes": 100
   *   },
   *   "workload2": {
   *     "cpuCostNs": 50,
   *     "memoryCostBytes": 50
   *   }
   * }
   * }</pre>
   *
   * @param instanceName Helix instance name
   * @return Map of workload name to instance cost
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/queryWorkloadConfigs/instance/{instanceName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_INSTANCE_QUERY_WORKLOAD_CONFIG)
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Get all workload configs associated with the instance",
      notes = "Get all workload configs associated with the instance")
  public String getQueryWorkloadConfigForInstance(@PathParam("instanceName") String instanceName,
                                                  @Context HttpHeaders httpHeaders) {
    try {
      Map<String, InstanceCost> workloadToInstanceCostMap = _pinotHelixResourceManager.getQueryWorkloadManager()
          .getWorkloadToInstanceCostFor(instanceName);
      if (workloadToInstanceCostMap == null || workloadToInstanceCostMap.isEmpty()) {
        throw new ControllerApplicationException(LOGGER, "No workload configs found for instance: " + instanceName,
            Response.Status.NOT_FOUND, null);
      }
      return JsonUtils.objectToString(workloadToInstanceCostMap);
    } catch (Exception e) {
      if (e instanceof ControllerApplicationException) {
        throw (ControllerApplicationException) e;
      } else {
        String errorMessage = String.format("Error while getting workload config for instance: %s, error: %s",
            instanceName, e);
        throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
  }

  /**
   * Updates the query workload configuration for a given workload.
   * <p>
   * This API accepts a JSON body describing the {@code QueryWorkloadConfig} including
   * node-specific enforcement profiles and propagation schemes. The configuration
   * is validated and persisted in Helix, enabling Pinot to enforce resource
   * isolation based on workload classification.
   *
   * See {@link org.apache.pinot.spi.config.workload.PropagationScheme} and {@link EnforcementProfile} for more details
   * on the configuration definition and what each field means.
   *
   * </p>
   * <p><strong>Example:</strong></p>
   * <pre>{@code
   * {
   *   "queryWorkloadName": "workload-foo1",
   *   "nodeConfigs": [
   *     {
   *       "nodeType": "brokerNode",
   *       "enforcementProfile": {
   *         "cpuCostNs": 500,
   *         "memoryCostBytes": 1000
   *       },
   *       "propagationScheme": {
   *         "propagationType": "TABLE",
   *         "propagationEntities": [
   *           {
   *             "entity": "airlineStats",
   *             "cpuCostNs": 300,
   *             "memoryCostBytes": 600
   *           },
   *           {
   *             "entity": "baseballStats",
   *             "cpuCostNs": 200,
   *             "memoryCostBytes": 400
   *           }
   *         ]
   *       }
   *     },
   *     {
   *       "nodeType": "serverNode",
   *       "enforcementProfile": {
   *         "cpuCostNs": 1500,
   *         "memoryCostBytes": 12000
   *       },
   *       "propagationScheme": {
   *         "propagationType": "TENANT",
   *         "propagationEntities": [
   *           {
   *             "entity": "DefaultTenant",
   *             "cpuCostNs": 1000,
   *             "memoryCostBytes": 8000
   *           },
   *           {
   *             "entity": "PremiumTenant",
   *             "cpuCostNs": 500,
   *             "memoryCostBytes": 4000
   *           }
   *         ]
   *       }
   *     }
   *   ]
   * }
   * }</pre>
   *
   * @param requestString JSON string representing the QueryWorkloadConfig
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/queryWorkloadConfigs")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_QUERY_WORKLOAD_CONFIG)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Update query workload config", notes = "Update workload config for the workload name")
  public Response updateQueryWorkloadConfig(String requestString, @Context HttpHeaders httpHeaders) {
    try {
      LOGGER.info("Received request to update queryWorkloadConfig with request: {}", requestString);
      QueryWorkloadConfig queryWorkloadConfig = JsonUtils.stringToObject(requestString, QueryWorkloadConfig.class);
      List<String> validationErrors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(queryWorkloadConfig);
      if (!validationErrors.isEmpty()) {
        String errorMessage = String.format("Invalid query workload config: %s", validationErrors);
        throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.BAD_REQUEST, null);
      }
      _pinotHelixResourceManager.setQueryWorkloadConfig(queryWorkloadConfig);
      String successMessage = String.format("Query Workload config updated successfully for workload: %s",
          queryWorkloadConfig.getQueryWorkloadName());
      LOGGER.info(successMessage);
      return Response.ok().entity(successMessage).build();
    } catch (Exception e) {
      String errorMessage = String.format("Error when updating query workload request: %s, error: %s",
          requestString, e);
      throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Deletes the query workload configuration for the specified workload name.
   * <p>
   * This API removes the workload configuration from Helix. Once deleted,
   * the workload will no longer have resource enforcement or propagation
   * applied within the cluster.
   * </p>
   * <p><strong>Example:</strong></p>
   * <pre>{@code
   * DELETE /queryWorkloadConfigs/workload-foo1
   * }</pre>
   *
   * @param queryWorkloadName Name of the query workload to be deleted
   */
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/queryWorkloadConfigs/{queryWorkloadName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_QUERY_WORKLOAD_CONFIG)
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete query workload config", notes = "Delete workload config for the workload name")
  public Response deleteQueryWorkloadConfig(@PathParam("queryWorkloadName") String queryWorkloadName,
      @Context HttpHeaders httpHeaders) {
    try {
      _pinotHelixResourceManager.deleteQueryWorkloadConfig(queryWorkloadName);
      String successMessage = String.format("Query Workload config deleted successfully for workload: %s",
          queryWorkloadName);
      LOGGER.info(successMessage);
      return Response.ok().entity(successMessage).build();
    } catch (Exception e) {
      String errorMessage = String.format("Error when deleting query workload for workload: %s, error: %s",
          queryWorkloadName, e);
      throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * API to refresh workload propagation for workloads, tables, or tenants.
   * <ul>
   *   <li><strong>Workload refresh</strong>: If {@code type=workload}, refreshes the specific workload config(s)</li>
   *   <li><strong>Table refresh</strong>: If {@code type=table}, refreshes workloads associated with table(s)</li>
   *   <li><strong>Tenant refresh</strong>: If {@code type=tenant}, refreshes workloads associated with tenant(s)</li>
   * </ul>
   * <p><strong>Example:</strong></p>
   * <pre>{@code
   * POST /queryWorkloadConfigs/refresh?type=workload&resourceNames=workload-foo1
   * POST /queryWorkloadConfigs/refresh?type=workload&resourceNames=workload-foo1,workload-foo2
   * POST /queryWorkloadConfigs/refresh?type=table&resourceNames=myTable_OFFLINE
   * POST /queryWorkloadConfigs/refresh?type=table&resourceNames=myTable_OFFLINE,myTable_REALTIME
   * POST /queryWorkloadConfigs/refresh?type=tenant&resourceNames=DefaultTenant
   * POST /queryWorkloadConfigs/refresh?type=tenant&resourceNames=DefaultTenant,AnotherTenant
   * }</pre>
   *
   * @param type The type of entity to refresh ("workload", "table", or "tenant")
   * @param resourceNames Comma-separated list of entity names to refresh
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/queryWorkloadConfigs/refresh")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_QUERY_WORKLOAD_CONFIG)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Refresh API for workload propagation",
      notes = "Refresh workload propagation for workloads, tables, or tenants based on the type parameter")
  public Response refreshWorkload(@QueryParam("type") String type,
                                  @QueryParam("resourceNames") String resourceNames,
                                  @Context HttpHeaders httpHeaders) {
    try {
      LOGGER.info("Received refresh request - type: {}, names: {}", type, resourceNames);
      List<String> nameList = validateAndParseRefreshRequest(type, resourceNames);
      switch (type) {
        case WORKLOAD:
          return refreshWorkloadsByNames(nameList);
        case TABLE:
          return refreshWorkloadsByTables(nameList);
        case TENANT:
          return refreshWorkloadsByTenants(nameList);
        default:
          throw new ControllerApplicationException(LOGGER,
              String.format("Invalid type: '%s'. Must be 'workload', 'table', or 'tenant'", type),
              Response.Status.BAD_REQUEST, null);
      }
    } catch (Exception e) {
      if (e instanceof ControllerApplicationException) {
        throw (ControllerApplicationException) e;
      }
      String errorMessage = String.format("Error when refreshing workload - type: %s, names: %s, error: %s",
          type, resourceNames, e);
      throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Validates and parses the refresh request parameters.
   */
  private List<String> validateAndParseRefreshRequest(String type, String resourceNames) {
    // Validate type parameter
    if (type == null || type.trim().isEmpty() || !(type.equals(WORKLOAD) || type.equals(TABLE)
        || type.equals(TENANT))) {
      throw new ControllerApplicationException(LOGGER,
          "Query parameter 'type' is required. Must be 'workload', 'table', or 'tenant'",
          Response.Status.BAD_REQUEST, null);
    }

    // Validate resourceNames parameter
    if (resourceNames == null || resourceNames.trim().isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Query parameter 'resourceNames' is required",
          Response.Status.BAD_REQUEST, null);
    }

    // Split comma-separated names and trim whitespace
    String[] names = resourceNames.split(",");
    List<String> nameList = new ArrayList<>();
    for (String name : names) {
      String trimmed = name.trim();
      if (!trimmed.isEmpty()) {
        nameList.add(trimmed);
      }
    }
    // Ensure at least one valid name exists
    if (nameList.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "At least one resource name is required",
          Response.Status.BAD_REQUEST, null);
    }
    return nameList;
  }

  /**
   * Helper method to refresh multiple workloads by name.
   */
  private Response refreshWorkloadsByNames(List<String> workloadNames) {
    LOGGER.info("Refreshing workload config propagation for workloads: {}", workloadNames);
    List<String> successfulWorkloads = new ArrayList<>();
    List<String> failedWorkloads = new ArrayList<>();

    for (String workloadName : workloadNames) {
      try {
        QueryWorkloadConfig existingConfig = _pinotHelixResourceManager.getQueryWorkloadConfig(workloadName);
        if (existingConfig == null) {
          LOGGER.warn("Workload config not found for workload: {}", workloadName);
          failedWorkloads.add(workloadName + " (not found)");
          continue;
        }
        _pinotHelixResourceManager.getQueryWorkloadManager().propagateWorkloadUpdateMessage(existingConfig);
        successfulWorkloads.add(workloadName);
      } catch (Exception e) {
        LOGGER.error("Failed to refresh workload: {}", workloadName, e);
        failedWorkloads.add(workloadName + " (" + e.getMessage() + ")");
      }
    }

    String successMessage = String.format("Workload propagation completed. Successful: %s, Failed: %s",
        successfulWorkloads, failedWorkloads);
    LOGGER.info(successMessage);
    return Response.ok().entity(successMessage).build();
  }

  /**
   * Helper method to refresh workloads for multiple tables.
   */
  private Response refreshWorkloadsByTables(List<String> tableNames) {
    LOGGER.info("Refreshing workload propagation for tables: {}", tableNames);
    List<String> successfulTables = new ArrayList<>();
    List<String> failedTables = new ArrayList<>();

    for (String tableName : tableNames) {
      try {
        _pinotHelixResourceManager.getQueryWorkloadManager().propagateWorkloadForTable(tableName);
        successfulTables.add(tableName);
      } catch (Exception e) {
        LOGGER.error("Failed to refresh workload for table: {}", tableName, e);
        failedTables.add(tableName + " (" + e.getMessage() + ")");
      }
    }

    String successMessage = String.format("Workload propagation completed. Successful: %s, Failed: %s",
        successfulTables, failedTables);
    LOGGER.info(successMessage);
    return Response.ok().entity(successMessage).build();
  }

  /**
   * Helper method to refresh workloads for multiple tenants.
   */
  private Response refreshWorkloadsByTenants(List<String> tenantNames) {
    LOGGER.info("Refreshing workload propagation for tenants: {}", tenantNames);
    List<String> successfulTenants = new ArrayList<>();
    List<String> failedTenants = new ArrayList<>();

    for (String tenantName : tenantNames) {
      try {
        _pinotHelixResourceManager.getQueryWorkloadManager().propagateWorkloadForTenant(tenantName);
        successfulTenants.add(tenantName);
      } catch (Exception e) {
        LOGGER.error("Failed to refresh workload for tenant: {}", tenantName, e);
        failedTenants.add(tenantName + " (" + e.getMessage() + ")");
      }
    }

    String successMessage = String.format("Workload propagation completed. Successful: %s, Failed: %s",
        successfulTenants, failedTenants);
    LOGGER.info(successMessage);
    return Response.ok().entity(successMessage).build();
  }
}
