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
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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
   * API to fetch query workload config
   * @param queryWorkloadName Name of the query workload
   * Example request:
   * /queryWorkloadConfigs/workload-foo1
   * Example response:
   * {
   *   "queryWorkloadName" : "workload-foo1",
   *   "nodeConfigs" : {
   *   {
   *       "nodeType" : "brokerNode",
   *       "enforcementProfile": {
   *         "cpuCostNs": 500,
   *         "memoryCostBytes": 1000
   *       },
   *       "propagationScheme": {
   *         "propagationType": "TABLE",
   *         "values": ["airlineStats"]
   *       }
   *     },
   *     {
   *       "nodeType" : "serverNode",
   *       "enforcementProfile": {
   *         "cpuCostNs": 1500,
   *         "memoryCostBytes": 12000
   *       },
   *       "propagationScheme": {
   *         "propagationType": "TENANT",
   *         "values": ["DefaultTenant"]
   *       }
   *     }
   *   }
   * }
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
   * API to get all workload configs associated with the instance
   * @param instanceName Helix instance name
   * @return Map of workload name to instance cost
   * Example request:
   * /queryWorkloadConfigs/instance/Server_localhost_1234
   * Example response:
   * {
   *  "workload1": {
   *    "cpuCostNs": 100,
   *    "memoryCostBytes":100
   *  },
   *  "workload2": {
   *    "cpuCostNs": 50,
   *    "memoryCostBytes": 50
   *  }
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
   * Updates the query workload config
   * @param requestString JSON string representing the QueryWorkloadConfig
   * Example request:
   * {
   *   "queryWorkloadName" : "workload-foo1",
   *   "nodeConfigs" : {
   *    {
   *       "nodeType" : "brokerNode",
   *       "enforcementProfile": {
   *         "cpuCostNs": 500,
   *         "memoryCostBytes": 1000
   *       },
   *       "propagationScheme": {
   *         "propagationType": "TABLE",
   *         "values": ["airlineStats"]
   *       }
   *     },
   *     {
   *       "nodeType" : "serverNode",
   *       "enforcementProfile": {
   *         "cpuCostNs": 1500,
   *         "memoryCostBytes": 12000
   *       },
   *       "propagationScheme": {
   *         "propagationType": "TENANT",
   *         "values": ["DefaultTenant"]
   *       }
   *     }
   *   }
   * }
   *
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
   * Deletes the query workload config
   * @param queryWorkloadName Name of the query workload to be deleted
   * Example request:
   * /queryWorkloadConfigs/workload-foo1
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
   * API to refresh propagation for a single query workload config
   * This API doesn't update the config, it only triggers the propagation of an existing workload config
   *
   * @param queryWorkloadName Name of the query workload to refresh
   * Example request:
   * POST /queryWorkloadConfigs/{queryWorkloadName}/refresh
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/queryWorkloadConfigs/{queryWorkloadName}/refresh")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_QUERY_WORKLOAD_CONFIG)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Refresh query workload config propagation", notes = "Force propagation of an existing config")
  public Response refreshQueryWorkloadConfig(@PathParam("queryWorkloadName") String queryWorkloadName,
                                             @Context HttpHeaders httpHeaders) {
    try {
      LOGGER.info("Received request to refresh workload config propagation for workload: {}", queryWorkloadName);
      // Fetch existing config
      QueryWorkloadConfig existingConfig = _pinotHelixResourceManager.getQueryWorkloadConfig(queryWorkloadName);
      if (existingConfig == null) {
        throw new ControllerApplicationException(LOGGER, "Workload config not found for workload: " + queryWorkloadName,
            Response.Status.NOT_FOUND, null);
      }
      _pinotHelixResourceManager.getQueryWorkloadManager().propagateWorkloadUpdateMessage(existingConfig);
      String successMessage = String.format("Query workload config propagation triggered for workload: %s",
          queryWorkloadName);
      LOGGER.info(successMessage);
      return Response.ok().entity(successMessage).build();
    } catch (Exception e) {
      if (e instanceof ControllerApplicationException) {
        throw (ControllerApplicationException) e;
      } else {
        String errorMessage = String.format("Error when refreshing query workload config for workload: %s, error: %s",
            queryWorkloadName, e);
        throw new ControllerApplicationException(LOGGER, errorMessage,
            Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
  }
}
