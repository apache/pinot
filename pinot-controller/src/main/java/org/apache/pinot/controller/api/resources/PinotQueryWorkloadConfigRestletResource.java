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
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
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
public class PinotQueryWorkloadConfigRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotQueryWorkloadConfigRestletResource.class);

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
      List<QueryWorkloadConfig> queryWorkloadConfigs = _pinotHelixResourceManager.getQueryWorkloadConfigs();
      return JsonUtils.objectToString(queryWorkloadConfigs);
    } catch (Exception e) {
      String errorMessage = String.format("Caught exception while getting all workload configs, error: %s", e);
      throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * API to specific query workload config
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/queryWorkloadConfigs/{workloadName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_QUERY_WORKLOAD_CONFIG)
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Get query workload config", notes = "Get workload configs for the workload name")
  public String getQueryWorkloadConfig(@PathParam("workloadName") String workloadName,
      @Context HttpHeaders httpHeaders) {
    try {
      LOGGER.info("Received request to get workload config for workload: {}", workloadName);
      QueryWorkloadConfig queryWorkloadConfig = _pinotHelixResourceManager.getQueryWorkloadConfig(workloadName);
      if (queryWorkloadConfig == null) {
        throw new ControllerApplicationException(LOGGER, "Workload config not found for workload: " + workloadName,
            Response.Status.NOT_FOUND, null);
      }
      return queryWorkloadConfig.toJsonString();
    } catch (Exception e) {
      if (e instanceof ControllerApplicationException) {
        throw (ControllerApplicationException) e;
      } else {
        String errorMessage = String.format("Caught exception while getting workload config for workload: %s, error: %s",
            workloadName, e);
        throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
  }

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
      _pinotHelixResourceManager.setQueryWorkloadConfig(queryWorkloadConfig);
      String successMessage = String.format("Query Workload config updated successfully for workload: %s",
          queryWorkloadConfig.getQueryWorkloadName());
      LOGGER.info(successMessage);
      return Response.ok().entity(successMessage).build();
    } catch (Exception e) {
      String errorMessage = String.format("Caught exception when updating query workload request: %s, error: %s",
          requestString, e);
      throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/queryWorkloadConfigs/{workloadName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_QUERY_WORKLOAD_CONFIG)
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete query workload config", notes = "Delete workload config for the workload name")
  public Response deleteQueryWorkloadConfig(@PathParam("workloadName") String workloadName,
      @Context HttpHeaders httpHeaders) {
    try {
      _pinotHelixResourceManager.deleteQueryWorkloadConfig(workloadName);
      String successMessage = String.format("Query Workload config deleted successfully for workload: %s",
          workloadName);
      LOGGER.info(successMessage);
      return Response.ok().entity(successMessage).build();
    } catch (Exception e) {
      String errorMessage = String.format("Caught exception when deleting query workload for workload: %s, error: %s",
          workloadName, e);
      throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
