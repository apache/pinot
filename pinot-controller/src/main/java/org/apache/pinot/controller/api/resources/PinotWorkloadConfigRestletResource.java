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
import org.apache.pinot.spi.config.workload.WorkloadConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;

@Api(tags = Constants.APPLICATION_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key =
        SWAGGER_AUTHORIZATION_KEY, description =
        "The format of the key is  ```\"Basic <token>\" or \"Bearer "
            + "<token>\"```"), @ApiKeyAuthDefinition(name = CommonConstants.WORKLOAD, in =
    ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = CommonConstants.WORKLOAD, description =
    "Workload context passed through http header. If no context is provided 'default' workload "
        + "context will be considered.")
}))
@Path("/")
public class PinotWorkloadConfigRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotWorkloadConfigRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  /**
   * API to get all workload configs
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/workloadConfigs/{workloadName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_WORKLOAD_CONFIG)
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Get workload configs", notes = "Get workload configs for the workload name")
  public String getWorkloadConfig(@PathParam("workloadName") String workloadName, @Context HttpHeaders httpHeaders) {
    try {
      LOGGER.info("Received request to get workload config for workload: {}", workloadName);
      WorkloadConfig workloadConfig = _pinotHelixResourceManager.getWorkloadConfig(workloadName);
      if (workloadConfig == null) {
        throw new ControllerApplicationException(LOGGER, "Workload config not found for workload: " + workloadName,
            Response.Status.NOT_FOUND, null);
      }
      return workloadConfig.toJsonString();
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting workload config for workload: {}", workloadName, e);
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/workloadConfigs")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_WORKLOAD_CONFIG)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Update workload config", notes = "Update workload config for the workload name")
  public void updateWorkloadConfig(String requestString, @Context HttpHeaders httpHeaders) {
    try {
      WorkloadConfig workloadConfig = JsonUtils.stringToObject(requestString, WorkloadConfig.class);
      LOGGER.info("Received request to update workload config for workload: {}", workloadConfig.getWorkloadName());
      _pinotHelixResourceManager.setWorkloadConfig(workloadConfig);
    } catch (Exception e) {
      LOGGER.error("Caught exception while updating workload config for request: {}", requestString, e);
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/workloadConfigs/{workloadName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_WORKLOAD_CONFIG)
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete workload config", notes = "Delete workload config for the workload name")
  public void deleteWorkloadConfig(@PathParam("workloadName") String workloadName, @Context HttpHeaders httpHeaders) {
    try {
      LOGGER.info("Received request to delete workload config for workload: {}", workloadName);
      _pinotHelixResourceManager.deleteWorkloadConfig(workloadName);
    } catch (Exception e) {
      LOGGER.error("Caught exception while deleting workload config for workload: {}", workloadName, e);
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}

