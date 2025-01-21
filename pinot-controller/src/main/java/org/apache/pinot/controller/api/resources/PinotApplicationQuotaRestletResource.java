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
import java.util.Collections;
import java.util.Map;
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
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.APPLICATION_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key =
        SWAGGER_AUTHORIZATION_KEY, description =
        "The format of the key is  ```\"Basic <token>\" or \"Bearer "
            + "<token>\"```"), @ApiKeyAuthDefinition(name = CommonConstants.APPLICATION, in =
    ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = CommonConstants.APPLICATION, description =
    "Application context passed through http header. If no context is provided 'default' application "
        + "context will be considered.")
}))
@Path("/")
public class PinotApplicationQuotaRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotApplicationQuotaRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  /**
   * API to get application quota configs. Will return empty map if application quotas are not defined at all.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/applicationQuotas")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_APPLICATION_QUERY_QUOTA)
  @ApiOperation(value = "Get all application qps quotas", notes = "Get all application qps quotas")
  public Map<String, Double> getApplicationQuotas(@Context HttpHeaders httpHeaders) {
    Map<String, Double> quotas = _pinotHelixResourceManager.getApplicationQuotas();
    if (quotas != null) {
      return quotas;
    } else {
      return Collections.emptyMap();
    }
  }

  /**
   * API to get application quota config. Will return null if application quotas is not defined.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/applicationQuotas/{appName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_APPLICATION_QUERY_QUOTA)
  @ApiOperation(value = "Get application qps quota", notes = "Get application qps quota")
  public Double getApplicationQuota(@Context HttpHeaders httpHeaders, @PathParam("appName") String appName) {

    Map<String, Double> quotas = _pinotHelixResourceManager.getApplicationQuotas();
    if (quotas != null && quotas.containsKey(appName)) {
      return quotas.get(appName);
    }

    HelixConfigScope scope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(
        _pinotHelixResourceManager.getHelixClusterName()).build();

    HelixAdmin helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
    String defaultQuota =
        helixAdmin.getConfig(scope, Collections.singletonList(CommonConstants.Helix.APPLICATION_MAX_QUERIES_PER_SECOND))
            .get(CommonConstants.Helix.APPLICATION_MAX_QUERIES_PER_SECOND);
    return defaultQuota != null ? Double.parseDouble(defaultQuota) : null;
  }

  /**
   * API to update the quota config for application.
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/applicationQuotas/{appName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_APPLICATION_QUOTA)
  @ApiOperation(value = "Update application quota", notes = "Update application quota")
  public SuccessResponse setApplicationQuota(@PathParam("appName") String appName,
      @QueryParam("maxQueriesPerSecond") String queryQuota, @Context HttpHeaders httpHeaders) {
    try {
      try {
        Double newQuota = queryQuota != null ? Double.parseDouble(queryQuota) : null;
        _pinotHelixResourceManager.updateApplicationQpsQuota(appName, newQuota);
      } catch (NumberFormatException nfe) {
        throw new ControllerApplicationException(LOGGER, "Application query quota value is not a number",
            Response.Status.INTERNAL_SERVER_ERROR, nfe);
      }

      return new SuccessResponse("Query quota for application " + appName + " successfully updated");
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
