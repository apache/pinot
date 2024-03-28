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
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.HEALTH_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotControllerHealthCheck {

  @Inject
  @Named(BaseControllerStarter.CONTROLLER_INSTANCE_ID)
  private String _instanceId;

  @Inject
  ControllerConf _controllerConf;

  @Inject
  private ControllerMetrics _controllerMetrics;

  @Inject
  @Named(ControllerAdminApiApplication.START_TIME)
  private Instant _startTime;

  @GET
  @Path("pinot-controller/admin")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_HEALTH)
  @ApiOperation(value = "Check controller health")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Good")})
  @Produces(MediaType.TEXT_PLAIN)
  public String checkHealthLegacy() {
    if (StringUtils.isNotBlank(_controllerConf.generateVipUrl())) {
      return "GOOD";
    }
    return "";
  }

  @GET
  @Path("health")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_HEALTH)
  @ApiOperation(value = "Check controller health")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Good")})
  @Produces(MediaType.TEXT_PLAIN)
  public String checkHealth() {
    ServiceStatus.Status status = ServiceStatus.getServiceStatus(_instanceId);
    if (status == ServiceStatus.Status.GOOD) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.HEALTHCHECK_OK_CALLS, 1);
      return "OK";
    }
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.HEALTHCHECK_BAD_CALLS, 1);
    String errMessage = String.format("Pinot controller status is %s", status);
    Response response =
        Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(errMessage).build();
    throw new WebApplicationException(errMessage, response);
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("uptime")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_HEALTH)
  @ApiOperation(value = "Get controller uptime")
  public long getUptime() {
    if (_startTime == null) {
      return 0;
    }
    Instant now = Instant.now();
    Duration uptime = Duration.between(_startTime, now);
    return uptime.getSeconds();
  }

  @GET
  @Path("start-time")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_HEALTH)
  @ApiOperation(value = "Get controller start time")
  @Produces(MediaType.TEXT_PLAIN)
  public String getStartTime() {
    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
    return _startTime != null ? formatter.format(_startTime) : "";
  }
}
