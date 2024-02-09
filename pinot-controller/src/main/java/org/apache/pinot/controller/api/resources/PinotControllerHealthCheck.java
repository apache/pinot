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
import javax.inject.Named;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Singleton
@Api(tags = Constants.HEALTH_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotControllerHealthCheck {

  private String _instanceId;

  ControllerConf _controllerConf;

  private ControllerMetrics _controllerMetrics;

  private final Instant _startTime;

  public PinotControllerHealthCheck(Instant startTime,
      @Named(BaseControllerStarter.CONTROLLER_INSTANCE_ID) String instanceId,
      ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    _startTime = startTime;
    _instanceId = instanceId;
    _controllerConf = controllerConf;
    _controllerMetrics = controllerMetrics;
  }

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
  @Path("uptime")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_HEALTH)
  @ApiOperation(value = "Get controller uptime")
  @Produces(MediaType.TEXT_PLAIN)
  public String getUptime() {
    ServiceStatus.Status status = ServiceStatus.getServiceStatus(_instanceId);
    if (status == ServiceStatus.Status.GOOD) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.HEALTHCHECK_OK_CALLS, 1);
      Instant now = Instant.now();
      Duration uptime = Duration.between(_startTime, now);
      return "Uptime: " + uptime.getSeconds() + " seconds";
    }
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.HEALTHCHECK_BAD_CALLS, 1);
    String errMessage = String.format("Cannot tell Pinot controller uptime. Pinot controller status is %s", status);
    Response response =
        Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(errMessage).build();
    throw new WebApplicationException(errMessage, response);
  }

  @GET
  @Path("start-time")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_HEALTH)
  @ApiOperation(value = "Get controller start time")
  @Produces(MediaType.TEXT_PLAIN)
  public String getStartTime() {
    ServiceStatus.Status status = ServiceStatus.getServiceStatus(_instanceId);
    if (status == ServiceStatus.Status.GOOD) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.HEALTHCHECK_OK_CALLS, 1);
    } else {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.HEALTHCHECK_BAD_CALLS, 1);
    }
    String returnMessage = String.format("Pinot controller started at: %s. "
        + "Pinot controller status is %s", _startTime, status);
    return returnMessage;
  }
}
