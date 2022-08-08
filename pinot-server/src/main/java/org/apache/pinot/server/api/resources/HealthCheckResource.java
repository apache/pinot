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
package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.ServiceStatus.Status;
import org.apache.pinot.server.starter.helix.AdminApiApplication;


/**
 * REST API to do health check through ServiceStatus.
 */
@Api(tags = "Health")
@Path("/")
public class HealthCheckResource {

  @Inject
  @Named(AdminApiApplication.SERVER_INSTANCE_ID)
  private String _instanceId;

  @Inject
  private ServerMetrics _serverMetrics;

  @GET
  @Path("/health")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Checking server health")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Server is healthy"),
      @ApiResponse(code = 503, message = "Server is not healthy")
  })
  public String checkHealth(
      @ApiParam(value = "health check type: liveness or readiness") @QueryParam("checkType") String checkType) {
    if ("liveness".equalsIgnoreCase(checkType)) {
      return "OK";
    } else {
      return getReadinessStatus();
    }
  }

  @GET
  @Path("/health/liveness")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Checking server liveness status.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Server is live"),
      @ApiResponse(code = 503, message = "Server is not live")
  })
  public String checkLiveness() {
    // Returns OK since if we reached here, the admin application is running.
    return "OK";
  }

  @GET
  @Path("/health/readiness")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Checking server readiness status")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Server is ready to serve queries"),
      @ApiResponse(code = 503, message = "Server is not ready to serve queries")
  })
  public String checkReadiness() {
    return getReadinessStatus();
  }

  private String getReadinessStatus() throws WebApplicationException {
    Status status = ServiceStatus.getServiceStatus(_instanceId);
    if (status == Status.GOOD) {
      _serverMetrics.addMeteredGlobalValue(ServerMeter.READINESS_CHECK_OK_CALLS, 1);
      return "OK";
    }
    _serverMetrics.addMeteredGlobalValue(ServerMeter.READINESS_CHECK_BAD_CALLS, 1);
    String errMessage = String.format("Pinot server status is %s", status);
    Response response =
        Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(errMessage).build();
    throw new WebApplicationException(errMessage, response);
  }
}
