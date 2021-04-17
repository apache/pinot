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

package org.apache.pinot.tools.service.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.ServiceStatus;


@Api(tags = "Health")
@Path("/")
public class PinotServiceManagerHealthCheck {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("health")
  @ApiOperation(value = "Checking Pinot Service health")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Pinot Starter is healthy"), @ApiResponse(code = 503,
      message = "Pinot Starter is not healthy")})
  public String getStarterHealth() {
    ServiceStatus.Status status = ServiceStatus.getServiceStatus();
    if (status == ServiceStatus.Status.GOOD) {
      return "OK";
    }
    throw new WebApplicationException(String.format("Pinot starter status is [ %s ]", status),
        Response.Status.SERVICE_UNAVAILABLE);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/health/services")
  @ApiOperation(value = "Checking all services health for a service")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Pinot Service is healthy"), @ApiResponse(code = 503,
      message = "Pinot Service is not healthy")})
  public Map<String, Map<String, String>> getAllServicesHealth() {
    return ServiceStatus.getServiceStatusMap();
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/health/services/{instanceName}")
  @ApiOperation(value = "Checking service health for an instance")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Pinot Instance is healthy"), @ApiResponse(code = 503,
      message = "Pinot Instance is not healthy")})
  public String getServiceHealth(
      @ApiParam(value = "Name of the Instance") @PathParam("instanceName") String instanceName) {
    ServiceStatus.Status status = ServiceStatus.getServiceStatus(instanceName);
    if (status == ServiceStatus.Status.GOOD) {
      return "OK";
    }
    throw new WebApplicationException(String.format("Pinot instance [ %s ] status is [ %s ]", instanceName, status),
        Response.Status.SERVICE_UNAVAILABLE);
  }
}
