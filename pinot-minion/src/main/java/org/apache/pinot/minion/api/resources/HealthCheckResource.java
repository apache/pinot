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
package org.apache.pinot.minion.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.ServiceStatus.Status;
import org.apache.pinot.minion.MinionAdminApiApplication;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * REST API to do health check through ServiceStatus.
 */
@Api(tags = "Health", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class HealthCheckResource {
  @Inject
  @Named(MinionAdminApiApplication.MINION_INSTANCE_ID)
  private String _instanceId;

  @GET
  @Path("/health")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Checking minion health")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Minion is healthy"),
      @ApiResponse(code = 503, message = "Minion is not healthy")
  })
  public String checkHealth() {
    Status status = ServiceStatus.getServiceStatus(_instanceId);
    if (status == Status.GOOD) {
      return "OK";
    }
    String errMessage = String.format("Pinot minion status is %s", status);
    Response response =
        Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(errMessage).build();
    throw new WebApplicationException(errMessage, response);
  }
}
