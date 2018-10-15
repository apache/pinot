/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.server.api.resources;

import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.common.utils.ServiceStatus.Status;
import com.linkedin.pinot.server.starter.ServerInstance;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * REST API to do health check through ServiceStatus.
 */
@Api(tags = "Health")
@Path("/")
public class HealthCheckResource {

  @GET
  @Path("/health")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Checking server health")
  @ApiResponses(value = { @ApiResponse(code = 200, message = "Server is healthy"), @ApiResponse(code = 503, message = "Server is not healthy")
  })
  public String checkHealth() {
    Status status = ServiceStatus.getServiceStatus();
    if (status == Status.GOOD) {
      return "OK";
    }
    throw new WebApplicationException(String.format("Pinot server status is %s", status), Response.Status.SERVICE_UNAVAILABLE);
  }
}
