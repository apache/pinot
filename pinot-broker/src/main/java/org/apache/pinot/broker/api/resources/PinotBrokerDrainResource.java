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
package org.apache.pinot.broker.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.broker.BrokerDrainManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = "Broker")
@Path("/")
public class PinotBrokerDrainResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotBrokerDrainResource.class);

  @Inject
  private BrokerDrainManager _brokerDrainManager;

  @POST
  @Path("drain")
  @Produces(MediaType.APPLICATION_JSON)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_INSTANCE)
  @ApiOperation(value = "Drain this broker",
      notes = "Rejects new queries, removes this broker from brokerResource, waits for accepted queries to finish, "
          + "and optionally shuts down the broker. The timeout defaults to pinot.broker.delayShutdownTimeMs.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Broker drained"),
      @ApiResponse(code = 400, message = "Bad request"),
      @ApiResponse(code = 408, message = "Timed out waiting for in-flight queries"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public Response drain(
      @ApiParam(value = "Maximum time to wait for in-flight queries. -1 uses broker default.")
      @QueryParam("timeoutMs") @DefaultValue("-1") long timeoutMs,
      @ApiParam(value = "Whether to stop the broker after it drains")
      @QueryParam("shutdown") @DefaultValue("true") boolean shutdown) {
    if (timeoutMs < -1) {
      throw new WebApplicationException("timeoutMs must be -1 or non-negative", Response.Status.BAD_REQUEST);
    }
    try {
      BrokerDrainManager.DrainStatus status = _brokerDrainManager.drain(timeoutMs, shutdown);
      return Response.status(status.isDrained() ? Response.Status.OK : Response.Status.REQUEST_TIMEOUT)
          .entity(status)
          .build();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new WebApplicationException("Interrupted while draining broker", e,
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (Exception e) {
      LOGGER.error("Caught exception while draining broker", e);
      throw new WebApplicationException("Failed to drain broker: " + e.getMessage(), e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("drain/status")
  @Produces(MediaType.APPLICATION_JSON)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_HEALTH)
  @ApiOperation(value = "Get broker drain status")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Broker drain status"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public BrokerDrainManager.DrainStatus getDrainStatus() {
    return _brokerDrainManager.getStatus();
  }
}
