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
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.broker.routing.RoutingManager;


@Api(tags = "Routing")
@Path("/")
public class PinotBrokerRouting {

  @Inject
  RoutingManager _routingManager;

  @PUT
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/routing/{tableName}")
  @ApiOperation(value = "Build/rebuild the routing for a table", notes = "Build/rebuild the routing for a table")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")})
  public String buildRouting(
      @ApiParam(value = "Table name (with type)") @PathParam("tableName") String tableNameWithType) {
    _routingManager.buildRouting(tableNameWithType);
    return "Success";
  }

  @PUT
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/routing/refresh/{tableName}/{segmentName}")
  @ApiOperation(value = "Refresh the routing for a segment", notes = "Refresh the routing for a segment")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")})
  public String refreshRouting(
      @ApiParam(value = "Table name (with type)") @PathParam("tableName") String tableNameWithType,
      @ApiParam(value = "Segment name") @PathParam("segmentName") String segmentName) {
    _routingManager.refreshSegment(tableNameWithType, segmentName);
    return "Success";
  }

  @DELETE
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/routing/{tableName}")
  @ApiOperation(value = "Remove the routing for a table", notes = "Remove the routing for a table")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")})
  public String removeRouting(
      @ApiParam(value = "Table name (with type)") @PathParam("tableName") String tableNameWithType) {
    _routingManager.removeRouting(tableNameWithType);
    return "Success";
  }
}
