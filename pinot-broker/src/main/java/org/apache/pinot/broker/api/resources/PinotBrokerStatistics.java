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
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.List;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.broker.stats.BrokerTableStatsManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.jvnet.hk2.annotations.Optional;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/// Operator control over this broker's local statistics store.
@Api(tags = "Statistics", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```")}))
@Path("/")
public class PinotBrokerStatistics {

  /// Absent when statistics collection is disabled, which every endpoint here reports as 404.
  @Inject
  @Optional
  @Nullable
  private BrokerTableStatsManager _statsManager;

  @Inject
  private BrokerRoutingManager _routingManager;

  /// Drops statistics for tables this broker no longer serves.
  ///
  /// A table dropped while this broker was running is cleaned up as its routing entry goes away.
  /// One dropped while the broker was DOWN is not: on restart no routing entry — and so no
  /// listener — is ever created for it, leaving rows nothing would revisit. This endpoint reclaims
  /// those.
  ///
  /// Deliberately operator-triggered rather than automatic. "This broker has no routing for table
  /// T" only means "T is gone" once routing has settled; during startup it also matches a table
  /// that simply has not loaded yet, and purging then would discard column statistics that are
  /// expensive to re-fetch from servers. Calling this on a running broker removes that ambiguity.
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/statistics/orphaned")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_STATISTICS)
  @ApiOperation(value = "Drop statistics for tables this broker no longer serves",
      notes = "Returns the tables whose statistics were dropped. Call this on a running broker: "
          + "before routing has settled, a table that has merely not loaded yet is "
          + "indistinguishable from one that is gone.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Statistics collection is disabled on this broker"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public List<String> purgeOrphanedStatistics() {
    return statsManager().purgeTablesNoLongerServed(_routingManager::routingExists);
  }

  private BrokerTableStatsManager statsManager() {
    if (_statsManager == null) {
      throw new WebApplicationException(
          "Statistics collection is disabled on this broker (pinot.broker.stats.enabled)",
          Response.Status.NOT_FOUND);
    }
    return _statsManager;
  }
}
