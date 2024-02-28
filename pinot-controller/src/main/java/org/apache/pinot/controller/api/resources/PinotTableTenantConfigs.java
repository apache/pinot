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
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Path("/")
@Api(tags = {
    Constants.TABLE_TAG, Constants.TENANT_TAG
}, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
public class PinotTableTenantConfigs {

  @Inject
  PinotHelixResourceManager _helixResourceManager;

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableTenantConfigs.class);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/tables/{tableName}/rebuildBrokerResourceFromHelixTags")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.REBUILD_BROKER_RESOURCE)
  @ApiOperation(value = "Rebuild broker resource for table", notes = "when new brokers are added")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad request: table name has to be with table type"),
      @ApiResponse(code = 500, message = "Internal error rebuilding broker resource or serializing response")
  })
  public SuccessResponse rebuildBrokerResource(
      @ApiParam(value = "Table name (with type)", required = true) @PathParam("tableName") String tableNameWithType,
      @Context HttpHeaders headers) {
    tableNameWithType = _helixResourceManager.translateTableName(tableNameWithType,
        headers.getHeaderString(CommonConstants.DATABASE));
    return rebuildBrokerResourceV2(tableNameWithType);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/v2/tables/rebuildBrokerResourceFromHelixTags")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.REBUILD_BROKER_RESOURCE)
  @ApiOperation(value = "Rebuild broker resource for table", notes = "when new brokers are added")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad request: table name has to be with table type"),
      @ApiResponse(code = 500, message = "Internal error rebuilding broker resource or serializing response")
  })
  public SuccessResponse rebuildBrokerResourceV2(
      @ApiParam(value = "Table name (with type)", required = true) @QueryParam("tableName") String tableNameWithType) {
    try {
      final PinotResourceManagerResponse pinotResourceManagerResponse =
          _helixResourceManager.rebuildBrokerResourceFromHelixTags(tableNameWithType);
      return new SuccessResponse(pinotResourceManagerResponse.getMessage());
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
