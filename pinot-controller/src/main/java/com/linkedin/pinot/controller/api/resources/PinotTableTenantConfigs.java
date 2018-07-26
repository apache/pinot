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
package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/")
@Api(tags = {Constants.TABLE_TAG, Constants.TENANT_TAG})
public class PinotTableTenantConfigs {

  @Inject
  PinotHelixResourceManager _helixResourceManager;

  private static final Logger LOGGER = LoggerFactory.getLogger( PinotTableTenantConfigs.class);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/rebuildBrokerResourceFromHelixTags")
  @ApiOperation(value = "Rebuild broker resource for table", notes = "when new brokers are added")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad request: table name has to be with table type"),
      @ApiResponse(code = 500, message = "Internal error rebuilding broker resource or serializing response")})
  public SuccessResponse rebuildBrokerResource(
      @ApiParam(value = "Table name (with type)", required = true) @PathParam("tableName") String tableNameWithType
  ) {
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
