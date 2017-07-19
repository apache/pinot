/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;


@Api(tags = {"table", "tenant"})
public class PinotTableTenantConfigs extends BasePinotControllerRestletResource {

  @Inject
  PinotHelixResourceManager _helixResourceManager;

  private static final Logger LOGGER = LoggerFactory.getLogger( PinotTableTenantConfigs.class);

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/rebuildBrokerResourceFromHelixTags")
  @ApiOperation(value = "Rebuild broker resource for table", notes = "when new brokers are added")
  @ApiResponses(value = {@ApiResponse(code=200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found")})
  public String rebuildBrokerResource(
      @ApiParam(value = "Table name (with type)", required = true)
      @PathParam("tableName") String tableNameWithType
  ) {
    try {
      final PinotResourceManagerResponse pinotResourceManagerResponse =
          _helixResourceManager.rebuildBrokerResourceFromHelixTags(tableNameWithType);

      return pinotResourceManagerResponse.toJSON().toString();
    } catch (Exception e) {
      throw new WebApplicationException("Failed to update broker resource", e);
    }
  }
}
