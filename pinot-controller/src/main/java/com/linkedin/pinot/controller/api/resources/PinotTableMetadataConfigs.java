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

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotTableMetadataConfigs {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableMetadataConfigs.class);

  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;

  @Deprecated
  @PUT
  @Path("/tables/{tableName}/metadataConfigs")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update table metadata", notes = "Updates table configuration")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"),
  @ApiResponse(code = 500, message = "Internal server error"),
  @ApiResponse(code = 404, message = "Table not found")})
  public SuccessResponse updateTableMetadata(@PathParam("tableName")String tableName, String requestBody
  ) {
    try {
      TableConfig tableConfig = TableConfig.fromJSONConfig(new JSONObject(requestBody));
      pinotHelixResourceManager.updateMetadataConfigFor(tableConfig.getTableName(), tableConfig.getTableType(),
        tableConfig.getCustomConfig());
      return new SuccessResponse("Successfully updated " + tableName + " configuration");
    } catch (Exception e) {
      String errStr = "Error while updating table configuration, table: " + tableName;
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
