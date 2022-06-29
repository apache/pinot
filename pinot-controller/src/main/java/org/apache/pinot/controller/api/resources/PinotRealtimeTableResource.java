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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.validation.RealtimeSegmentValidationManager;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.TABLE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotRealtimeTableResource {
  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @POST
  @Path("/tables/{tableName}/resumeConsumption")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Resume the consumption of a realtime table",
      notes = "Resume the consumption of a realtime table")
  public String resumeConsumption(
      @ApiParam(value = "Name of the table", required = true)
      @PathParam("tableName") String tableName) throws JsonProcessingException {
    // TODO: Add util method for invoking periodic tasks
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    Map<String, String> taskProperties = new HashMap<>();
    taskProperties.put(RealtimeSegmentValidationManager.RECREATE_DELETED_CONSUMING_SEGMENT_KEY, "true");

    Pair<String, Integer> taskExecutionDetails = _pinotHelixResourceManager
        .invokeControllerPeriodicTask(tableNameWithType, Constants.REALTIME_SEGMENT_VALIDATION_MANAGER, taskProperties);

    return "{\"Log Request Id\": \"" + taskExecutionDetails.getLeft()
        + "\",\"Controllers notified\":" + (taskExecutionDetails.getRight() > 0) + "}";
  }
}
