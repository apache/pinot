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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.validation.RealtimeSegmentValidationManager;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

@Api(tags = Constants.TABLE_TAG)
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
