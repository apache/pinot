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
import java.io.IOException;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.dto.PinotTableReloadStatusResponse;
import org.apache.pinot.controller.services.PinotTableReloadService;
import org.apache.pinot.controller.services.PinotTableReloadStatusReporter;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;

/**
 * REST API resource for reloading table segments.
 * <ul>
 *   <li>
 *     POST requests:
 *     <ul>
 *       <li>"/segments/{tableName}/{segmentName}/reload": reload a specific segment</li>
 *       <li>"/segments/{tableName}/reload": reload all segments in a table</li>
 *     </ul>
 *   </li>
 *   <li>
 *     GET requests:
 *     <ul>
 *       <li>"/segments/segmentReloadStatus/{jobId}": get status for a submitted reload job</li>
 *       <li>"/segments/{tableNameWithType}/needReload": check if table segments need reloading</li>
 *     </ul>
 *   </li>
 * </ul>
 */
@Api(tags = Constants.SEGMENT_TAG, authorizations = {
    @Authorization(value = SWAGGER_AUTHORIZATION_KEY), @Authorization(value = DATABASE)
})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key =
        SWAGGER_AUTHORIZATION_KEY, description = "The format of the key is  ```\"Basic <token>\" or \"Bearer "
        + "<token>\"```"), @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
    key = DATABASE, description =
    "Database context passed through http header. If no context is provided 'default' database "
        + "context will be considered.")
}))
@Path("/")
public class PinotTableReloadResource {
  private static final Logger LOG = LoggerFactory.getLogger(PinotTableReloadResource.class);

  private final PinotTableReloadService _service;
  private final PinotTableReloadStatusReporter _statusReporter;

  @Inject
  public PinotTableReloadResource(PinotTableReloadService service,
      PinotTableReloadStatusReporter statusReporter) {
    _service = service;
    _statusReporter = statusReporter;
  }

  @POST
  @Path("segments/{tableName}/{segmentName}/reload")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.RELOAD_SEGMENT)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a specific segment",
      notes = "Triggers segment reload on servers. Returns job ID and message count.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Reload job submitted successfully"),
      @ApiResponse(code = 404, message = "Segment or table not found")
  })
  public SuccessResponse reloadSegment(
      @ApiParam(value = "Table name with or without type suffix", required = true, example = "myTable_OFFLINE")
      @PathParam("tableName") String tableName,
      @ApiParam(value = "Segment name", required = true, example = "myTable_0")
      @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "Force server to re-download segment from deep store", defaultValue = "false")
      @QueryParam("forceDownload") @DefaultValue("false") boolean forceDownload,
      @ApiParam(value = "Target specific server instance") @QueryParam("targetInstance") @Nullable
      String targetInstance, @Context HttpHeaders headers) {
    return _service.reloadSegment(tableName, segmentName, forceDownload, targetInstance, headers);
  }

  @POST
  @Path("segments/{tableName}/reload")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.RELOAD_SEGMENT)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments in a table",
      notes = "Reloads all segments for the specified table. Supports filtering by type, instance, or custom mapping.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Reload jobs submitted successfully"),
      @ApiResponse(code = 404, message = "No segments found")
  })
  public SuccessResponse reloadAllSegments(
      @ApiParam(value = "Table name with or without type suffix", required = true, example = "myTable")
      @PathParam("tableName") String tableName,
      @ApiParam(value = "Table type filter", allowableValues = "OFFLINE,REALTIME") @QueryParam("type")
      String tableTypeStr,
      @ApiParam(value = "Force server to re-download segments from deep store", defaultValue = "false")
      @QueryParam("forceDownload") @DefaultValue("false") boolean forceDownload,
      @ApiParam(value = "Target specific server instance") @QueryParam("targetInstance") @Nullable
      String targetInstance,
      @ApiParam(value = "JSON map of instance to segment lists (overrides targetInstance)")
      @QueryParam("instanceToSegmentsMap") @Nullable String instanceToSegmentsMapInJson, @Context HttpHeaders headers)
      throws IOException {
    return _service.reloadAllSegments(tableName, tableTypeStr, forceDownload, targetInstance,
        instanceToSegmentsMapInJson, headers);
  }

  @GET
  @Path("segments/segmentReloadStatus/{jobId}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_SEGMENT_RELOAD_STATUS)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get reload job status",
      notes = "Returns progress and metadata for a reload job including completion stats and time estimates.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Job status retrieved successfully"),
      @ApiResponse(code = 404, message = "Job ID not found")
  })
  public PinotTableReloadStatusResponse getReloadJobStatus(
      @ApiParam(value = "Reload job ID returned from reload endpoint", required = true) @PathParam("jobId")
      String reloadJobId) throws Exception {
    return _statusReporter.getReloadJobStatus(reloadJobId);
  }

  @GET
  @Path("segments/{tableNameWithType}/needReload")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableNameWithType", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Check if table needs reload",
      notes = "Queries all servers hosting the table to determine if segments need reloading.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Reload check completed successfully"),
      @ApiResponse(code = 400, message = "Invalid table configuration")
  })
  public String needReload(
      @ApiParam(value = "Table name with type suffix", required = true, example = "myTable_REALTIME")
      @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Include detailed server responses", defaultValue = "false") @QueryParam("verbose")
      @DefaultValue("false") boolean verbose, @Context HttpHeaders headers) {
    return _service.needReload(tableNameWithType, verbose, headers);
  }
}
