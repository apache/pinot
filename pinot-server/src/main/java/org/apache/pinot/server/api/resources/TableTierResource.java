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
package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.TableTierInfo;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.server.starter.ServerInstance;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * A server-side API to get the storage tiers of immutable segments of the given table from the server being requested.
 */
@Api(tags = "Table", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class TableTierResource {

  @Inject
  private ServerInstance _serverInstance;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableNameWithType}/tiers")
  @ApiOperation(value = "Get storage tiers of immutable segments of the given table", notes = "Get storage tiers of "
      + "immutable segments of the given table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Table not found")
  })
  public String getTableTiers(@ApiParam(value = "Table name with type", required = true) @PathParam("tableNameWithType")
      String tableNameWithType)
      throws WebApplicationException {
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();
    if (instanceDataManager == null) {
      throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
    }
    TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableNameWithType);
    if (tableDataManager == null) {
      throw new WebApplicationException("Table: " + tableNameWithType + " is not found", Response.Status.NOT_FOUND);
    }
    Set<String> mutableSegments = new HashSet<>();
    Map<String, String> segmentTiers = new HashMap<>();
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        if (segmentDataManager instanceof ImmutableSegmentDataManager) {
          ImmutableSegment immutableSegment = (ImmutableSegment) segmentDataManager.getSegment();
          segmentTiers.put(immutableSegment.getSegmentName(), immutableSegment.getTier());
        } else {
          mutableSegments.add(segmentDataManager.getSegmentName());
        }
      }
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
    TableTierInfo tableTierInfo = new TableTierInfo(tableDataManager.getTableName(), segmentTiers, mutableSegments);
    return ResourceUtils.convertToJsonString(tableTierInfo);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableNameWithType}/{segmentName}/tiers")
  @ApiOperation(value = "Get storage tiers of the immutable segment of the given table", notes = "Get storage tiers "
      + "of the immutable segment of the given table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Table or segment not found")
  })
  public String getTableSegmentTiers(
      @ApiParam(value = "Table name with type", required = true) @PathParam("tableNameWithType")
          String tableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName)
      throws WebApplicationException {
    segmentName = URIUtils.decode(segmentName);
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();
    if (instanceDataManager == null) {
      throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
    }
    TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableNameWithType);
    if (tableDataManager == null) {
      throw new WebApplicationException(String.format("Table: %s is not found", tableNameWithType),
          Response.Status.NOT_FOUND);
    }
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(
          String.format("Segment: %s is not found in table: %s", segmentName, tableNameWithType),
          Response.Status.NOT_FOUND);
    }
    Set<String> mutableSegments = new HashSet<>();
    Map<String, String> segmentTiers = new HashMap<>();
    try {
      if (segmentDataManager instanceof ImmutableSegmentDataManager) {
        ImmutableSegment immutableSegment = (ImmutableSegment) segmentDataManager.getSegment();
        segmentTiers.put(immutableSegment.getSegmentName(), immutableSegment.getTier());
      } else {
        mutableSegments.add(segmentDataManager.getSegmentName());
      }
    } finally {
      tableDataManager.releaseSegment(segmentDataManager);
    }
    TableTierInfo tableTierInfo = new TableTierInfo(tableDataManager.getTableName(), segmentTiers, mutableSegments);
    return ResourceUtils.convertToJsonString(tableTierInfo);
  }
}
