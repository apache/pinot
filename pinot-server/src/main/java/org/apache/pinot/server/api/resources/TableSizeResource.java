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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.server.starter.ServerInstance;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * API to provide table sizes
 */
@Api(tags = "Table", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class TableSizeResource {

  @Inject
  private ServerInstance _serverInstance;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/size")
  @ApiOperation(value = "Show table storage size", notes = "Lists size of all the segments of the table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Table not found")
  })
  public String getTableSize(
      @ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Provide detailed information") @DefaultValue("true") @QueryParam("detailed") boolean detailed)
      throws WebApplicationException {
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();

    if (instanceDataManager == null) {
      throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
    }

    TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableName);
    if (tableDataManager == null) {
      throw new WebApplicationException("Table: " + tableName + " is not found", Response.Status.NOT_FOUND);
    }

    long tableSizeInBytes = 0L;
    List<SegmentSizeInfo> segmentSizeInfos = new ArrayList<>();
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        if (segmentDataManager instanceof ImmutableSegmentDataManager) {
          ImmutableSegment immutableSegment = (ImmutableSegment) segmentDataManager.getSegment();
          long segmentSizeBytes = immutableSegment.getSegmentSizeBytes();
          if (detailed) {
            segmentSizeInfos.add(new SegmentSizeInfo(immutableSegment.getSegmentName(), segmentSizeBytes));
          }
          tableSizeInBytes += segmentSizeBytes;
        }
      }
    } finally {
      // we could release segmentDataManagers as we iterate in the loop above
      // but this is cleaner with clear semantics of usage. Also, above loop
      // executes fast so duration of holding segments is not a concern
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }

    TableSizeInfo tableSizeInfo =
        new TableSizeInfo(tableDataManager.getTableName(), tableSizeInBytes, segmentSizeInfos);
    //invalid to use the segmentDataManagers below
    return ResourceUtils.convertToJsonString(tableSizeInfo);
  }

  // same as above but with /tables (plural) path for consistency.
  // /table was by mistake. We will use plural from hereon
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/table/{tableName}/size")
  @ApiOperation(value = "Show table storage size", notes = "Lists size of all the segments of the table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Table not found")
  })
  @Deprecated
  public String getTableSizeOld(
      @ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Provide detailed information") @DefaultValue("true") @QueryParam("detailed") boolean detailed)
      throws WebApplicationException {
    return this.getTableSize(tableName, detailed);
  }
}
