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
package com.linkedin.pinot.server.api.resources;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.restlet.resources.SegmentSizeInfo;
import com.linkedin.pinot.common.restlet.resources.TableSizeInfo;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.server.starter.ServerInstance;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API to provide table sizes
 */
@Api(tags = "Table")
@Path("/")
public class TableSizeResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSizeResource.class);

  @Inject
  ServerInstance serverInstance;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/size")
  @ApiOperation(value = "Show table storage size", notes = "Lists size of all the segments of the table")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error"), @ApiResponse(code = 404, message = "Table not found")})
  public TableSizeInfo getTableSize(
      @ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Provide detailed information", required = false) @DefaultValue("true") @QueryParam("detailed") boolean detailed)
      throws WebApplicationException {
    InstanceDataManager instanceDataManager = serverInstance.getInstanceDataManager();

    if (instanceDataManager == null) {
      throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
    }

    TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableName);
    if (tableDataManager == null) {
      throw new WebApplicationException("Table: " + tableName + " is not found", Response.Status.NOT_FOUND);
    }

    TableSizeInfo tableSizeInfo = new TableSizeInfo();
    tableSizeInfo.tableName = tableDataManager.getTableName();
    tableSizeInfo.diskSizeInBytes = 0L;

    ImmutableList<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        IndexSegment segment = segmentDataManager.getSegment();
        long segmentSizeBytes = segment.getDiskSizeBytes();
        if (detailed) {
          SegmentSizeInfo segmentSizeInfo = new SegmentSizeInfo(segment.getSegmentName(), segmentSizeBytes);
          tableSizeInfo.segments.add(segmentSizeInfo);
        }
        tableSizeInfo.diskSizeInBytes += segmentSizeBytes;
      }
    } finally {

      // we could release segmentDataManagers as we iterate in the loop above
      // but this is cleaner with clear semantics of usage. Also, above loop
      // executes fast so duration of holding segments is not a concern
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
    //invalid to use the segmentDataManagers below
    return tableSizeInfo;
  }

  // same as above but with /tables (plural) path for consistency.
  // /table was by mistake. We will use plural from hereon
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/table/{tableName}/size")
  @ApiOperation(value = "Show table storage size", notes = "Lists size of all the segments of the table")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error"), @ApiResponse(code = 404, message = "Table not found")})
  @Deprecated
  public TableSizeInfo getTableSizeOld(
      @ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Provide detailed information", required = false) @DefaultValue("true") @QueryParam("detailed") boolean detailed)
      throws WebApplicationException {
    return this.getTableSize(tableName, detailed);
  }

}
