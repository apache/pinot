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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Debug endpoint to check memory allocation.
 */
@Api(value = "debug", description = "Debug information", tags = "Debug")
@Path("debug")
public class MmapDebugResource {

  @Inject
  private ServerInstance _serverInstance;

  @GET
  @Path("memory/offheap")
  @ApiOperation(value = "View current off-heap allocations",
      notes = "Lists all off-heap allocations and their associated sizes")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success")})
  @Produces(MediaType.APPLICATION_JSON)
  public List<String> getOffHeapSizes() {
    return PinotDataBuffer.getBufferInfo();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/memory/offheap/table/{tableName}")
  @ApiOperation(value = "Show off heap memory consumed by latest mutable segment",
      notes = "Returns off heap memory consumed by latest consuming segment of realtime table")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500,
      message = "Internal server error"), @ApiResponse(code = 404, message = "Table not found")})
  public String getTableSize(
      @ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName)
      throws WebApplicationException {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME) {
      throw new WebApplicationException("This api cannot be used with non real-time table: " + tableName,
          Response.Status.BAD_REQUEST);
    }

    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();
    if (instanceDataManager == null) {
      throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
    }
    RealtimeTableDataManager realtimeTableDataManager =
        (RealtimeTableDataManager) instanceDataManager.getTableDataManager(tableName);
    if (realtimeTableDataManager == null) {
      throw new WebApplicationException("Table: " + tableName + " is not found", Response.Status.NOT_FOUND);
    }

    long memoryConsumed = realtimeTableDataManager.getStatsHistory().getLatestSegmentMemoryConsumed();
    return ResourceUtils.convertToJsonString(Collections.singletonMap("offheapMemoryConsumed", memoryConsumed));
  }
}
