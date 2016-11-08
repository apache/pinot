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
import com.linkedin.pinot.common.restlet.resources.TableSegments;
import com.linkedin.pinot.common.restlet.resources.TablesList;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.server.starter.ServerInstance;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = "Table")
@Path("/")
public class TablesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(TablesResource.class);

  @Inject
  ServerInstance serverInstance;


  @GET
  @Path("/tables")
  @Produces(MediaType.APPLICATION_JSON)
  //swagger annotations
  @ApiOperation(value = "List tables", notes = "List all the tables on this server")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success", response = TablesList.class),
      @ApiResponse(code = 500, message = "Server initialization error", response = ErrorInfo.class)})
  public TablesList listTables() {
    InstanceDataManager dataManager = checkGetInstanceDataManager();
    Collection<TableDataManager> tableDataManagers = dataManager.getTableDataManagers();
    List<String> tables = new ArrayList<>(tableDataManagers.size());
    for (TableDataManager tableDataManager : tableDataManagers) {
      tables.add(tableDataManager.getTableName());
    }
    return new TablesList(tables);
  }

  private InstanceDataManager checkGetInstanceDataManager() {
    if (serverInstance == null) {
      throw new WebApplicationException("Server initialization error. Missing server instance");
    }
    InstanceDataManager dataManager = (InstanceDataManager) serverInstance.getInstanceDataManager();
    if (dataManager == null) {
      throw new WebApplicationException("Server initialization error. Missing data manager",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return dataManager;
  }

  @GET
  @Path("/tables/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List table segments", notes = "List segments of table hosted on this server")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success", response = TableSegments.class),
      @ApiResponse(code = 500, message = "Server initialization error", response = ErrorInfo.class)})
  public TableSegments listTableSegments(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_OFFLINE")
      @PathParam("tableName") String tableName) {
    InstanceDataManager dataManager = checkGetInstanceDataManager();
    TableDataManager tableDataManager = dataManager.getTableDataManager(tableName);
    if (tableDataManager == null) {
      throw new WebApplicationException("Table " + tableName + " does not exist", Response.Status.NOT_FOUND);
    }
    ImmutableList<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    List<String> segments = new ArrayList<>(segmentDataManagers.size());
    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      segments.add(segmentDataManager.getSegmentName());
      tableDataManager.releaseSegment(segmentDataManager);
    }

    return new TableSegments(segments);
  }


}
