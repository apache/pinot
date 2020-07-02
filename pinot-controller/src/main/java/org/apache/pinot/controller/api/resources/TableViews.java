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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class TableViews {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableViews.class);
  public static final String IDEALSTATE = "idealstate";
  public static final String EXTERNALVIEW = "externalview";

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  public static class TableView {
    @JsonProperty("OFFLINE")
    public Map<String, Map<String, String>> offline;
    @JsonProperty("REALTIME")
    public Map<String, Map<String, String>> realtime;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/idealstate")
  @ApiOperation(value = "Get table ideal state", notes = "Get table ideal state")
  public TableView getIdealState(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("tableType") String tableTypeStr) {
    TableType tableType = validateTableType(tableTypeStr);
    return getTableState(tableName, IDEALSTATE, tableType);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/externalview")
  @ApiOperation(value = "Get table external view", notes = "Get table external view")
  public TableView getExternalView(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("tableType") String tableTypeStr) {
    TableType tableType = validateTableType(tableTypeStr);
    return getTableState(tableName, EXTERNALVIEW, tableType);
  }

  // we use name "view" to closely match underlying names and to not
  // confuse with table state of enable/disable
  private TableView getTableState(String tableName, String view, TableType tableType) {
    TableView tableView;
    if (view.equalsIgnoreCase(IDEALSTATE)) {
      tableView = getTableIdealState(tableName, tableType);
    } else if (view.equalsIgnoreCase(EXTERNALVIEW)) {
      tableView = getTableExternalView(tableName, tableType);
    } else {
      throw new ControllerApplicationException(LOGGER,
          "Bad view name: " + view + ". Expected idealstate or externalview", Response.Status.BAD_REQUEST);
    }

    if (tableView.offline == null && tableView.realtime == null) {
      throw new ControllerApplicationException(LOGGER, "Table not found", Response.Status.NOT_FOUND);
    }
    return tableView;
  }

  private TableView getTableIdealState(String tableNameOptType, TableType tableType) {
    TableView tableView = new TableView();
    if (tableType == null || tableType == TableType.OFFLINE) {
      tableView.offline = getIdealState(tableNameOptType, TableType.OFFLINE);
    }
    if (tableType == null || tableType == TableType.REALTIME) {
      tableView.realtime = getIdealState(tableNameOptType, TableType.REALTIME);
    }
    return tableView;
  }

  private TableView getTableExternalView(@Nonnull String tableNameOptType, @Nullable TableType tableType) {
    TableView tableView = new TableView();
    if (tableType == null || tableType == TableType.OFFLINE) {
      tableView.offline = getExternalView(tableNameOptType, TableType.OFFLINE);
    }
    if (tableType == null || tableType == TableType.REALTIME) {
      tableView.realtime = getExternalView(tableNameOptType, TableType.REALTIME);
    }
    return tableView;
  }

  private TableType validateTableType(String tableTypeStr) {
    if (tableTypeStr == null) {
      return null;
    }
    try {
      return TableType.valueOf(tableTypeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      String errStr = "Illegal table type '" + tableTypeStr + "'";
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST, e);
    }
  }

  @Nullable
  public Map<String, Map<String, String>> getIdealState(@Nonnull String tableNameOptType,
      @Nullable TableType tableType) {
    String tableNameWithType = getTableNameWithType(tableNameOptType, tableType);
    IdealState resourceIdealState = _pinotHelixResourceManager.getHelixAdmin()
        .getResourceIdealState(_pinotHelixResourceManager.getHelixClusterName(), tableNameWithType);
    return resourceIdealState == null ? null : resourceIdealState.getRecord().getMapFields();
  }

  @Nullable
  public Map<String, Map<String, String>> getExternalView(@Nonnull String tableNameOptType, TableType tableType) {
    String tableNameWithType = getTableNameWithType(tableNameOptType, tableType);
    ExternalView resourceEV = _pinotHelixResourceManager.getHelixAdmin()
        .getResourceExternalView(_pinotHelixResourceManager.getHelixClusterName(), tableNameWithType);
    return resourceEV == null ? null : resourceEV.getRecord().getMapFields();
  }

  private String getTableNameWithType(@Nonnull String tableNameOptType, @Nullable TableType tableType) {
    if (tableType != null) {
      if (tableType == TableType.OFFLINE) {
        return TableNameBuilder.OFFLINE.tableNameWithType(tableNameOptType);
      } else {
        return TableNameBuilder.REALTIME.tableNameWithType(tableNameOptType);
      }
    } else {
      return tableNameOptType;
    }
  }
}
