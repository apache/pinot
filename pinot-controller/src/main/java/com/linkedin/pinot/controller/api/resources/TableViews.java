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
package com.linkedin.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableViews extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      TableViews.class);
  public static final String IDEALSTATE = "idealstate";
  public static final String EXTERNALVIEW = "externalview";

  @Get
  @Override
  public Representation get() {
    final String tableName = (String) getRequest().getAttributes().get("tableName");
    final int viewPositon = 3;

    String[] path = getReference().getPath().split("/");
    // first part is "" because paths start with /
    if (path.length < (viewPositon + 1) ||
        (!path[viewPositon].equalsIgnoreCase(EXTERNALVIEW) && !path[viewPositon].equalsIgnoreCase(IDEALSTATE))) {
      // this is unexpected condition
      LOGGER.error("Invalid path: {} while reading views", path);
      return responseRepresentation(Status.SERVER_ERROR_INTERNAL, "{\"error\":\"Invalid reqeust path\"");
    }

    final String view = path[viewPositon];
    String tableTypeStr = getQuery().getValues("tableType");

    if (tableTypeStr == null) {
      tableTypeStr = "";
    }
    TableType tableType = null;
    if (! tableTypeStr.isEmpty()) {
      try {
        tableType = TableType.valueOf(tableTypeStr.toUpperCase());
      } catch(IllegalArgumentException e) {
        return responseRepresentation(Status.CLIENT_ERROR_BAD_REQUEST,
            "{\"error\":\"Illegal value for table type: " + tableTypeStr + "\"}");
      } catch (Exception e) {
        LOGGER.error("Error", e);
        return responseRepresentation(Status.SERVER_ERROR_INTERNAL, "error");

      }
    }
    if (tableName == null || view == null) {
      return responseRepresentation(Status.CLIENT_ERROR_BAD_REQUEST,
          "{\"error\" : \"Table name and view type are required\"}");
    }
    return getTableState(tableName, view, tableType);
  }

  public static class TableView
  {
    @JsonProperty("OFFLINE")
    Map<String, Map<String, String>> offline;
    @JsonProperty("REALTIME")
    Map<String, Map<String, String>> realtime;
  }

  // we use name "view" to closely match underlying names and to not
  // confuse with table state of enable/disable
  private Representation getTableState(
      String tableName,
      String view,
      TableType tableType) {
    TableView tableView;
    if (view.equalsIgnoreCase(IDEALSTATE)) {
      tableView = getTableIdealState(tableName, tableType);
    } else if (view.equalsIgnoreCase(EXTERNALVIEW)) {
      tableView = getTableExternalView(tableName, tableType);
    } else {
      return responseRepresentation(Status.CLIENT_ERROR_BAD_REQUEST,
          "{\"error\" : \"Bad view name: " + view + ". Expected idealstate or externalview\" }");
    }

    if (tableView.offline == null && tableView.realtime == null) {
      return responseRepresentation(Status.CLIENT_ERROR_NOT_FOUND,
          "{\"error\" : \"Table not found}");
    }

    ObjectMapper mapper = new ObjectMapper();
    try {
      String response = mapper.writeValueAsString(tableView);
      return responseRepresentation(Status.SUCCESS_OK, response);
    } catch (JsonProcessingException e) {
      LOGGER.error("Failed to serialize {} for table {}", tableName, view, e);
      return responseRepresentation(Status.SERVER_ERROR_INTERNAL, "{\"error\": \"Error serializing response\"}");
    }
  }

  @HttpVerb("get")
  @Summary("Get table external view")
  @Tags({"table"})
  @Paths({"/tables/{tableName}/externalview"})
  private TableView getTableExternalView(
      @Parameter(name = "tableName", in = "path", description = "Table name(without type)", required = true)
      @Nonnull String tableNameOptType,
      @Parameter(name = "tableType", in="query", description = "Table type", required = false)
      @Nullable TableType tableType) {
    TableView tableView = new TableView();
    if (tableType == null || tableType == TableType.OFFLINE) {
      tableView.offline = getExternalView(tableNameOptType, TableType.OFFLINE);
    }
    if (tableType == null || tableType == TableType.REALTIME) {
      tableView.realtime = getExternalView(tableNameOptType, TableType.REALTIME);
    }
    return tableView;
  }

  @HttpVerb("get")
  @Summary("Get table idealstate")
  @Tags({"table"})
  @Paths({"/tables/{tableName}/idealstate"})
  private TableView getTableIdealState(
      @Parameter(name = "tableName", in = "path", description = "Table name(without type)", required = true)
      String tableNameOptType,
      @Parameter(name = "tableType", in="query", description = "Table type", required = false)
      TableType tableType) {
    TableView tableView = new TableView();
    if (tableType == null || tableType == TableType.OFFLINE) {
      tableView.offline = getIdealState(tableNameOptType, TableType.OFFLINE);
    }
    if (tableType == null || tableType == TableType.REALTIME) {
      tableView.realtime = getIdealState(tableNameOptType, TableType.REALTIME);
    }
    return tableView;
  }

  @Nullable
  public Map<String, Map<String, String>> getIdealState(@Nonnull String tableNameOptType,
      @Parameter(name = "tableType", in = "query", description = "Table type", required = false) @Nullable TableType tableType) {
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
