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
import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.TABLE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class TableViews {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableViews.class);
  public static final String IDEALSTATE = "idealstate";
  public static final String EXTERNALVIEW = "externalview";

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  public static class TableView {
    @JsonProperty("OFFLINE")
    public Map<String, Map<String, String>> _offline;
    @JsonProperty("REALTIME")
    public Map<String, Map<String, String>> _realtime;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/idealstate")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_IDEAL_STATE)
  @ApiOperation(value = "Get table ideal state", notes = "Get table ideal state")
  public TableView getIdealState(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("tableType") String tableTypeStr,
      @ApiParam(value = "Comma separated segment names", required = false) @QueryParam("segmentNames")
      String segmentNames, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableType tableType = validateTableType(tableTypeStr);
    TableViews.TableView tableIdealStateView = getTableState(tableName, IDEALSTATE, tableType);
    if (StringUtils.isNotEmpty(segmentNames)) {
      List<String> segmentNamesList =
          Arrays.stream(segmentNames.split(",")).map(String::trim).collect(Collectors.toList());
      return getSegmentsView(tableIdealStateView, segmentNamesList);
    }
    return tableIdealStateView;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/externalview")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_EXTERNAL_VIEW)
  @ApiOperation(value = "Get table external view", notes = "Get table external view")
  public TableView getExternalView(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("tableType") String tableTypeStr,
      @ApiParam(value = "Comma separated segment names", required = false) @QueryParam("segmentNames")
      String segmentNames, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableType tableType = validateTableType(tableTypeStr);
    TableViews.TableView tableExternalView = getTableState(tableName, EXTERNALVIEW, tableType);
    if (StringUtils.isNotEmpty(segmentNames)) {
      List<String> segmentNamesList =
          Arrays.stream(segmentNames.split(",")).map(String::trim).collect(Collectors.toList());
      return getSegmentsView(tableExternalView, segmentNamesList);
    }
    return tableExternalView;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/segmentsStatus")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SEGMENT_STATUS)
  @ApiOperation(value = "Get segment names to segment status map", notes = "Get segment statuses of each segment")
  public String getSegmentsStatusDetails(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("tableType") String tableTypeStr,
      @Context HttpHeaders headers)
      throws JsonProcessingException {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableType tableType = validateTableType(tableTypeStr);
    TableViews.TableView externalView = getTableState(tableName, TableViews.EXTERNALVIEW, tableType);
    TableViews.TableView idealStateView = getTableState(tableName, TableViews.IDEALSTATE, tableType);
    List<SegmentStatusInfo> segmentStatusInfoListMap = new ArrayList<>();
    segmentStatusInfoListMap = getSegmentStatuses(externalView, idealStateView);
    return JsonUtils.objectToPrettyString(segmentStatusInfoListMap);
  }

  public List<SegmentStatusInfo> getSegmentStatuses(TableViews.TableView externalView,
      TableViews.TableView idealStateView) {
    Map<String, Map<String, String>> idealStateMap = getStateMap(idealStateView);
    Map<String, Map<String, String>> externalViewMap = getStateMap(externalView);
    List<SegmentStatusInfo> segmentStatusInfoList = new ArrayList<>();

    for (Map.Entry<String, Map<String, String>> entry : externalViewMap.entrySet()) {
      String segment = entry.getKey();
      Map<String, String> externalViewEntryValue = entry.getValue();
      Map<String, String> idealViewEntryValue = idealStateMap.get(segment);
      if (isErrorSegment(externalViewEntryValue)) {
        segmentStatusInfoList.add(
            new SegmentStatusInfo(segment, CommonConstants.Helix.StateModel.DisplaySegmentStatus.BAD));
      } else {
        boolean isViewsEqual = externalViewEntryValue.equals(idealViewEntryValue);
        if (isViewsEqual) {
          if (isOnlineOrConsumingSegment(externalViewEntryValue)) {
            segmentStatusInfoList.add(
                new SegmentStatusInfo(segment, CommonConstants.Helix.StateModel.DisplaySegmentStatus.GOOD));
          } else if (isOfflineSegment(externalViewEntryValue)) {
            segmentStatusInfoList.add(
                new SegmentStatusInfo(segment, CommonConstants.Helix.StateModel.DisplaySegmentStatus.GOOD));
          } else {
            segmentStatusInfoList.add(
                new SegmentStatusInfo(segment, CommonConstants.Helix.StateModel.DisplaySegmentStatus.UPDATING));
          }
        } else {
          segmentStatusInfoList.add(
              new SegmentStatusInfo(segment, CommonConstants.Helix.StateModel.DisplaySegmentStatus.UPDATING));
        }
      }
    }
    return segmentStatusInfoList;
  }

  public TableView getSegmentsView(TableViews.TableView tableView, List<String> segmentNames) {
    TableView tableViewResult = new TableView();
    if (tableView._offline != null) {
      tableViewResult._offline = getTableTypeSegmentsView(tableView._offline, segmentNames);
    }
    if (tableView._realtime != null) {
      tableViewResult._realtime = getTableTypeSegmentsView(tableView._realtime, segmentNames);
    }
    return tableViewResult;
  }

  private Map<String, Map<String, String>> getTableTypeSegmentsView(Map<String, Map<String, String>> tableTypeView,
      List<String> segmentNames) {
    Map<String, Map<String, String>> tableTypeViewResult = new HashMap<>();
    for (String segmentName : segmentNames) {
      Map<String, String> segmentView = tableTypeView.get(segmentName);
      if (segmentView != null) {
        tableTypeViewResult.put(segmentName, segmentView);
      }
    }
    return tableTypeViewResult;
  }

  private Map<String, Map<String, String>> getStateMap(TableViews.TableView view) {
    if (view != null && view._offline != null && !view._offline.isEmpty()) {
      return view._offline;
    } else if (view != null && view._realtime != null && !view._realtime.isEmpty()) {
      return view._realtime;
    } else {
      return new HashMap<>();
    }
  }

  private boolean isErrorSegment(Map<String, String> stateMap) {
    return stateMap.values().contains(CommonConstants.Helix.StateModel.SegmentStateModel.ERROR);
  }

  private boolean isOnlineOrConsumingSegment(Map<String, String> stateMap) {
    return stateMap.values().stream().allMatch(
        state -> state.equals(CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING) || state.equals(
            CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE));
  }

  private boolean isOfflineSegment(Map<String, String> stateMap) {
    return stateMap.values().contains(CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE);
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

    if (tableView._offline == null && tableView._realtime == null) {
      throw new ControllerApplicationException(LOGGER, "Table not found", Response.Status.NOT_FOUND);
    }
    return tableView;
  }

  private TableView getTableIdealState(String tableNameOptType, TableType tableType) {
    TableView tableView = new TableView();
    if (tableType == null || tableType == TableType.OFFLINE) {
      tableView._offline = getIdealState(tableNameOptType, TableType.OFFLINE);
    }
    if (tableType == null || tableType == TableType.REALTIME) {
      tableView._realtime = getIdealState(tableNameOptType, TableType.REALTIME);
    }
    return tableView;
  }

  private TableView getTableExternalView(@Nonnull String tableNameOptType, @Nullable TableType tableType) {
    TableView tableView = new TableView();
    if (tableType == null || tableType == TableType.OFFLINE) {
      tableView._offline = getExternalView(tableNameOptType, TableType.OFFLINE);
    }
    if (tableType == null || tableType == TableType.REALTIME) {
      tableView._realtime = getExternalView(tableNameOptType, TableType.REALTIME);
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
