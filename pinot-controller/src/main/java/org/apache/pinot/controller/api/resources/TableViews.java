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
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
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
import org.apache.helix.HelixAdmin;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.tables.SegmentStatusInfo;
import org.apache.pinot.common.utils.tables.TableViewsUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
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

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/idealstate")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_IDEAL_STATE)
  @ApiOperation(value = "Get table ideal state", notes = "Get table ideal state")
  public TableViewsUtils.TableView getIdealState(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("tableType") String tableTypeStr,
      @ApiParam(value = "Comma separated segment names", required = false) @QueryParam("segmentNames")
      String segmentNames, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableType tableType = null;
    try {
      tableType = TableViewsUtils.validateTableType(tableTypeStr);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
    HelixAdmin helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
    String helixClusterName = _pinotHelixResourceManager.getHelixClusterName();
    TableViewsUtils.TableView tableIdealStateView = null;
    try {
      tableIdealStateView =
          TableViewsUtils.getTableState(tableName, TableViewsUtils.IDEALSTATE, tableType, helixAdmin, helixClusterName);
    } catch (Exception e) {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.NOT_FOUND);
    }
    if (StringUtils.isNotEmpty(segmentNames)) {
      List<String> segmentNamesList =
          Arrays.stream(segmentNames.split(",")).map(String::trim).collect(Collectors.toList());
      return TableViewsUtils.getSegmentsView(tableIdealStateView, segmentNamesList);
    }
    return tableIdealStateView;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/externalview")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_EXTERNAL_VIEW)
  @ApiOperation(value = "Get table external view", notes = "Get table external view")
  public TableViewsUtils.TableView getExternalView(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("tableType") String tableTypeStr,
      @ApiParam(value = "Comma separated segment names", required = false) @QueryParam("segmentNames")
      String segmentNames, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableType tableType = null;
    try {
      tableType = TableViewsUtils.validateTableType(tableTypeStr);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
    HelixAdmin helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
    String helixClusterName = _pinotHelixResourceManager.getHelixClusterName();
    TableViewsUtils.TableView tableExternalView = null;
    try {
      tableExternalView = TableViewsUtils.getTableState(tableName, TableViewsUtils.EXTERNALVIEW, tableType, helixAdmin,
          helixClusterName);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.NOT_FOUND);
    }
    if (StringUtils.isNotEmpty(segmentNames)) {
      List<String> segmentNamesList =
          Arrays.stream(segmentNames.split(",")).map(String::trim).collect(Collectors.toList());
      return TableViewsUtils.getSegmentsView(tableExternalView, segmentNamesList);
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
      @ApiParam(value = "Include segments being replaced", required = false) @QueryParam("includeReplacedSegments")
      @DefaultValue("true") boolean includeReplacedSegments, @Context HttpHeaders headers)
      throws JsonProcessingException {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableType tableType = null;
    try {
      tableType = TableViewsUtils.validateTableType(tableTypeStr);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
    HelixAdmin helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
    String helixClusterName = _pinotHelixResourceManager.getHelixClusterName();
    TableViewsUtils.TableView externalView = null;
    TableViewsUtils.TableView idealStateView = null;
    try {
      externalView = TableViewsUtils.getTableState(tableName, TableViewsUtils.EXTERNALVIEW, tableType, helixAdmin,
          helixClusterName);
      idealStateView =
          TableViewsUtils.getTableState(tableName, TableViewsUtils.IDEALSTATE, tableType, helixAdmin, helixClusterName);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.NOT_FOUND);
    }

    Map<String, Map<String, String>> externalViewStateMap = TableViewsUtils.getStateMap(externalView);
    Map<String, Map<String, String>> idealStateMap = TableViewsUtils.getStateMap(idealStateView);
    Set<String> segments = idealStateMap.keySet();

    if (!includeReplacedSegments) {
      SegmentLineage segmentLineage =
          SegmentLineageAccessHelper.getSegmentLineage(_pinotHelixResourceManager.getPropertyStore(), tableName);
      SegmentLineageUtils.filterSegmentsBasedOnLineageInPlace(segments, segmentLineage);
    }

    List<SegmentStatusInfo> segmentStatusInfoListMap =
        TableViewsUtils.getSegmentStatuses(externalViewStateMap, idealStateMap);

    return JsonUtils.objectToPrettyString(segmentStatusInfoListMap);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/badLLCSegmentsPerPartition")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SEGMENT_STATUS)
  @ApiOperation(value = "Get bad LLC segment names per partition id for a realtime table.", notes = "Get a sorted "
      + "list of bad segments per partition id (sort order is in increasing order of segment sequence number)")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Returns a map of partition IDs to the list of segment names in sorted"
          + " order of sequence number"), @ApiResponse(code = 500, message = "Internal Server Error")
  })
  public String getBadLLCSegmentsPerPartition(
      @ApiParam(value = "Name of the table.", required = true) @PathParam("tableName") String tableName,
      @Context HttpHeaders headers) {
    try {
      tableName = DatabaseUtils.translateTableName(tableName, headers);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    }
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, TableType.REALTIME, LOGGER)
            .get(0);
    try {
      HelixAdmin helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
      String helixClusterName = _pinotHelixResourceManager.getHelixClusterName();
      TableViewsUtils.TableView idealStateView =
          TableViewsUtils.getTableState(tableNameWithType, TableViewsUtils.IDEALSTATE, null, helixAdmin,
              helixClusterName);
      TableViewsUtils.TableView externalView =
          TableViewsUtils.getTableState(tableNameWithType, TableViewsUtils.EXTERNALVIEW, null, helixAdmin,
              helixClusterName);

      Map<String, Map<String, String>> idealStateMap = TableViewsUtils.getStateMap(idealStateView);
      Map<String, Map<String, String>> externalViewStateMap = TableViewsUtils.getStateMap(externalView);

      List<SegmentStatusInfo> segmentStatusInfoList =
          TableViewsUtils.getSegmentStatuses(externalViewStateMap, idealStateMap,
              CommonConstants.Helix.StateModel.DisplaySegmentStatus.BAD);

      if (segmentStatusInfoList.isEmpty()) {
        return JsonUtils.objectToPrettyString(new HashMap<>());
      }

      Map<Integer, SortedSet<LLCSegmentName>> partitionIdToSegments = new HashMap<>();
      for (SegmentStatusInfo segmentStatusInfo : segmentStatusInfoList) {
        String segmentName = segmentStatusInfo.getSegmentName();
        LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
        if (llcSegmentName == null) {
          continue;
        }
        partitionIdToSegments.computeIfAbsent(llcSegmentName.getPartitionGroupId(), k -> new TreeSet<>())
            .add(llcSegmentName);
      }

      return JsonUtils.objectToPrettyString(partitionIdToSegments);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
