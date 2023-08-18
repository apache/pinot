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
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.controllerjob.ControllerJobType;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.controller.util.TableMetadataReader;
import org.apache.pinot.controller.util.TableTierReader;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Segment admin rest APIs:
 * <ul>
 *   <li>
 *     GET requests:
 *     <ul>
 *       <li>"/segments/{tableName}": get the name of all segments</li>
 *       <li>"/segments/{tableName}/servers": get a map from server to segments hosted by the server</li>
 *       <li>"/segments/{tableName}/crc": get a map from segment to CRC of the segment (OFFLINE table only)</li>
 *       <li>"/segments/{tableName}/{segmentName}/metadata: get the metadata for a segment</li>
 *       <li>"/segments/{tableName}/metadata: get the metadata for all segments from the server</li>
 *       <li>"/segments/{tableName}/zkmetadata: get the zk metadata for all segments of a table</li>
 *       <li>"/segments/{tableName}/{segmentName}/tiers": get storage tier for the segment in the table</li>
 *       <li>"/segments/{tableName}/tiers": get storage tier for all segments in the table</li>
 *     </ul>
 *   </li>
 *   <li>
 *     POST requests:
 *     <ul>
 *       <li>"/segments/{tableName}/{segmentName}/reload": reload a segment</li>
 *       <li>"/segments/{tableName}/reload": reload all segments</li>
 *       <li>"/segments/{tableNameWithType}/{segmentName}/reset": reset a segment</li>
 *       <li>"/segments/{tableNameWithType}/reset": reset all segments</li>
 *       <li>"/segments/{tableName}/delete": delete the segments in the payload</li>
 *     </ul>
 *   </li>
 *   <li>
 *     DELETE requests:
 *     <ul>
 *       <li>"/segments/{tableName}/{segmentName}": delete a segment</li>
 *       <li>"/segments/{tableName}: delete all segments</li>
 *     </ul>
 *   </li>
 *   <li>
 *     The following requests can take a query parameter "type" (OFFLINE or REALTIME) for table type. The request will
 *     be performed to tables that match the table name and type.
 *     E.g. "foobar_OFFLINE" matches:
 *     ("foobar_OFFLINE", null), ("foobar_OFFLINE", OFFLINE), ("foobar", null), ("foobar", OFFLINE);
 *     "foobar_OFFLINE" does not match:
 *     ("foo", null), ("foobar_REALTIME", null), ("foobar_REALTIME", OFFLINE), ("foobar_OFFLINE", REALTIME).
 *     <ul>
 *       <li>
 *         Requests with optional "type":
 *         <ul>
 *           <li>"GET /segments/{tableName}"</li>
 *           <li>"GET /segments/{tableName}/servers"</li>
 *           <li>"POST /segments/{tableName}/reload"</li>
 *         </ul>
 *       </li>
 *       <li>
 *         Requests with mandatory "type":
 *         <ul>
 *           <li>"DELETE /segments/{tableName}"</li>
 *         </ul>
 *       </li>
 *     </ul>
 *   </li>
 *   <li>
 *     Deprecated APIs:
 *     <ul>
 *       <li>"GET /tables/{tableName}/segments"</li>
 *       <li>"GET /tables/{tableName}/segments/metadata"</li>
 *       <li>"GET /tables/{tableName}/segments/crc"</li>
 *       <li>"GET /tables/{tableName}/segments/{segmentName}"</li>
 *       <li>"GET /tables/{tableName}/segments/{segmentName}/metadata"</li>
 *       <li>"GET /tables/{tableName}/segments/{segmentName}/reload"</li>
 *       <li>"POST /tables/{tableName}/segments/{segmentName}/reload"</li>
 *       <li>"GET /tables/{tableName}/segments/reload"</li>
 *       <li>"POST /tables/{tableName}/segments/reload"</li>
 *     </ul>
 *   </li>
 * </ul>
 */
@Api(tags = Constants.SEGMENT_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotSegmentRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRestletResource.class);

  @Inject
  ControllerConf _controllerConf;

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  Executor _executor;

  @Inject
  HttpClientConnectionManager _connectionManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Cluster.GET_SEGMENT)
  @ApiOperation(value = "List all segments. An optional 'excludeReplacedSegments' parameter is used to get the"
      + " list of segments which has not yet been replaced (determined by segment lineage entries) and can be queried"
      + " from the table. The value is false by default.",
      // TODO: more and more filters can be added later on, like excludeErrorSegments, excludeConsumingSegments, etc.
      notes = "List all segments")
  public List<Map<TableType, List<String>>> getSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Whether to exclude replaced segments in the response, which have been replaced"
          + " specified in the segment lineage entries and cannot be queried from the table")
      @QueryParam("excludeReplacedSegments") String excludeReplacedSegments,
      @ApiParam(value = "Start timestamp (inclusive)") @QueryParam("startTimestamp") @DefaultValue("")
      String startTimestampStr,
      @ApiParam(value = "End timestamp (exclusive)") @QueryParam("endTimestamp") @DefaultValue("")
      String endTimestampStr,
      @ApiParam(value = "Whether to exclude the segments overlapping with the timestamps, false by default")
      @QueryParam("excludeOverlapping") @DefaultValue("false") boolean excludeOverlapping) {
    boolean shouldExcludeReplacedSegments = Boolean.parseBoolean(excludeReplacedSegments);
    return selectSegments(tableName, tableTypeStr, shouldExcludeReplacedSegments,
        startTimestampStr, endTimestampStr, excludeOverlapping)
        .stream()
        .map(pair -> Collections.singletonMap(pair.getKey(), pair.getValue()))
        .collect(Collectors.toList());
  }

  @GET
  @Path("segments/{tableName}/servers")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SERVER_MAP)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from server to segments hosted by the server",
      notes = "Get a map from server to segments hosted by the server")
  public List<Map<String, Object>> getServerToSegmentsMap(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    List<String> tableNamesWithType = ResourceUtils
        .getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, Constants.validateTableType(tableTypeStr),
            LOGGER);
    List<Map<String, Object>> resultList = new ArrayList<>(tableNamesWithType.size());
    for (String tableNameWithType : tableNamesWithType) {
      Map<String, Object> resultForTable = new LinkedHashMap<>();
      resultForTable.put("tableName", tableNameWithType);
      resultForTable.put("serverToSegmentsMap", _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType));
      resultList.add(resultForTable);
    }
    return resultList;
  }

  @GET
  @Path("segments/{tableName}/lineage")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SEGMENT_LINEAGE)
  @Authenticate(AccessType.READ)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List segment lineage", notes = "List segment lineage in chronologically sorted order")
  public Response listSegmentLineage(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr) {
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type should either be offline or realtime",
          Response.Status.BAD_REQUEST);
    }
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    try {
      Response.ResponseBuilder builder = Response.ok();
      SegmentLineage segmentLineage = _pinotHelixResourceManager.listSegmentLineage(tableNameWithType);
      if (segmentLineage != null) {
        builder.entity(segmentLineage.toJsonObject());
      }
      return builder.build();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Exception while listing segment lineage: %s for table: %s.", e.getMessage(),
              tableNameWithType),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SERVER_MAP)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from server to segments hosted by the server (deprecated, use 'GET "
      + "/segments/{tableName}/servers' instead)",
      notes = "Get a map from server to segments hosted by the server (deprecated, use 'GET "
          + "/segments/{tableName}/servers' instead)")
  public List<Map<String, String>> getServerToSegmentsMapDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "MUST be null") @QueryParam("state") String stateStr,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr)
      throws JsonProcessingException {
    if (stateStr != null) {
      throw new WebApplicationException("Cannot toggle segment state", Status.FORBIDDEN);
    }

    List<String> tableNamesWithType = ResourceUtils
        .getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, Constants.validateTableType(tableTypeStr),
            LOGGER);
    List<Map<String, String>> resultList = new ArrayList<>(tableNamesWithType.size());
    for (String tableNameWithType : tableNamesWithType) {
      // NOTE: DO NOT change the format for backward-compatibility
      Map<String, String> resultForTable = new LinkedHashMap<>();
      resultForTable.put("tableName", tableNameWithType);
      resultForTable.put("segments",
          JsonUtils.objectToString(_pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType)));
      resultList.add(resultForTable);
    }
    return resultList;
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/metadata")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SERVER_MAP)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from server to segments hosted by the server (deprecated, use 'GET "
      + "/segments/{tableName}/servers' instead)",
      notes = "Get a map from server to segments hosted by the server (deprecated, use 'GET "
          + "/segments/{tableName}/servers' instead)")
  public List<Map<String, String>> getServerToSegmentsMapDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "MUST be null") @QueryParam("state") String stateStr,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr)
      throws JsonProcessingException {
    return getServerToSegmentsMapDeprecated1(tableName, stateStr, tableTypeStr);
  }

  @GET
  @Path("segments/{tableName}/crc")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SEGMENT_MAP)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from segment to CRC of the segment (only apply to OFFLINE table)",
      notes = "Get a map from segment to CRC of the segment (only apply to OFFLINE table)")
  public Map<String, String> getSegmentToCrcMap(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName) {
    String offlineTableName =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, TableType.OFFLINE, LOGGER)
            .get(0);
    return _pinotHelixResourceManager.getSegmentsCrcForTable(offlineTableName);
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/crc")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SEGMENT_MAP)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(
      value = "Get a map from segment to CRC of the segment (deprecated, use 'GET /segments/{tableName}/crc' instead)",
      notes = "Get a map from segment to CRC of the segment (deprecated, use 'GET /segments/{tableName}/crc' instead)")
  public Map<String, String> getSegmentToCrcMapDeprecated(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName) {
    return getSegmentToCrcMap(tableName);
  }

  @GET
  @Path("segments/{tableName}/{segmentName}/metadata")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the metadata for a segment", notes = "Get the metadata for a segment")
  public Map<String, Object> getSegmentMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "Columns name", allowMultiple = true) @QueryParam("columns") List<String> columns) {
    segmentName = URIUtils.decode(segmentName);
    Map<String, String> segmentMetadata = null;
    if (TableNameBuilder.getTableTypeFromTableName(tableName) != null) {
      segmentMetadata = getSegmentMetadataInternal(tableName, segmentName);
    } else {
      segmentMetadata = getSegmentMetadataInternal(TableNameBuilder.OFFLINE.tableNameWithType(tableName), segmentName);
      if (segmentMetadata == null) {
        segmentMetadata =
            getSegmentMetadataInternal(TableNameBuilder.REALTIME.tableNameWithType(tableName), segmentName);
      }
    }

    if (segmentMetadata != null) {
      Map<String, Object> result = new HashMap<>(segmentMetadata);
      if (columns.size() > 0) {
        JsonNode segmentsMetadataJson = getExtraMetaData(tableName, segmentName, columns);
        if (segmentsMetadataJson.has("indexes")) {
          result.put("indexes", segmentsMetadataJson.get("indexes"));
        }
        if (segmentsMetadataJson.has("columns")) {
          result.put("columns", segmentsMetadataJson.get("columns"));
        }
      }
      return result;
    } else {
      throw new ControllerApplicationException(LOGGER,
          "Failed to find segment: " + segmentName + " in table: " + tableName, Status.NOT_FOUND);
    }
  }

  private JsonNode getExtraMetaData(String tableName, String segmentName, List<String> columns) {
    try {
      TableMetadataReader tableMetadataReader = new TableMetadataReader(_executor,
          _connectionManager, _pinotHelixResourceManager);
      return tableMetadataReader.getSegmentMetadata(tableName, segmentName, columns,
          _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Status.BAD_REQUEST);
    } catch (IOException ioe) {
      throw new ControllerApplicationException(LOGGER, "Error parsing Pinot server response: " + ioe.getMessage(),
          Status.INTERNAL_SERVER_ERROR, ioe);
    }
  }

  @Nullable
  private Map<String, String> getSegmentMetadataInternal(String tableNameWithType, String segmentName) {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(propertyStore, tableNameWithType, segmentName);
    return segmentZKMetadata != null ? segmentZKMetadata.toMap() : null;
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/{segmentName}/metadata")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(
      value = "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' "
          + "instead)",
      notes = "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' "
          + "instead)")
  public List<List<Map<String, Object>>> getSegmentMetadataDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    segmentName = URIUtils.decode(segmentName);
    TableType tableType = Constants.validateTableType(tableTypeStr);
    List<String> tableNamesWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER);
    List<List<Map<String, Object>>> resultList = new ArrayList<>(tableNamesWithType.size());
    for (String tableNameWithType : tableNamesWithType) {
      Map<String, Object> segmentMetadata = getSegmentMetadata(tableNameWithType, segmentName, Collections.emptyList());
      if (segmentMetadata != null) {
        // NOTE: DO NOT change the format for backward-compatibility
        Map<String, Object> resultForTable = new LinkedHashMap<>();
        resultForTable.put("tableName", tableNameWithType);
        resultForTable.put("state", segmentMetadata);
        resultList.add(Collections.singletonList(resultForTable));
      }
    }
    if (resultList.isEmpty()) {
      String errorMessage = "Failed to find segment: " + segmentName + " in table: " + tableName;
      if (tableType != null) {
        errorMessage += " of type: " + tableType;
      }
      throw new ControllerApplicationException(LOGGER, errorMessage, Status.NOT_FOUND);
    }
    return resultList;
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/{segmentName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(
      value = "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' "
          + "instead)",
      notes = "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' "
          + "instead)")
  public List<List<Map<String, Object>>> getSegmentMetadataDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "MUST be null") @QueryParam("state") String stateStr,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    if (stateStr != null) {
      throw new WebApplicationException("Cannot toggle segment state", Status.FORBIDDEN);
    }

    return getSegmentMetadataDeprecated1(tableName, segmentName, tableTypeStr);
  }

  @POST
  @Path("segments/{tableName}/{segmentName}/reload")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.RELOAD_SEGMENT)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a segment", notes = "Reload a segment")
  public SuccessResponse reloadSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "Whether to force server to download segment") @QueryParam("forceDownload")
      @DefaultValue("false") boolean forceDownload) {
    segmentName = URIUtils.decode(segmentName);
    String tableNameWithType = getExistingTable(tableName, segmentName);
    Pair<Integer, String> msgInfo =
        _pinotHelixResourceManager.reloadSegment(tableNameWithType, segmentName, forceDownload);
    boolean zkJobMetaWriteSuccess = false;
    if (msgInfo.getLeft() > 0) {
      try {
        if (_pinotHelixResourceManager.addNewReloadSegmentJob(tableNameWithType, segmentName, msgInfo.getRight(),
            msgInfo.getLeft())) {
          zkJobMetaWriteSuccess = true;
        } else {
          LOGGER.error("Failed to add reload segment job meta into zookeeper for table: {}, segment: {}",
              tableNameWithType, segmentName);
        }
      } catch (Exception e) {
        LOGGER.error("Failed to add reload segment job meta into zookeeper for table: {}, segment: {}",
            tableNameWithType, segmentName, e);
      }
      return new SuccessResponse(
          String.format("Submitted reload job id: %s, sent %d reload messages. Job meta ZK storage status: %s",
              msgInfo.getRight(), msgInfo.getLeft(), zkJobMetaWriteSuccess ? "SUCCESS" : "FAILED"));
    } else {
      throw new ControllerApplicationException(LOGGER,
          "Failed to find segment: " + segmentName + " in table: " + tableName, Status.NOT_FOUND);
    }
  }

  /**
   * Helper method to find the existing table based on the given table name (with or without type suffix) and segment
   * name.
   */
  private String getExistingTable(String tableName, String segmentName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      // Derive table type from segment name if the given table name doesn't have type suffix
      tableType = SegmentName.isRealtimeSegmentName(segmentName) ? TableType.REALTIME : TableType.OFFLINE;
    }
    return ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
  }

  /**
   * Resets the segment of the table, by disabling and then enabling it.
   * This API will take segments to OFFLINE state, wait for External View to stabilize, and then back to
   * ONLINE/CONSUMING state,
   * thus effective in resetting segments or consumers in error states.
   */
  @POST
  @Path("segments/{tableNameWithType}/{segmentName}/reset")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.RESET_SEGMENT)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(
      value = "Resets a segment by first disabling it, waiting for external view to stabilize, and finally enabling "
          + "it again", notes = "Resets a segment by disabling and then enabling it")
  public SuccessResponse resetSegment(
      @ApiParam(value = "Name of the table with type", required = true) @PathParam("tableNameWithType")
          String tableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "Name of the target instance to reset") @QueryParam("targetInstance") @Nullable
          String targetInstance) {
    segmentName = URIUtils.decode(segmentName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    try {
      Preconditions.checkState(tableType != null, "Must provide table name with type: %s", tableNameWithType);
      _pinotHelixResourceManager.resetSegment(tableNameWithType, segmentName, targetInstance);
      return new SuccessResponse(
          String.format("Successfully reset segment: %s of table: %s", segmentName, tableNameWithType));
    } catch (IllegalStateException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to reset segments in table: %s. %s", tableNameWithType, e.getMessage()),
          Status.NOT_FOUND);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to reset segment: %s of table: %s. %s", segmentName, tableNameWithType, e.getMessage()),
          Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Resets all segments or segments with Error state of the given table
   * This API will take segments to OFFLINE state, wait for External View to stabilize, and then back to
   * ONLINE/CONSUMING state,
   * thus effective in resetting segments or consumers in error states.
   */
  @POST
  @Path("segments/{tableNameWithType}/reset")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.RESET_SEGMENT)
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(
      value = "Resets all segments (when errorSegmentsOnly = false) or segments with Error state (when "
          + "errorSegmentsOnly = true) of the table, by first disabling them, waiting for external view to stabilize,"
          + " and finally enabling them", notes = "Resets segments by disabling and then enabling them")
  public SuccessResponse resetSegments(
      @ApiParam(value = "Name of the table with type", required = true) @PathParam("tableNameWithType")
          String tableNameWithType,
      @ApiParam(value = "Name of the target instance to reset") @QueryParam("targetInstance") @Nullable
          String targetInstance,
      @ApiParam(value = "Whether to reset only segments with error state") @QueryParam("errorSegmentsOnly")
      @DefaultValue("false") boolean errorSegmentsOnly) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    try {
      Preconditions.checkState(tableType != null, "Must provide table name with type: %s", tableNameWithType);
      _pinotHelixResourceManager.resetSegments(tableNameWithType, targetInstance, errorSegmentsOnly);
      return new SuccessResponse(String.format("Successfully reset segments of table: %s", tableNameWithType));
    } catch (IllegalStateException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to reset segments in table: %s. %s", tableNameWithType, e.getMessage()),
          Status.NOT_FOUND);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to reset segments in table: %s. %s", tableNameWithType, e.getMessage()),
          Status.INTERNAL_SERVER_ERROR);
    }
  }

  @Deprecated
  @POST
  @Path("tables/{tableName}/segments/{segmentName}/reload")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.RELOAD_SEGMENT)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)",
      notes = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)")
  public SuccessResponse reloadSegmentDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    segmentName = URIUtils.decode(segmentName);
    List<String> tableNamesWithType = ResourceUtils
        .getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, Constants.validateTableType(tableTypeStr),
            LOGGER);
    int numMessagesSent = 0;
    for (String tableNameWithType : tableNamesWithType) {
      numMessagesSent += _pinotHelixResourceManager.reloadSegment(tableNameWithType, segmentName, false).getLeft();
    }
    return new SuccessResponse("Sent " + numMessagesSent + " reload messages");
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/{segmentName}/reload")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.RELOAD_SEGMENT)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)",
      notes = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)")
  public SuccessResponse reloadSegmentDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    return reloadSegmentDeprecated1(tableName, segmentName, tableTypeStr);
  }

  @GET
  @Path("segments/segmentReloadStatus/{jobId}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_SEGMENT_RELOAD_STATUS)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get status for a submitted reload operation",
      notes = "Get status for a submitted reload operation")
  public ServerReloadControllerJobStatusResponse getReloadJobStatus(
      @ApiParam(value = "Reload job id", required = true) @PathParam("jobId") String reloadJobId)
      throws Exception {
    Map<String, String> controllerJobZKMetadata = _pinotHelixResourceManager.
            getControllerJobZKMetadata(reloadJobId, ControllerJobType.RELOAD_SEGMENT);
    if (controllerJobZKMetadata == null) {
      throw new ControllerApplicationException(LOGGER, "Failed to find controller job id: " + reloadJobId,
          Status.NOT_FOUND);
    }

    String tableNameWithType =
        controllerJobZKMetadata.get(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE);
    Map<String, List<String>> serverToSegments;

    String singleSegmentName =
        controllerJobZKMetadata.get(CommonConstants.ControllerJob.SEGMENT_RELOAD_JOB_SEGMENT_NAME);
    if (singleSegmentName != null) {
      // No need to query servers where this segment is not supposed to be hosted
      serverToSegments = new HashMap<>();
      List<String> segmentList = Arrays.asList(singleSegmentName);
      _pinotHelixResourceManager.getServers(tableNameWithType, singleSegmentName).forEach(server -> {
        serverToSegments.put(server, segmentList);
      });
    } else {
      serverToSegments = _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType);
    }

    BiMap<String, String> serverEndPoints =
        _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, serverEndPoints);

    List<String> serverUrls = new ArrayList<>();
    BiMap<String, String> endpointsToServers = serverEndPoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String reloadTaskStatusEndpoint =
          endpoint + "/controllerJob/reloadStatus/" + tableNameWithType + "?reloadJobTimestamp="
              + controllerJobZKMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS);
      if (singleSegmentName != null) {
        reloadTaskStatusEndpoint = reloadTaskStatusEndpoint + "&segmentName=" + singleSegmentName;
      }
      serverUrls.add(reloadTaskStatusEndpoint);
    }

    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, null, true, 10000);

    ServerReloadControllerJobStatusResponse serverReloadControllerJobStatusResponse =
        new ServerReloadControllerJobStatusResponse();
    serverReloadControllerJobStatusResponse.setSuccessCount(0);

    int totalSegments = 0;
    for (Map.Entry<String, List<String>> entry: serverToSegments.entrySet()) {
      totalSegments += entry.getValue().size();
    }
    serverReloadControllerJobStatusResponse.setTotalSegmentCount(totalSegments);
    serverReloadControllerJobStatusResponse.setTotalServersQueried(serverUrls.size());
    serverReloadControllerJobStatusResponse.setTotalServerCallsFailed(serviceResponse._failedResponseCount);

    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      String responseString = streamResponse.getValue();
      try {
        ServerReloadControllerJobStatusResponse response =
            JsonUtils.stringToObject(responseString, ServerReloadControllerJobStatusResponse.class);
        serverReloadControllerJobStatusResponse.setSuccessCount(
            serverReloadControllerJobStatusResponse.getSuccessCount() + response.getSuccessCount());
      } catch (Exception e) {
        serverReloadControllerJobStatusResponse.setTotalServerCallsFailed(
            serverReloadControllerJobStatusResponse.getTotalServerCallsFailed() + 1
        );
      }
    }

    // Add ZK fields
    serverReloadControllerJobStatusResponse.setMetadata(controllerJobZKMetadata);

    // Add derived fields
    long submissionTime =
        Long.parseLong(controllerJobZKMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS));
    double timeElapsedInMinutes = ((double) System.currentTimeMillis() - (double) submissionTime) / (1000.0 * 60.0);
    int remainingSegments = serverReloadControllerJobStatusResponse.getTotalSegmentCount()
        - serverReloadControllerJobStatusResponse.getSuccessCount();

    double estimatedRemainingTimeInMinutes = -1;
    if (serverReloadControllerJobStatusResponse.getSuccessCount() > 0) {
      estimatedRemainingTimeInMinutes =
          ((double) remainingSegments / (double) serverReloadControllerJobStatusResponse.getSuccessCount())
              * timeElapsedInMinutes;
    }

    serverReloadControllerJobStatusResponse.setTimeElapsedInMinutes(timeElapsedInMinutes);
    serverReloadControllerJobStatusResponse.setEstimatedTimeRemainingInMinutes(estimatedRemainingTimeInMinutes);

    return serverReloadControllerJobStatusResponse;
  }

  @POST
  @Path("segments/{tableName}/reload")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.RELOAD_SEGMENT)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments", notes = "Reload all segments")
  public SuccessResponse reloadAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Whether to force server to download segment") @QueryParam("forceDownload")
      @DefaultValue("false") boolean forceDownload)
      throws JsonProcessingException {
    TableType tableTypeFromTableName = TableNameBuilder.getTableTypeFromTableName(tableName);
    TableType tableTypeFromRequest = Constants.validateTableType(tableTypeStr);
    // When rawTableName is provided but w/o table type, Pinot tries to reload both OFFLINE
    // and REALTIME tables for the raw table. But forceDownload option only works with
    // OFFLINE table currently, so we limit the table type to OFFLINE to let Pinot continue
    // to reload w/o being accidentally aborted upon REALTIME table type.
    // TODO: support to force download immutable segments from RealTime table.
    if (forceDownload && (tableTypeFromTableName == null && tableTypeFromRequest == null)) {
      tableTypeFromRequest = TableType.OFFLINE;
    }
    List<String> tableNamesWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableTypeFromRequest,
            LOGGER);
    Map<String, Map<String, String>> perTableMsgData = new LinkedHashMap<>();
    for (String tableNameWithType : tableNamesWithType) {
      Pair<Integer, String> msgInfo = _pinotHelixResourceManager.reloadAllSegments(tableNameWithType, forceDownload);
      Map<String, String> tableReloadMeta = new HashMap<>();
      tableReloadMeta.put("numMessagesSent", String.valueOf(msgInfo.getLeft()));
      tableReloadMeta.put("reloadJobId", msgInfo.getRight());
      perTableMsgData.put(tableNameWithType, tableReloadMeta);
      // Store in ZK
      try {
        if (_pinotHelixResourceManager.addNewReloadAllSegmentsJob(tableNameWithType, msgInfo.getRight(),
            msgInfo.getLeft())) {
          tableReloadMeta.put("reloadJobMetaZKStorageStatus", "SUCCESS");
        } else {
          tableReloadMeta.put("reloadJobMetaZKStorageStatus", "FAILED");
          LOGGER.error("Failed to add reload all segments job meta into zookeeper for table: {}", tableNameWithType);
        }
      } catch (Exception e) {
        tableReloadMeta.put("reloadJobMetaZKStorageStatus", "FAILED");
        LOGGER.error("Failed to add reload all segments job meta into zookeeper for table: {}", tableNameWithType, e);
      }
    }
    return new SuccessResponse("Segment reload details: " + JsonUtils.objectToString(perTableMsgData));
  }

  @Deprecated
  @POST
  @Path("tables/{tableName}/segments/reload")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.RELOAD_SEGMENT)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)",
      notes = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)")
  public SuccessResponse reloadAllSegmentsDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    List<String> tableNamesWithType = ResourceUtils
        .getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, Constants.validateTableType(tableTypeStr),
            LOGGER);
    int numMessagesSent = 0;
    for (String tableNameWithType : tableNamesWithType) {
      numMessagesSent += _pinotHelixResourceManager.reloadAllSegments(tableNameWithType, false).getLeft();
    }
    return new SuccessResponse("Sent " + numMessagesSent + " reload messages");
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/reload")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.RELOAD_SEGMENT)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)",
      notes = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)")
  public SuccessResponse reloadAllSegmentsDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    return reloadAllSegmentsDeprecated1(tableName, tableTypeStr);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/{segmentName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_SEGMENT)
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete a segment", notes = "Delete a segment")
  public SuccessResponse deleteSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "Retention period for the table segments (e.g. 12h, 3d); If not set, the retention period "
          + "will default to the first config that's not null: the table config, then to cluster setting, then '7d'. "
          + "Using 0d or -1d will instantly delete segments without retention")
      @QueryParam("retention") String retentionPeriod) {
    segmentName = URIUtils.decode(segmentName);
    String tableNameWithType = getExistingTable(tableName, segmentName);
    deleteSegmentsInternal(tableNameWithType, Collections.singletonList(segmentName), retentionPeriod);
    return new SuccessResponse("Segment deleted");
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_SEGMENT)
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete all segments", notes = "Delete all segments")
  public SuccessResponse deleteAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Retention period for the table segments (e.g. 12h, 3d); If not set, the retention period "
          + "will default to the first config that's not null: the table config, then to cluster setting, then '7d'. "
          + "Using 0d or -1d will instantly delete segments without retention")
      @QueryParam("retention") String retentionPeriod) {
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type must not be null", Status.BAD_REQUEST);
    }
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    deleteSegmentsInternal(tableNameWithType,
        _pinotHelixResourceManager.getSegmentsFromPropertyStore(tableNameWithType), retentionPeriod);
    return new SuccessResponse("All segments of table " + tableNameWithType + " deleted");
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/delete")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_SEGMENT)
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete the segments in the JSON array payload",
      notes = "Delete the segments in the JSON array payload")
  public SuccessResponse deleteSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Retention period for the table segments (e.g. 12h, 3d); If not set, the retention period "
          + "will default to the first config that's not null: the table config, then to cluster setting, then '7d'. "
          + "Using 0d or -1d will instantly delete segments without retention")
      @QueryParam("retention") String retentionPeriod, List<String> segments) {
    int numSegments = segments.size();
    if (numSegments == 0) {
      throw new ControllerApplicationException(LOGGER, "Segments must be provided", Status.BAD_REQUEST);
    }
    String tableNameWithType = getExistingTable(tableName, segments.get(0));
    deleteSegmentsInternal(tableNameWithType, segments, retentionPeriod);
    if (numSegments <= 5) {
      return new SuccessResponse("Deleted segments: " + segments + " from table: " + tableNameWithType);
    } else {
      return new SuccessResponse("Deleted " + numSegments + " segments from table: " + tableNameWithType);
    }
  }

  @DELETE
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/choose")
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete selected segments. An optional 'excludeReplacedSegments' parameter is used to get the"
      + " list of segments which has not yet been replaced (determined by segment lineage entries) and can be queried"
      + " from the table. The value is false by default.",
      // TODO: more and more filters can be added later on, like excludeErrorSegments, excludeConsumingSegments, etc.
      notes = "List all segments")
  public SuccessResponse deleteSegmentsWithTimeWindow(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Whether to ignore replaced segments for deletion, which have been replaced"
          + " specified in the segment lineage entries and cannot be queried from the table, false by default")
      @QueryParam("excludeReplacedSegments") @DefaultValue("false") boolean excludeReplacedSegments,
      @ApiParam(value = "Start timestamp (inclusive) in milliseconds", required = true) @QueryParam("startTimestamp")
          String startTimestampStr,
      @ApiParam(value = "End timestamp (exclusive) in milliseconds", required = true) @QueryParam("endTimestamp")
          String endTimestampStr,
      @ApiParam(value = "Whether to ignore segments that are partially overlapping with the [start, end)"
          + "for deletion, true by default")
      @QueryParam("excludeOverlapping") @DefaultValue("true") boolean excludeOverlapping,
      @ApiParam(value = "Retention period for the table segments (e.g. 12h, 3d); If not set, the retention period "
          + "will default to the first config that's not null: the table config, then to cluster setting, then '7d'. "
          + "Using 0d or -1d will instantly delete segments without retention")
      @QueryParam("retention") String retentionPeriod) {
    if (Strings.isNullOrEmpty(startTimestampStr) || Strings.isNullOrEmpty(endTimestampStr)) {
      throw new ControllerApplicationException(LOGGER, "start and end timestamp must by non empty", Status.BAD_REQUEST);
    }

    int numSegments = 0;
    for (Pair<TableType, List<String>> tableTypeSegments : selectSegments(
        tableName, tableTypeStr, excludeReplacedSegments, startTimestampStr, endTimestampStr, excludeOverlapping)) {
      TableType tableType = tableTypeSegments.getLeft();
      List<String> segments = tableTypeSegments.getRight();
      numSegments += segments.size();
      if (segments.isEmpty()) {
        continue;
      }
      String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
      deleteSegmentsInternal(tableNameWithType, segments, retentionPeriod);
    }
    return new SuccessResponse("Deleted " + numSegments + " segments from table: " + tableName);
  }

  private void deleteSegmentsInternal(String tableNameWithType, List<String> segments, String retentionPeriod) {
    PinotResourceManagerResponse response = _pinotHelixResourceManager.deleteSegments(tableNameWithType, segments,
        retentionPeriod);
    if (!response.isSuccessful()) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to delete segments from table: " + tableNameWithType + ", error message: " + response.getMessage(),
          Status.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("segments/{tableName}/metadata")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the server metadata for all table segments",
      notes = "Get the server metadata for all table segments")
  public String getServerMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Columns name", allowMultiple = true) @QueryParam("columns") @DefaultValue("")
          List<String> columns) {
    LOGGER.info("Received a request to fetch metadata for all segments for table {}", tableName);
    TableType tableType = Constants.validateTableType(tableTypeStr);

    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    String segmentsMetadata;
    try {
      JsonNode segmentsMetadataJson = getSegmentsMetadataFromServer(tableNameWithType, columns);
      segmentsMetadata = JsonUtils.objectToPrettyString(segmentsMetadataJson);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Status.BAD_REQUEST);
    } catch (IOException ioe) {
      throw new ControllerApplicationException(LOGGER, "Error parsing Pinot server response: " + ioe.getMessage(),
          Status.INTERNAL_SERVER_ERROR, ioe);
    }
    return segmentsMetadata;
  }

  @GET
  @Path("segments/{tableName}/zkmetadata")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the zookeeper metadata for all table segments", notes = "Get the zookeeper metadata for "
      + "all table segments")
  public Map<String, Map<String, String>> getZookeeperMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr)
      throws JsonProcessingException {
    LOGGER.info("Received a request to fetch zookeeper metadata for all segments for table {}", tableName);
    TableType tableType = Constants.validateTableType(tableTypeStr);

    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    Map<String, Map<String, String>> segmentToMetadataMap = new HashMap<>();
    List<SegmentZKMetadata> segmentZKMetadataList =
        _pinotHelixResourceManager.getSegmentsZKMetadata(tableNameWithType);

    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      segmentToMetadataMap.put(segmentZKMetadata.getSegmentName(), segmentZKMetadata.toMap());
    }
    return segmentToMetadataMap;
  }

  @GET
  @Path("segments/{tableName}/tiers")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_STORAGE_TIER)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get storage tier for all segments in the given table", notes = "Get storage tier for all "
      + "segments in the given table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Table not found")
  })
  public TableTierReader.TableTierDetails getTableTiers(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr) {
    LOGGER.info("Received a request to get storage tier for all segments for table {}", tableName);
    return getTableTierInternal(tableName, null, tableTypeStr);
  }

  @GET
  @Path("segments/{tableName}/{segmentName}/tiers")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_STORAGE_TIER)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get storage tiers for the given segment", notes = "Get storage tiers for the given segment")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Table or segment not found")
  })
  public TableTierReader.TableTierDetails getSegmentTiers(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr) {
    segmentName = URIUtils.decode(segmentName);
    LOGGER.info("Received a request to get storage tier for segment {} in table {}", segmentName, tableName);
    return getTableTierInternal(tableName, segmentName, tableTypeStr);
  }

  private TableTierReader.TableTierDetails getTableTierInternal(String tableName, @Nullable String segmentName,
      @Nullable String tableTypeStr) {
    TableType tableType = Constants.validateTableType(tableTypeStr);
    Preconditions.checkNotNull(tableType, "Table type is required to get table tiers");
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    TableTierReader tableTierReader = new TableTierReader(_executor, _connectionManager, _pinotHelixResourceManager);
    TableTierReader.TableTierDetails tableTierDetails;
    try {
      tableTierDetails = tableTierReader.getTableTierDetails(tableNameWithType, segmentName,
          _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
    } catch (Throwable t) {
      throw new ControllerApplicationException(LOGGER, String
          .format("Failed to get tier info for segment: %s in table: %s of type: %s", segmentName, tableName,
              tableTypeStr), Response.Status.INTERNAL_SERVER_ERROR, t);
    }
    if (segmentName != null && !tableTierDetails.getSegmentTiers().containsKey(segmentName)) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Segment: %s is not found in table: %s of type: %s", segmentName, tableName, tableTypeStr),
          Response.Status.NOT_FOUND);
    }
    return tableTierDetails;
  }

  @Deprecated
  @GET
  @Path("segments/{tableName}/select")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SEGMENT)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the selected segments given the (inclusive) start and (exclusive) end timestamps"
      + " in milliseconds. These timestamps will be compared against the minmax values of the time column in each"
      + " segment. If the table is a refresh use case, the value of start and end timestamp is voided,"
      + " since there is no time column for refresh use case; instead, the whole qualified segments will be returned."
      + " If no timestamps are provided, all the qualified segments will be returned."
      + " For the segments that partially belong to the time range, the boolean flag 'excludeOverlapping' is introduced"
      + " in order for user to determine whether to exclude this kind of segments in the response.",
      notes = "Get the selected segments given the start and end timestamps in milliseconds")
  public List<Map<TableType, List<String>>> getSelectedSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Start timestamp (inclusive) in milliseconds") @QueryParam("startTimestamp") @DefaultValue("")
          String startTimestampStr,
      @ApiParam(value = "End timestamp (exclusive) in milliseconds") @QueryParam("endTimestamp") @DefaultValue("")
          String endTimestampStr,
      @ApiParam(value = "Whether to exclude the segments overlapping with the timestamps, false by default")
      @QueryParam("excludeOverlapping") @DefaultValue("false") boolean excludeOverlapping) {
    long startTimestamp;
    long endTimestamp;
    try {
      startTimestamp = Strings.isNullOrEmpty(startTimestampStr) ? Long.MIN_VALUE : Long.parseLong(startTimestampStr);
      endTimestamp = Strings.isNullOrEmpty(endTimestampStr) ? Long.MAX_VALUE : Long.parseLong(endTimestampStr);
    } catch (NumberFormatException e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to parse the start/end timestamp. Please make sure they are in 'millisSinceEpoch' format.",
          Response.Status.BAD_REQUEST, e);
    }
    Preconditions.checkArgument(startTimestamp < endTimestamp,
        "The value of startTimestamp should be smaller than the one of endTimestamp. Start timestamp: %d. End "
            + "timestamp: %d",
        startTimestamp, endTimestamp);

    List<String> tableNamesWithType = ResourceUtils
        .getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, Constants.validateTableType(tableTypeStr),
            LOGGER);
    List<Map<TableType, List<String>>> resultList = new ArrayList<>(tableNamesWithType.size());
    for (String tableNameWithType : tableNamesWithType) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      List<String> segments =
          _pinotHelixResourceManager.getSegmentsFor(tableNameWithType, true, startTimestamp, endTimestamp,
              excludeOverlapping);
      resultList.add(Collections.singletonMap(tableType, segments));
    }
    return resultList;
  }

  /**
   * This is a helper method to get the metadata for all segments for a given table name.
   * @param tableNameWithType name of the table along with its type
   * @param columns name of the columns
   * @return Map<String, String>  metadata of the table segments -> map of segment name to its metadata
   */
  private JsonNode getSegmentsMetadataFromServer(String tableNameWithType, List<String> columns)
      throws InvalidConfigException, IOException {
    TableMetadataReader tableMetadataReader =
        new TableMetadataReader(_executor, _connectionManager, _pinotHelixResourceManager);
    return tableMetadataReader
        .getSegmentsMetadata(tableNameWithType, columns, _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
  }

  @POST
  @Path("/segments/{tableNameWithType}/updateZKTimeInterval")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_TIME_INTERVAL)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the start and end time of the segments based on latest schema",
      notes = "Update the start and end time of the segments based on latest schema")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public SuccessResponse updateTimeIntervalZK(
      @ApiParam(value = "Table name with type", required = true,
          example = "myTable_REALTIME") @PathParam("tableNameWithType") String tableNameWithType) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      if (tableType == null) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Table type not provided with table name %s", tableNameWithType),
            Status.BAD_REQUEST);
      }
      return updateZKTimeIntervalInternal(tableNameWithType);
  }

  /**
   * Internal method to update schema
   * @param tableNameWithType  name of the table
   * @return
   */
  private SuccessResponse updateZKTimeIntervalInternal(String tableNameWithType) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (tableConfig == null) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to find table config for table: " + tableNameWithType, Status.NOT_FOUND);
      }

      Schema tableSchema = _pinotHelixResourceManager.getTableSchema(tableNameWithType);
      if (tableSchema == null) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to find schema for table: " + tableNameWithType, Status.NOT_FOUND);
      }

      String timeColumn = tableConfig.getValidationConfig().getTimeColumnName();
      if (StringUtils.isEmpty(timeColumn)) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to find time column for table : " + tableNameWithType, Status.NOT_FOUND);
      }

      DateTimeFieldSpec timeColumnFieldSpec = tableSchema.getSpecForTimeColumn(timeColumn);
      if (timeColumnFieldSpec == null) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Failed to find field spec for column: %s and table: %s", timeColumn, tableNameWithType),
            Status.NOT_FOUND);
      }

      try {
        _pinotHelixResourceManager.updateSegmentsZKTimeInterval(tableNameWithType, timeColumnFieldSpec);
      } catch (Exception e) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Failed to update time interval zk metadata for table %s", tableNameWithType),
            Response.Status.INTERNAL_SERVER_ERROR, e);
      }
      return new SuccessResponse("Successfully updated time interval zk metadata for table: " + tableNameWithType);
  }

  private List<Pair<TableType, List<String>>> selectSegments(
      String tableName, String tableTypeStr, boolean excludeReplacedSegments, String startTimestampStr,
      String endTimestampStr, boolean excludeOverlapping) {
    long startTimestamp;
    long endTimestamp;
    try {
      startTimestamp = Strings.isNullOrEmpty(startTimestampStr) ? Long.MIN_VALUE : Long.parseLong(startTimestampStr);
      endTimestamp = Strings.isNullOrEmpty(endTimestampStr) ? Long.MAX_VALUE : Long.parseLong(endTimestampStr);
    } catch (NumberFormatException e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to parse the start/end timestamp. Please make sure they are in 'millisSinceEpoch' format.",
          Response.Status.BAD_REQUEST, e);
    }
    Preconditions.checkArgument(startTimestamp < endTimestamp,
        "The value of startTimestamp should be smaller than the one of endTimestamp. Start timestamp: %d. End "
            + "timestamp: %d", startTimestamp, endTimestamp);

    List<String> tableNamesWithType = ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName,
        Constants.validateTableType(tableTypeStr), LOGGER);
    List<Pair<TableType, List<String>>> resultList = new ArrayList<>(tableNamesWithType.size());
    for (String tableNameWithType : tableNamesWithType) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      List<String> segments =
          _pinotHelixResourceManager.getSegmentsFor(tableNameWithType, excludeReplacedSegments, startTimestamp,
              endTimestamp, excludeOverlapping);
      resultList.add(Pair.of(tableType, segments));
    }
    return resultList;
  }
}
