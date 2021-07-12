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
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader;
import org.apache.pinot.controller.util.TableMetadataReader;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
@Api(tags = Constants.SEGMENT_TAG)
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
  HttpConnectionManager _connectionManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "List all segments", notes = "List all segments")
  public List<Map<TableType, List<String>>> getSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    List<String> tableNamesWithType = Utils
        .getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, Constants.validateTableType(tableTypeStr),
            LOGGER);
    List<Map<TableType, List<String>>> resultList = new ArrayList<>(tableNamesWithType.size());
    for (String tableNameWithType : tableNamesWithType) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      List<String> segments = _pinotHelixResourceManager.getSegmentsFor(tableNameWithType);
      resultList.add(Collections.singletonMap(tableType, segments));
    }
    return resultList;
  }

  @GET
  @Path("segments/{tableName}/servers")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from server to segments hosted by the server", notes = "Get a map from server to segments hosted by the server")
  public List<Map<String, Object>> getServerToSegmentsMap(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    List<String> tableNamesWithType = Utils
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

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from server to segments hosted by the server (deprecated, use 'GET /segments/{tableName}/servers' instead)", notes = "Get a map from server to segments hosted by the server (deprecated, use 'GET /segments/{tableName}/servers' instead)")
  public List<Map<String, String>> getServerToSegmentsMapDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "MUST be null") @QueryParam("state") String stateStr,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr)
      throws JsonProcessingException {
    if (stateStr != null) {
      throw new WebApplicationException("Cannot toggle segment state", Status.FORBIDDEN);
    }

    List<String> tableNamesWithType = Utils
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
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from server to segments hosted by the server (deprecated, use 'GET /segments/{tableName}/servers' instead)", notes = "Get a map from server to segments hosted by the server (deprecated, use 'GET /segments/{tableName}/servers' instead)")
  public List<Map<String, String>> getServerToSegmentsMapDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "MUST be null") @QueryParam("state") String stateStr,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr)
      throws JsonProcessingException {
    return getServerToSegmentsMapDeprecated1(tableName, stateStr, tableTypeStr);
  }

  @GET
  @Path("segments/{tableName}/crc")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from segment to CRC of the segment (only apply to OFFLINE table)", notes = "Get a map from segment to CRC of the segment (only apply to OFFLINE table)")
  public Map<String, String> getSegmentToCrcMap(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName) {
    String offlineTableName =
        Utils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, TableType.OFFLINE, LOGGER).get(0);
    return _pinotHelixResourceManager.getSegmentsCrcForTable(offlineTableName);
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/crc")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from segment to CRC of the segment (deprecated, use 'GET /segments/{tableName}/crc' instead)", notes = "Get a map from segment to CRC of the segment (deprecated, use 'GET /segments/{tableName}/crc' instead)")
  public Map<String, String> getSegmentToCrcMapDeprecated(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName) {
    return getSegmentToCrcMap(tableName);
  }

  @GET
  @Path("segments/{tableName}/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the metadata for a segment", notes = "Get the metadata for a segment")
  public Map<String, String> getSegmentMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName) {
    segmentName = URIUtils.decode(segmentName);
    TableType tableType = SegmentName.isRealtimeSegmentName(segmentName) ? TableType.REALTIME : TableType.OFFLINE;
    String tableNameWithType =
        Utils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    Map<String, String> segmentMetadata = getSegmentMetadataInternal(tableNameWithType, segmentName);
    if (segmentMetadata != null) {
      return segmentMetadata;
    } else {
      throw new ControllerApplicationException(LOGGER,
          "Failed to find segment: " + segmentName + " in table: " + tableName, Status.NOT_FOUND);
    }
  }

  @Nullable
  private Map<String, String> getSegmentMetadataInternal(String tableNameWithType, String segmentName) {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata =
          ZKMetadataProvider.getOfflineSegmentZKMetadata(propertyStore, tableNameWithType, segmentName);
      return offlineSegmentZKMetadata != null ? offlineSegmentZKMetadata.toMap() : null;
    } else {
      RealtimeSegmentZKMetadata realtimeSegmentZKMetadata =
          ZKMetadataProvider.getRealtimeSegmentZKMetadata(propertyStore, tableNameWithType, segmentName);
      return realtimeSegmentZKMetadata != null ? realtimeSegmentZKMetadata.toMap() : null;
    }
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' instead)", notes = "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' instead)")
  public List<List<Map<String, Object>>> getSegmentMetadataDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    segmentName = URIUtils.decode(segmentName);
    TableType tableType = Constants.validateTableType(tableTypeStr);
    List<String> tableNamesWithType =
        Utils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER);
    List<List<Map<String, Object>>> resultList = new ArrayList<>(tableNamesWithType.size());
    for (String tableNameWithType : tableNamesWithType) {
      Map<String, String> segmentMetadata = getSegmentMetadata(tableNameWithType, segmentName);
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
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' instead)", notes = "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' instead)")
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
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a segment", notes = "Reload a segment")
  public SuccessResponse reloadSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName) {
    segmentName = URIUtils.decode(segmentName);
    TableType tableType = SegmentName.isRealtimeSegmentName(segmentName) ? TableType.REALTIME : TableType.OFFLINE;
    String tableNameWithType =
        Utils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    int numMessagesSent = _pinotHelixResourceManager.reloadSegment(tableNameWithType, segmentName);
    if (numMessagesSent > 0) {
      return new SuccessResponse("Sent " + numMessagesSent + " reload messages");
    } else {
      throw new ControllerApplicationException(LOGGER,
          "Failed to find segment: " + segmentName + " in table: " + tableName, Status.NOT_FOUND);
    }
  }

  /**
   * Resets the segment of the table, by disabling and then enabling it.
   * This API will take segments to OFFLINE state, wait for External View to stabilize, and then back to ONLINE/CONSUMING state,
   * thus effective in resetting segments or consumers in error states.
   */
  @POST
  @Path("segments/{tableNameWithType}/{segmentName}/reset")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Resets a segment by first disabling it, waiting for external view to stabilize, and finally enabling it again", notes = "Resets a segment by disabling and then enabling the segment")
  public SuccessResponse resetSegment(
      @ApiParam(value = "Name of the table with type", required = true) @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "Maximum time in milliseconds to wait for reset to be completed. By default, uses serverAdminRequestTimeout") @QueryParam("maxWaitTimeMs") long maxWaitTimeMs) {
    segmentName = URIUtils.decode(segmentName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    try {
      Preconditions.checkState(tableType != null, "Must provide table name with type: %s", tableNameWithType);
      _pinotHelixResourceManager.resetSegment(tableNameWithType, segmentName,
          maxWaitTimeMs > 0 ? maxWaitTimeMs : _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
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
   * Resets all segments of the given table
   * This API will take segments to OFFLINE state, wait for External View to stabilize, and then back to ONLINE/CONSUMING state,
   * thus effective in resetting segments or consumers in error states.
   */
  @POST
  @Path("segments/{tableNameWithType}/reset")
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Resets all segments of the table, by first disabling them, waiting for external view to stabilize, and finally enabling the segments", notes = "Resets a segment by disabling and then enabling a segment")
  public SuccessResponse resetAllSegments(
      @ApiParam(value = "Name of the table with type", required = true) @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Maximum time in milliseconds to wait for reset to be completed. By default, uses serverAdminRequestTimeout") @QueryParam("maxWaitTimeMs") long maxWaitTimeMs) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    try {
      Preconditions.checkState(tableType != null, "Must provide table name with type: %s", tableNameWithType);
      _pinotHelixResourceManager.resetAllSegments(tableNameWithType,
          maxWaitTimeMs > 0 ? maxWaitTimeMs : _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
      return new SuccessResponse(String.format("Successfully reset all segments of table: %s", tableNameWithType));
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
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)", notes = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)")
  public SuccessResponse reloadSegmentDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    segmentName = URIUtils.decode(segmentName);
    List<String> tableNamesWithType = Utils
        .getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, Constants.validateTableType(tableTypeStr),
            LOGGER);
    int numMessagesSent = 0;
    for (String tableNameWithType : tableNamesWithType) {
      numMessagesSent += _pinotHelixResourceManager.reloadSegment(tableNameWithType, segmentName);
    }
    return new SuccessResponse("Sent " + numMessagesSent + " reload messages");
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/{segmentName}/reload")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)", notes = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)")
  public SuccessResponse reloadSegmentDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    return reloadSegmentDeprecated1(tableName, segmentName, tableTypeStr);
  }

  @POST
  @Path("segments/{tableName}/reload")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments", notes = "Reload all segments")
  public SuccessResponse reloadAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    List<String> tableNamesWithType = Utils
        .getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, Constants.validateTableType(tableTypeStr),
            LOGGER);
    Map<String, Integer> numMessagesSentPerTable = new LinkedHashMap<>();
    for (String tableNameWithType : tableNamesWithType) {
      numMessagesSentPerTable.put(tableNameWithType, _pinotHelixResourceManager.reloadAllSegments(tableNameWithType));
    }
    return new SuccessResponse("Sent " + numMessagesSentPerTable + " reload messages");
  }

  @Deprecated
  @POST
  @Path("tables/{tableName}/segments/reload")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)", notes = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)")
  public SuccessResponse reloadAllSegmentsDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    List<String> tableNamesWithType = Utils
        .getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, Constants.validateTableType(tableTypeStr),
            LOGGER);
    int numMessagesSent = 0;
    for (String tableNameWithType : tableNamesWithType) {
      numMessagesSent += _pinotHelixResourceManager.reloadAllSegments(tableNameWithType);
    }
    return new SuccessResponse("Sent " + numMessagesSent + " reload messages");
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/reload")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)", notes = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)")
  public SuccessResponse reloadAllSegmentsDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    return reloadAllSegmentsDeprecated1(tableName, tableTypeStr);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/{segmentName}")
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete a segment", notes = "Delete a segment")
  public SuccessResponse deleteSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName) {
    segmentName = URIUtils.decode(segmentName);
    TableType tableType = SegmentName.isRealtimeSegmentName(segmentName) ? TableType.REALTIME : TableType.OFFLINE;
    String tableNameWithType =
        Utils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    deleteSegmentsInternal(tableNameWithType, Collections.singletonList(segmentName));
    return new SuccessResponse("Segment deleted");
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete all segments", notes = "Delete all segments")
  public SuccessResponse deleteAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr) {
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type must not be null", Status.BAD_REQUEST);
    }
    String tableNameWithType =
        Utils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    deleteSegmentsInternal(tableNameWithType, _pinotHelixResourceManager.getSegmentsFor(tableNameWithType));
    return new SuccessResponse("All segments of table " + tableNameWithType + " deleted");
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/delete")
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete the segments in the JSON array payload", notes = "Delete the segments in the JSON array payload")
  public SuccessResponse deleteSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      List<String> segments) {
    int numSegments = segments.size();
    if (numSegments == 0) {
      throw new ControllerApplicationException(LOGGER, "Segments must be provided", Status.BAD_REQUEST);
    }
    boolean isRealtimeSegment = SegmentName.isRealtimeSegmentName(segments.get(0));
    for (int i = 1; i < numSegments; i++) {
      if (SegmentName.isRealtimeSegmentName(segments.get(i)) != isRealtimeSegment) {
        throw new ControllerApplicationException(LOGGER, "All segments must be of the same type (OFFLINE|REALTIME)",
            Status.BAD_REQUEST);
      }
    }
    TableType tableType = isRealtimeSegment ? TableType.REALTIME : TableType.OFFLINE;
    String tableNameWithType =
        Utils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    deleteSegmentsInternal(tableNameWithType, segments);
    if (numSegments <= 5) {
      return new SuccessResponse("Deleted segments: " + segments + " from table: " + tableNameWithType);
    } else {
      return new SuccessResponse("Deleted " + numSegments + " segments from table: " + tableNameWithType);
    }
  }

  private void deleteSegmentsInternal(String tableNameWithType, List<String> segments) {
    PinotResourceManagerResponse response = _pinotHelixResourceManager.deleteSegments(tableNameWithType, segments);
    if (!response.isSuccessful()) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to delete segments from table: " + tableNameWithType + ", error message: " + response.getMessage(),
          Status.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("segments/{tableName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the server metadata for all table segments", notes = "Get the server metadata for all table segments")
  public String getServerMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Columns name", allowMultiple = true) @QueryParam("columns") @DefaultValue("") List<String> columns) {
    LOGGER.info("Received a request to fetch metadata for all segments for table {}", tableName);
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == TableType.REALTIME) {
      throw new ControllerApplicationException(LOGGER, "Table type : " + tableTypeStr + " not yet supported.",
          Status.NOT_IMPLEMENTED);
    }

    String tableNameWithType =
        Utils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
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

  // TODO: Move this API into PinotTableRestletResource
  @GET
  @Path("/tables/{realtimeTableName}/consumingSegmentsInfo")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Returns state of consuming segments", notes = "Gets the status of consumers from all servers")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 404, message = "Table not found"), @ApiResponse(code = 500, message = "Internal server error")})
  public ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap getConsumingSegmentsInfo(
      @ApiParam(value = "Realtime table name with or without type", required = true, example = "myTable | myTable_REALTIME") @PathParam("realtimeTableName") String realtimeTableName) {
    try {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(realtimeTableName);
      if (TableType.OFFLINE == tableType) {
        throw new IllegalStateException("Cannot get consuming segments info for OFFLINE table: " + realtimeTableName);
      }
      String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(realtimeTableName);
      ConsumingSegmentInfoReader consumingSegmentInfoReader =
          new ConsumingSegmentInfoReader(_executor, _connectionManager, _pinotHelixResourceManager);
      return consumingSegmentInfoReader
          .getConsumingSegmentsInfo(tableNameWithType, _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to get consuming segments info for table %s. %s", realtimeTableName, e.getMessage()),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
