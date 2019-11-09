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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
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
 *       <li>"/segments/{tableName}/crc": get a map from segment to CRC of the segment</li>
 *       <li>"/segments/{tableName}/{segmentName}/metadata: get the metadata for a segment</li>
 *     </ul>
 *   </li>
 *   <li>
 *     POST requests:
 *     <ul>
 *       <li>"/segments/{tableName}/{segmentName}/reload": reload a segment</li>
 *       <li>"/segments/{tableName}/reload": reload all segments</li>
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
 *     All requests (except for crc fetch which only apply to OFFLINE table) take an optional query parameter "type"
 *     (OFFLINE or REALTIME) for table type. The request will be performed to tables that match the table name and type,
 *     e.g. "foobar_OFFLINE" matches ("foobar_OFFLINE", null), ("foobar_OFFLINE", OFFLINE), ("foobar", null),
 *     ("foobar", OFFLINE), but does not match ("foo", null), ("foobar_REALTIME", null), ("foobar_REALTIME", OFFLINE),
 *     ("foobar_OFFLINE", REALTIME).
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
  private static Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "List all segments", notes = "List all segments")
  public List<Map<TableType, List<String>>> getSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    List<String> tableNamesWithType =
        getExistingTableNamesWithType(tableName, Constants.validateTableType(tableTypeStr));
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
    List<String> tableNamesWithType =
        getExistingTableNamesWithType(tableName, Constants.validateTableType(tableTypeStr));
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

    List<String> tableNamesWithType =
        getExistingTableNamesWithType(tableName, Constants.validateTableType(tableTypeStr));
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
    String offlineTableName = getExistingTableNamesWithType(tableName, TableType.OFFLINE).get(0);
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
  public List<Map<String, Object>> getSegmentMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    segmentName = URIUtils.decode(segmentName);
    TableType tableType = Constants.validateTableType(tableTypeStr);
    List<String> tableNamesWithType = getExistingTableNamesWithType(tableName, tableType);
    List<Map<String, Object>> resultList = new ArrayList<>(tableNamesWithType.size());
    for (String tableNameWithType : tableNamesWithType) {
      Map<String, String> segmentMetadata = getSegmentMetadata(tableNameWithType, segmentName);
      if (segmentMetadata != null) {
        Map<String, Object> resultForTable = new LinkedHashMap<>();
        resultForTable.put("tableName", tableNameWithType);
        resultForTable.put("segmentMetadata", segmentMetadata);
        resultList.add(resultForTable);
      }
    }
    if (resultList.isEmpty()) {
      String errorMessage =
          "Failed to find segment: " + segmentName + " in table: " + tableName + (tableType != null ? (" of type: "
              + tableType) : "");
      throw new ControllerApplicationException(LOGGER, errorMessage, Status.NOT_FOUND);
    }
    return resultList;
  }

  @Nullable
  private Map<String, String> getSegmentMetadata(String tableNameWithType, String segmentName) {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    if (TableNameBuilder.OFFLINE.tableHasTypeSuffix(tableNameWithType)) {
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
    List<String> tableNamesWithType = getExistingTableNamesWithType(tableName, tableType);
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
      String errorMessage =
          "Failed to find segment: " + segmentName + " in table: " + tableName + (tableType != null ? (" of type: "
              + tableType) : "");
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
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a segment", notes = "Reload a segment")
  public SuccessResponse reloadSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    segmentName = URIUtils.decode(segmentName);
    List<String> tableNamesWithType =
        getExistingTableNamesWithType(tableName, Constants.validateTableType(tableTypeStr));
    int numReloadMessagesSent = 0;
    for (String tableNameWithType : tableNamesWithType) {
      numReloadMessagesSent += _pinotHelixResourceManager.reloadSegment(tableNameWithType, segmentName);
    }
    return new SuccessResponse("Sent " + numReloadMessagesSent + " reload messages");
  }

  @Deprecated
  @POST
  @Path("tables/{tableName}/segments/{segmentName}/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)", notes = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)")
  public SuccessResponse reloadSegmentDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    return reloadSegment(tableName, segmentName, tableTypeStr);
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/{segmentName}/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)", notes = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)")
  public SuccessResponse reloadSegmentDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    return reloadSegment(tableName, segmentName, tableTypeStr);
  }

  @POST
  @Path("segments/{tableName}/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments", notes = "Reload all segments")
  public SuccessResponse reloadAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    List<String> tableNamesWithType =
        getExistingTableNamesWithType(tableName, Constants.validateTableType(tableTypeStr));
    int numReloadMessagesSent = 0;
    for (String tableNameWithType : tableNamesWithType) {
      numReloadMessagesSent += _pinotHelixResourceManager.reloadAllSegments(tableNameWithType);
    }
    return new SuccessResponse("Sent " + numReloadMessagesSent + " reload messages");
  }

  @Deprecated
  @POST
  @Path("tables/{tableName}/segments/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)", notes = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)")
  public SuccessResponse reloadAllSegmentsDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    return reloadAllSegments(tableName, tableTypeStr);
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)", notes = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)")
  public SuccessResponse reloadAllSegmentsDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    return reloadAllSegments(tableName, tableTypeStr);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/{segmentName}")
  @ApiOperation(value = "Delete a segment", notes = "Delete a segment")
  public SuccessResponse deleteSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    segmentName = URIUtils.decode(segmentName);
    List<String> tableNamesWithType =
        getExistingTableNamesWithType(tableName, Constants.validateTableType(tableTypeStr));
    for (String tableNameWithType : tableNamesWithType) {
      deleteSegments(tableNameWithType, Collections.singletonList(segmentName));
    }
    return new SuccessResponse("Segment deleted");
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "Delete all segments", notes = "Delete all segments")
  public SuccessResponse deleteAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr) {
    List<String> tableNamesWithType =
        getExistingTableNamesWithType(tableName, Constants.validateTableType(tableTypeStr));
    for (String tableNameWithType : tableNamesWithType) {
      deleteSegments(tableNameWithType, _pinotHelixResourceManager.getSegmentsFor(tableNameWithType));
    }
    return new SuccessResponse("All segments for table: " + tableNamesWithType + " deleted");
  }

  private void deleteSegments(String tableNameWithType, List<String> segments) {
    PinotResourceManagerResponse response = _pinotHelixResourceManager.deleteSegments(tableNameWithType, segments);
    if (!response.isSuccessful()) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to delete segments from table: " + tableNameWithType + ", error message: " + response.getMessage(),
          Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Helper method to return a list of tables that exists and matches the given table name and type, or throws
   * {@link ControllerApplicationException} if no table found.
   * <p>When table type is <code>null</code>, try to match both OFFLINE and REALTIME table.
   *
   * @param tableName Table name with or without type suffix
   * @param tableType Table type
   * @return List of existing table names with type suffix
   */
  private List<String> getExistingTableNamesWithType(String tableName, @Nullable TableType tableType) {
    List<String> tableNamesWithType = new ArrayList<>(2);

    TableType tableTypeFromTableName = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableTypeFromTableName != null) {
      // Table name has type suffix

      if (tableType != null && tableType != tableTypeFromTableName) {
        throw new ControllerApplicationException(LOGGER,
            "Table name: " + tableName + " does not match table type: " + tableType, Status.FORBIDDEN);
      }

      if (_pinotHelixResourceManager.getTableConfig(tableName) != null) {
        tableNamesWithType.add(tableName);
      }
    } else {
      // Raw table name

      if (tableType == null || tableType == TableType.OFFLINE) {
        String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        if (_pinotHelixResourceManager.getTableConfig(offlineTableName) != null) {
          tableNamesWithType.add(offlineTableName);
        }
      }
      if (tableType == null || tableType == TableType.REALTIME) {
        String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
        if (_pinotHelixResourceManager.getTableConfig(realtimeTableName) != null) {
          tableNamesWithType.add(realtimeTableName);
        }
      }
    }

    if (tableNamesWithType.isEmpty()) {
      String errorMessage = "Failed to find table: " + tableName;
      if (tableType != null) {
        errorMessage += " of type: " + tableType;
      }
      throw new ControllerApplicationException(LOGGER, errorMessage, Status.NOT_FOUND);
    }

    return tableNamesWithType;
  }
}
