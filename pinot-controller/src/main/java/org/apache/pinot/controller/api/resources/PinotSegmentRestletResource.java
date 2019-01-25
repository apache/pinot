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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.controller.api.resources.Constants.TABLE_NAME;
import static org.apache.pinot.controller.api.resources.FileUploadPathProvider.STATE;


/**
 * Current URI Mappings:
 * <ul>
 *   <li>
 *     "/tables/{tableName}/segments/{segmentName}":
 *     "/tables/{tableName}/segments/{segmentName}/metadata":
 *     Get segment metadata for a given segment
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments":
 *     "/tables/{tableName}/segments/metadata":
 *     List segment metadata for a given table
 *   </li>
 *   <li>
 *      "/tables/{tableName}/segments/crc":
 *      Get crc information for a given table
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments/{segmentName}?state={state}":
 *     Change the state of the segment to specified {state} (enable|disable|drop)
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments?state={state}":
 *     Change the state of all segments of the table to specified {state} (enable|disable|drop)
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments/{segmentName}/reload":
 *     Reload the segment
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments/reload":
 *     Reload all segments of the table
 *   </li>
 * </ul>
 *
 * {@inheritDoc}
 */

@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotSegmentRestletResource {
  public static Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRestletResource.class);
  public static final Response.Status BAD_REQUEST = Response.Status.BAD_REQUEST;
  public static final Response.Status INTERNAL_ERROR = Response.Status.INTERNAL_SERVER_ERROR;

  private static final long _offlineToOnlineTimeoutInseconds = 5L;

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Path("tables/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata or toggles segment states", notes = "Toggles segment states if 'state' is specified in query param, otherwise lists metadata")
  // TODO: when state is not specified, this method returns the map from server to segments instead of segment metadata
  public String toggleStateOrListMetadataForAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "enable|disable|drop", required = false) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    StateType state = Constants.validateState(stateStr);

    if (stateStr == null) {
      return getInstanceToSegmentsMap(tableName, tableType);
    }
    return toggleStateInternal(tableName, state, tableType, null, _pinotHelixResourceManager);
  }

  @GET
  @Path("tables/{tableName}/segments/{segmentName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata or toggles state of a segment", notes = "Toggles segment state if 'state' is specified in query param, otherwise lists segment metadata")
  public String toggleStateOrListMetadataForOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "enable|disable|drop", required = false) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr) {
    segmentName = checkGetEncodedParam(segmentName);
    // segmentName will never be null,otherwise we would reach the method toggleStateOrListMetadataForAllSegments()
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    StateType stateType = Constants.validateState(stateStr);
    if (stateStr == null) {
      // This is a list metadata operation
      return listSegmentMetadataInternal(tableName, segmentName, tableType);
    } else {
      // We need to toggle state
      if (tableType == null) {
        List<String> realtimeSegments =
            _pinotHelixResourceManager.getSegmentsFor(TableNameBuilder.REALTIME.tableNameWithType(tableName));
        List<String> offlineSegments =
            _pinotHelixResourceManager.getSegmentsFor(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
        if (realtimeSegments.contains(segmentName)) {
          tableType = CommonConstants.Helix.TableType.REALTIME;
        } else if (offlineSegments.contains(segmentName)) {
          tableType = CommonConstants.Helix.TableType.OFFLINE;
        }
      }
      return toggleStateInternal(tableName, stateType, tableType, segmentName, _pinotHelixResourceManager);
    }
  }

  private String checkGetEncodedParam(String encoded) {
    try {
      return URLDecoder.decode(encoded, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      String errStr = "Could not decode parameter '" + encoded + "'";
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST);
    }
  }

  @GET
  @Path("tables/{tableName}/segments/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata all segments of table", notes = "Lists segment metadata")
  // TODO: 1. stateStr is not used
  // TODO: 2. this method returns the map from server to segments instead of segment metadata
  public String listMetadataForAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "enable|disable|drop", required = false) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    return getInstanceToSegmentsMap(tableName, tableType);
  }

  @GET
  @Path("tables/{tableName}/segments/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists one segment's metadata", notes = "Lists one segment's metadata")
  public String listMetadataForOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr) {
    segmentName = checkGetEncodedParam(segmentName);
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    return listSegmentMetadataInternal(tableName, segmentName, tableType);
  }

  private String listSegmentMetadataInternal(@Nonnull String tableName, @Nonnull String segmentName,
      @Nullable CommonConstants.Helix.TableType tableType) {
    List<String> tableNamesWithType = getExistingTableNamesWithType(tableName, tableType);

    ArrayNode result = JsonUtils.newArrayNode();
    for (String tableNameWithType : tableNamesWithType) {
      ArrayNode segmentMetaData = getSegmentMetaData(tableNameWithType, segmentName);
      if (segmentMetaData != null) {
        result.add(segmentMetaData);
      }
    }

    if (result.size() == 0) {
      String errorMessage = "Failed to find segment: " + segmentName + " in table: " + tableName;
      if (tableType != null) {
        errorMessage += " of type: " + tableType;
      }
      throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.NOT_FOUND);
    }

    return result.toString();
  }

  @POST
  @Path("tables/{tableName}/segments/{segmentName}/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reloads one segment", notes = "Reloads one segment")
  public SuccessResponse reloadOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    segmentName = checkGetEncodedParam(segmentName);
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    return reloadSegmentForTable(tableName, segmentName, tableType);
  }

  @POST
  @Path("tables/{tableName}/segments/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reloads all segments of a table", notes = "Reloads all segments")
  public SuccessResponse reloadAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    return reloadAllSegmentsForTable(tableName, tableType);
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/{segmentName}/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reloads one segment", notes = "Reloads one segment")
  public SuccessResponse reloadOneSegmentDeprecated(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    segmentName = checkGetEncodedParam(segmentName);
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    return reloadSegmentForTable(tableName, segmentName, tableType);
  }

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reloads all segments of a table", notes = "Reloads all segments")
  public SuccessResponse reloadAllSegmentsDeprecated(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    return reloadAllSegmentsForTable(tableName, tableType);
  }

  @GET
  @Path("tables/{tableName}/segments/crc")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Gets crc of all segments of a table", notes = "Gets crc of all segments")
  public String getCrcForAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName) {
    return getAllCrcMetadataForTable(tableName);
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
  private List<String> getExistingTableNamesWithType(@Nonnull String tableName,
      @Nullable CommonConstants.Helix.TableType tableType) {
    // TODO: check table config instead of fetching all resources
    Set<String> allTables = new HashSet<>(_pinotHelixResourceManager.getAllTables());
    List<String> tableNamesWithType = new ArrayList<>(2);

    CommonConstants.Helix.TableType tableTypeFromTableName = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableTypeFromTableName != null) {
      // Table name has type suffix

      if (tableType != null && tableType != tableTypeFromTableName) {
        throw new ControllerApplicationException(LOGGER,
            "Table name: " + tableName + " does not match table type: " + tableType, Response.Status.BAD_REQUEST);
      }
      if (allTables.contains(tableName)) {
        tableNamesWithType.add(tableName);
      }
    } else {
      // Raw table name

      if (tableType == null || tableType == CommonConstants.Helix.TableType.OFFLINE) {
        String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        if (allTables.contains(offlineTableName)) {
          tableNamesWithType.add(offlineTableName);
        }
      }
      if (tableType == null || tableType == CommonConstants.Helix.TableType.REALTIME) {
        String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
        if (allTables.contains(realtimeTableName)) {
          tableNamesWithType.add(realtimeTableName);
        }
      }
    }

    if (tableNamesWithType.isEmpty()) {
      String errorMessage = "Failed to find table: " + tableName;
      if (tableType != null) {
        errorMessage += " of type: " + tableType;
      }
      throw new ControllerApplicationException(LOGGER, errorMessage, Response.Status.NOT_FOUND);
    }

    return tableNamesWithType;
  }

  private SuccessResponse reloadSegmentForTable(@Nonnull String tableName, @Nonnull String segmentName,
      @Nullable CommonConstants.Helix.TableType tableType) {
    List<String> tableNamesWithType = getExistingTableNamesWithType(tableName, tableType);
    int numReloadMessagesSent = 0;
    for (String tableNameWithType : tableNamesWithType) {
      numReloadMessagesSent += _pinotHelixResourceManager.reloadSegment(tableNameWithType, segmentName);
    }
    return new SuccessResponse("Sent " + numReloadMessagesSent + " reload messages");
  }

  private SuccessResponse reloadAllSegmentsForTable(@Nonnull String tableName,
      @Nullable CommonConstants.Helix.TableType tableType) {
    List<String> tableNamesWithType = getExistingTableNamesWithType(tableName, tableType);
    int numReloadMessagesSent = 0;
    for (String tableNameWithType : tableNamesWithType) {
      numReloadMessagesSent += _pinotHelixResourceManager.reloadAllSegments(tableNameWithType);
    }
    return new SuccessResponse("Sent " + numReloadMessagesSent + " reload messages");
  }

  // TODO: refactor this method using getExistingTableNamesWithType()
  public static String toggleStateInternal(@Nonnull String tableName, StateType state,
      CommonConstants.Helix.TableType tableType, String segmentName, PinotHelixResourceManager helixResourceManager) {
    ArrayNode ret = JsonUtils.newArrayNode();
    List<String> segmentsToToggle = new ArrayList<>();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    String tableNameWithType = "";
    List<String> realtimeSegments = helixResourceManager.getSegmentsFor(realtimeTableName);
    List<String> offlineSegments = helixResourceManager.getSegmentsFor(offlineTableName);
    if (tableType == null) {
      PinotResourceManagerResponse responseRealtime =
          toggleSegmentsForTable(realtimeSegments, realtimeTableName, segmentName, state, helixResourceManager);
      PinotResourceManagerResponse responseOffline =
          toggleSegmentsForTable(offlineSegments, offlineTableName, segmentName, state, helixResourceManager);
      if (!responseOffline.isSuccessful() || !responseRealtime.isSuccessful()) {
        throw new ControllerApplicationException(LOGGER,
            "OFFLINE response : " + responseOffline.getMessage() + ", REALTIME response"
                + responseRealtime.getMessage(), INTERNAL_ERROR);
      }
      List<PinotResourceManagerResponse> responses = new ArrayList<>();
      responses.add(responseRealtime);
      responses.add(responseOffline);
      ret.add(JsonUtils.objectToJsonNode(responses));
      return ret.toString();
    } else if (CommonConstants.Helix.TableType.REALTIME == tableType) {
      if (helixResourceManager.hasRealtimeTable(tableName)) {
        tableNameWithType = realtimeTableName;
        if (segmentName != null) {
          segmentsToToggle = Collections.singletonList(segmentName);
        } else {
          segmentsToToggle.addAll(realtimeSegments);
        }
      } else {
        throw new ControllerApplicationException(LOGGER, "There is no realtime table for " + tableName, BAD_REQUEST);
      }
    } else {
      if (helixResourceManager.hasOfflineTable(tableName)) {
        tableNameWithType = offlineTableName;
        if (segmentName != null) {
          segmentsToToggle = Collections.singletonList(segmentName);
        } else {
          segmentsToToggle.addAll(offlineSegments);
        }
      } else {
        throw new ControllerApplicationException(LOGGER, "There is no offline table for: " + tableName, BAD_REQUEST);
      }
    }
    PinotResourceManagerResponse resourceManagerResponse =
        toggleSegmentsForTable(segmentsToToggle, tableNameWithType, segmentName, state, helixResourceManager);
    ret.add(JsonUtils.objectToJsonNode(resourceManagerResponse));
    return ret.toString();
  }

  /**
   * Helper method to toggle state of segment for a given table. The tableName expected is the internally
   * stored name (with offline/realtime annotation).
   *
   * @param segmentsToToggle: segments that we want to perform operations on
   * @param tableName: Internal name (created by TableNameBuilder) for the table
   * @param segmentName: Segment to set the state for.
   * @param state: Value of state to set.
   */
  // TODO: move this method into PinotHelixResourceManager
  private static PinotResourceManagerResponse toggleSegmentsForTable(@Nonnull List<String> segmentsToToggle,
      @Nonnull String tableName, String segmentName, @Nonnull StateType state,
      PinotHelixResourceManager helixResourceManager) {
    long timeOutInSeconds = 10L;
    if (segmentName == null) {
      // For enable, allow 5 seconds per segment for an instance as timeout.
      if (state == StateType.ENABLE) {
        int instanceCount = helixResourceManager.getAllInstances().size();
        if (instanceCount != 0) {
          timeOutInSeconds = (_offlineToOnlineTimeoutInseconds * segmentsToToggle.size()) / instanceCount;
        } else {
          return PinotResourceManagerResponse.failure("No instance found for table " + tableName);
        }
      }
    }
    if (segmentsToToggle == null || segmentsToToggle.isEmpty()) {
      // Nothing to do. Not sure if 404 is a response here, but since they are asking us to toggle states of all
      // segments, and there are none to do, 200 is an equally valid response, in that we succeeded in changing the
      // state of 0 segments.
      //
      // If a table has only one type of segment (e.g. only realtime), the restlet API returned a 500, since helixi
      // throws a NPE. It tried the realtime segments first, and if there were none, then the operation would fail.
      // If only realtime segments existed, then we would still return 500 even though the realtime segments were
      // changed state correctly.
      //
      // In jersey, API, if there are no segments in realtime (or in offline), we succeed the operation AND return 200
      // if we succeeded.
      return PinotResourceManagerResponse.success("No segment to toggle");
    }

    switch (state) {
      case ENABLE:
        return helixResourceManager.toggleSegmentState(tableName, segmentsToToggle, true, timeOutInSeconds);
      case DISABLE:
        return helixResourceManager.toggleSegmentState(tableName, segmentsToToggle, false, timeOutInSeconds);
      case DROP:
        return helixResourceManager.deleteSegments(tableName, segmentsToToggle);
      default:
        throw new ControllerApplicationException(LOGGER, "Invalid state", BAD_REQUEST);
    }
  }

  private String getInstanceToSegmentsMap(@Nonnull String tableName,
      @Nullable CommonConstants.Helix.TableType tableType) {
    List<String> tableNamesWithType = getExistingTableNamesWithType(tableName, tableType);

    ArrayNode result = JsonUtils.newArrayNode();
    for (String tableNameWithType : tableNamesWithType) {
      ObjectNode resultForTable = JsonUtils.newObjectNode();
      resultForTable.put(FileUploadPathProvider.TABLE_NAME, tableNameWithType);
      try {
        // NOTE: for backward-compatibility, we put serialized map string as the value
        resultForTable.put("segments",
            JsonUtils.objectToString(_pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType)));
      } catch (JsonProcessingException e) {
        throw new ControllerApplicationException(LOGGER,
            "Caught JSON exception while getting instance to segments map for table: " + tableNameWithType,
            Response.Status.INTERNAL_SERVER_ERROR, e);
      }
      result.add(resultForTable);
    }

    return result.toString();
  }

  /**
   * Returns the segment metadata.
   *
   * @param tableNameWithType Table name with type suffix
   * @param segmentName Segment name
   * @return Singleton JSON array of the segment metadata
   */
  private ArrayNode getSegmentMetaData(String tableNameWithType, String segmentName) {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    if (!ZKMetadataProvider.isSegmentExisted(propertyStore, tableNameWithType, segmentName)) {
      return null;
    }

    ArrayNode ret = JsonUtils.newArrayNode();
    ObjectNode jsonObj = JsonUtils.newObjectNode();
    jsonObj.put(TABLE_NAME, tableNameWithType);

    if (TableNameBuilder.OFFLINE.tableHasTypeSuffix(tableNameWithType)) {
      // OFFLINE table
      OfflineSegmentZKMetadata offlineSegmentZKMetadata =
          ZKMetadataProvider.getOfflineSegmentZKMetadata(propertyStore, tableNameWithType, segmentName);
      jsonObj.set(STATE, JsonUtils.objectToJsonNode(offlineSegmentZKMetadata.toMap()));
    } else {
      // REALTIME table
      RealtimeSegmentZKMetadata realtimeSegmentZKMetadata =
          ZKMetadataProvider.getRealtimeSegmentZKMetadata(propertyStore, tableNameWithType, segmentName);
      jsonObj.set(STATE, JsonUtils.objectToJsonNode(realtimeSegmentZKMetadata.toMap()));
    }

    ret.add(jsonObj);
    return ret;
  }

  private String getAllCrcMetadataForTable(String tableName) {
    // TODO
    // In the restlet.resource version, we see this code block below
    // seems to be wrong comparing the table name to have the table type, but we copy it here anyway.
    // Realtime table is not supported.
    if (TableNameBuilder.getTableTypeFromTableName(tableName) == CommonConstants.Helix.TableType.REALTIME) {
      throw new ControllerApplicationException(LOGGER, "Realtime table is not supported", Response.Status.FORBIDDEN);
    }

    // Check that the offline table exists.
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (!_pinotHelixResourceManager.getAllTables().contains(offlineTableName)) {
      throw new ControllerApplicationException(LOGGER, "Offline table " + tableName + " does not exist.", BAD_REQUEST);
    }

    Map<String, String> segmentCrcForTable = _pinotHelixResourceManager.getSegmentsCrcForTable(offlineTableName);
    String result;
    try {
      result = JsonUtils.objectToPrettyString(segmentCrcForTable);
    } catch (JsonProcessingException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to write segment crc values for table: %s", tableName),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return result;
  }
}
