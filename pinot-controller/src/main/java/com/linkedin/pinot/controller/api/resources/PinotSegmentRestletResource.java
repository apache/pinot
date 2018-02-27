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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.controller.api.resources.Constants.TABLE_NAME;
import static com.linkedin.pinot.controller.api.resources.FileUploadPathProvider.*;


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
  public String toggleStateOrListMetadataForAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "enable|disable|drop", required = false) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    StateType state = Constants.validateState(stateStr);

    if (stateStr == null) {
      JSONArray result = getAllSegmentsMetadataForTable(tableName, tableType);
      return result.toString();
    }
    return toggleStateInternal(tableName, state, tableType, null, _pinotHelixResourceManager).toString();
  }

  @GET
  @Path("tables/{tableName}/segments/{segmentName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata or toggles state of a segment", notes = "Toggles segment state if 'state' is specified in query param, otherwise lists segment metadata")
  public String toggleStateOrListMetadataForOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam (value = "Name of segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "online|offline|drop", required = false) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) throws JSONException {
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
        List<String> realtimeSegments = _pinotHelixResourceManager.getSegmentsFor(TableNameBuilder.REALTIME.tableNameWithType(tableName));
        List<String> offlineSegments = _pinotHelixResourceManager.getSegmentsFor(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
        if (realtimeSegments.contains(segmentName)) {
          tableType = CommonConstants.Helix.TableType.REALTIME;
        } else if (offlineSegments.contains(segmentName)) {
          tableType = CommonConstants.Helix.TableType.OFFLINE;
        }
      }
      return toggleStateInternal(tableName, stateType, tableType, segmentName, _pinotHelixResourceManager).toString();
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
  public String listMetadataForAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "enable|disable|drop", required = false) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    JSONArray result = getAllSegmentsMetadataForTable(tableName, tableType);
    return result.toString();
  }

  @GET
  @Path("tables/{tableName}/segments/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata one segments of table", notes = "Lists segment metadata")
  public String listMetadataForOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam (value = "Name of segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) throws JSONException {
    segmentName = checkGetEncodedParam(segmentName);
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    return listSegmentMetadataInternal(tableName, segmentName, tableType);
  }

  private String listSegmentMetadataInternal(@Nonnull  String tableName, @Nonnull String segmentName,
      @Nullable CommonConstants.Helix.TableType tableType) throws JSONException {
    // The code in restlet.resources seems to return an array of arrays, so we will do the same
    // to maintain backward compatibility
    JSONArray result = new JSONArray();
    if (tableType != null) {
      String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
      JSONArray metadata = getSegmentMetaData(tableNameWithType, segmentName, tableType);
      if (metadata == null) {
        throw new ControllerApplicationException(LOGGER, "Segment " + segmentName + " not found in table " +
            tableNameWithType, Response.Status.NOT_FOUND);
      }
      result.put(metadata);
      return result.toString();
    }
    // Again,keeping backward compatibility, returning metadata from both table types.
    // The segment should appear only in one
    JSONArray metadata;

    if (_pinotHelixResourceManager.hasOfflineTable(tableName)) {
      metadata = getSegmentMetaData(TableNameBuilder.OFFLINE.tableNameWithType(tableName), segmentName, CommonConstants.Helix.TableType.OFFLINE);
      if (metadata != null) {
        result.put(metadata);
        return result.toString();
      }
    }

    if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      metadata = getSegmentMetaData(tableName, segmentName, CommonConstants.Helix.TableType.REALTIME);
      if (metadata != null) {
        result.put(metadata);
        return result.toString();
      }
    }
    throw new ControllerApplicationException(LOGGER, "Segment " + segmentName + " not found in table " +
        tableName, Response.Status.NOT_FOUND);
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
    return new SuccessResponse(reloadSegmentForTable(tableName, segmentName, tableType));
  }

  @POST
  @Path("tables/{tableName}/segments/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reloads all segments of a table", notes = "Reloads all segments")
  public SuccessResponse reloadAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    return new SuccessResponse((reloadAllSegmentsForTable(tableName, tableType)));
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
    return new SuccessResponse(reloadSegmentForTable(tableName, segmentName, tableType));
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
    return new SuccessResponse((reloadAllSegmentsForTable(tableName, tableType)));
  }

  @GET
  @Path("tables/{tableName}/segments/crc")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Gets crc of all segments of a table", notes = "Gets crc of all segments")
  public String getCrcForAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName) {
    return getAllCrcMetadataForTable(tableName);
  }

  //////////////////////////////////////////////////////////////////////////////////////

  private String reloadSegmentForTable(String tableName, String segmentName,
      CommonConstants.Helix.TableType tableType) {
    int numReloadMessagesSent = 0;

    if ((tableType == null) || CommonConstants.Helix.TableType.OFFLINE == tableType) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadSegment(offlineTableName, segmentName);
    }

    if ((tableType == null) || CommonConstants.Helix.TableType.REALTIME == tableType) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadSegment(realtimeTableName, segmentName);
    }

    return "Sent " + numReloadMessagesSent + " reload messages";
  }

  private String reloadAllSegmentsForTable(String tableName, @Nullable CommonConstants.Helix.TableType tableType) {
    int numReloadMessagesSent = 0;

    if ((tableType == null) || CommonConstants.Helix.TableType.OFFLINE == tableType) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadAllSegments(offlineTableName);
    }

    if ((tableType == null) || CommonConstants.Helix.TableType.REALTIME == tableType) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadAllSegments(realtimeTableName);
    }

    return "Sent " + numReloadMessagesSent + " reload messages";
  }

  public static JSONArray toggleStateInternal(String tableName, StateType state,
      CommonConstants.Helix.TableType tableType, String segmentName, PinotHelixResourceManager helixResourceManager) {
    JSONArray ret = new JSONArray();
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
        throw new ControllerApplicationException(LOGGER, "OFFLINE response : " + responseOffline.getMessage() + ", REALTIME response"
            + responseRealtime.getMessage(), INTERNAL_ERROR);
      }
      List<PinotResourceManagerResponse> responses = new ArrayList<>();
      responses.add(responseRealtime);
      responses.add(responseOffline);
      ret.put(responses);
      return ret;
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
    ret.put(resourceManagerResponse);
    return ret;
  }

  /**
   * Helper method to toggle state of segment for a given table. The tableName expected is the internally
   * stored name (with offline/realtime annotation).
   *
   * @param segmentsToToggle: segments that we want to perform operations on
   * @param tableName: Internal name (created by TableNameBuilder) for the table
   * @param segmentName: Segment to set the state for.
   * @param state: Value of state to set.
   * @return
   * @throws JSONException
   */
  private static PinotResourceManagerResponse toggleSegmentsForTable(@Nonnull List<String> segmentsToToggle,
      @Nonnull String tableName, String segmentName, @Nonnull StateType state,
      PinotHelixResourceManager helixResourceManager) {
    long timeOutInSeconds = 10L;
    if (segmentName == null) {
      // For enable, allow 5 seconds per segment for an instance as timeout.
      if (state == StateType.ENABLE) {
        int instanceCount = helixResourceManager.getAllInstances().size();
        if (instanceCount != 0) {
          timeOutInSeconds = (long) ((_offlineToOnlineTimeoutInseconds * segmentsToToggle.size()) / instanceCount);
        } else {
          return new PinotResourceManagerResponse("Error: could not find any instances in table " + tableName, false);
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
      return new PinotResourceManagerResponse("No segments to toggle", true);
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

  private JSONArray getAllSegmentsMetadataForTable(String tableName, CommonConstants.Helix.TableType tableType) {
    boolean foundRealtimeTable = false;
    boolean foundOfflineTable = false;
    JSONArray ret = new JSONArray();
    try {
      if ((tableType == null || tableType == CommonConstants.Helix.TableType.REALTIME)
          && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
        JSONObject realtime = new JSONObject();
        realtime.put(FileUploadPathProvider.TABLE_NAME, realtimeTableName);
        realtime.put("segments", new ObjectMapper().writeValueAsString(
            _pinotHelixResourceManager.getInstanceToSegmentsInATableMap(realtimeTableName)));
        ret.put(realtime);
        foundRealtimeTable = true;
      }

      if ((tableType == null
          || tableType == CommonConstants.Helix.TableType.OFFLINE && _pinotHelixResourceManager.hasOfflineTable(
          tableName))) {
        String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        JSONObject offline = new JSONObject();
        offline.put(FileUploadPathProvider.TABLE_NAME, offlineTableName);
        offline.put("segments", new ObjectMapper().writeValueAsString(
            _pinotHelixResourceManager.getInstanceToSegmentsInATableMap(offlineTableName)));
        ret.put(offline);
        foundOfflineTable = true;
      }
    } catch (JSONException | JsonProcessingException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to convert segment metadata to json, table: %s", tableName),
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    if (foundOfflineTable || foundRealtimeTable) {
      return ret;
    } else {
      throw new ControllerApplicationException(LOGGER, "Table " + tableName + " not found.", Response.Status.NOT_FOUND);
    }
  }

  /**
   * Get meta-data for segment of table. Table name is the suffixed (offline/realtime)
   * name.
   * @param tableNameWithType: Suffixed (realtime/offline) table Name
   * @param segmentName: Segment for which to get the meta-data.
   * @return A json array that is to be returned to the client.
   * @throws JSONException
   * XXX
   * @note: Previous restlet API retuned a toString from this method, so if compat is desired, then change this method to return a String type
   */
  private @Nullable JSONArray getSegmentMetaData(String tableNameWithType, String segmentName, @Nonnull  CommonConstants.Helix.TableType tableType)
      throws JSONException {
    if (!ZKMetadataProvider.isSegmentExisted(_pinotHelixResourceManager.getPropertyStore(), tableNameWithType, segmentName)) {
      return null;
    }

    JSONArray ret = new JSONArray();
    JSONObject jsonObj = new JSONObject();
    jsonObj.put(TABLE_NAME, tableNameWithType);

    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();

    if (tableType == tableType.OFFLINE) {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata =
          ZKMetadataProvider.getOfflineSegmentZKMetadata(propertyStore, tableNameWithType, segmentName);
      jsonObj.put(STATE, offlineSegmentZKMetadata.toMap());

    }

    if (tableType == CommonConstants.Helix.TableType.REALTIME) {
      RealtimeSegmentZKMetadata realtimeSegmentZKMetadata =
          ZKMetadataProvider.getRealtimeSegmentZKMetadata(propertyStore, tableNameWithType, segmentName);
      jsonObj.put(STATE, realtimeSegmentZKMetadata.toMap());
    }

    ret.put(jsonObj);
    return ret;
  }

  private String getAllCrcMetadataForTable(String tableName) {
    PinotResourceManagerResponse response;

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
    ObjectMapper mapper = new ObjectMapper();
    String result = null;
    try {
      result = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(segmentCrcForTable);
    } catch (JsonProcessingException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to write segment crc values for table: %s", tableName),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return result;
  }
}
