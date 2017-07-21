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

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
 * @see org.restlet.resource.ServerResource#get()
 */

@Api(tags = "table")
@Path("/")
public class PinotSegmentRestletResourceModel {
  public static Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRestletResourceModel.class);
  public static final Response.Status BAD_REQUEST = Response.Status.BAD_REQUEST;
  public static final Response.Status INTERNAL_ERROR = Response.Status.INTERNAL_SERVER_ERROR;

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Path("tables/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata or toggles segment states",
      notes = "Toggles segment states if 'state' is specified in query param, otherwise lists metadata")
  public String toggleStateOrListMetadata(
    @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
    @ApiParam(value = "enable|disable|drop", required = false) @QueryParam("state") String stateStr,
    @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    if (tableName == null) {
      throw new WebApplicationException("Table type cannot be null", BAD_REQUEST);
    }
    CommonConstants.Helix.TableType tableType = validateTableType(tableTypeStr);
    StateType state = validateState(stateStr);

    if (stateStr == null) {
      return listMetadataInternal(tableName, tableType);
    }
    return toggleStateInternal(tableName, state, tableType);
  }

  @GET
  @Path("tables/{tableName}/segments/{segmentName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata or toggles state of a segment",
      notes = "Toggles segment state if 'state' is specified in query param, otherwise lists segment metadata")
  public String toggleStateOrListMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam (value = "Name of segment", required = true) @PathParam("segmentName") String segmentName,
      @ApiParam(value = "online|offline|drop", required = false) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    if (tableName == null || tableName.length() == 0) {
      throw new WebApplicationException("Table name cannot be null", BAD_REQUEST);
    }
    if (segmentName == null || segmentName.length() == 0) {
      throw new WebApplicationException("Segment name cannot be null", BAD_REQUEST);
    }

    if (stateStr == null) {
      // This is a list metadata operation
      return "{\"msg\" : \"List metadata for segment " + segmentName + " of table " + tableName + "\"}";
    } else {
      // This is a toggle state operation
      return "Toggle state for segment " + segmentName + " of table " + tableName;
    }
  }

  @GET
  @Path("tables/{tableName}/segments/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata all segments of table",
      notes = "Lists segment metadata")
  public String listMetadataForAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "enable|disable|drop", required = false) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    if (tableName == null || tableName.length() == 0) {
      throw new WebApplicationException("Table name cannot be null", BAD_REQUEST);
    }
    CommonConstants.Helix.TableType tableType = validateTableType(tableTypeStr);
    return listMetadataInternal(tableName, tableType);
  }

  @GET
  @Path("tables/{tableName}/segments/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Lists metadata one segments of table",
      notes = "Lists segment metadata")
  public String listMetadataForOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam (value = "Name of segment", required = true) @PathParam("segmentName") String segmentName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    if (tableName == null || tableName.length() == 0) {
      throw new WebApplicationException("Table name cannot be null", BAD_REQUEST);
    }
    if (segmentName == null || segmentName.length() == 0) {
      throw new WebApplicationException("Segment name cannot be null", BAD_REQUEST);
    }
    CommonConstants.Helix.TableType tableType = validateTableType(tableTypeStr);
    if (tableType != null) {
      return "List metadata for segment " + segmentName + " in table type " + tableTypeStr;
    }
    return "List metadata for segment " + segmentName + " in any table type";
  }

  @GET
  @Path("tables/{tableName}/segments/{segmentName}/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reloads one segment", notes = "Reloads one segment")
  public String reloadOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam (value = "Name of segment", required = true) @PathParam("segmentName") String segmentName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    if (tableName == null || tableName.length() == 0) {
      throw new WebApplicationException("Table name cannot be null", BAD_REQUEST);
    }
    if (segmentName == null || segmentName.length() == 0) {
      throw new WebApplicationException("Segment name cannot be null", BAD_REQUEST);
    }
    CommonConstants.Helix.TableType tableType = validateTableType(tableTypeStr);
    if (tableType != null) {
      return "Reload segment " + segmentName + " in table type " + tableTypeStr;
    }
    return "Reload segment " + segmentName + " in any table type";
  }

  @GET
  @Path("tables/{tableName}/segments/reload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reloads all segments of a table", notes = "Reloads all segments")
  public String reloadAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    if (tableName == null) {
      throw new WebApplicationException("Table type cannot be null", BAD_REQUEST);
    }
    CommonConstants.Helix.TableType tableType = validateTableType(tableTypeStr);
    if (tableType != null) {
      return "Reload all segments for table type " + tableTypeStr;
    }
    return "Reload all segments";
  }

  @GET
  @Path("tables/{tableName}/segments/crc")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Gets crc of all segments of a table", notes = "Gets crc of all segments")
  public String getCrcForAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    if (tableName == null) {
      throw new WebApplicationException("Table type cannot be null", BAD_REQUEST);
    }
    CommonConstants.Helix.TableType tableType = validateTableType(tableTypeStr);
    if (tableType != null) {
      return "get crc all segments for table type " + tableTypeStr;
    }
    return "get crc for all segments";
  }

  //////////////////////////////////////////////////////////////////////////////////////

  private CommonConstants.Helix.TableType validateTableType(String tableTypeStr) {
    if (tableTypeStr == null) {
      return null;
    }
    try {
      return CommonConstants.Helix.TableType.valueOf(tableTypeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.info("Illegal table type '{}'", tableTypeStr);
      throw new WebApplicationException("Illegal table type '" + tableTypeStr + "'", BAD_REQUEST);
    }
  }

  private StateType validateState(String stateStr) {
    if (stateStr == null) {
      return null;
    }
    try {
      return StateType.valueOf(stateStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.info("Illegal state '{}'", stateStr);
      throw new WebApplicationException("Illegal state '" + stateStr + "'", BAD_REQUEST);
    }
  }

  private String toggleStateInternal(String tableName, StateType state, CommonConstants.Helix.TableType tableType) {
    String rv;
    switch (state) {
      case DISABLE:
        if (tableType == null) {
          rv = "set offline and realtime segment states to be DISABLED for table " + tableName;
        } else  if (tableType.equals(CommonConstants.Helix.TableType.OFFLINE)) {
          rv = "set offline segment states to be DISABLED for table " + tableName;
        } else {
          rv = "set realtime segment states to DISABLED for table " + tableName;
        }
        break;
      case ENABLE:
        if (tableType == null) {
          rv = "set offline and realtime segment states to be ENABLED for table " + tableName;
        } else if (tableType.equals(CommonConstants.Helix.TableType.OFFLINE)) {
          rv = "set offline segment states to be ENABLED for table " + tableName;
        } else {
          rv = "set realtime segment states to ENABLED for table " + tableName;
        }
        break;
      case DROP:
        if (tableType == null) {
          rv = "set offline and realtime segment states to be DROPPED for table " + tableName;
        } else if (tableType.equals(CommonConstants.Helix.TableType.OFFLINE)) {
          rv = "set offline segment states to be DROPPED for table " + tableName;
        } else {
          rv = "set realtime segment states to DROPPED for table " + tableName;
        }
        break;
      default:
        throw new WebApplicationException("Illegal state", INTERNAL_ERROR);
    }
    return rv;
  }

  private String listMetadataInternal(String tableName, CommonConstants.Helix.TableType tableType) {
    String rv;
    if (tableType == null) {
      rv = "List metadata for all segments of table " + tableName;
    } else {
      if (tableType.equals(CommonConstants.Helix.TableType.OFFLINE)) {
        rv = "List metadata for offline segments of table " + tableName;
      } else {
        rv = "List metadata for realtime segments of table " + tableName;
      }
    }
    return rv;
  }

  private JSONArray getAllSegmentsMetadataForTable(String tableName, String tableType) {
    boolean foundRealtimeTable = false;
    boolean foundOfflineTable = false;
    JSONArray ret = new JSONArray();

    try {
      if ((tableType == null || CommonConstants.Helix.TableType.REALTIME.name().equalsIgnoreCase(tableType))
          && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
        JSONObject realtime = new JSONObject();
        realtime.put(FileUploadPathProvider.TABLE_NAME, realtimeTableName);
        realtime.put("segments", new ObjectMapper()
            .writeValueAsString(_pinotHelixResourceManager.getInstanceToSegmentsInATableMap(realtimeTableName)));
        ret.put(realtime);
        foundRealtimeTable = true;
      }

      if ((tableType == null || CommonConstants.Helix.TableType.OFFLINE.name().equalsIgnoreCase(tableType)) && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
        String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        JSONObject offline = new JSONObject();
        offline.put(FileUploadPathProvider.TABLE_NAME, offlineTableName);
        offline.put("segments", new ObjectMapper()
            .writeValueAsString(_pinotHelixResourceManager.getInstanceToSegmentsInATableMap(offlineTableName)));
        ret.put(offline);
        foundOfflineTable = true;
      }
    } catch (Exception e) {
      throw new WebApplicationException(e, INTERNAL_ERROR);
    }

    if (foundOfflineTable || foundRealtimeTable) {
      return ret;
    } else {
      throw new WebApplicationException("Table " + tableName + " not found.", Response.Status.NOT_FOUND);
    }

  }
}
