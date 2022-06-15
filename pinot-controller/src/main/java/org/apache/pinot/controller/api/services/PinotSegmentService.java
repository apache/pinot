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
package org.apache.pinot.controller.api.services;

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
import java.util.List;
import java.util.Map;
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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.resources.Constants;
import org.apache.pinot.controller.api.resources.SuccessResponse;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader;
import org.apache.pinot.spi.config.table.TableType;

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
@SwaggerDefinition(securityDefinition = @SecurityDefinition(
    apiKeyAuthDefinitions = @ApiKeyAuthDefinition(
        name = HttpHeaders.AUTHORIZATION,
        in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public interface PinotSegmentService {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "List all segments. An optional 'excludeReplacedSegments' parameter is used to get the"
      + " list of segments which has not yet been replaced (determined by segment lineage entries) and can be queried"
      + " from the table. The value is false by default.",
      // TODO: more and more filters can be added later on, like excludeErrorSegments, excludeConsumingSegments, etc.
      notes = "List all segments")
  List<Map<TableType, List<String>>> getSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Whether to exclude replaced segments in the response, which have been replaced"
          + " specified in the segment lineage entries and cannot be queried from the table")
      @QueryParam("excludeReplacedSegments") String excludeReplacedSegments);

  @GET
  @Path("segments/{tableName}/servers")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from server to segments hosted by the server", notes = "Get a map from server to "
      + "segments hosted by the server")
  List<Map<String, Object>> getServerToSegmentsMap(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr);

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from server to segments hosted by the server (deprecated, use 'GET "
      + "/segments/{tableName}/servers' instead)", notes =
      "Get a map from server to segments hosted by the server (deprecated, use 'GET "
          + "/segments/{tableName}/servers' instead)")
  List<Map<String, String>> getServerToSegmentsMapDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "MUST be null") @QueryParam("state") String stateStr,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr)
      throws JsonProcessingException;

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from server to segments hosted by the server (deprecated, use 'GET "
      + "/segments/{tableName}/servers' instead)", notes =
      "Get a map from server to segments hosted by the server (deprecated, use 'GET "
          + "/segments/{tableName}/servers' instead)")
  List<Map<String, String>> getServerToSegmentsMapDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "MUST be null") @QueryParam("state") String stateStr,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr)
      throws JsonProcessingException;

  @GET
  @Path("segments/{tableName}/crc")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from segment to CRC of the segment (only apply to OFFLINE table)", notes = "Get a "
      + "map from segment to CRC of the segment (only apply to OFFLINE table)")
  Map<String, String> getSegmentToCrcMap(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName);

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/crc")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a map from segment to CRC of the segment (deprecated, use 'GET "
      + "/segments/{tableName}/crc' instead)", notes = "Get a map from segment to CRC of the segment (deprecated, use"
      + " 'GET /segments/{tableName}/crc' instead)")
  Map<String, String> getSegmentToCrcMapDeprecated(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName);

  @GET
  @Path("segments/{tableName}/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the metadata for a segment", notes = "Get the metadata for a segment")
  Map<String, Object> getSegmentMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "Columns name", allowMultiple = true) @QueryParam("columns") List<String> columns);

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value =
      "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' "
          + "instead)", notes =
      "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' "
          + "instead)")
  List<List<Map<String, Object>>> getSegmentMetadataDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr);

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/{segmentName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value =
      "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' "
          + "instead)", notes =
      "Get the metadata for a segment (deprecated, use 'GET /segments/{tableName}/{segmentName}/metadata' "
          + "instead)")
  List<List<Map<String, Object>>> getSegmentMetadataDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "MUST be null") @QueryParam("state") String stateStr,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr);

  @POST
  @Path("segments/{tableName}/{segmentName}/reload")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a segment", notes = "Reload a segment")
  SuccessResponse reloadSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "Whether to force server to download segment") @QueryParam("forceDownload")
      @DefaultValue("false") boolean forceDownload);

  /**
   * Resets the segment of the table, by disabling and then enabling it.
   * This API will take segments to OFFLINE state, wait for External View to stabilize, and then back to
   * ONLINE/CONSUMING state,
   * thus effective in resetting segments or consumers in error states.
   */
  @POST
  @Path("segments/{tableNameWithType}/{segmentName}/reset")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value =
      "Resets a segment by first disabling it, waiting for external view to stabilize, and finally enabling "
          + "it again", notes = "Resets a segment by disabling and then enabling the segment")
  SuccessResponse resetSegment(
      @ApiParam(value = "Name of the table with type", required = true) @PathParam("tableNameWithType")
          String tableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "Maximum time in milliseconds to wait for reset to be completed. By default, uses "
          + "serverAdminRequestTimeout") @QueryParam("maxWaitTimeMs") long maxWaitTimeMs);

  /**
   * Resets all segments of the given table
   * This API will take segments to OFFLINE state, wait for External View to stabilize, and then back to
   * ONLINE/CONSUMING state,
   * thus effective in resetting segments or consumers in error states.
   */
  @POST
  @Path("segments/{tableNameWithType}/reset")
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value =
      "Resets all segments of the table, by first disabling them, waiting for external view to stabilize, and"
          + " finally enabling the segments", notes = "Resets a segment by disabling and then enabling a segment")
  SuccessResponse resetAllSegments(
      @ApiParam(value = "Name of the table with type", required = true) @PathParam("tableNameWithType")
          String tableNameWithType,
      @ApiParam(value = "Maximum time in milliseconds to wait for reset to be completed. By default, uses "
          + "serverAdminRequestTimeout") @QueryParam("maxWaitTimeMs") long maxWaitTimeMs);

  @Deprecated
  @POST
  @Path("tables/{tableName}/segments/{segmentName}/reload")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(
      value = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)",
      notes = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)")
  SuccessResponse reloadSegmentDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr);

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/{segmentName}/reload")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)",
      notes = "Reload a segment (deprecated, use 'POST /segments/{tableName}/{segmentName}/reload' instead)")
  SuccessResponse reloadSegmentDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr);

  @POST
  @Path("segments/{tableName}/reload")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments", notes = "Reload all segments")
  SuccessResponse reloadAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Whether to force server to download segment") @QueryParam("forceDownload")
      @DefaultValue("false") boolean forceDownload);

  @Deprecated
  @POST
  @Path("tables/{tableName}/segments/reload")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)", notes =
      "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)")
  SuccessResponse reloadAllSegmentsDeprecated1(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr);

  @Deprecated
  @GET
  @Path("tables/{tableName}/segments/reload")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)", notes =
      "Reload all segments (deprecated, use 'POST /segments/{tableName}/reload' instead)")
  SuccessResponse reloadAllSegmentsDeprecated2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr);

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/{segmentName}")
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete a segment", notes = "Delete a segment")
  SuccessResponse deleteSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "Retention period for the deleted segments (e.g. 12h, 3d); Using 0d or -1d will instantly "
          + "delete segments without retention") @QueryParam("retention") String retentionPeriod);

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete all segments", notes = "Delete all segments")
  SuccessResponse deleteAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Retention period for the deleted segments (e.g. 12h, 3d); Using 0d or -1d will instantly "
          + "delete segments without retention") @QueryParam("retention") String retentionPeriod);

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/delete")
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Delete the segments in the JSON array payload", notes = "Delete the segments in the JSON "
      + "array payload")
  SuccessResponse deleteSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Retention period for the deleted segments (e.g. 12h, 3d); Using 0d or -1d will instantly "
          + "delete segments without retention") @QueryParam("retention") String retentionPeriod,
      List<String> segments);

  @GET
  @Path("segments/{tableName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the server metadata for all table segments", notes = "Get the server metadata for all "
      + "table segments")
  String getServerMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Columns name", allowMultiple = true) @QueryParam("columns") @DefaultValue("")
          List<String> columns);

  @GET
  @Path("segments/{tableName}/select")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the selected segments given the (inclusive) start and (exclusive) end timestamps"
      + " in milliseconds. These timestamps will be compared against the minmax values of the time column in each"
      + " segment. If the table is a refresh use case, the value of start and end timestamp is voided,"
      + " since there is no time column for refresh use case; instead, the whole qualified segments will be returned."
      + " If no timestamps are provided, all the qualified segments will be returned."
      + " For the segments that partially belong to the time range, the boolean flag 'excludeOverlapping' is introduced"
      + " in order for user to determine whether to exclude this kind of segments in the response.", notes = "Get the"
      + " selected segments given the start and end timestamps in milliseconds")
  List<Map<TableType, List<String>>> getSelectedSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Start timestamp (inclusive)") @QueryParam("startTimestamp") @DefaultValue("")
          String startTimestampStr,
      @ApiParam(value = "End timestamp (exclusive)") @QueryParam("endTimestamp") @DefaultValue("")
          String endTimestampStr,
      @ApiParam(value = "Whether to exclude the segments overlapping with the timestamps, false by default")
      @QueryParam("excludeOverlapping") @DefaultValue("false") boolean excludeOverlapping);

  // TODO: Move this API into PinotTableRestletResource
  @GET
  @Path("/tables/{realtimeTableName}/consumingSegmentsInfo")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Returns state of consuming segments", notes = "Gets the status of consumers from all servers")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap getConsumingSegmentsInfo(
      @ApiParam(value = "Realtime table name with or without type", required = true, example = "myTable | "
          + "myTable_REALTIME") @PathParam("realtimeTableName") String realtimeTableName);
}
