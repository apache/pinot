/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.restlet.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.restlet.swagger.Response;
import com.linkedin.pinot.common.restlet.swagger.Responses;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotSegmentRestletResource extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRestletResource.class);

  private final ObjectMapper mapper;
  private long _offlineToOnlineTimeoutInseconds;

  public PinotSegmentRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
    mapper = new ObjectMapper();

    // Timeout of 5 seconds per segment to come up to online from offline
    _offlineToOnlineTimeoutInseconds = 5L;
  }

  /**
   * URI Mappings:
   * - "/tables/{tableName}/segments", "tables/{tableName}/segments/":
   *   List all segments in the table.
   *
   * - "/tables/{tableName}/segments/{segmentName}", "/tables/{tableName}/segments/{segmentName}/":
   *   List meta-data for the specified segment.
   *
   * - "/tables/{tableName}/segments/{segmentName}?state={state}",
   *    Change the state of the segment to specified {state} (enable|disable|drop)
   *
   * - "/tables/{tableName}/segments?state={state}"
   *    Change the state of all segments of the table to specified {state} (enable|disable|drop)
   *
   * {@inheritDoc}
   * @see org.restlet.resource.ServerResource#get()
   */
  @Override
  @Get
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      final String tableName = (String) getRequest().getAttributes().get(TABLE_NAME);
      final String segmentName = (String) getRequest().getAttributes().get(SEGMENT_NAME);
      final String state = getReference().getQueryAsForm().getValues(STATE);
      final String tableType = getReference().getQueryAsForm().getValues(TABLE_TYPE);

      if (tableType != null && !isValidTableType(tableType)) {
        LOGGER.error(INVALID_TABLE_TYPE_ERROR);
        setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
        return new StringRepresentation(INVALID_TABLE_TYPE_ERROR);
      }

      if (state != null) {
        if (!isValidState(state)) {
          LOGGER.error(INVALID_STATE_ERROR);
          setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
          return new StringRepresentation(INVALID_STATE_ERROR);
        } else {
          return toggleSegmentState(tableName, segmentName, state, tableType);
        }
      } else if (segmentName != null) {
        return getSegmentMetadataForTable(tableName, segmentName, tableType);
      } else {
        return getAllSegmentsMetadataForTable(tableName, tableType);
      }

    } catch (final Exception e) {
      presentation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      LOGGER.error("Caught exception while processing get request", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_GET_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }

    return presentation;
  }

  @HttpVerb("get")
  @Summary("Lists segment metadata for a given table")
  @Tags({ "segment", "table" })
  @Paths({ "/tables/{tableName}/segments/metadata", "/tables/{tableName}/segments/metadata/" })
  @Responses({
      @Response(statusCode = "200", description = "A list of segment metadata"),
      @Response(statusCode = "404", description = "The table does not exist")
  })
  private Representation getAllSegmentsMetadataForTable(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to list segment metadata",
          required = true) String tableName,
      @Parameter(name = "type", in = "query", description = "Type of table {offline|realtime}",
          required = false) String tableType)
      throws JSONException, JsonProcessingException {
    boolean foundRealtimeTable = false;
    boolean foundOfflineTable = false;

    JSONArray ret = new JSONArray();
    if ((tableType == null || TableType.REALTIME.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
      JSONObject realtime = new JSONObject();
      realtime.put(TABLE_NAME, realtimeTableName);
      realtime.put("segments", new ObjectMapper().writeValueAsString(_pinotHelixResourceManager
          .getInstanceToSegmentsInATableMap(realtimeTableName)));
      ret.put(realtime);
      foundRealtimeTable = true;
    }

    if ((tableType == null || TableType.OFFLINE.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
      JSONObject offline = new JSONObject();
      offline.put(TABLE_NAME, offlineTableName);
      offline.put("segments", new ObjectMapper().writeValueAsString(_pinotHelixResourceManager
          .getInstanceToSegmentsInATableMap(offlineTableName)));
      ret.put(offline);
      foundOfflineTable = true;
    }

    if (foundOfflineTable || foundRealtimeTable) {
      return new StringRepresentation(ret.toString());
    } else {
      setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation("Table " + tableName + " not found.");
    }
  }

  /**
   * Toggle state of provided segment between {enable|disable|drop}.
   *
   * @throws JsonProcessingException
   * @throws JSONException
   */
  @HttpVerb("get")
  @Summary("Enable, disable or drop a segment from a table")
  @Tags({ "segment", "table" })
  @Paths({ "/tables/{tableName}/segments/{segmentName}", "/tables/{tableName}/segments/{segmentName}/" })
  protected Representation toggleOneSegmentState(
      @Parameter(name = "tableName", in = "path", description = "The name of the table to which segment belongs",
          required = true) String tableName,
      @Parameter(name = "segmentName", in = "path", description = "Segment to enable, disable or drop",
          required = true) String segmentName,
      @Parameter(name = "state", in = "query", description = "state to set for segment {enable|disable|drop}",
          required = true) String state,
      @Parameter(name = "type", in = "query", description = "Type of table {offline|realtime}",
          required = false) String tableType)
      throws JsonProcessingException, JSONException {
    return toggleSegmentState(tableName, segmentName, state, tableType);
  }

  /**
   * Toggle state of provided segment between {enable|disable|drop}.
   *
   * @throws JsonProcessingException
   * @throws JSONException
   */
  @HttpVerb("get")
  @Summary("Enable, disable or drop *ALL* segments from a table")
  @Tags({ "segment", "table" })
  @Paths({ "/tables/{tableName}/segments", "/tables/{tableName}/segments/" })
  protected Representation toggleAllSegmentsState(
        @Parameter(name = "tableName", in = "path", description = "The name of the table to which segment belongs",
            required = true) String tableName,
        @Parameter(name = "state", in = "query", description = "state to set for segment {enable|disable|drop}",
            required = true) String state,
        @Parameter(name = "type", in = "query", description = "Type of table {offline|realtime}",
            required = false) String tableType)
        throws JsonProcessingException, JSONException {
    return toggleSegmentState(tableName, null, state, tableType);
  }

  /**
   * Handler to toggle state of segment for a given table.
   *
   * @param tableName: External name for the table
   * @param segmentName: Segment to set the state for
   * @param state: Value of state to set
   * @param tableType: Offline or realtime
   * @return
   * @throws JsonProcessingException
   * @throws JSONException
   */
  protected Representation toggleSegmentState(String tableName, String segmentName, String state, String tableType)
      throws JsonProcessingException, JSONException {

    JSONArray ret = new JSONArray();
    String tableNameWithType = null;
    List<String> segmentsToToggle = new ArrayList<>();
    String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);

    if (segmentName != null) {
      segmentsToToggle = Collections.singletonList(segmentName);
    } else {
      // Check if there is a realtime table. If there is, remove all segments for realtime
      if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        segmentsToToggle.addAll(_pinotHelixResourceManager.getAllSegmentsForResource(realtimeTableName));
      }
      segmentsToToggle.addAll(_pinotHelixResourceManager.getAllSegmentsForResource(offlineTableName));
    }
    if ((tableType == null
        && SegmentName.getSegmentType(segmentsToToggle.get(0)).equals(SegmentName.RealtimeSegmentType.UNSUPPORTED)
        || TableType.OFFLINE.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      tableNameWithType = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);

    }

    if ((tableType == null
        && !SegmentName.getSegmentType(segmentsToToggle.get(0)).equals(SegmentName.RealtimeSegmentType.UNSUPPORTED)
        || TableType.REALTIME.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      tableNameWithType = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    }


    if (tableNameWithType != null) {

      PinotResourceManagerResponse resourceManagerResponse = toggleSegmentsForTable(segmentsToToggle, tableNameWithType, segmentName, state);
      setStatus(resourceManagerResponse.isSuccessful() ? Status.SUCCESS_OK : Status.SERVER_ERROR_INTERNAL);
      ret.put(resourceManagerResponse);
    }
    return new StringRepresentation(ret.toString());
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
  private PinotResourceManagerResponse toggleSegmentsForTable(List<String> segmentsToToggle, String tableName, String segmentName, String state) throws JSONException {
    long timeOutInSeconds = 10L;
    if (segmentName == null) {
      // For enable, allow 5 seconds per segment for an instance as timeout.
      if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
        int instanceCount = _pinotHelixResourceManager.getAllInstanceNames().size();
        if (instanceCount != 0) {
          timeOutInSeconds = (long) ((_offlineToOnlineTimeoutInseconds * segmentsToToggle.size()) / instanceCount);
        } else {
          return new PinotResourceManagerResponse("Error: could not find any instances in table " + tableName, false);
        }
      }
    }

    if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.toggleSegmentState(tableName, segmentsToToggle, true, timeOutInSeconds);
    } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.toggleSegmentState(tableName, segmentsToToggle, false, timeOutInSeconds);
    } else if (StateType.DROP.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.deleteSegments(tableName, segmentsToToggle);
    } else {
      return new PinotResourceManagerResponse(INVALID_STATE_ERROR, false);
    }
  }

  @HttpVerb("get")
  @Summary("Gets segment metadata for a given segment")
  @Tags({ "segment", "table" })
  @Paths({ "/tables/{tableName}/segments/{segmentName}/metadata", "/tables/{tableName}/segments/{segmentName}/metadata/" })
  private Representation getSegmentMetadataForTable(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to list segment metadata",
          required = true) String tableName,
      @Parameter(name = "segmentName", in = "path", description = "The name of the segment for which to fetch metadata",
          required = true) String segmentName,
      @Parameter(name = "type", in = "query", description = "Type of table {offline|realtime}",
          required = false) String tableType)
      throws JsonProcessingException, JSONException {

    JSONArray ret = new JSONArray();
    if ((tableType == null || TableType.OFFLINE.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
      ret.put(getSegmentMetaData(offlineTableName, segmentName, TableType.OFFLINE));
    }

    if ((tableType == null || TableType.REALTIME.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
      ret.put(getSegmentMetaData(realtimeTableName, segmentName, TableType.REALTIME));
    }

    return new StringRepresentation(ret.toString());
  }

  /**
   * Get meta-data for segment of table. Table name is the suffixed (offline/realtime)
   * name.
   * @param tableName: Suffixed (realtime/offline) table Name
   * @param segmentName: Segment for which to get the meta-data.
   * @return
   * @throws JSONException
   */
  private StringRepresentation getSegmentMetaData(String tableName, String segmentName, TableType tableType)
      throws JSONException {
    if (!ZKMetadataProvider.isSegmentExisted(_pinotHelixResourceManager.getPropertyStore(), tableName, segmentName)) {
      String error = new String("Error: segment " + segmentName + " not found.");
      LOGGER.error(error);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation(error);
    }

    JSONArray ret = new JSONArray();
    JSONObject jsonObj = new JSONObject();
    jsonObj.put(TABLE_NAME, tableName);

    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();

    if (tableType == tableType.OFFLINE) {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata =
          ZKMetadataProvider.getOfflineSegmentZKMetadata(propertyStore, tableName, segmentName);
      jsonObj.put(STATE, offlineSegmentZKMetadata.toMap());

    }

    if (tableType == TableType.REALTIME) {
      RealtimeSegmentZKMetadata realtimeSegmentZKMetadata =
          ZKMetadataProvider.getRealtimeSegmentZKMetadata(propertyStore, tableName, segmentName);
      jsonObj.put(STATE, realtimeSegmentZKMetadata.toMap());
    }

    ret.put(jsonObj);
    return new StringRepresentation(ret.toString());
  }
}
