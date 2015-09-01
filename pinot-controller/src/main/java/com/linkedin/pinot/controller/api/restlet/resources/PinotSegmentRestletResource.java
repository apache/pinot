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

import java.util.ArrayList;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.api.swagger.HttpVerb;
import com.linkedin.pinot.controller.api.swagger.Parameter;
import com.linkedin.pinot.controller.api.swagger.Paths;
import com.linkedin.pinot.controller.api.swagger.Summary;
import com.linkedin.pinot.controller.api.swagger.Tags;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;


/**
 * Sep 29, 2014
 */

public class PinotSegmentRestletResource extends PinotRestletResourceBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRestletResource.class);

  private final ObjectMapper mapper;

  public PinotSegmentRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
    mapper = new ObjectMapper();
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
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }

    return presentation;
  }

  @HttpVerb("get")
  @Summary("Lists segment metadata for a given table")
  @Tags({ "segment", "table" })
  @Paths({ "/tables/{tableName}/segments/metadata", "/tables/{tableName}/segments/metadata/" })
  private Representation getAllSegmentsMetadataForTable(
      @Parameter(name = "tableName", in = "path",
          description = "The name of the table for which to list segment metadata", required = true) String tableName,
      @Parameter(name = "type", in = "query", description = "Type of table {offline|realtime}", required = false) String tableType)
      throws JSONException, JsonProcessingException {

    JSONArray ret = new JSONArray();
    if ((tableType == null || TableType.REALTIME.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
      JSONObject realtime = new JSONObject();
      realtime.put(TABLE_NAME, realtimeTableName);
      realtime.put("segments", new ObjectMapper().writeValueAsString(_pinotHelixResourceManager
          .getInstanceToSegmentsInATableMap(realtimeTableName)));
      ret.put(realtime);
    }

    if ((tableType == null || TableType.OFFLINE.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
      JSONObject offline = new JSONObject();
      offline.put(TABLE_NAME, offlineTableName);
      offline.put("segments", new ObjectMapper().writeValueAsString(_pinotHelixResourceManager
          .getInstanceToSegmentsInATableMap(offlineTableName)));
      ret.put(offline);
    }

    return new StringRepresentation(ret.toString());
  }

  /**
   * Toggle state of provided segment between {enable|disable|drop}. If segmentName not provided,
   * toggles the state for all segments of the table.
   *
   * @throws JsonProcessingException
   * @throws JSONException
   */
  @HttpVerb("get")
  @Summary("Enable, disable or drop specified or all segments")
  @Tags({ "segment", "table" })
  @Paths({ "/tables/{tableName}/segments/{segmentName}", "/tables/{tableName}/segments/{segmentName}/", "/tables/{tableName}/segments", "/tables/{tableName}/segments/" })
  private Representation toggleSegmentState(@Parameter(name = "tableName", in = "path",
      description = "The name of the table to which segment belongs", required = true) String tableName, @Parameter(
      name = "segmentName", in = "path", description = "Segment to enable, disable or drop", required = false) String segmentName,
      @Parameter(name = "state", in = "query", description = "state to set for segment {enable|disable|drop}",
          required = true) String state, @Parameter(name = "type", in = "query",
          description = "Type of table {offline|realtime}", required = false) String tableType)
      throws JsonProcessingException, JSONException {

    JSONArray ret = new JSONArray();
    if ((tableType == null || TableType.OFFLINE.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
      ret.put(toggleSegmentsForTable(offlineTableName, segmentName, state));
    }

    if ((tableType == null || TableType.REALTIME.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      String realTimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
      ret.put(toggleSegmentsForTable(realTimeTableName, segmentName, state));
    }

    return new StringRepresentation(ret.toString());
  }

  /**
   * Helper method to toggle state of segment for a given table. The tableName expected is the internally
   * stored name (with offline/realtime annotation).
   *
   * @param tableName: Internal name (created by TableNameBuilder) for the table
   * @param segmentName: Segment to set the state for.
   * @param state: Value of state to set.
   * @return
   * @throws JSONException
   */
  private JSONArray toggleSegmentsForTable(String tableName, String segmentName, String state) throws JSONException {
    List<String> segmentsToToggle;
    JSONArray ret = new JSONArray();

    if (segmentName != null) {
      segmentsToToggle = new ArrayList<String>();
      segmentsToToggle.add(segmentName);
    } else {
      segmentsToToggle = _pinotHelixResourceManager.getAllSegmentsForResource(tableName);
    }

    long timeOutInSeconds = 10;
    for (String segmentToToggle : segmentsToToggle) {
      JSONObject jsonObj = new JSONObject();
      jsonObj.put(TABLE_NAME, tableName);

      if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
        jsonObj.put(STATE,
            _pinotHelixResourceManager.toggleSegmentState(tableName, segmentToToggle, true, timeOutInSeconds).toJSON()
                .toString());
      } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
        jsonObj.put(STATE,
            _pinotHelixResourceManager.toggleSegmentState(tableName, segmentToToggle, false, timeOutInSeconds));
      } else if (StateType.DROP.name().equalsIgnoreCase(state)) {
        jsonObj.put(STATE, _pinotHelixResourceManager.dropSegment(tableName, segmentToToggle).toJSON().toString());
      } else {
        jsonObj.put(STATE, new PinotResourceManagerResponse(INVALID_STATE_ERROR, false).toJSON().toString());
      }

      ret.put(jsonObj);
    }

    return ret;
  }

  @HttpVerb("get")
  @Summary("Gets segment metadata for a given segment")
  @Tags({ "segment", "table" })
  @Paths({ "/tables/{tableName}/segments/{segmentName}/metadata", "/tables/{tableName}/segments/{segmentName}/metadata/" })
  private Representation getSegmentMetadataForTable(
      @Parameter(name = "tableName", in = "path",
          description = "The name of the table for which to list segment metadata", required = true) String tableName,
      @Parameter(name = "segmentName", in = "path",
          description = "The name of the segment for which to fetch metadata", required = true) String segmentName,
      @Parameter(name = "type", in = "query", description = "Type of table {offline|realtime}", required = false) String tableType)
      throws JsonProcessingException, JSONException {

    JSONArray ret = new JSONArray();
    if ((tableType == null || TableType.OFFLINE.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
      ret.put(getSegmentMetaData(offlineTableName, segmentName, TableType.OFFLINE));
    }

    if ((tableType == null || TableType.REALTIME.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      String realtimeTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
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
