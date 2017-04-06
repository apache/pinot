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
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;
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
  @Override
  @Get
  public Representation get() {
    StringRepresentation representation;
    try {
      String tableName = (String) getRequest().getAttributes().get(TABLE_NAME);
      String segmentName = (String) getRequest().getAttributes().get(SEGMENT_NAME);
      if (segmentName != null) {
        segmentName = URLDecoder.decode(segmentName, "UTF-8");
      }

      Reference reference = getReference();
      String state = reference.getQueryAsForm().getValues(STATE);
      String tableType = reference.getQueryAsForm().getValues(TABLE_TYPE);

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
          if (segmentName != null) {
            // '/tables/{tableName}/segments/{segmentName}?state={state}'
            return toggleOneSegmentState(tableName, segmentName, state, tableType);
          } else {
            // '/tables/{tableName}/segments?state={state}'
            return toggleAllSegmentsState(tableName, state, tableType);
          }
        }
      }

      String lastPart = reference.getLastSegment();
      if (lastPart.equals("reload")) {
        // RELOAD

        if (segmentName != null) {
          // '/tables/{tableName}/segments/{segmentName}/reload'
          return reloadSegmentForTable(tableName, segmentName, tableType);
        } else {
          // '/tables/{tableName}/segments/reload'
          return reloadAllSegmentsForTable(tableName, tableType);
        }
      } else {
        // METADATA

        if (segmentName != null) {
          // '/tables/{tableName}/segments/{segmentName}'
          // '/tables/{tableName}/segments/{segmentName}/metadata'
          return getSegmentMetadataForTable(tableName, segmentName, tableType);
        } else {
          // '/tables/{tableName}/segments'
          // '/tables/{tableName}/segments/metadata'
          return getAllSegmentsMetadataForTable(tableName, tableType);
        }
      }
    } catch (final Exception e) {
      representation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      LOGGER.error("Caught exception while processing get request", e);
      ControllerRestApplication.getControllerMetrics()
          .addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_GET_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return representation;
    }
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
  private Representation toggleOneSegmentState(
      @Parameter(name = "tableName", in = "path", description = "The name of the table to which segment belongs",
          required = true) String tableName,
      @Parameter(name = "segmentName", in = "path", description = "Segment to enable, disable or drop",
          required = true) String segmentName,
      @Parameter(name = "state", in = "query", description = "state to set for segment {enable|disable|drop}",
          required = true) String state,
      @Parameter(name = "type", in = "query", description = "Type of table {offline|realtime}",
          required = false) String tableType)
      throws JsonProcessingException, JSONException {
    String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    List<String> offlineSegments = _pinotHelixResourceManager.getAllSegmentsForResource(offlineTableName);
    List<String> realtimeSegments = _pinotHelixResourceManager.getAllSegmentsForResource(realtimeTableName);

    if (tableType == null) {
      if(offlineSegments.contains(segmentName)) {
        tableType = "OFFLINE";
      } else if (realtimeSegments.contains(segmentName)) {
        tableType = "REALTIME";
      } else {
        throw new UnsupportedOperationException(segmentName + " does not exist");
      }
    }

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
  private Representation toggleAllSegmentsState(
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
    List<String> segmentsToToggle = new ArrayList<>();
    String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    String tableNameWithType = "";
    List<String> realtimeSegments = _pinotHelixResourceManager.getAllSegmentsForResource(realtimeTableName);
    List<String> offlineSegments = _pinotHelixResourceManager.getAllSegmentsForResource(offlineTableName);

    if (tableType == null) {
      PinotResourceManagerResponse responseRealtime = toggleSegmentsForTable(realtimeSegments, realtimeTableName, segmentName, state);
      PinotResourceManagerResponse responseOffline = toggleSegmentsForTable(offlineSegments, offlineTableName, segmentName, state);
      setStatus(responseRealtime.isSuccessful() && responseOffline.isSuccessful() ? Status.SUCCESS_OK : Status.SERVER_ERROR_INTERNAL);
      List<PinotResourceManagerResponse> responses = new ArrayList<>();
      responses.add(responseRealtime);
      responses.add(responseOffline);
      ret.put(responses);
      return new StringRepresentation(ret.toString());
    }
    else if (TableType.REALTIME.name().equalsIgnoreCase(tableType)) {
      if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        tableNameWithType = realtimeTableName;
        if (segmentName != null) {
          segmentsToToggle = Collections.singletonList(segmentName);
        } else {
          segmentsToToggle.addAll(realtimeSegments);
        }
      } else {
        throw new UnsupportedOperationException("There is no realtime table for " + tableName);
      }
    } else {
      if (_pinotHelixResourceManager.hasOfflineTable(tableName)) {
        tableNameWithType = offlineTableName;
        if (segmentName != null) {
          segmentsToToggle = Collections.singletonList(segmentName);
        } else {
          segmentsToToggle.addAll(offlineSegments);
        }
      } else {
        throw new UnsupportedOperationException("There is no offline table for " + tableName);
      }
    }

    PinotResourceManagerResponse resourceManagerResponse = toggleSegmentsForTable(segmentsToToggle, tableNameWithType, segmentName, state);
    setStatus(resourceManagerResponse.isSuccessful() ? Status.SUCCESS_OK : Status.SERVER_ERROR_INTERNAL);
    ret.put(resourceManagerResponse);
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
  private PinotResourceManagerResponse toggleSegmentsForTable(@Nonnull List<String> segmentsToToggle, @Nonnull String tableName, String segmentName, @Nonnull String state) throws JSONException {
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

  @HttpVerb("get")
  @Summary("Reloads a given segment")
  @Tags({ "segment", "table" })
  @Paths({ "/tables/{tableName}/segments/{segmentName}/reload", "/tables/{tableName}/segments/{segmentName}/reload/" })
  private Representation reloadSegmentForTable(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to reload segment",
          required = true) String tableName,
      @Parameter(name = "segmentName", in = "path", description = "The name of the segment for which to reload",
          required = true) String segmentName,
      @Parameter(name = "type", in = "query", description = "Type of table {offline|realtime}",
          required = false) String tableType) {
    int numReloadMessagesSent = 0;

    if ((tableType == null) || TableType.OFFLINE.name().equalsIgnoreCase(tableType)) {
      String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadSegment(offlineTableName, segmentName);
    }

    if ((tableType == null) || TableType.REALTIME.name().equalsIgnoreCase(tableType)) {
      String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadSegment(realtimeTableName, segmentName);
    }

    return new StringRepresentation("Sent " + numReloadMessagesSent + " reload messages");
  }


  @HttpVerb("get")
  @Summary("Reloads all segments in a given table")
  @Tags({ "segment", "table" })
  @Paths({ "/tables/{tableName}/segments/reload", "/tables/{tableName}/segments/reload/" })
  private Representation reloadAllSegmentsForTable(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to list segment metadata",
          required = true) String tableName,
      @Parameter(name = "type", in = "query", description = "Type of table {offline|realtime}",
          required = false) String tableType) {
    int numReloadMessagesSent = 0;

    if ((tableType == null) || TableType.OFFLINE.name().equalsIgnoreCase(tableType)) {
      String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadAllSegments(offlineTableName);
    }

    if ((tableType == null) || TableType.REALTIME.name().equalsIgnoreCase(tableType)) {
      String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
      numReloadMessagesSent += _pinotHelixResourceManager.reloadAllSegments(realtimeTableName);
    }

    return new StringRepresentation("Sent " + numReloadMessagesSent + " reload messages");
  }
}
