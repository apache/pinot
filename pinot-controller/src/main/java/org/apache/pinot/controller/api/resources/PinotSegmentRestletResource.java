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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
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


@Path("/")
public class PinotSegmentRestletResource implements org.apache.pinot.controller.api.services.PinotSegmentService {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRestletResource.class);

  @Inject
  ControllerConf _controllerConf;

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  Executor _executor;

  @Inject
  HttpConnectionManager _connectionManager;

  @Override
  public List<Map<TableType, List<String>>> getSegments(String tableName, String tableTypeStr,
      String excludeReplacedSegments) {
    List<String> tableNamesWithType = ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName,
        Constants.validateTableType(tableTypeStr), LOGGER);
    boolean shouldExcludeReplacedSegments = Boolean.parseBoolean(excludeReplacedSegments);
    List<Map<TableType, List<String>>> resultList = new ArrayList<>(tableNamesWithType.size());
    for (String tableNameWithType : tableNamesWithType) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      List<String> segments =
          _pinotHelixResourceManager.getSegmentsFor(tableNameWithType, shouldExcludeReplacedSegments);
      resultList.add(Collections.singletonMap(tableType, segments));
    }
    return resultList;
  }

  @Override
  public List<Map<String, Object>> getServerToSegmentsMap(String tableName, String tableTypeStr) {
    List<String> tableNamesWithType = ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName,
        Constants.validateTableType(tableTypeStr), LOGGER);
    List<Map<String, Object>> resultList = new ArrayList<>(tableNamesWithType.size());
    for (String tableNameWithType : tableNamesWithType) {
      Map<String, Object> resultForTable = new LinkedHashMap<>();
      resultForTable.put("tableName", tableNameWithType);
      resultForTable.put("serverToSegmentsMap", _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType));
      resultList.add(resultForTable);
    }
    return resultList;
  }

  @Override
  @Deprecated
  public List<Map<String, String>> getServerToSegmentsMapDeprecated1(String tableName, String stateStr,
      String tableTypeStr)
      throws JsonProcessingException {
    if (stateStr != null) {
      throw new WebApplicationException("Cannot toggle segment state", Status.FORBIDDEN);
    }

    List<String> tableNamesWithType = ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName,
        Constants.validateTableType(tableTypeStr), LOGGER);
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

  @Override
  @Deprecated
  public List<Map<String, String>> getServerToSegmentsMapDeprecated2(String tableName, String stateStr,
      String tableTypeStr)
      throws JsonProcessingException {
    return getServerToSegmentsMapDeprecated1(tableName, stateStr, tableTypeStr);
  }

  @Override
  public Map<String, String> getSegmentToCrcMap(String tableName) {
    String offlineTableName =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, TableType.OFFLINE, LOGGER)
            .get(0);
    return _pinotHelixResourceManager.getSegmentsCrcForTable(offlineTableName);
  }

  @Override
  public Map<String, String> getSegmentToCrcMapDeprecated(String tableName) {
    return getSegmentToCrcMap(tableName);
  }

  @Override
  public Map<String, Object> getSegmentMetadata(String tableName, @Encoded String segmentName, List<String> columns) {
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
      TableMetadataReader tableMetadataReader =
          new TableMetadataReader(_executor, _connectionManager, _pinotHelixResourceManager);
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

  @Override
  @Deprecated
  public List<List<Map<String, Object>>> getSegmentMetadataDeprecated1(String tableName, @Encoded String segmentName,
      String tableTypeStr) {
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

  @Override
  @Deprecated
  public List<List<Map<String, Object>>> getSegmentMetadataDeprecated2(String tableName, @Encoded String segmentName,
      String stateStr, String tableTypeStr) {
    if (stateStr != null) {
      throw new WebApplicationException("Cannot toggle segment state", Status.FORBIDDEN);
    }

    return getSegmentMetadataDeprecated1(tableName, segmentName, tableTypeStr);
  }

  @Override
  public SuccessResponse reloadSegment(String tableName, @Encoded String segmentName,

      @DefaultValue("false") boolean forceDownload) {
    segmentName = URIUtils.decode(segmentName);
    TableType tableType = SegmentName.isRealtimeSegmentName(segmentName) ? TableType.REALTIME : TableType.OFFLINE;
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    int numMessagesSent = _pinotHelixResourceManager.reloadSegment(tableNameWithType, segmentName, forceDownload);
    if (numMessagesSent > 0) {
      return new SuccessResponse("Sent " + numMessagesSent + " reload messages");
    } else {
      throw new ControllerApplicationException(LOGGER,
          "Failed to find segment: " + segmentName + " in table: " + tableName, Status.NOT_FOUND);
    }
  }

  @Override
  public SuccessResponse resetSegment(

      String tableNameWithType, String segmentName, long maxWaitTimeMs) {
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

  @Override
  public SuccessResponse resetAllSegments(

      String tableNameWithType, long maxWaitTimeMs) {
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

  @Override
  @Deprecated
  public SuccessResponse reloadSegmentDeprecated1(String tableName, @Encoded String segmentName, String tableTypeStr) {
    segmentName = URIUtils.decode(segmentName);
    List<String> tableNamesWithType = ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName,
        Constants.validateTableType(tableTypeStr), LOGGER);
    int numMessagesSent = 0;
    for (String tableNameWithType : tableNamesWithType) {
      numMessagesSent += _pinotHelixResourceManager.reloadSegment(tableNameWithType, segmentName, false);
    }
    return new SuccessResponse("Sent " + numMessagesSent + " reload messages");
  }

  @Override
  public SuccessResponse reloadSegmentDeprecated2(String tableName, @Encoded String segmentName, String tableTypeStr) {
    return reloadSegmentDeprecated1(tableName, segmentName, tableTypeStr);
  }

  @Override
  public SuccessResponse reloadAllSegments(String tableName, String tableTypeStr, boolean forceDownload) {
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
    Map<String, Integer> numMessagesSentPerTable = new LinkedHashMap<>();
    for (String tableNameWithType : tableNamesWithType) {
      int numMsgSent = _pinotHelixResourceManager.reloadAllSegments(tableNameWithType, forceDownload);
      numMessagesSentPerTable.put(tableNameWithType, numMsgSent);
    }
    return new SuccessResponse("Sent " + numMessagesSentPerTable + " reload messages");
  }

  @Override
  @Deprecated
  public SuccessResponse reloadAllSegmentsDeprecated1(String tableName, String tableTypeStr) {
    List<String> tableNamesWithType = ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName,
        Constants.validateTableType(tableTypeStr), LOGGER);
    int numMessagesSent = 0;
    for (String tableNameWithType : tableNamesWithType) {
      numMessagesSent += _pinotHelixResourceManager.reloadAllSegments(tableNameWithType, false);
    }
    return new SuccessResponse("Sent " + numMessagesSent + " reload messages");
  }

  @Override
  @Deprecated
  public SuccessResponse reloadAllSegmentsDeprecated2(String tableName, String tableTypeStr) {
    return reloadAllSegmentsDeprecated1(tableName, tableTypeStr);
  }

  @Override
  public SuccessResponse deleteSegment(String tableName, String segmentName, String retentionPeriod) {
    segmentName = URIUtils.decode(segmentName);
    TableType tableType = SegmentName.isRealtimeSegmentName(segmentName) ? TableType.REALTIME : TableType.OFFLINE;
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    deleteSegmentsInternal(tableNameWithType, Collections.singletonList(segmentName), retentionPeriod);
    return new SuccessResponse("Segment deleted");
  }

  @Override
  public SuccessResponse deleteAllSegments(String tableName, String tableTypeStr, String retentionPeriod) {
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type must not be null", Status.BAD_REQUEST);
    }
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    deleteSegmentsInternal(tableNameWithType, _pinotHelixResourceManager.getSegmentsFor(tableNameWithType, false),
        retentionPeriod);
    return new SuccessResponse("All segments of table " + tableNameWithType + " deleted");
  }

  @Override
  public SuccessResponse deleteSegments(String tableName, String retentionPeriod, List<String> segments) {
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
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    deleteSegmentsInternal(tableNameWithType, segments, retentionPeriod);
    if (numSegments <= 5) {
      return new SuccessResponse("Deleted segments: " + segments + " from table: " + tableNameWithType);
    } else {
      return new SuccessResponse("Deleted " + numSegments + " segments from table: " + tableNameWithType);
    }
  }

  private void deleteSegmentsInternal(String tableNameWithType, List<String> segments, String retentionPeriod) {
    PinotResourceManagerResponse response =
        _pinotHelixResourceManager.deleteSegments(tableNameWithType, segments, retentionPeriod);
    if (!response.isSuccessful()) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to delete segments from table: " + tableNameWithType + ", error message: " + response.getMessage(),
          Status.INTERNAL_SERVER_ERROR);
    }
  }

  @Override
  public String getServerMetadata(String tableName, String tableTypeStr, List<String> columns) {
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

  @Override
  public List<Map<TableType, List<String>>> getSelectedSegments(String tableName, String tableTypeStr,
      String startTimestampStr, String endTimestampStr, boolean excludeOverlapping) {
    long startTimestamp = Strings.isNullOrEmpty(startTimestampStr) ? Long.MIN_VALUE : Long.parseLong(startTimestampStr);
    long endTimestamp = Strings.isNullOrEmpty(endTimestampStr) ? Long.MAX_VALUE : Long.parseLong(endTimestampStr);
    Preconditions.checkArgument(startTimestamp < endTimestamp,
        "The value of startTimestamp should be smaller than the one of endTimestamp. Start timestamp: %d. End "
            + "timestamp: %d", startTimestamp, endTimestamp);

    List<String> tableNamesWithType = ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName,
        Constants.validateTableType(tableTypeStr), LOGGER);
    List<Map<TableType, List<String>>> resultList = new ArrayList<>(tableNamesWithType.size());
    for (String tableNameWithType : tableNamesWithType) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      List<String> segments =
          _pinotHelixResourceManager.getSegmentsForTableWithTimestamps(tableNameWithType, startTimestamp, endTimestamp,
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
    return tableMetadataReader.getSegmentsMetadata(tableNameWithType, columns,
        _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
  }

  // TODO: Move this API into PinotTableRestletResource
  @Override
  public ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap getConsumingSegmentsInfo(String realtimeTableName) {
    try {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(realtimeTableName);
      if (TableType.OFFLINE == tableType) {
        throw new IllegalStateException("Cannot get consuming segments info for OFFLINE table: " + realtimeTableName);
      }
      String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(realtimeTableName);
      ConsumingSegmentInfoReader consumingSegmentInfoReader =
          new ConsumingSegmentInfoReader(_executor, _connectionManager, _pinotHelixResourceManager);
      return consumingSegmentInfoReader.getConsumingSegmentsInfo(tableNameWithType,
          _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to get consuming segments info for table %s. %s", realtimeTableName, e.getMessage()),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
