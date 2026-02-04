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
package org.apache.pinot.controller.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.ServerSegmentsReloadCheckResponse;
import org.apache.pinot.common.restlet.resources.TableSegmentsReloadCheckResponse;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.UploadedRealtimeSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.resources.Constants;
import org.apache.pinot.controller.api.resources.ResourceUtils;
import org.apache.pinot.controller.api.resources.SuccessResponse;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableMetadataReader;
import org.apache.pinot.segment.spi.creator.name.SegmentNameUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class PinotTableReloadService {

  private static final Logger LOG = LoggerFactory.getLogger(PinotTableReloadService.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ControllerConf _controllerConf;
  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;

  @Inject
  public PinotTableReloadService(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf controllerConf,
      Executor executor, HttpClientConnectionManager connectionManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerConf = controllerConf;
    _executor = executor;
    _connectionManager = connectionManager;
  }

  public SuccessResponse reloadSegment(String tableName, String segmentName, boolean forceDownload,
      String targetInstance, HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    long startTimeMs = System.currentTimeMillis();
    segmentName = URIUtils.decode(segmentName);
    String tableNameWithType = getExistingTable(tableName, segmentName);
    Pair<Integer, String> msgInfo =
        _pinotHelixResourceManager.reloadSegment(tableNameWithType, segmentName, forceDownload, targetInstance);
    boolean zkJobMetaWriteSuccess = false;
    int numReloadMsgSent = msgInfo.getLeft();
    if (numReloadMsgSent > 0) {
      try {
        if (_pinotHelixResourceManager.addNewReloadSegmentJob(tableNameWithType, segmentName, targetInstance,
            msgInfo.getRight(), startTimeMs, numReloadMsgSent)) {
          zkJobMetaWriteSuccess = true;
        } else {
          LOG.error("Failed to add reload segment job meta into zookeeper for table: {}, segment: {}",
              tableNameWithType, segmentName);
        }
      } catch (Exception e) {
        LOG.error("Failed to add reload segment job meta into zookeeper for table: {}, segment: {}",
            tableNameWithType, segmentName, e);
      }
      return new SuccessResponse(
          String.format("Submitted reload job id: %s, sent %d reload messages. Job meta ZK storage status: %s",
              msgInfo.getRight(), numReloadMsgSent, zkJobMetaWriteSuccess ? "SUCCESS" : "FAILED"));
    }
    throw new ControllerApplicationException(LOG,
        String.format("Failed to find segment: %s in table: %s on %s", segmentName, tableName,
            targetInstance == null ? "every instance" : targetInstance), Response.Status.NOT_FOUND);
  }

  public SuccessResponse reloadAllSegments(String tableName, String tableTypeStr, boolean forceDownload,
      String targetInstance, String instanceToSegmentsMapInJson, String startTimestampStr, String endTimestampStr,
      boolean excludeOverlapping, HttpHeaders headers)
      throws IOException {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableType tableTypeFromTableName = TableNameBuilder.getTableTypeFromTableName(tableName);
    TableType tableTypeFromRequest = Constants.validateTableType(tableTypeStr);
    // When rawTableName is provided but without table type, Pinot tries to reload both OFFLINE
    // and REALTIME tables for the raw table. But forceDownload option only works with
    // OFFLINE table currently, so we limit the table type to OFFLINE to let Pinot continue
    // to reload without being accidentally aborted upon REALTIME table type.
    // TODO: support to force download immutable segments from RealTime table.
    if (forceDownload && (tableTypeFromTableName == null && tableTypeFromRequest == null)) {
      tableTypeFromRequest = TableType.OFFLINE;
    }
    boolean hasStartTimestamp = !Strings.isNullOrEmpty(startTimestampStr);
    boolean hasEndTimestamp = !Strings.isNullOrEmpty(endTimestampStr);
    if (hasStartTimestamp || hasEndTimestamp) {
      if (!(hasStartTimestamp && hasEndTimestamp)) {
        throw new ControllerApplicationException(LOG, "startTimestamp and endTimestamp must be provided together.",
            Response.Status.BAD_REQUEST);
      }
      if (targetInstance != null || instanceToSegmentsMapInJson != null) {
        throw new ControllerApplicationException(LOG,
            "startTimestamp/endTimestamp/excludeOverlapping cannot be used with targetInstance/instanceToSegmentsMap.",
            Response.Status.BAD_REQUEST);
      }
      return reloadSegmentsInTimeRange(tableName, tableTypeStr, startTimestampStr, endTimestampStr,
          excludeOverlapping, forceDownload, null, headers);
    } else if (excludeOverlapping) {
      throw new ControllerApplicationException(LOG,
          "excludeOverlapping can only be used together with startTimestamp/endTimestamp.",
          Response.Status.BAD_REQUEST);
    }
    List<String> tableNamesWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableTypeFromRequest, LOG);
    if (instanceToSegmentsMapInJson != null) {
      Map<String, List<String>> instanceToSegmentsMap =
          JsonUtils.stringToObject(instanceToSegmentsMapInJson, new TypeReference<>() {
          });
      Map<String, Map<String, Map<String, String>>> tableInstanceMsgData = new LinkedHashMap<>();
      for (String tableNameWithType : tableNamesWithType) {
        Map<String, Map<String, String>> instanceMsgData =
            reloadSegmentsForTable(tableNameWithType, forceDownload, instanceToSegmentsMap);
        if (!instanceMsgData.isEmpty()) {
          tableInstanceMsgData.put(tableNameWithType, instanceMsgData);
        }
      }
      if (tableInstanceMsgData.isEmpty()) {
        throw new ControllerApplicationException(LOG,
            String.format("Failed to find any segments in table: %s with instanceToSegmentsMap: %s", tableName,
                instanceToSegmentsMap), Response.Status.NOT_FOUND);
      }
      return new SuccessResponse(JsonUtils.objectToString(tableInstanceMsgData));
    }
    long startTimeMs = System.currentTimeMillis();
    Map<String, Map<String, String>> perTableMsgData = new LinkedHashMap<>();
    for (String tableNameWithType : tableNamesWithType) {
      Pair<Integer, String> msgInfo =
          _pinotHelixResourceManager.reloadAllSegments(tableNameWithType, forceDownload, targetInstance);
      int numReloadMsgSent = msgInfo.getLeft();
      if (numReloadMsgSent <= 0) {
        continue;
      }
      Map<String, String> tableReloadMeta = new HashMap<>();
      tableReloadMeta.put("numMessagesSent", String.valueOf(numReloadMsgSent));
      tableReloadMeta.put("reloadJobId", msgInfo.getRight());
      perTableMsgData.put(tableNameWithType, tableReloadMeta);
      // Store in ZK
      try {
        if (_pinotHelixResourceManager.addNewReloadAllSegmentsJob(tableNameWithType, targetInstance, msgInfo.getRight(),
            startTimeMs, numReloadMsgSent)) {
          tableReloadMeta.put("reloadJobMetaZKStorageStatus", "SUCCESS");
        } else {
          tableReloadMeta.put("reloadJobMetaZKStorageStatus", "FAILED");
          LOG.error("Failed to add reload all segments job meta into zookeeper for table: {}", tableNameWithType);
        }
      } catch (Exception e) {
        tableReloadMeta.put("reloadJobMetaZKStorageStatus", "FAILED");
        LOG.error("Failed to add reload all segments job meta into zookeeper for table: {}", tableNameWithType, e);
      }
    }
    if (perTableMsgData.isEmpty()) {
      throw new ControllerApplicationException(LOG,
          String.format("Failed to find any segments in table: %s on %s", tableName,
              targetInstance == null ? "every instance" : targetInstance), Response.Status.NOT_FOUND);
    }
    return new SuccessResponse(JsonUtils.objectToString(perTableMsgData));
  }

  public SuccessResponse reloadSegmentsInTimeRange(String tableName, String tableTypeStr, String startTimestampStr,
      String endTimestampStr, boolean excludeOverlapping, boolean forceDownload, @Nullable String targetInstance,
      HttpHeaders headers) {
    if (Strings.isNullOrEmpty(startTimestampStr) || Strings.isNullOrEmpty(endTimestampStr)) {
      throw new ControllerApplicationException(LOG, "startTimestamp and endTimestamp must be provided.",
          Response.Status.BAD_REQUEST);
    }
    long startTimestamp;
    long endTimestamp;
    try {
      startTimestamp = Long.parseLong(startTimestampStr);
      endTimestamp = Long.parseLong(endTimestampStr);
    } catch (NumberFormatException e) {
      throw new ControllerApplicationException(LOG,
          "Failed to parse the start/end timestamp. Please make sure they are in 'milliseconds since epoch' format.",
          Response.Status.BAD_REQUEST, e);
    }
    if (startTimestamp >= endTimestamp) {
      throw new ControllerApplicationException(LOG, String.format(
          "startTimestamp must be less than endTimestamp. Provided: start=%d, end=%d", startTimestamp, endTimestamp),
          Response.Status.BAD_REQUEST);
    }

    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableType tableTypeFromTableName = TableNameBuilder.getTableTypeFromTableName(tableName);
    TableType tableTypeFromRequest = Constants.validateTableType(tableTypeStr);
    // When rawTableName is provided but without table type, Pinot tries to reload both OFFLINE
    // and REALTIME tables for the raw table. But forceDownload option only works with
    // OFFLINE table currently, so we limit the table type to OFFLINE to let Pinot continue
    // to reload without being accidentally aborted upon REALTIME table type.
    // TODO: support to force download immutable segments from RealTime table.
    if (forceDownload && (tableTypeFromTableName == null && tableTypeFromRequest == null)) {
      tableTypeFromRequest = TableType.OFFLINE;
    }
    List<String> tableNamesWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableTypeFromRequest, LOG);
    Map<String, Map<String, String>> perTableMsgData = new LinkedHashMap<>();
    for (String tableNameWithType : tableNamesWithType) {
      List<String> segments =
          _pinotHelixResourceManager.getSegmentsFor(tableNameWithType, true, startTimestamp, endTimestamp,
              excludeOverlapping);
      if (segments.isEmpty()) {
        continue;
      }
      Set<String> selectedSegments = new HashSet<>(segments);
      Map<String, List<String>> serverToSegmentsMap =
          _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType, targetInstance, false);
      Map<String, List<String>> filteredInstanceToSegmentsMap = new HashMap<>();
      for (Map.Entry<String, List<String>> entry : serverToSegmentsMap.entrySet()) {
        List<String> instanceSegments =
            entry.getValue().stream().filter(selectedSegments::contains).collect(Collectors.toList());
        if (!instanceSegments.isEmpty()) {
          filteredInstanceToSegmentsMap.put(entry.getKey(), instanceSegments);
        }
      }
      if (filteredInstanceToSegmentsMap.isEmpty()) {
        continue;
      }
      String reloadJobId = UUID.randomUUID().toString();
      long startTimeMs = System.currentTimeMillis();
      Map<String, Pair<Integer, String>> instanceMsgInfoMap =
          _pinotHelixResourceManager.reloadSegments(tableNameWithType, forceDownload, filteredInstanceToSegmentsMap,
              reloadJobId);
      int numReloadMsgSent = instanceMsgInfoMap.values().stream().mapToInt(Pair::getLeft).sum();
      if (numReloadMsgSent <= 0) {
        continue;
      }
      List<String> segmentsToReload = filteredInstanceToSegmentsMap.values().stream()
          .flatMap(List::stream)
          .distinct()
          .sorted()
          .collect(Collectors.toList());
      String segmentNamesStr =
          StringUtils.join(segmentsToReload, SegmentNameUtils.SEGMENT_NAME_SEPARATOR);
      Map<String, String> tableReloadMeta = new HashMap<>();
      tableReloadMeta.put("numMessagesSent", String.valueOf(numReloadMsgSent));
      tableReloadMeta.put("reloadJobId", reloadJobId);
      perTableMsgData.put(tableNameWithType, tableReloadMeta);
      try {
        if (_pinotHelixResourceManager.addNewReloadSegmentJob(tableNameWithType, segmentNamesStr, targetInstance,
            reloadJobId, startTimeMs, numReloadMsgSent)) {
          tableReloadMeta.put("reloadJobMetaZKStorageStatus", "SUCCESS");
        } else {
          tableReloadMeta.put("reloadJobMetaZKStorageStatus", "FAILED");
          LOG.error("Failed to add reload segments job meta into zookeeper for table: {}", tableNameWithType);
        }
      } catch (Exception e) {
        tableReloadMeta.put("reloadJobMetaZKStorageStatus", "FAILED");
        LOG.error("Failed to add reload segments job meta into zookeeper for table: {}", tableNameWithType, e);
      }
    }
    if (perTableMsgData.isEmpty()) {
      throw new ControllerApplicationException(LOG, String.format(
          "Failed to find any segments in table: %s in the time range [%s, %s) on %s", tableName, startTimestampStr,
          endTimestampStr, targetInstance == null ? "every instance" : targetInstance), Response.Status.NOT_FOUND);
    }
    try {
      return new SuccessResponse(JsonUtils.objectToString(perTableMsgData));
    } catch (IOException e) {
      throw new ControllerApplicationException(LOG, "Failed to encode reload response",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }


  public String needReload(String tableNameWithType, boolean verbose, HttpHeaders headers) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    LOG.info("Received a request to check reload for all servers hosting segments for table {}", tableNameWithType);
    try {
      TableMetadataReader tableMetadataReader =
          new TableMetadataReader(_executor, _connectionManager, _pinotHelixResourceManager);
      Map<String, JsonNode> needReloadMetadata =
          tableMetadataReader.getServerCheckSegmentsReloadMetadata(tableNameWithType,
              _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000).getServerReloadJsonResponses();
      boolean needReload =
          needReloadMetadata.values().stream().anyMatch(value -> value.get("needReload").booleanValue());
      Map<String, ServerSegmentsReloadCheckResponse> serverResponses = new HashMap<>();
      TableSegmentsReloadCheckResponse tableNeedReloadResponse;
      if (verbose) {
        for (Map.Entry<String, JsonNode> entry : needReloadMetadata.entrySet()) {
          serverResponses.put(entry.getKey(),
              new ServerSegmentsReloadCheckResponse(entry.getValue().get("needReload").booleanValue(),
                  entry.getValue().get("instanceId").asText()));
        }
        tableNeedReloadResponse = new TableSegmentsReloadCheckResponse(needReload, serverResponses);
      } else {
        tableNeedReloadResponse = new TableSegmentsReloadCheckResponse(needReload, serverResponses);
      }
      return JsonUtils.objectToPrettyString(tableNeedReloadResponse);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOG, e.getMessage(), Response.Status.BAD_REQUEST);
    } catch (IOException ioe) {
      throw new ControllerApplicationException(LOG, "Error parsing Pinot server response: " + ioe.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, ioe);
    }
  }


  /**
   * Helper method to find the existing table based on the given table name (with or without type suffix) and segment
   * name.
   */
  private String getExistingTable(String tableName, String segmentName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      // Derive table type from segment name if the given table name doesn't have type suffix
      tableType = LLCSegmentName.isLLCSegment(segmentName) ? TableType.REALTIME
          : (UploadedRealtimeSegmentName.isUploadedRealtimeSegmentName(segmentName) ? TableType.REALTIME
              : TableType.OFFLINE);
    }
    return ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOG).get(0);
  }

  private Map<String, Map<String, String>> reloadSegmentsForTable(String tableNameWithType, boolean forceDownload,
      Map<String, List<String>> instanceToSegmentsMap) {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Pair<Integer, String>> instanceMsgInfoMap =
        _pinotHelixResourceManager.reloadSegments(tableNameWithType, forceDownload, instanceToSegmentsMap);
    Map<String, Map<String, String>> instanceMsgData = new HashMap<>();
    for (Map.Entry<String, Pair<Integer, String>> instanceMsgInfo : instanceMsgInfoMap.entrySet()) {
      String instance = instanceMsgInfo.getKey();
      Pair<Integer, String> msgInfo = instanceMsgInfo.getValue();
      int numReloadMsgSent = msgInfo.getLeft();
      if (numReloadMsgSent <= 0) {
        continue;
      }
      Map<String, String> tableReloadMeta = new HashMap<>();
      tableReloadMeta.put("numMessagesSent", String.valueOf(numReloadMsgSent));
      tableReloadMeta.put("reloadJobId", msgInfo.getRight());
      instanceMsgData.put(instance, tableReloadMeta);
      // Store in ZK
      try {
        String segmentNames =
            StringUtils.join(instanceToSegmentsMap.get(instance), SegmentNameUtils.SEGMENT_NAME_SEPARATOR);
        if (_pinotHelixResourceManager.addNewReloadSegmentJob(tableNameWithType, segmentNames, instance,
            msgInfo.getRight(), startTimeMs, numReloadMsgSent)) {
          tableReloadMeta.put("reloadJobMetaZKStorageStatus", "SUCCESS");
        } else {
          tableReloadMeta.put("reloadJobMetaZKStorageStatus", "FAILED");
          LOG.error("Failed to add batch reload job meta into zookeeper for table: {} targeted instance: {}",
              tableNameWithType, instance);
        }
      } catch (Exception e) {
        tableReloadMeta.put("reloadJobMetaZKStorageStatus", "FAILED");
        LOG.error("Failed to add batch reload job meta into zookeeper for table: {} targeted instance: {}",
            tableNameWithType, instance, e);
      }
    }
    return instanceMsgData;
  }
}
