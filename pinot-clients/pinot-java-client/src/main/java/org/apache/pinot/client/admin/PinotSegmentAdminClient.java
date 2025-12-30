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
package org.apache.pinot.client.admin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * Client for segment administration operations.
 * Provides methods to manage and query Pinot segments.
 */
public class PinotSegmentAdminClient {

  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  public PinotSegmentAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  /**
   * Lists all segments for a table.
   *
   * @param tableName Name of the table
   * @param excludeReplacedSegments Whether to exclude replaced segments
   * @return List of segment names
   * @throws PinotAdminException If the request fails
   */
  public List<String> listSegments(String tableName, boolean excludeReplacedSegments)
      throws PinotAdminException {
    return listSegments(tableName, null, excludeReplacedSegments);
  }

  /**
   * Lists all segments for a table (including replaced segments).
   *
   * @param tableName Name of the table
   * @return List of segment names
   * @throws PinotAdminException If the request fails
   */
  public List<String> listSegments(String tableName)
      throws PinotAdminException {
    return listSegments(tableName, null, false);
  }

  /**
   * Lists segments for a table with optional table type filtering.
   *
   * @param tableName Name of the table
   * @param tableType Table type (OFFLINE/REALTIME), nullable
   * @param excludeReplacedSegments Whether to exclude replaced segments
   * @return List of segment names
   * @throws PinotAdminException If the request fails
   */
  public List<String> listSegments(String tableName, String tableType, boolean excludeReplacedSegments)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    if (excludeReplacedSegments) {
      queryParams.put("excludeReplacedSegments", "true");
    }

    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableName,
        queryParams.isEmpty() ? null : queryParams, _headers);
    return parseSegmentList(response);
  }

  /**
   * Gets a map from server to segments hosted by the server for a table.
   *
   * @param tableName Name of the table
   * @return Server to segments map
   * @throws PinotAdminException If the request fails
   */
  public String getServerToSegmentsMap(String tableName)
      throws PinotAdminException {
    JsonNode response =
        _transport.executeGet(_controllerAddress, "/segments/" + tableName + "/servers", null, _headers);
    return response.toString();
  }

  /**
   * Lists segment lineage for a table in chronologically sorted order.
   *
   * @param tableName Name of the table
   * @return Segment lineage as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String listSegmentLineage(String tableName)
      throws PinotAdminException {
    return listSegmentLineage(tableName, null);
  }

  /**
   * Lists segment lineage for a table in chronologically sorted order with explicit table type.
   */
  public String listSegmentLineage(String tableName, String tableType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }

    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableName + "/lineage",
        queryParams.isEmpty() ? null : queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets a map from segment to CRC of the segment (only for OFFLINE tables).
   *
   * @param tableName Name of the table
   * @return Segment to CRC map
   * @throws PinotAdminException If the request fails
   */
  public Map<String, String> getSegmentToCrcMap(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableName + "/crc", null, _headers);
    JsonNode crcNode = response.get("segmentCrcMap");
    if (crcNode == null) {
      crcNode = response;
    }
    if (crcNode == null || crcNode.isNull()) {
      return java.util.Collections.emptyMap();
    }
    Map<String, String> crcMap =
        PinotAdminTransport.getObjectMapper().convertValue(crcNode, new TypeReference<Map<String, String>>() {
        });
    return crcMap != null ? crcMap : java.util.Collections.emptyMap();
  }

  /**
   * Gets a map from server to segments hosted by the server for a table.
   *
   * @param tableName Name of the table
   * @param tableType Table type (OFFLINE/REALTIME), nullable
   * @return Server to segments map
   * @throws PinotAdminException If the request fails
   */
  public Map<String, List<String>> getServerToSegmentsMapAsMap(String tableName, String tableType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableName + "/servers",
        queryParams.isEmpty() ? null : queryParams, _headers);

    if (response == null || !response.isArray() || response.isEmpty()) {
      return Collections.emptyMap();
    }

    JsonNode serversMapNode = response.get(0).get("serverToSegmentsMap");
    if (serversMapNode == null || !serversMapNode.isObject()) {
      return Collections.emptyMap();
    }

    Map<String, List<String>> result = new HashMap<>();
    serversMapNode.fields().forEachRemaining(entry -> {
      List<String> segments = new ArrayList<>();
      JsonNode value = entry.getValue();
      if (value != null && value.isArray()) {
        value.forEach(segmentNode -> segments.add(segmentNode.asText()));
      }
      result.put(entry.getKey(), segments);
    });
    return result;
  }

  /**
   * Gets the metadata for a specific segment.
   *
   * @param tableName Name of the table
   * @param segmentName Name of the segment
   * @param columns Specific columns to include (optional)
   * @return Segment metadata
   * @throws PinotAdminException If the request fails
   */
  public Map<String, Object> getSegmentMetadata(String tableName, String segmentName,
      List<String> columns)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (columns != null && !columns.isEmpty()) {
      queryParams.put("columns", String.join(",", columns));
    }

    JsonNode response = _transport.executeGet(_controllerAddress,
        "/segments/" + tableName + "/" + encodePath(segmentName) + "/metadata", queryParams, _headers);
    return PinotAdminTransport.getObjectMapper().convertValue(response,
        new TypeReference<Map<String, Object>>() {
        });
  }

  /**
   * Downloads a segment tarball for the given table and segment.
   *
   * @param tableName Name of the table (with or without type suffix)
   * @param segmentName Name of the segment
   * @return Segment tarball bytes
   * @throws PinotAdminException If the request fails
   */
  public byte[] downloadSegment(String tableName, String segmentName)
      throws PinotAdminException {
    String path = "/segments/" + tableName + "/" + encodePath(segmentName) + "/download";
    return _transport.executeGetBinary(_controllerAddress, path, null, _headers);
  }

  /**
   * Gets metadata for all segments of a table, optionally filtered by table type.
   *
   * @param tableName Name of the table
   * @param tableType Optional table type (OFFLINE/REALTIME)
   * @return Segment metadata map as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getSegmentsMetadata(String tableName, String tableType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableName + "/metadata",
        queryParams.isEmpty() ? null : queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets metadata for all segments of a table with only table type specified.
   */
  public String getSegmentsMetadata(String tableNameWithType)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableNameWithType + "/metadata", null,
        _headers);
    return response.toString();
  }

  /**
   * Gets metadata for all segments of a table with optional column and segment filters.
   *
   * @param tableName Name of the table
   * @param columns Optional list of columns to include
   * @param segments Optional list of segments to include
   * @param tableType Optional table type (OFFLINE/REALTIME)
   * @return Segment metadata map as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getSegmentsMetadata(String tableName, List<String> columns, List<String> segments, String tableType)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (columns != null && !columns.isEmpty()) {
      queryParams.put("columns", String.join(",", columns));
    }
    if (segments != null && !segments.isEmpty()) {
      queryParams.put("segments", String.join(",", segments));
    }
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableName + "/metadata",
        queryParams.isEmpty() ? null : queryParams, _headers);
    return response.toString();
  }

  /**
   * Resets a segment by disabling it, waiting for external view to stabilize, and enabling it again.
   *
   * @param tableNameWithType Table name with type suffix
   * @param segmentName Name of the segment
   * @param targetInstance Target instance to reset (optional)
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String resetSegment(String tableNameWithType, String segmentName, String targetInstance)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (targetInstance != null) {
      queryParams.put("targetInstance", targetInstance);
    }

    JsonNode response = _transport.executePost(_controllerAddress,
        "/segments/" + tableNameWithType + "/" + encodePath(segmentName) + "/reset", null, queryParams, _headers);
    return response.toString();
  }

  /**
   * Resets all segments or error segments only for a table.
   *
   * @param tableNameWithType Table name with type suffix
   * @param errorSegmentsOnly Whether to reset only error segments
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String resetSegments(String tableNameWithType, boolean errorSegmentsOnly)
      throws PinotAdminException {
    return resetSegments(tableNameWithType, errorSegmentsOnly, null);
  }

  /**
   * Resets all segments or error segments only for a table, optionally scoped to a target instance.
   */
  public String resetSegments(String tableNameWithType, boolean errorSegmentsOnly, String targetInstance)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("errorSegmentsOnly", String.valueOf(errorSegmentsOnly));
    if (targetInstance != null) {
      queryParams.put("targetInstance", targetInstance);
    }

    JsonNode response = _transport.executePost(_controllerAddress, "/segments/" + tableNameWithType + "/reset",
        null, queryParams, _headers);
    return response.toString();
  }

  /**
   * Deletes a specific segment.
   *
   * @param tableName Name of the table
   * @param segmentName Name of the segment
   * @param retentionPeriod Retention period for the segment (optional)
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String deleteSegment(String tableName, String segmentName, String retentionPeriod)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (retentionPeriod != null) {
      queryParams.put("retention", retentionPeriod);
    }

    JsonNode response = _transport.executeDelete(_controllerAddress,
        "/segments/" + tableName + "/" + encodePath(segmentName), queryParams, _headers);
    return response.toString();
  }

  /**
   * Deletes multiple segments specified in query parameters or all segments if none specified.
   *
   * @param tableName Name of the table
   * @param segmentNames Comma-separated list of segment names to delete (optional)
   * @param retentionPeriod Retention period for the segments (optional)
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String deleteMultipleSegments(String tableName, String segmentNames,
      String retentionPeriod)
      throws PinotAdminException {
    return deleteMultipleSegments(tableName, null, segmentNames, retentionPeriod);
  }

  /**
   * Deletes multiple segments specified in query parameters or all segments if none specified, with explicit table
   * type support.
   */
  public String deleteMultipleSegments(String tableName, String tableType, String segmentNames,
      String retentionPeriod)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType == null) {
      org.apache.pinot.spi.config.table.TableType inferred = org.apache.pinot.spi.utils.builder.TableNameBuilder
          .getTableTypeFromTableName(tableName);
      if (inferred != null) {
        tableType = inferred.name();
      }
    }
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    if (segmentNames != null) {
      queryParams.put("segments", segmentNames);
      queryParams.put("segmentNames", segmentNames); // backward compatibility
    }
    if (retentionPeriod != null) {
      queryParams.put("retention", retentionPeriod);
    }

    JsonNode response = _transport.executeDelete(_controllerAddress, "/segments/" + tableName,
        queryParams.isEmpty() ? null : queryParams, _headers);
    return response.toString();
  }

  public String deleteMultipleSegments(String tableName, String tableType, List<String> segments,
      String retentionPeriod)
      throws PinotAdminException {
    String segmentNames = (segments == null || segments.isEmpty()) ? null : String.join(",", segments);
    return deleteMultipleSegments(tableName, tableType, segmentNames, retentionPeriod);
  }

  /**
   * Deletes segments specified in JSON array payload.
   *
   * @param tableName Name of the table
   * @param segmentDeleteRequest Segment delete request as JSON string
   * @return Success response
   * @throws PinotAdminException If the request fails
   */
  public String deleteSegments(String tableName, String segmentDeleteRequest)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/segments/" + tableName + "/delete",
        segmentDeleteRequest, null, _headers);
    return response.toString();
  }

  /**
   * Deletes segments within a time window.
   */
  public String deleteSegmentsByTimeWindow(String tableName, String tableType, long startTimestampMs,
      long endTimestampMs, boolean excludeOverlapping, boolean excludeReplacedSegments, String retentionPeriod)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("startTimestamp", String.valueOf(startTimestampMs));
    queryParams.put("endTimestamp", String.valueOf(endTimestampMs));
    queryParams.put("excludeOverlapping", String.valueOf(excludeOverlapping));
    queryParams.put("excludeReplacedSegments", String.valueOf(excludeReplacedSegments));
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    if (retentionPeriod != null) {
      queryParams.put("retention", retentionPeriod);
    }

    JsonNode response = _transport.executeDelete(_controllerAddress, "/segments/" + tableName + "/choose",
        queryParams, _headers);
    return response.toString();
  }

  public String deleteSegmentsByTimeWindow(String tableName, long startTimestampMs, long endTimestampMs)
      throws PinotAdminException {
    return deleteSegmentsByTimeWindow(tableName, null, startTimestampMs, endTimestampMs, true, false, null);
  }

  /**
   * Selects segments based on time range criteria.
   *
   * @param tableName Name of the table
   * @param startTimestampMs Start timestamp in milliseconds (inclusive)
   * @param endTimestampMs End timestamp in milliseconds (exclusive)
   * @param excludeReplacedSegments Whether to exclude replaced segments
   * @return Selected segments as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String selectSegments(String tableName, long startTimestampMs, long endTimestampMs,
      boolean excludeReplacedSegments)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("startTimestampMs", String.valueOf(startTimestampMs));
    queryParams.put("endTimestampMs", String.valueOf(endTimestampMs));
    queryParams.put("excludeReplacedSegments", String.valueOf(excludeReplacedSegments));

    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableName + "/select",
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets server metadata for all table segments.
   *
   * @param tableName Name of the table
   * @return Server metadata as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getServerMetadata(String tableName)
      throws PinotAdminException {
    JsonNode response =
        _transport.executeGet(_controllerAddress, "/segments/" + tableName + "/metadata", null, _headers);
    return response.toString();
  }

  /**
   * Reloads all segments for a table.
   *
   * @param tableName Name of the table
   * @param tableType Table type (OFFLINE/REALTIME)
   * @param forceDownload Whether to force download of segments
   * @return Reload response
   * @throws PinotAdminException If the request fails
   */
  public String reloadTable(String tableName, String tableType, boolean forceDownload)
      throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    queryParams.put("forceDownload", String.valueOf(forceDownload));

    JsonNode response = _transport.executePost(_controllerAddress, "/segments/" + tableName + "/reload", null,
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Checks whether a reload is needed for a table.
   *
   * @param tableNameWithType Table name with type suffix
   * @param verbose Whether to return verbose information
   * @return Reload status response
   * @throws PinotAdminException If the request fails
   */
  public String checkIfReloadIsNeeded(String tableNameWithType, boolean verbose)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("verbose", String.valueOf(verbose));
    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableNameWithType + "/needReload",
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Reloads a specific segment for a table.
   *
   * @param tableName Name of the table
   * @param segmentName Name of the segment
   * @param forceDownload Whether to force download
   * @return Reload response
   * @throws PinotAdminException If the request fails
   */
  public String reloadSegment(String tableName, String segmentName, boolean forceDownload)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("forceDownload", String.valueOf(forceDownload));

    JsonNode response = _transport.executePost(_controllerAddress,
        "/segments/" + tableName + "/" + encodePath(segmentName) + "/reload", null, queryParams, _headers);
    return response.toString();
  }

  /**
   * Gets the status of a segment reload job.
   *
   * @param jobId Job id returned from the reload request
   * @return Reload status response
   * @throws PinotAdminException If the request fails
   */
  public String getSegmentReloadStatus(String jobId)
      throws PinotAdminException {
    JsonNode response =
        _transport.executeGet(_controllerAddress, "/segments/segmentReloadStatus/" + jobId, null, _headers);
    return response.toString();
  }

  /**
   * Gets a list of segments that are stale from servers hosting the table.
   *
   * @param tableNameWithType Table name with type suffix
   * @return Stale segments response
   * @throws PinotAdminException If the request fails
   */
  public String getStaleSegments(String tableNameWithType)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableNameWithType + "/isStale",
        null, _headers);
    return response.toString();
  }

  /**
   * Gets the Zookeeper metadata for all table segments.
   *
   * @param tableName Name of the table
   * @return Zookeeper metadata
   * @throws PinotAdminException If the request fails
   */
  public Map<String, Map<String, String>> getZookeeperMetadata(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableName + "/zkmetadata",
        null, _headers);
    return PinotAdminTransport.getObjectMapper().convertValue(response.get("zkMetadata"),
        new TypeReference<Map<String, Map<String, String>>>() {
        });
  }

  /**
   * Gets storage tier for all segments in the given table.
   *
   * @param tableName Name of the table
   * @return Storage tiers as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getStorageTiers(String tableName)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/segments/" + tableName + "/tiers", null, _headers);
    return response.toString();
  }

  /**
   * Gets storage tiers for a specific segment.
   *
   * @param tableName Name of the table
   * @param segmentName Name of the segment
   * @param tableType Table type (OFFLINE or REALTIME)
   * @return Storage tiers as JSON string
   * @throws PinotAdminException If the request fails
   */
  public String getSegmentStorageTiers(String tableName, String segmentName, String tableType)
      throws PinotAdminException {
    Map<String, String> queryParams = Map.of("type", tableType);

    JsonNode response = _transport.executeGet(_controllerAddress,
        "/segments/" + tableName + "/" + encodePath(segmentName) + "/tiers", queryParams, _headers);
    return response.toString();
  }

  // Async versions of key methods

  /**
   * Lists all segments for a table (async).
   */
  public CompletableFuture<List<String>> listSegmentsAsync(String tableName, boolean excludeReplacedSegments) {
    Map<String, String> queryParams = Map.of("excludeReplacedSegments", String.valueOf(excludeReplacedSegments));

    return _transport.executeGetAsync(_controllerAddress, "/segments/" + tableName, queryParams, _headers)
        .thenApply(this::parseSegmentList);
  }

  /**
   * Gets the metadata for a specific segment (async).
   */
  public CompletableFuture<Map<String, Object>> getSegmentMetadataAsync(String tableName, String segmentName,
      List<String> columns) {
    Map<String, String> queryParams = new HashMap<>();
    if (columns != null && !columns.isEmpty()) {
      queryParams.put("columns", String.join(",", columns));
    }

    return _transport.executeGetAsync(_controllerAddress,
            "/segments/" + tableName + "/" + encodePath(segmentName) + "/metadata", queryParams, _headers)
        .thenApply(response -> PinotAdminTransport.getObjectMapper().convertValue(response,
            new TypeReference<Map<String, Object>>() {
            }));
  }

  /**
   * Deletes a specific segment (async).
   */
  public CompletableFuture<String> deleteSegmentAsync(String tableName, String segmentName,
      String retentionPeriod) {
    Map<String, String> queryParams = new HashMap<>();
    if (retentionPeriod != null) {
      queryParams.put("retention", retentionPeriod);
    }

    return _transport.executeDeleteAsync(_controllerAddress, "/segments/" + tableName + "/" + encodePath(segmentName),
            queryParams, _headers)
        .thenApply(JsonNode::toString);
  }

  /**
   * Parses the segment list from the controller response, handling both legacy and current formats.
   */
  private List<String> parseSegmentList(JsonNode response) {
    if (response == null || response.isNull()) {
      return Collections.emptyList();
    }

    if (response.has("segments")) {
      return _transport.parseStringArraySafe(response, "segments");
    }

    if (response.isArray()) {
      List<String> segments = new ArrayList<>();
      for (JsonNode tableNode : response) {
        if (tableNode != null && tableNode.isObject()) {
          tableNode.elements().forEachRemaining(value -> {
            if (value != null && value.isArray()) {
              value.forEach(segmentNode -> segments.add(segmentNode.asText()));
            }
          });
        }
      }
      return segments;
    }

    return Collections.emptyList();
  }

  private static String encodePath(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
