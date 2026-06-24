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
package org.apache.pinot.controller.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.CompressionStatsSummary;
import org.apache.pinot.common.restlet.resources.StorageBreakdownInfo;
import org.apache.pinot.common.restlet.resources.TableMetadataInfo;
import org.apache.pinot.common.restlet.resources.TableSegments;
import org.apache.pinot.common.restlet.resources.ValidDocIdsBitmapResponse;
import org.apache.pinot.common.restlet.resources.ValidDocIdsMetadataInfo;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.controller.api.resources.TableStaleSegmentResponse;
import org.apache.pinot.segment.local.data.manager.StaleSegment;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.UrlBuilderUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a helper class that calls the server API endpoints to fetch server metadata and the segment reload status
 * Only the servers returning success are returned by the method. For servers returning errors (http error or
 * otherwise),
 * no entry is created in the return list
 */
public class ServerSegmentMetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerSegmentMetadataReader.class);
  private static final String COLUMNS_KEY = "columns";
  private static final String SEGMENTS_KEY = "segments";

  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;

  public ServerSegmentMetadataReader() {
    _executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    _connectionManager = new PoolingHttpClientConnectionManager();
  }

  public ServerSegmentMetadataReader(Executor executor, HttpClientConnectionManager connectionManager) {
    _executor = executor;
    _connectionManager = connectionManager;
  }

  /**
   * This method is called when the API request is to fetch aggregated segment metadata for all segments of the table.
   * This method makes a MultiGet call to all servers that host their respective segments and gets the results.
   * This method accept a list of column names as filter, and will return column metadata for the column in the
   * list.
   * TODO Some performance improvement ideas to explore:
   * - If table has replica groups, only send requests to one replica group.
   * - If table does not have replica groups, send requests to a minimal set of servers hosting all segments of the
   *   table.
   */
  public TableMetadataInfo getAggregatedTableMetadataFromServer(String tableNameWithType,
      BiMap<String, String> serverEndPoints, List<String> columns, int numReplica, int timeoutMs,
      boolean compressionStatsEnabled) {
    int numServers = serverEndPoints.size();
    LOGGER.info("Reading aggregated segment metadata from {} servers for table: {} with timeout: {}ms", numServers,
        tableNameWithType, timeoutMs);

    List<String> serverUrls = new ArrayList<>(numServers);
    BiMap<String, String> endpointsToServers = serverEndPoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String serverUrl =
          generateAggregateSegmentMetadataServerURL(tableNameWithType, columns, endpoint, compressionStatsEnabled);
      serverUrls.add(serverUrl);
    }

    // Helper service to run a http get call to the server
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, tableNameWithType, false, timeoutMs);

    long totalDiskSizeInBytes = 0;
    int totalNumSegments = 0;
    long totalNumRows = 0;
    int failedParses = 0;
    final Map<String, Double> columnLengthMap = new HashMap<>();
    final Map<String, Double> columnCardinalityMap = new HashMap<>();
    final Map<String, Double> maxNumMultiValuesMap = new HashMap<>();
    final Map<String, Map<String, Double>> columnIndexSizeMap = new HashMap<>();
    final Map<Integer, Map<String, Long>> partitionToServerPrimaryKeyCountMap = new HashMap<>();
    // Per-column compression stats accumulators: [0]=uncompressed, [1]=compressed
    final Map<String, long[]> columnCompressionAccum = new HashMap<>();
    final Map<String, String> columnCodecMap = new HashMap<>();
    // Secondary per-codec breakdown accumulators: column → codec → [rawIngest, onDisk, segmentCount]
    final Map<String, Map<String, long[]>> columnCodecBreakdownAccum = new HashMap<>();
    final Map<String, Set<String>> columnIndexNamesMap = new HashMap<>();
    long aggRawSize = 0;
    long aggCompressedSize = 0;
    int aggSegmentsWithStats = 0;
    int aggTotalSegments = 0;
    boolean hasCompressionSummary = false;
    final Map<String, long[]> tierAccum = new HashMap<>(); // [count, size]
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        TableMetadataInfo tableMetadataInfo =
            JsonUtils.stringToObject(streamResponse.getValue(), TableMetadataInfo.class);
        totalDiskSizeInBytes += tableMetadataInfo.getDiskSizeInBytes();
        totalNumRows += tableMetadataInfo.getNumRows();
        totalNumSegments += tableMetadataInfo.getNumSegments();
        tableMetadataInfo.getColumnLengthMap().forEach((k, v) -> columnLengthMap.merge(k, v, Double::sum));
        tableMetadataInfo.getColumnCardinalityMap().forEach((k, v) -> columnCardinalityMap.merge(k, v, Double::sum));
        tableMetadataInfo.getMaxNumMultiValuesMap().forEach((k, v) -> maxNumMultiValuesMap.merge(k, v, Double::sum));
        tableMetadataInfo.getColumnIndexSizeMap().forEach((k, v) -> columnIndexSizeMap.merge(k, v, (l, r) -> {
          for (Map.Entry<String, Double> e : r.entrySet()) {
            l.put(e.getKey(), l.getOrDefault(e.getKey(), 0d) + e.getValue());
          }
          return l;
        }));
        tableMetadataInfo.getPartitionToServerPrimaryKeyCountMap().forEach(
            (partition, serverToPrimaryKeyCount) -> partitionToServerPrimaryKeyCountMap.merge(partition,
                new HashMap<>(serverToPrimaryKeyCount), (l, r) -> {
                  for (Map.Entry<String, Long> serverToPKCount : r.entrySet()) {
                    l.merge(serverToPKCount.getKey(), serverToPKCount.getValue(), Long::sum);
                  }
                  return l;
                }));
        // Aggregate per-column compression stats from server responses
        List<ColumnCompressionStatsInfo> serverColStats = tableMetadataInfo.getColumnCompressionStats();
        if (serverColStats != null) {
          for (ColumnCompressionStatsInfo info : serverColStats) {
            // Skip columns with no codec — these are old raw segments built before compression stats
            // tracking was enabled and carry no meaningful data.
            if (info.getCodec() == null) {
              continue;
            }
            String col = info.getColumn();
            long[] accum = columnCompressionAccum.computeIfAbsent(col, k -> new long[2]);
            // Only accumulate uncompressed size when it is a real value (not the -1 sentinel from dict columns)
            if (info.getRawIngestSizeInBytes() >= 0) {
              accum[0] += info.getRawIngestSizeInBytes();
            }
            accum[1] += info.getOnDiskSizeInBytes();
            if (info.getCodec() != null) {
              columnCodecMap.merge(col, info.getCodec(),
                  (existing, incoming) -> existing.equals(incoming) ? existing : "MIXED");
            }
            if (info.getIndexes() != null) {
              columnIndexNamesMap.computeIfAbsent(col, k -> new HashSet<>()).addAll(info.getIndexes());
            }
            // Aggregate per-codec breakdown from server info
            if (info.getCodecBreakdown() != null) {
              Map<String, long[]> localBreakdown =
                  columnCodecBreakdownAccum.computeIfAbsent(col, k -> new HashMap<>());
              for (Map.Entry<String, ColumnCompressionStatsInfo.CodecBreakdownEntry> bdEntry
                  : info.getCodecBreakdown().entrySet()) {
                long[] bdAccum = localBreakdown.computeIfAbsent(bdEntry.getKey(), k -> new long[3]);
                bdAccum[0] += bdEntry.getValue().getRawIngestSizeInBytes();
                bdAccum[1] += bdEntry.getValue().getOnDiskSizeInBytes();
                bdAccum[2] += bdEntry.getValue().getSegments();
              }
            }
          }
        }
        // Aggregate compressionStats summary (sum raw/compressed across servers)
        CompressionStatsSummary serverSummary = tableMetadataInfo.getCompressionStats();
        if (serverSummary != null) {
          aggRawSize += serverSummary.getRawIngestSizePerReplicaInBytes();
          aggCompressedSize += serverSummary.getOnDiskSizePerReplicaInBytes();
          aggSegmentsWithStats += serverSummary.getSegmentsWithStats();
          aggTotalSegments += serverSummary.getTotalSegments();
          hasCompressionSummary = true;
        }
        // Aggregate storageBreakdown (sum counts and sizes per tier)
        StorageBreakdownInfo serverBreakdown = tableMetadataInfo.getStorageBreakdown();
        if (serverBreakdown != null && serverBreakdown.getTiers() != null) {
          for (Map.Entry<String, StorageBreakdownInfo.TierInfo> tierEntry
              : serverBreakdown.getTiers().entrySet()) {
            long[] vals = tierAccum.computeIfAbsent(tierEntry.getKey(), k -> new long[2]);
            vals[0] += tierEntry.getValue().getCount();
            vals[1] += tierEntry.getValue().getSizePerReplicaInBytes();
          }
        }
      } catch (IOException e) {
        failedParses++;
        LOGGER.error("Unable to parse server {} response due to an error: ", streamResponse.getKey(), e);
      }
    }
    int finalTotalNumSegments = totalNumSegments;
    columnLengthMap.replaceAll((k, v) -> v / finalTotalNumSegments);
    columnCardinalityMap.replaceAll((k, v) -> v / finalTotalNumSegments);
    maxNumMultiValuesMap.replaceAll((k, v) -> v / finalTotalNumSegments);
    columnIndexSizeMap.replaceAll((k, v) -> {
      v.replaceAll((key, value) -> v.get(key) / finalTotalNumSegments);
      return v;
    });

    // Since table segments may have multiple replicas, divide diskSizeInBytes, numRows and numSegments by numReplica
    // to avoid double counting, for columnAvgLengthMap, columnAvgCardinalityMap and maxNumMultiValuesMap, dividing by
    // numReplica is not needed since totalNumSegments already contains replicas.
    totalDiskSizeInBytes /= numReplica;
    totalNumSegments /= numReplica;
    totalNumRows /= numReplica;

    // Build per-column compression stats list (divide by numReplica since each replica reports the same stats)
    List<ColumnCompressionStatsInfo> columnCompressionStats = null;
    if (!columnCompressionAccum.isEmpty()) {
      columnCompressionStats = new ArrayList<>();
      for (Map.Entry<String, long[]> entry : columnCompressionAccum.entrySet()) {
        String col = entry.getKey();
        long[] accum = entry.getValue();
        String colCodec = columnCodecMap.get(col);
        // Dict-only columns have no uncompressed size; preserve -1 sentinel instead of dividing 0
        long uncompressed = (ColumnCompressionStatsInfo.CODEC_DICT_ENCODED.equals(colCodec)
            && accum[0] == 0) ? -1 : accum[0] / numReplica;
        long compressed = accum[1] / numReplica;
        double ratio = (uncompressed > 0 && compressed > 0) ? (double) uncompressed / compressed : 0;
        Set<String> idxNames = columnIndexNamesMap.get(col);
        List<String> indexes = idxNames != null ? new ArrayList<>(idxNames) : null;
        // Build codecBreakdown only when codec is MIXED; divide sizes by numReplica
        Map<String, ColumnCompressionStatsInfo.CodecBreakdownEntry> codecBreakdown = null;
        if ("MIXED".equals(colCodec)) {
          Map<String, long[]> bdAccum = columnCodecBreakdownAccum.get(col);
          if (bdAccum != null) {
            codecBreakdown = new HashMap<>();
            for (Map.Entry<String, long[]> bdEntry : bdAccum.entrySet()) {
              long[] bd = bdEntry.getValue();
              codecBreakdown.put(bdEntry.getKey(), new ColumnCompressionStatsInfo.CodecBreakdownEntry(
                  (int) (bd[2] / numReplica), bd[0] / numReplica, bd[1] / numReplica));
            }
          }
        }
        columnCompressionStats.add(new ColumnCompressionStatsInfo(
            col, uncompressed, compressed, ratio, colCodec, indexes, codecBreakdown));
      }
      columnCompressionStats.sort((a, b) -> a.getColumn().compareTo(b.getColumn()));
    }

    // Build aggregated compression summary (divide by numReplica to avoid double counting)
    CompressionStatsSummary compressionStatsSummary = null;
    if (hasCompressionSummary) {
      long rawPerReplica = aggRawSize / numReplica;
      long compressedPerReplica = aggCompressedSize / numReplica;
      double ratio = (rawPerReplica > 0 && compressedPerReplica > 0)
          ? (double) rawPerReplica / compressedPerReplica : 0;
      int segmentsWithStats = aggSegmentsWithStats / numReplica;
      int totalSegments = aggTotalSegments / numReplica;
      if (segmentsWithStats > 0) {
        boolean isPartialCoverage = segmentsWithStats < totalSegments;
        compressionStatsSummary = new CompressionStatsSummary(rawPerReplica, compressedPerReplica, ratio,
            segmentsWithStats, totalSegments, isPartialCoverage);
      }
    }

    // Build aggregated storage breakdown (divide by numReplica to avoid double counting)
    StorageBreakdownInfo storageBreakdownInfo = null;
    if (!tierAccum.isEmpty()) {
      Map<String, StorageBreakdownInfo.TierInfo> tiers = new HashMap<>();
      for (Map.Entry<String, long[]> entry : tierAccum.entrySet()) {
        int count = (int) (entry.getValue()[0] / numReplica);
        long size = entry.getValue()[1] / numReplica;
        tiers.put(entry.getKey(), new StorageBreakdownInfo.TierInfo(count, size));
      }
      storageBreakdownInfo = new StorageBreakdownInfo(tiers);
    }

    // When compression stats flag is OFF, suppress compressionStats and columnCompressionStats
    // but always keep storageBreakdown (tier breakdown is independent of the compression stats flag)
    if (!compressionStatsEnabled) {
      columnCompressionStats = null;
      compressionStatsSummary = null;
    }

    TableMetadataInfo aggregateTableMetadataInfo =
        new TableMetadataInfo(tableNameWithType, totalDiskSizeInBytes, totalNumSegments, totalNumRows, columnLengthMap,
            columnCardinalityMap, maxNumMultiValuesMap, columnIndexSizeMap, partitionToServerPrimaryKeyCountMap,
            columnCompressionStats, compressionStatsSummary, storageBreakdownInfo);
    if (failedParses != 0) {
      LOGGER.warn("Failed to parse {} / {} aggregated segment metadata responses from servers.", failedParses,
          serverUrls.size());
    }
    return aggregateTableMetadataInfo;
  }

  /**
   * This method is called when the API request is to fetch segment metadata for all segments of the table.
   * This method makes a MultiGet call to all servers that host their respective segments and gets the results.
   * This method accept a list of column names as filter, and will return column metadata for the column in the
   * list.
   * @return list of segments and their metadata as a JSON string
   */
  public List<String> getSegmentMetadataFromServer(String tableNameWithType,
      Map<String, List<String>> serversToSegmentsMap, BiMap<String, String> endpoints, List<String> columns,
      int timeoutMs) {
    LOGGER.debug("Reading segment metadata from servers for table {}.", tableNameWithType);
    List<String> serverURLs = new ArrayList<>();
    for (Map.Entry<String, List<String>> serverToSegments : serversToSegmentsMap.entrySet()) {
      List<String> segments = serverToSegments.getValue();
      for (String segment : segments) {
        serverURLs.add(generateSegmentMetadataServerURL(tableNameWithType, segment, columns,
            endpoints.get(serverToSegments.getKey())));
      }
    }
    BiMap<String, String> endpointsToServers = endpoints.inverse();
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverURLs, tableNameWithType, true, timeoutMs);
    List<String> segmentsMetadata = new ArrayList<>();

    int failedParses = 0;
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        String segmentMetadata = streamResponse.getValue();
        segmentsMetadata.add(segmentMetadata);
      } catch (Exception e) {
        failedParses++;
        LOGGER.error("Unable to parse server {} response due to an error: ", streamResponse.getKey(), e);
      }
    }
    if (failedParses != 0) {
      LOGGER.error("Unable to parse server {} / {} response due to an error: ", failedParses, serverURLs.size());
    }

    LOGGER.debug("Retrieved segment metadata from servers.");
    return segmentsMetadata;
  }

  /**
   * This method is called when the API request is to fetch data about segment reload of the table.
   * This method makes a MultiGet call to all servers that host their respective segments and gets the results.
   * This method will return metadata of all the servers along with need reload flag.
   * In future additional details like segments list can also be added
   */
  public TableReloadResponse getCheckReloadSegmentsFromServer(String tableNameWithType,
      Set<String> serverInstances, BiMap<String, String> endpoints, int timeoutMs) {
    LOGGER.debug("Checking if reload is needed on segments from servers for table {}.", tableNameWithType);
    List<String> serverURLs = new ArrayList<>();
    for (String serverInstance : serverInstances) {
      serverURLs.add(generateCheckReloadSegmentsServerURL(tableNameWithType, endpoints.get(serverInstance)));
    }
    BiMap<String, String> endpointsToServers = endpoints.inverse();
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverURLs, tableNameWithType, true, timeoutMs);
    List<String> serversNeedReloadResponses = new ArrayList<>();

    int failedParses = 0;
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        serversNeedReloadResponses.add(streamResponse.getValue());
      } catch (Exception e) {
        failedParses++;
        LOGGER.error("Unable to parse server {} response due to an error: ", streamResponse.getKey(), e);
      }
    }
    if (failedParses != 0) {
      LOGGER.error("Unable to parse server {} / {} response due to an error: ", failedParses, serverURLs.size());
    }

    LOGGER.debug("Retrieved metadata of reload check from servers.");
    return new TableReloadResponse(serviceResponse._failedResponseCount, serversNeedReloadResponses);
  }

  /**
   * This method is called when the API request is to fetch validDocId metadata for a list segments of the given table.
   * This method will pick one server randomly that hosts the target segment and fetch the segment metadata result.
   *
   * @return list of valid doc id metadata, one per segment processed.
   */
  public List<ValidDocIdsMetadataInfo> getValidDocIdsMetadataFromServer(String tableNameWithType,
      Map<String, List<String>> serverToSegmentsMap, BiMap<String, String> serverToEndpoints,
      @Nullable List<String> segmentNames, int timeoutMs, String validDocIdsType,
      int numSegmentsBatchPerServerRequest) {
    return getSegmentToValidDocIdsMetadataFromServer(tableNameWithType, serverToSegmentsMap, serverToEndpoints,
        segmentNames, timeoutMs, validDocIdsType, numSegmentsBatchPerServerRequest).values().stream()
        .filter(list -> list != null && !list.isEmpty()).map(list -> list.get(0)).collect(Collectors.toList());
  }

  /**
   * This method is called when the API request is to fetch validDocId metadata for a list segments of the given table.
   * This method will pick all servers that hosts the target segment and fetch the segment metadata result and
   * return as a list.
   *
   * @return map of segment name to list of valid doc id metadata where each element is every server's metadata.
   */
  public Map<String, List<ValidDocIdsMetadataInfo>> getSegmentToValidDocIdsMetadataFromServer(String tableNameWithType,
      Map<String, List<String>> serverToSegmentsMap, BiMap<String, String> serverToEndpoints,
      @Nullable List<String> segmentNames, int timeoutMs, String validDocIdsType,
      int numSegmentsBatchPerServerRequest) {
    List<Pair<String, String>> serverURLsAndBodies = new ArrayList<>();
    for (Map.Entry<String, List<String>> serverToSegments : serverToSegmentsMap.entrySet()) {
      List<String> segmentsForServer = serverToSegments.getValue();
      List<String> segmentsToQuery = new ArrayList<>();
      if (segmentNames == null || segmentNames.isEmpty()) {
        segmentsToQuery.addAll(segmentsForServer);
      } else {
        Set<String> segmentNamesLookUpTable = new HashSet<>(segmentNames);
        for (String segment : segmentsForServer) {
          if (segmentNamesLookUpTable.contains(segment)) {
            segmentsToQuery.add(segment);
          }
        }
      }

      // Number of segments to query per server request. If a table has a lot of segments, then we might send a
      // huge payload to pinot-server in request. Batching the requests will help in reducing the payload size.
      Lists.partition(segmentsToQuery, numSegmentsBatchPerServerRequest).forEach(segmentsToQueryBatch ->
          serverURLsAndBodies.add(generateValidDocIdsMetadataURL(tableNameWithType, segmentsToQueryBatch,
              validDocIdsType, serverToEndpoints.get(serverToSegments.getKey()))));
    }

    BiMap<String, String> endpointsToServers = serverToEndpoints.inverse();

    // request the urls from the servers
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);

    Map<String, String> requestHeaders = Map.of("Content-Type", "application/json");
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiPostRequest(serverURLsAndBodies, tableNameWithType, true, requestHeaders,
            timeoutMs, null);

    Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataInfos = new HashMap<>();
    int failedParses = 0;
    int returnedServerRequestsCount = 0;
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        String validDocIdsMetadataList = streamResponse.getValue();
        List<ValidDocIdsMetadataInfo> validDocIdsMetadataInfoList =
            JsonUtils.stringToObject(validDocIdsMetadataList, new TypeReference<ArrayList<ValidDocIdsMetadataInfo>>() {
            });
        for (ValidDocIdsMetadataInfo validDocIdsMetadataInfo : validDocIdsMetadataInfoList) {
          validDocIdsMetadataInfos.computeIfAbsent(validDocIdsMetadataInfo.getSegmentName(), k -> new ArrayList<>())
              .add(validDocIdsMetadataInfo);
        }
        returnedServerRequestsCount++;
      } catch (Exception e) {
        failedParses++;
        LOGGER.error("Unable to parse {} server-request response due to an error: ", streamResponse.getKey(), e);
      }
    }

    if (failedParses != 0) {
      LOGGER.error("Unable to parse {} / {} server-request responses due to an error: ", failedParses,
          serverURLsAndBodies.size());
    }

    if (returnedServerRequestsCount != serverURLsAndBodies.size()) {
      LOGGER.error("Unable to get validDocIdsMetadata from all server requests. Expected: {}, Actual: {}",
          serverURLsAndBodies.size(), returnedServerRequestsCount);
    }

    if (segmentNames != null && !segmentNames.isEmpty() && segmentNames.size() != validDocIdsMetadataInfos.size()) {
      LOGGER.error("Unable to get validDocIdsMetadata for all segments. Expected: {}, Actual: {}", segmentNames.size(),
          validDocIdsMetadataInfos.size());
    }

    LOGGER.info("Retrieved validDocIds metadata for {} segments from {} server requests.",
        validDocIdsMetadataInfos.size(), returnedServerRequestsCount);
    return validDocIdsMetadataInfos;
  }

  /**
   * This method is called when the API request is to fetch validDocIds for a segment of the given table. This method
   * will pick a server that hosts the target segment and fetch the validDocIds result.
   *
   * @return a bitmap of validDocIds
   */
  @Deprecated
  public RoaringBitmap getValidDocIdsFromServer(String tableNameWithType, String segmentName, String validDocIdsType,
      String endpoint, int timeoutMs) {
    // Build the endpoint url
    String url = generateValidDocIdsURL(tableNameWithType, segmentName, validDocIdsType, endpoint);

    // Set timeout
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.property(ClientProperties.CONNECT_TIMEOUT, timeoutMs);
    clientConfig.property(ClientProperties.READ_TIMEOUT, timeoutMs);

    Response response = ClientBuilder.newClient(clientConfig).target(url).request().get(Response.class);
    Preconditions.checkState(response.getStatus() == Response.Status.OK.getStatusCode(),
        "Unable to retrieve validDocIds from %s", url);
    byte[] validDocIds = response.readEntity(byte[].class);
    return RoaringBitmapUtils.deserialize(validDocIds);
  }

  /**
   * This method is called when the API request is to fetch validDocIds for a segment of the given table. This method
   * will pick a server that hosts the target segment and fetch the validDocIds result.
   *
   * @return a bitmap of validDocIds
   */
  public ValidDocIdsBitmapResponse getValidDocIdsBitmapFromServer(String tableNameWithType, String segmentName,
      String endpoint, String validDocIdsType, int timeoutMs) {
    // Build the endpoint url
    String url = generateValidDocIdsBitmapURL(tableNameWithType, segmentName, validDocIdsType, endpoint);

    // Set timeout
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.property(ClientProperties.CONNECT_TIMEOUT, timeoutMs);
    clientConfig.property(ClientProperties.READ_TIMEOUT, timeoutMs);

    ValidDocIdsBitmapResponse response =
        ClientBuilder.newClient(clientConfig).target(url).request(MediaType.APPLICATION_JSON)
            .get(ValidDocIdsBitmapResponse.class);
    Preconditions.checkNotNull(response, "Unable to retrieve validDocIdsBitmap from %s", url);
    return response;
  }

  public Map<String, TableStaleSegmentResponse> getStaleSegmentsFromServer(
      String tableNameWithType, Set<String> serverInstances, BiMap<String, String> endpoints, int timeoutMs) {
    LOGGER.debug("Getting list of segments for refresh from servers for table {}.", tableNameWithType);
    List<String> serverURLs = new ArrayList<>();
    for (String serverInstance : serverInstances) {
      serverURLs.add(generateStaleSegmentsServerURL(tableNameWithType, endpoints.get(serverInstance)));
    }
    BiMap<String, String> endpointsToServers = endpoints.inverse();
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverURLs, tableNameWithType, false, timeoutMs);
    Map<String, TableStaleSegmentResponse> serverResponses = new HashMap<>();

    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        List<StaleSegment> staleSegments = JsonUtils.stringToObject(streamResponse.getValue(),
            new TypeReference<List<StaleSegment>>() { });
        serverResponses.put(streamResponse.getKey(), new TableStaleSegmentResponse(staleSegments));
      } catch (Exception e) {
        serverResponses.put(streamResponse.getKey(), new TableStaleSegmentResponse(e.getMessage()));
        LOGGER.error("Unable to parse server {} response for needRefresh for table {} due to an error: ",
            streamResponse.getKey(), tableNameWithType, e);
      }
    }
    return serverResponses;
  }

  private String generateAggregateSegmentMetadataServerURL(String tableNameWithType, @Nullable List<String> columns,
      String endpoint, boolean includeColumnStats) {
    tableNameWithType = encode(tableNameWithType);
    String columnsParam = UrlBuilderUtils.generateColumnsParam(columns);
    String url = String.format("%s/tables/%s/metadata", endpoint, tableNameWithType);
    StringBuilder sb = new StringBuilder(url);
    if (columnsParam != null) {
      sb.append("?").append(columnsParam);
      if (includeColumnStats) {
        sb.append("&includeColumnStats=true");
      }
    } else if (includeColumnStats) {
      sb.append("?includeColumnStats=true");
    }
    return sb.toString();
  }

  public String generateSegmentMetadataServerURL(String tableNameWithType, String segmentName,
      @Nullable List<String> columns, String endpoint) {
    tableNameWithType = encode(tableNameWithType);
    segmentName = UrlBuilderUtils.encode(segmentName);
    String columnsParam = UrlBuilderUtils.generateColumnsParam(columns);
    String url = String.format("%s/tables/%s/segments/%s/metadata", endpoint, tableNameWithType, segmentName);
    return columnsParam != null ? url + "?" + columnsParam : url;
  }

  public String generateTableMetadataServerURL(String tableNameWithType, @Nullable List<String> columns,
      @Nullable List<String> segments, String endpoint) {
    tableNameWithType = encode(tableNameWithType);
    String columnsAndSegmentsParam = UrlBuilderUtils.generateColumnsAndSegmentsParam(columns, segments);
    String url = String.format("%s/tables/%s/segments/metadata", endpoint, tableNameWithType);
    return columnsAndSegmentsParam != null ? url + "?" + columnsAndSegmentsParam : url;
  }

  private String generateCheckReloadSegmentsServerURL(String tableNameWithType, String endpoint) {
    tableNameWithType = encode(tableNameWithType);
    return String.format("%s/tables/%s/segments/needReload", endpoint, tableNameWithType);
  }

  @Deprecated
  private String generateValidDocIdsURL(String tableNameWithType, String segmentName, String validDocIdsType,
      String endpoint) {
    tableNameWithType = encode(tableNameWithType);
    segmentName = encode(segmentName);
    String url = String.format("%s/segments/%s/%s/validDocIds", endpoint, tableNameWithType, segmentName);
    if (validDocIdsType != null) {
      url = url + "?validDocIdsType=" + validDocIdsType;
    }
    return url;
  }

  private String generateValidDocIdsBitmapURL(String tableNameWithType, String segmentName, String validDocIdsType,
      String endpoint) {
    tableNameWithType = encode(tableNameWithType);
    segmentName = encode(segmentName);
    String url = String.format("%s/segments/%s/%s/validDocIdsBitmap", endpoint, tableNameWithType, segmentName);
    return validDocIdsType != null ? url + "?validDocIdsType=" + validDocIdsType : url;
  }

  private Pair<String, String> generateValidDocIdsMetadataURL(String tableNameWithType, List<String> segmentNames,
      String validDocIdsType, String endpoint) {
    tableNameWithType = encode(tableNameWithType);
    TableSegments tableSegments = new TableSegments(segmentNames);
    String jsonTableSegments;
    try {
      jsonTableSegments = JsonUtils.objectToString(tableSegments);
    } catch (JsonProcessingException e) {
      LOGGER.error("Failed to convert segment names to json request body: segmentNames={}", segmentNames);
      throw new RuntimeException(e);
    }
    String url = String.format("%s/tables/%s/validDocIdsMetadata", endpoint, tableNameWithType);
    if (validDocIdsType != null) {
      url = url + "?validDocIdsType=" + validDocIdsType;
    }
    return Pair.of(url, jsonTableSegments);
  }

  private String generateStaleSegmentsServerURL(String tableNameWithType, String endpoint) {
    tableNameWithType = encode(tableNameWithType);
    return String.format("%s/tables/%s/segments/isStale", endpoint, tableNameWithType);
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  public static class TableReloadResponse {
    private final int _numFailedResponses;
    private final List<String> _serverReloadResponses;

    private TableReloadResponse(int numFailedResponses, List<String> serverReloadResponses) {
      _numFailedResponses = numFailedResponses;
      _serverReloadResponses = serverReloadResponses;
    }

    public int getNumFailedResponses() {
      return _numFailedResponses;
    }

    public List<String> getServerReloadResponses() {
      return _serverReloadResponses;
    }
  }
}
