package org.apache.pinot.controller.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
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
import org.apache.pinot.controller.api.resources.ServerReloadControllerJobStatusResponse;
import org.apache.pinot.controller.api.resources.SuccessResponse;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.controller.util.TableMetadataReader;
import org.apache.pinot.segment.spi.creator.name.SegmentNameUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
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
      String targetInstance, String instanceToSegmentsMapInJson, HttpHeaders headers)
      throws IOException {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
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
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableTypeFromRequest, LOG);
    if (instanceToSegmentsMapInJson != null) {
      Map<String, List<String>> instanceToSegmentsMap =
          JsonUtils.stringToObject(instanceToSegmentsMapInJson, new TypeReference<>() {
          });
      Map<String, Map<String, Map<String, String>>> tableInstanceMsgData =
          reloadSegments(tableNamesWithType, forceDownload, instanceToSegmentsMap);
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

  public ServerReloadControllerJobStatusResponse getReloadJobStatus(String reloadJobId)
      throws InvalidConfigException {
    Map<String, String> controllerJobZKMetadata =
        _pinotHelixResourceManager.getControllerJobZKMetadata(reloadJobId, ControllerJobTypes.RELOAD_SEGMENT);
    if (controllerJobZKMetadata == null) {
      throw new ControllerApplicationException(LOG, "Failed to find controller job id: " + reloadJobId,
          Response.Status.NOT_FOUND);
    }

    String tableNameWithType = controllerJobZKMetadata.get(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE);
    String segmentNames = controllerJobZKMetadata.get(CommonConstants.ControllerJob.SEGMENT_RELOAD_JOB_SEGMENT_NAME);
    String instanceName = controllerJobZKMetadata.get(CommonConstants.ControllerJob.SEGMENT_RELOAD_JOB_INSTANCE_NAME);
    Map<String, List<String>> serverToSegments = getServerToSegments(tableNameWithType, segmentNames, instanceName);

    BiMap<String, String> serverEndPoints =
        _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, serverEndPoints);

    List<String> serverUrls = new ArrayList<>();
    for (Map.Entry<String, String> entry : serverEndPoints.entrySet()) {
      String server = entry.getKey();
      String endpoint = entry.getValue();
      String reloadTaskStatusEndpoint =
          endpoint + "/controllerJob/reloadStatus/" + tableNameWithType + "?reloadJobTimestamp="
              + controllerJobZKMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS);
      if (segmentNames != null) {
        List<String> segmentsForServer = serverToSegments.get(server);
        StringBuilder encodedSegmentsBuilder = new StringBuilder();
        if (!segmentsForServer.isEmpty()) {
          Iterator<String> segmentIterator = segmentsForServer.iterator();
          // Append first segment without a leading separator
          encodedSegmentsBuilder.append(URIUtils.encode(segmentIterator.next()));
          // Append remaining segments, each prefixed by the separator
          while (segmentIterator.hasNext()) {
            encodedSegmentsBuilder.append(SegmentNameUtils.SEGMENT_NAME_SEPARATOR)
                .append(URIUtils.encode(segmentIterator.next()));
          }
        }
        reloadTaskStatusEndpoint += "&segmentName=" + encodedSegmentsBuilder;
      }
      serverUrls.add(reloadTaskStatusEndpoint);
    }

    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, null, true, 10000);

    ServerReloadControllerJobStatusResponse serverReloadControllerJobStatusResponse =
        new ServerReloadControllerJobStatusResponse();
    serverReloadControllerJobStatusResponse.setSuccessCount(0);

    int totalSegments = 0;
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      totalSegments += entry.getValue().size();
    }
    serverReloadControllerJobStatusResponse.setTotalSegmentCount(totalSegments);
    serverReloadControllerJobStatusResponse.setTotalServersQueried(serverUrls.size());
    serverReloadControllerJobStatusResponse.setTotalServerCallsFailed(serviceResponse._failedResponseCount);

    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      String responseString = streamResponse.getValue();
      try {
        ServerReloadControllerJobStatusResponse response =
            JsonUtils.stringToObject(responseString, ServerReloadControllerJobStatusResponse.class);
        serverReloadControllerJobStatusResponse.setSuccessCount(
            serverReloadControllerJobStatusResponse.getSuccessCount() + response.getSuccessCount());
      } catch (Exception e) {
        serverReloadControllerJobStatusResponse.setTotalServerCallsFailed(
            serverReloadControllerJobStatusResponse.getTotalServerCallsFailed() + 1);
      }
    }

    // Add ZK fields
    serverReloadControllerJobStatusResponse.setMetadata(controllerJobZKMetadata);

    // Add derived fields
    long submissionTime = Long.parseLong(controllerJobZKMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS));
    double timeElapsedInMinutes = ((double) System.currentTimeMillis() - (double) submissionTime) / (1000.0 * 60.0);
    int remainingSegments = serverReloadControllerJobStatusResponse.getTotalSegmentCount()
        - serverReloadControllerJobStatusResponse.getSuccessCount();

    double estimatedRemainingTimeInMinutes = -1;
    if (serverReloadControllerJobStatusResponse.getSuccessCount() > 0) {
      estimatedRemainingTimeInMinutes =
          ((double) remainingSegments / (double) serverReloadControllerJobStatusResponse.getSuccessCount())
              * timeElapsedInMinutes;
    }

    serverReloadControllerJobStatusResponse.setTimeElapsedInMinutes(timeElapsedInMinutes);
    serverReloadControllerJobStatusResponse.setEstimatedTimeRemainingInMinutes(estimatedRemainingTimeInMinutes);

    return serverReloadControllerJobStatusResponse;
  }

  public String getTableReloadMetadata(String tableNameWithType, boolean verbose, HttpHeaders headers) {
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

  @VisibleForTesting
  Map<String, List<String>> getServerToSegments(String tableNameWithType, @Nullable String segmentNames,
      @Nullable String instanceName) {
    if (segmentNames == null) {
      // instanceName can be null or not null, and this method below can handle both cases.
      return _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType, instanceName, true);
    }
    // Skip servers and segments not involved in the segment reloading job.
    List<String> segmnetNameList = new ArrayList<>();
    Collections.addAll(segmnetNameList, StringUtils.split(segmentNames, SegmentNameUtils.SEGMENT_NAME_SEPARATOR));
    if (instanceName != null) {
      return Map.of(instanceName, segmnetNameList);
    }
    // If instance is null, then either one or all segments are being reloaded via current segment reload restful APIs.
    // And the if-check at the beginning of this method has handled the case of reloading all segments. So here we
    // expect only one segment name.
    Preconditions.checkState(segmnetNameList.size() == 1, "Only one segment is expected but got: %s", segmnetNameList);
    Map<String, List<String>> serverToSegments = new HashMap<>();
    Set<String> servers = _pinotHelixResourceManager.getServers(tableNameWithType, segmentNames);
    for (String server : servers) {
      serverToSegments.put(server, Collections.singletonList(segmentNames));
    }
    return serverToSegments;
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

  private Map<String, Map<String, Map<String, String>>> reloadSegments(List<String> tableNamesWithType,
      boolean forceDownload, Map<String, List<String>> instanceToSegmentsMap) {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Map<String, Map<String, String>>> tableInstanceMsgData = new LinkedHashMap<>();
    for (String tableNameWithType : tableNamesWithType) {
      Map<String, Pair<Integer, String>> instanceMsgInfoMap =
          _pinotHelixResourceManager.reloadSegments(tableNameWithType, forceDownload, instanceToSegmentsMap);
      Map<String, Map<String, String>> instanceMsgData =
          tableInstanceMsgData.computeIfAbsent(tableNameWithType, t -> new HashMap<>());
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
    }
    return tableInstanceMsgData;
  }
}
