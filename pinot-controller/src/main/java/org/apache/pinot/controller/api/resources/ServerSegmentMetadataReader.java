package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.pinot.common.http.MultiGetRequest;
import org.apache.pinot.common.restlet.resources.SegmentStatus;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerSegmentMetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerSegmentMetadataReader.class);

  private final Executor _executor;
  private final HttpConnectionManager _connectionManager;

  public ServerSegmentMetadataReader(Executor executor, HttpConnectionManager connectionManager) {
    _executor = executor;
    _connectionManager = connectionManager;
  }

  public Map<String, String> getSegmentMetadata(String tableNameWithType,
                                                Map<String, List<String>> serversToSegmentsMap,
                                                BiMap<String, String> endpoints, int timeoutMs) {
    LOGGER.info("Reading segment metadata from servers for table {}.", tableNameWithType);
    List<String> serverURLs = new ArrayList<>();
    for (Map.Entry<String, List<String>> serverToSegments : serversToSegmentsMap.entrySet()) {
      List<String> segments = serverToSegments.getValue();
      for (String segment : segments) {
        serverURLs.add(generateSegmentMetadataServerURL(tableNameWithType, segment, endpoints.get(serverToSegments.getKey())));
      }
    }
    CompletionService<GetMethod> completionService =
            new MultiGetRequest(_executor, _connectionManager).execute(serverURLs, timeoutMs);
    Map<String, String> segmentsMetadata = new HashMap<>();

    BiMap<String, String> endpointsToServers = endpoints.inverse();
    for (int i = 0; i < serverURLs.size(); i++) {
      GetMethod getMethod = null;
      try {
        getMethod = completionService.take().get();
        URI uri = getMethod.getURI();
        String instance = endpointsToServers.get(uri.getHost() + ":" + uri.getPort());
        if (getMethod.getStatusCode() >= 300) {
          LOGGER.error("Server: {} returned error: {}", instance, getMethod.getStatusCode());
          continue;
        }
        String segmentMetadata =
                JsonUtils.inputStreamToObject(getMethod.getResponseBodyAsStream(), String.class);
        segmentsMetadata.put(instance, segmentMetadata);
      } catch (Exception e) {
        // Ignore individual exceptions because the exception has been logged in MultiGetRequest
        // Log the number of failed servers after gathering all responses
      } finally {
        if (Objects.nonNull(getMethod)) {
          getMethod.releaseConnection();
        }
      }
    }
    return segmentsMetadata;
  }

  private String generateSegmentMetadataServerURL(String tableNameWithType, String segmentName, String endpoint) {
    return "http://" + endpoint + "/tables/" + tableNameWithType + "/segments/" + segmentName + "/metadata";
  }

  private String generateReloadStatusServerURL(String tableNameWithType, String segmentName, String endpoint) {
    return "http://" + endpoint + "/tables/" + tableNameWithType + "/segments/" + segmentName + "/reload-status";
  }

  public TableReloadStatus getSegmentReloadTime(String tableNameWithType,
                                                Map<String, List<String>> serversToSegmentsMap,
                                                BiMap<String, String> endpoints, int timeoutMs) {
    LOGGER.info("Reading segment reload status from servers for table {}.", tableNameWithType);
    List<String> serverURLs = new ArrayList<>();
    for (Map.Entry<String, List<String>> serverToSegments : serversToSegmentsMap.entrySet()) {
      List<String> segments = serverToSegments.getValue();
      for (String segment : segments) {
        serverURLs.add(generateSegmentMetadataServerURL(tableNameWithType, segment, endpoints.get(serverToSegments.getKey())));
      }
    }
    CompletionService<GetMethod> completionService =
            new MultiGetRequest(_executor, _connectionManager).execute(serverURLs, timeoutMs);
    BiMap<String, String> endpointsToServers = endpoints.inverse();
    TableReloadStatus tableReloadStatus = new TableReloadStatus();
    tableReloadStatus._tableName = tableNameWithType;

    for (int i = 0; i < serverURLs.size(); i++) {
      GetMethod getMethod = null;
      try {
        getMethod = completionService.take().get();
        URI uri = getMethod.getURI();
        String instance = endpointsToServers.get(uri.getHost() + ":" + uri.getPort());
        if (getMethod.getStatusCode() >= 300) {
          LOGGER.error("Server: {} returned error: {}", instance, getMethod.getStatusCode());
          continue;
        }
        SegmentStatus segmentStatus = JsonUtils.inputStreamToObject(getMethod.getResponseBodyAsStream(), SegmentStatus.class);
        tableReloadStatus._segmentStatus.add(segmentStatus);
      } catch (Exception e) {
        // Ignore individual exceptions because the exception has been logged in MultiGetRequest
        // Log the number of failed servers after gathering all responses
      } finally {
        if (Objects.nonNull(getMethod)) {
          getMethod.releaseConnection();
        }
      }
    }
    return tableReloadStatus;
  }

  private String parseSegmentName(String uriPath) {
    Preconditions.checkNotNull(uriPath, "Segment reload status URI path cannot be null!");
    String[] uriSplit = uriPath.split("//");
    return uriSplit[uriSplit.length - 2];
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public class TableReloadStatus {
    String _tableName;
    List<SegmentStatus> _segmentStatus;
  }
}
