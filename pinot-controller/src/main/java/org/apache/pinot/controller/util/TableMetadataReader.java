package org.apache.pinot.controller.util;

import com.google.common.collect.BiMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.controller.api.resources.ServerSegmentMetadataReader;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableMetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableMetadataReader.class);

  private final Executor executor;
  private final HttpConnectionManager connectionManager;
  private final PinotHelixResourceManager pinotHelixResourceManager;

  public TableMetadataReader(Executor executor, HttpConnectionManager connectionManager,
                             PinotHelixResourceManager helixResourceManager) {
    this.executor = executor;
    this.connectionManager = connectionManager;
    this.pinotHelixResourceManager = helixResourceManager;
  }

  public ServerSegmentMetadataReader.TableReloadStatus getReloadStatus(String tableNameWithType, Map<String, List<String>> serverToSegmentsMap,
                                                                       int timeoutMs)
          throws InvalidConfigException {
    BiMap<String, String> endpoints = pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegmentsMap.keySet());
    ServerSegmentMetadataReader serverSegmentMetadataReader = new ServerSegmentMetadataReader(executor, connectionManager);

    return serverSegmentMetadataReader.getSegmentReloadTime(tableNameWithType, serverToSegmentsMap, endpoints, timeoutMs);
  }

  public Map<String, String> getSegmentsMetadata(String tableNameWithType, int timeoutMs) throws InvalidConfigException {
    final Map<String, List<String>> serversToSegmentsMap =
            pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType);
    BiMap<String, String> endpoints = pinotHelixResourceManager.getDataInstanceAdminEndpoints(serversToSegmentsMap.keySet());
    ServerSegmentMetadataReader serverSegmentMetadataReader = new ServerSegmentMetadataReader(executor, connectionManager);

    return serverSegmentMetadataReader.getSegmentMetadata(tableNameWithType, serversToSegmentsMap, endpoints, timeoutMs);
  }

}
