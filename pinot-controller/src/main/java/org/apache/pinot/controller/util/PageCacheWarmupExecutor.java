package org.apache.pinot.controller.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.HttpVersion;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.PageCacheWarmupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TablePageCacheWarmupRequest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PageCacheWarmupExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageCacheWarmupExecutor.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ControllerMetrics _controllerMetrics;
  private final String _pageCacheWarmupQueriesDataDir;

  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 2.0f);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public PageCacheWarmupExecutor(PinotHelixResourceManager pinotHelixResourceManager,
                                 ControllerMetrics controllerMetrics,
                                 String pageCacheWarmupQueriesDataDir) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerMetrics = controllerMetrics;
    _pageCacheWarmupQueriesDataDir = pageCacheWarmupQueriesDataDir;
  }

  public void triggerPageCacheWarmup(String tableNameWithType, List<String> segmentsTo) {
    String rawTableName = null;
    try {
      rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      assert tableType == TableType.OFFLINE;
      TableConfig tableConfig =  _pinotHelixResourceManager.getOfflineTableConfig(rawTableName);
      if (tableConfig == null) {
        return;
      }
      PageCacheWarmupConfig pageCacheWarmupConfig = tableConfig.getPageCacheWarmupConfig();
      if (pageCacheWarmupConfig == null || !pageCacheWarmupConfig.isRefreshEnabled()) {
        return;
      }

      long maxWarmupDurationMs = pageCacheWarmupConfig.getMaxWarmupDurationSeconds() * 1000L;
      final String finalRawTableName = rawTableName;
      CompletableFuture<Void> warmupFuture = CompletableFuture.runAsync(() -> {
        LOGGER.info("Starting page cache warmup for table: {}, maxWarmupDurationMs: {}", finalRawTableName,
            maxWarmupDurationMs);
        _controllerMetrics.addMeteredTableValue(finalRawTableName, ControllerMeter.PAGE_CACHE_WARMUP_REQUESTS, 1);
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.PAGE_CACHE_WARMUP_REQUESTS, 1);

        PinotFS pinotFS = PinotFSFactory.create(URIUtils.getUri(_pageCacheWarmupQueriesDataDir).getScheme());
        // TODO: Add support to create file using PinotFS
        File queryFile = new File(new File(_pageCacheWarmupQueriesDataDir, tableNameWithType), "queries.txt");
        try (InputStream inputStream = pinotFS.open(queryFile.toURI())) {
          List<String> queries = OBJECT_MAPPER.readValue(inputStream, new TypeReference<>() { });
          if (queries.isEmpty()) {
            throw new RuntimeException("No queries found for tableName: " + finalRawTableName);
          }

          TablePageCacheWarmupRequest warmupRequest =
              new TablePageCacheWarmupRequest(appendSecondaryWorkload(queries), segmentsTo);

          List<String> serverInstancesForTable =
              _pinotHelixResourceManager.getServerInstancesForTable(finalRawTableName, tableType);
          BiMap<String, String> serverToEndPoints =
              _pinotHelixResourceManager.getDataInstanceAdminEndpoints(new HashSet<>(serverInstancesForTable));
          BiMap<String, String> endpointsToServers = serverToEndPoints.inverse();

          CompletableFuture.allOf(endpointsToServers.keySet().stream()
            .map(serverInstance -> {
              try {
                URI warmupUri = new URI(serverInstance + "/tables/" + tableNameWithType + "/triggerWarmup");
                return CompletableFuture.runAsync(() ->
                    sendWarmupRequestWithRetry(warmupUri, warmupRequest, serverInstance, finalRawTableName));
              } catch (Exception e) {
                throw new CompletionException("Invalid URI for server: " + serverInstance, e);
              }
            })
            .toArray(CompletableFuture[]::new))
        .join();
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      });
      warmupFuture.get(maxWarmupDurationMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      _controllerMetrics.addMeteredTableValue(rawTableName, ControllerMeter.PAGE_CACHE_WARMUP_REQUESTS_TIMEOUT, 1);
      LOGGER.error("Global warmup timed out for table: {}", rawTableName);
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(rawTableName, ControllerMeter.PAGE_CACHE_WARMUP_REQUESTS_ERRORS, 1);
      LOGGER.error("Failed to serve queries for table: {}", tableNameWithType, e);
    }
  }

  private void sendWarmupRequestWithRetry(URI uri, TablePageCacheWarmupRequest request, String serverInstance,
                                          String tableName) {
    try {
      DEFAULT_RETRY_POLICY.attempt(() -> {
        try {
          ClassicHttpRequest httpRequest = ClassicRequestBuilder.post(uri)
              .setVersion(HttpVersion.HTTP_1_1)
              .setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE)
              .setEntity(OBJECT_MAPPER.writeValueAsString(request))
              .build();
          LOGGER.info("Sending warmup request to server: {} for table: {}", serverInstance, tableName);
          SimpleHttpResponse response
              = HttpClient.wrapAndThrowHttpException(HttpClient.getInstance().sendRequest(httpRequest));
          if (response.getStatusCode() == HttpStatus.SC_OK) {
            LOGGER.info("Successfully sent warmup request to server: {} for table: {}", serverInstance, tableName);
            return true;
          } else {
            LOGGER.error("Failed to warmup server: {} for table: {} with response: {}, retrying..",
                serverInstance, tableName, response);
            return false;
          }
        } catch (Exception e) {
          LOGGER.error("Error sending warmup request to server: {} for table: {}, retrying..",
              serverInstance, tableName, e);
          return false;
        }
      });
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(tableName, ControllerMeter.PAGE_CACHE_WARMUP_REQUESTS_ERRORS, 1);
      LOGGER.error("Error sending warmup request to server: {} for table: {}", serverInstance, tableName, e);
    }
  }

  // Append SecondaryWorkload option to each query, to indicate that the query should be executed in the secondary queue
  // on the server, since we don't want to block the primary queue that is serving live queries with warmup queries.
  private static List<String> appendSecondaryWorkload(List<String> queries) {
    List<String> modifiedQueries = new ArrayList<>();
    for (String query : queries) {
      modifiedQueries.add("SET isSecondaryWorkload=true;" + query);
    }
    return modifiedQueries;
  }
}
