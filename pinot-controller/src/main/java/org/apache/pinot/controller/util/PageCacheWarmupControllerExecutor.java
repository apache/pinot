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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.BiMap;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
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
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.PageCacheWarmupConfig;
import org.apache.pinot.spi.config.table.PageCacheWarmupRequest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a "page‑cache warm‑up" after segments are refreshed.
 *
 * <p>The executor reads the warm‑up query file from
 * <code>{controllerConf.pageCacheWarmupDataDir}/{tableNameWithType}/queries</code>,
 * prepends {@code SET isSecondaryWorkload=true;} to intend  the server to run the query on
 * its secondary workload queue, wraps the list in a
 * {@link PageCacheWarmupRequest},
 * and POSTs the request to every server that hosts the table.</p>
 *
 * <p>The call is retried with an exponential back‑off (3 attempts, starting at
 * 3&nbsp;seconds) and the overall fan‑out is bounded by the controller‑level
 * {@code controller.page.cache.warmup.duration.ms} config.
 * </p>
 *
 * <h2>Sequence</h2>
 * <ol>
 *   <li>Validate that the table has warm‑up enabled.</li>
 *   <li>Load the most recently modified warm‑up file (or throws if none exist).</li>
 *   <li>Add the <em>secondary workload</em> hint to each query.</li>
 *   <li>Look up server admin endpoints via the Helix resource manager.</li>
 *   <li>Send parallel warm‑up requests and wait until all complete or the timeout elapses.</li>
 * </ol>
 *
 */
public class PageCacheWarmupControllerExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageCacheWarmupControllerExecutor.class);

  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(3, 3000L, 2.0f);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ControllerMetrics _controllerMetrics;
  private final String _pageCacheWarmupQueriesDataDir;
  private final long _maxPageCacheWarmupDurationMs;
  private final ExecutorService _warmupRequestExecutor;

  /**
   * Creates an executor bound to the given Controller services. The warmup query directory and the
   * overall warmup duration are read from the {@link ControllerConf} (defaults are used when it is
   * {@code null}).
   */
  public PageCacheWarmupControllerExecutor(PinotHelixResourceManager pinotHelixResourceManager,
                                           @Nullable ControllerConf controllerConf) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerMetrics = ControllerMetrics.get();
    _pageCacheWarmupQueriesDataDir =
        controllerConf != null ? controllerConf.getPageCacheWarmupQueriesDataDir() : null;
    _maxPageCacheWarmupDurationMs =
        controllerConf != null ? controllerConf.getPageCacheWarmupDurationMs()
            : ControllerConf.DEFAULT_PAGE_CACHE_WARMUP_DURATION_MS;
    _warmupRequestExecutor = Executors.newCachedThreadPool(runnable -> {
      Thread thread = new Thread(runnable, "page-cache-warmup-request");
      thread.setDaemon(true);
      return thread;
    });
  }

  /**
   * Orchestrates page‑cache warm‑up for the specified table.
   *
   * <p>The method spawns an asynchronous task that:
   * <ul>
   *   <li>Loads the most recently modified warm‑up file (or throws if none exist).</li>
   *   <li>Builds a {@link PageCacheWarmupRequest} that
   *       optionally restricts the warm‑up to the supplied segment list.</li>
   *   <li>Sends the request to every server in parallel with retry semantics.</li>
   * </ul>
   * The calling thread blocks only until the task finishes or the
   * warm‑up timeout defined in the table config expires.</p>
   *
   * @param tableNameWithType fully‑qualified table name, e.g. {@code myTable_OFFLINE}
   * @param segmentsTo        list of segment names to touch; {@code null} or empty means all segments
   */
  public void triggerPageCacheWarmup(String tableNameWithType, List<String> segmentsTo) {
    try {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      if (tableType != TableType.OFFLINE) {
        return;
      }
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      TableConfig tableConfig = _pinotHelixResourceManager.getOfflineTableConfig(rawTableName);
      if (tableConfig == null) {
        return;
      }
      PageCacheWarmupConfig pageCacheWarmupConfig = tableConfig.getPageCacheWarmupConfig();
      PageCacheWarmupConfig.Spec spec = pageCacheWarmupConfig != null ? pageCacheWarmupConfig.getOnRefresh() : null;
      if (spec == null || !spec.isEnabled()) {
        return;
      }

      LOGGER.info("Starting page cache warmup for table: {}, maxWarmupDurationMs: {}", tableNameWithType,
          _maxPageCacheWarmupDurationMs);
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.PAGE_CACHE_WARMUP_REQUESTS, 1);

      PinotFS pinotFS = PinotFSFactory.create(URIUtils.getUri(_pageCacheWarmupQueriesDataDir).getScheme());
      File tableDir = new File(_pageCacheWarmupQueriesDataDir, tableNameWithType);
      File[] files = tableDir.listFiles(File::isFile);
      if (files == null || files.length == 0) {
        LOGGER.warn("No warm‑up query files found for table: {}", tableNameWithType);
        return;
      }
      // If there are multiple files, use the most recently modified one.
      Arrays.sort(files, Comparator.comparingLong(File::lastModified).reversed());
      File queryFile = files[0];
      LOGGER.info("Using warm‑up query file: {}", queryFile.getName());

      List<String> queries;
      try (InputStream inputStream = pinotFS.open(queryFile.toURI())) {
        String json = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        queries = JsonUtils.stringToObject(json, new TypeReference<>() {
        });
      }
      if (queries == null || queries.isEmpty()) {
        LOGGER.warn("No queries found in warm‑up query file: {} for table: {}", queryFile.getName(), tableNameWithType);
        return;
      }

      PageCacheWarmupRequest warmupRequest =
          new PageCacheWarmupRequest(appendSecondaryWorkload(queries), segmentsTo);

      List<String> serverInstancesForTable =
          _pinotHelixResourceManager.getServerInstancesForTable(rawTableName, tableType);
      BiMap<String, String> serverToEndPoints =
          _pinotHelixResourceManager.getDataInstanceAdminEndpoints(new HashSet<>(serverInstancesForTable));
      BiMap<String, String> endpointsToServers = serverToEndPoints.inverse();

      List<CompletableFuture<Void>> futures = new ArrayList<>();
      for (String serverEndpoint : endpointsToServers.keySet()) {
        URI warmupUri;
        try {
          warmupUri = new URI(serverEndpoint + "/tables/" + tableNameWithType + "/triggerWarmup");
        } catch (Exception e) {
          LOGGER.error("Invalid warmup URI for server: {} table: {}", serverEndpoint, tableNameWithType, e);
          continue;
        }
        futures.add(CompletableFuture.runAsync(
            () -> sendWarmupRequestWithRetry(warmupUri, warmupRequest, serverEndpoint, rawTableName),
            _warmupRequestExecutor));
      }

      try {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .get(_maxPageCacheWarmupDurationMs, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.PAGE_CACHE_WARMUP_REQUEST_ERRORS, 1);
        LOGGER.error("Global warmup timed out for table: {}", tableNameWithType);
        // Stop awaiting any server requests that ran past the warmup budget.
        for (CompletableFuture<Void> future : futures) {
          if (!future.isDone()) {
            future.cancel(true);
          }
        }
      }
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.PAGE_CACHE_WARMUP_REQUEST_ERRORS, 1);
      LOGGER.error("Failed to serve queries for table: {}", tableNameWithType, e);
    }
  }

  /**
   * Sends a single warm‑up HTTP request with retries and logs the outcome.
   *
   * <p>The retry policy is {@link #DEFAULT_RETRY_POLICY}. Success increments no metrics,
   * but failures and retries are logged, and a final failure increments
   * {@link org.apache.pinot.common.metrics.ControllerMeter#PAGE_CACHE_WARMUP_REQUEST_ERRORS}.</p>
   */
  private void sendWarmupRequestWithRetry(URI uri, PageCacheWarmupRequest request, String serverInstance,
                                          String tableName) {
    try {
      DEFAULT_RETRY_POLICY.attempt(() -> {
        try {
          ClassicHttpRequest httpRequest = ClassicRequestBuilder.post(uri)
              .setVersion(HttpVersion.HTTP_1_1)
              .setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE)
              .setEntity(JsonUtils.objectToString(request))
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
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.PAGE_CACHE_WARMUP_REQUEST_ERRORS, 1);
      LOGGER.error("Error sending warmup request to server: {} for table: {}", serverInstance, tableName, e);
    }
  }

  /**
   * Shuts down the warmup request thread pool. Should be called when the owning resource manager stops.
   */
  public void shutdown() {
    _warmupRequestExecutor.shutdownNow();
  }

  /**
   * Prepends {@code SET isSecondaryWorkload=true;} to each query so that servers
   * enqueue the warm‑up on a secondary workload queue, avoiding interference with live traffic.
   *
   * @param queries original SQL queries
   * @return list with the secondary‑workload hint added
   */
  private static List<String> appendSecondaryWorkload(List<String> queries) {
    List<String> modifiedQueries = new ArrayList<>();
    for (String query : queries) {
      modifiedQueries.add("SET isSecondaryWorkload=true;" + query);
    }
    return modifiedQueries;
  }
}
