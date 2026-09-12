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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.PageCacheWarmupConfig;
import org.apache.pinot.spi.config.table.PageCacheWarmupRequest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Executes a "page‑cache warm‑up" after segments are refreshed.
///
/// <p>The executor reads the warm‑up query file from
/// <code>{controllerConf.pageCacheWarmupDataDir}/{tableNameWithType}/queries</code>,
/// prepends {@code SET isSecondaryWorkload=true;} to intend  the server to run the query on
/// its secondary workload queue, wraps the list in a
/// {@link PageCacheWarmupRequest},
/// and POSTs the request to every server that hosts the table.</p>
///
/// <p>The call is retried with an exponential back‑off (3 attempts, starting at
/// 3&nbsp;seconds) and the overall fan‑out is bounded by the controller‑level
/// {@code controller.page.cache.warmup.duration.ms} config.
/// </p>
///
/// <h2>Sequence</h2>
/// <ol>
///   <li>Validate that the table has warm‑up enabled.</li>
///   <li>Load the most recently modified warm‑up file (or throws if none exist).</li>
///   <li>Add the <em>secondary workload</em> hint to each query.</li>
///   <li>Look up server admin endpoints via the Helix resource manager.</li>
///   <li>Send parallel warm‑up requests and wait until all complete or the timeout elapses.</li>
/// </ol>
///
public class PageCacheWarmupControllerExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageCacheWarmupControllerExecutor.class);

  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(3, 3000L, 2.0f);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ControllerMetrics _controllerMetrics;
  private final String _pageCacheWarmupQueriesDataDir;
  private final long _maxPageCacheWarmupDurationMs;
  private final ExecutorService _warmupRequestExecutor;

  /// Creates an executor bound to the given Controller services. The warmup query directory and the
  /// overall warmup duration are read from the {@link ControllerConf} (defaults are used when it is
  /// {@code null}).
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

  /// Orchestrates page‑cache warm‑up for the specified table.
  ///
  /// <p>The method spawns an asynchronous task that:
  /// <ul>
  ///   <li>Loads the most recently modified warm‑up file (or throws if none exist).</li>
  ///   <li>Builds a {@link PageCacheWarmupRequest} that
  ///       optionally restricts the warm‑up to the supplied segment list.</li>
  ///   <li>Sends the request to every server in parallel with retry semantics.</li>
  /// </ul>
  /// The calling thread blocks only until the task finishes or the
  /// warm‑up timeout defined in the table config expires.</p>
  ///
  /// @param tableNameWithType fully‑qualified table name, e.g. {@code myTable_OFFLINE}
  /// @param segmentsTo        list of segment names to touch; {@code null} or empty means all segments
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

      URI tableDirUri = getTableDirectoryUri(_pageCacheWarmupQueriesDataDir, tableNameWithType);
      PinotFS pinotFS = PinotFSFactory.create(tableDirUri.getScheme());
      URI queryFileUri = getMostRecentlyModifiedFileUri(pinotFS, tableDirUri);
      if (queryFileUri == null) {
        LOGGER.warn("No warm‑up query files found for table: {}", tableNameWithType);
        return;
      }
      LOGGER.info("Using warm‑up query file: {}", queryFileUri);

      List<String> queries;
      try (InputStream inputStream = pinotFS.open(queryFileUri)) {
        String json = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        queries = JsonUtils.stringToObject(json, new TypeReference<>() {
        });
      }
      if (queries == null || queries.isEmpty()) {
        LOGGER.warn("No queries found in warm‑up query file: {} for table: {}", queryFileUri, tableNameWithType);
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

  @Nullable
  private static URI getMostRecentlyModifiedFileUri(PinotFS pinotFS, URI directoryUri)
      throws IOException, URISyntaxException {
    List<FileMetadata> fileMetadataList;
    try {
      fileMetadataList = pinotFS.listFilesWithMetadata(directoryUri, false);
    } catch (IOException | IllegalArgumentException e) {
      try {
        if (!pinotFS.exists(directoryUri)) {
          return null;
        }
      } catch (IOException | RuntimeException existsException) {
        e.addSuppressed(existsException);
      }
      throw e;
    }

    URI mostRecentlyModifiedFileUri = null;
    long mostRecentModificationTime = Long.MIN_VALUE;
    for (FileMetadata fileMetadata : fileMetadataList) {
      if (!fileMetadata.isDirectory()) {
        URI fileUri = getListedFileUri(fileMetadata.getFilePath(), directoryUri);
        long modificationTime = fileMetadata.getLastModifiedTime();
        if (modificationTime <= 0) {
          modificationTime = pinotFS.lastModified(fileUri);
        }
        if (mostRecentlyModifiedFileUri == null || modificationTime > mostRecentModificationTime) {
          mostRecentlyModifiedFileUri = fileUri;
          mostRecentModificationTime = modificationTime;
        }
      }
    }
    return mostRecentlyModifiedFileUri;
  }

  private static URI getTableDirectoryUri(String dataDir, String tableNameWithType) {
    try {
      URI dataDirUri = new URI(dataDir);
      if (dataDirUri.getScheme() != null) {
        String encodedTableName = URLEncoder.encode(tableNameWithType, StandardCharsets.UTF_8).replace("+", "%20");
        return new URI(dataDir + (dataDir.endsWith("/") ? "" : "/") + encodedTableName);
      }
    } catch (URISyntaxException e) {
      if (dataDir.matches("^[A-Za-z][A-Za-z0-9+.-]*://.*")) {
        throw new IllegalArgumentException("Invalid page cache warmup query data directory: " + dataDir, e);
      }
      // Preserve the existing File semantics for local paths that are not valid URI strings.
    }
    return new File(dataDir, tableNameWithType).toURI();
  }

  private static URI getListedFileUri(String filePath, URI directoryUri)
      throws URISyntaxException {
    if ("file".equalsIgnoreCase(directoryUri.getScheme())
        && !filePath.regionMatches(true, 0, "file:", 0, "file:".length())) {
      return new File(filePath).toURI();
    }

    try {
      URI fileUri = new URI(filePath);
      if (fileUri.getScheme() != null && fileUri.getRawQuery() == null && fileUri.getRawFragment() == null
          && !usesRawQualifiedListingPaths(fileUri.getScheme())) {
        return fileUri;
      }
    } catch (URISyntaxException e) {
      // Rebuild raw filesystem paths below so reserved filename characters are encoded as path components.
    }

    String scheme = directoryUri.getScheme();
    String authority = directoryUri.getAuthority();
    String path = filePath;
    int schemeSeparatorIndex = getSchemeSeparatorIndex(filePath);
    if (schemeSeparatorIndex >= 0) {
      scheme = filePath.substring(0, schemeSeparatorIndex);
      path = filePath.substring(schemeSeparatorIndex + 1);
      if (path.startsWith("//")) {
        int pathStartIndex = path.indexOf('/', 2);
        if (pathStartIndex < 0) {
          authority = path.substring(2);
          path = "";
        } else {
          authority = path.substring(2, pathStartIndex);
          path = path.substring(pathStartIndex);
        }
      } else if (!scheme.equalsIgnoreCase(directoryUri.getScheme())) {
        authority = null;
      }
    }
    return new URI(scheme, authority, path, null, null);
  }

  private static boolean usesRawQualifiedListingPaths(String scheme) {
    return "s3".equalsIgnoreCase(scheme) || "s3a".equalsIgnoreCase(scheme);
  }

  private static int getSchemeSeparatorIndex(String uri) {
    int separatorIndex = uri.indexOf(':');
    if (separatorIndex <= 0 || !Character.isLetter(uri.charAt(0))) {
      return -1;
    }
    for (int i = 1; i < separatorIndex; i++) {
      char character = uri.charAt(i);
      if (!Character.isLetterOrDigit(character) && character != '+' && character != '-' && character != '.') {
        return -1;
      }
    }
    return separatorIndex;
  }

  /// Sends a single warm‑up HTTP request with retries and logs the outcome.
  ///
  /// <p>The retry policy is {@link #DEFAULT_RETRY_POLICY}. Success increments no metrics,
  /// but failures and retries are logged, and a final failure increments
  /// {@link ControllerMeter#PAGE_CACHE_WARMUP_REQUEST_ERRORS}.</p>
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

  /// Shuts down the warmup request thread pool. Should be called when the owning resource manager stops.
  public void shutdown() {
    _warmupRequestExecutor.shutdownNow();
  }

  /// Prepends {@code SET isSecondaryWorkload=true;} to each query so that servers
  /// enqueue the warm‑up on a secondary workload queue, avoiding interference with live traffic.
  ///
  /// @param queries original SQL queries
  /// @return list with the secondary‑workload hint added
  private static List<String> appendSecondaryWorkload(List<String> queries) {
    List<String> modifiedQueries = new ArrayList<>();
    for (String query : queries) {
      modifiedQueries.add("SET isSecondaryWorkload=true;" + query);
    }
    return modifiedQueries;
  }
}
