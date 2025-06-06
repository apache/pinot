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
package org.apache.pinot.server.warmup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.PageCacheWarmupConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PageCacheWarmupQueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageCacheWarmupQueryExecutor.class);
  private final InstanceDataManager _instanceDataManager;
  private final QueryScheduler _queryScheduler;
  private final long _instanceWarmupEndTimeMs;
  private final double _refreshWarmupQpsRateLimit;
  private ServerMetrics _serverMetrics;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public PageCacheWarmupQueryExecutor(InstanceDataManager instanceDataManager, QueryScheduler queryScheduler,
                                      PinotConfiguration config) {
    _instanceDataManager = instanceDataManager;
    _queryScheduler = queryScheduler;
    _instanceWarmupEndTimeMs = System.currentTimeMillis()
        + Long.parseLong(config.getProperty(CommonConstants.Server.MAX_PAGECACHE_WARMUP_DURATION_MS));
    _refreshWarmupQpsRateLimit =
        Double.parseDouble(config.getProperty(CommonConstants.Server.MAX_PAGECACHE_REFRESH_WARMUP_QPS_RATE));
    _serverMetrics = ServerMetrics.get();
  }

  public void startWarmupOnRestart() {
    Set<String> tables = _instanceDataManager.getAllTables();
    for (String tableNameWithType : tables) {
      try {
        long instanceWarmupTimeRemainingMs = _instanceWarmupEndTimeMs - System.currentTimeMillis();
        if (instanceWarmupTimeRemainingMs <= 0) {
          _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.PAGE_CACHE_WARMUP_TIMEOUT_ERRORS, 1L);
          LOGGER.warn("Instance Warmup timeout exceeded");
          break;
        }
        TableConfig tableConfig = getTableConfig(tableNameWithType);
        PageCacheWarmupConfig warmupConfig = getPageCacheWarmupConfig(tableConfig);
        if (warmupConfig != null && warmupConfig.isRestartEnabled()) {
          double warmupQps = Math.max(getQpsPerReplica(tableConfig), 1);
          warmupTable(tableNameWithType, warmupConfig, null, null, warmupQps);
        }
      } catch (Exception e) {
        String errorMessage = String.format("PageCache warmup failed on restart for table: %s", tableNameWithType);
        handleError(tableNameWithType, errorMessage, e);
      }
    }
  }

  // For restart the warmup queries and refresh segments would be provided
  public void startWarmupOnRefresh(String tableNameWithType, List<String> warmupQueries, List<String> segmentFilter) {
    try {
      TableConfig tableConfig = getTableConfig(tableNameWithType);
      PageCacheWarmupConfig warmupConfig = getPageCacheWarmupConfig(tableConfig);
      if (warmupConfig != null && warmupConfig.isRefreshEnabled()) {
        double warmupQps = Math.max(getQpsPerReplica(tableConfig) * _refreshWarmupQpsRateLimit, 1);
        warmupTable(tableNameWithType, warmupConfig, warmupQueries, segmentFilter, warmupQps);
      }
    } catch (Exception e) {
      String errorMessage = String.format("PageCache warmup failed on refresh for table: %s", tableNameWithType);
      handleError(tableNameWithType, errorMessage, e);
    }
  }

  private TableConfig getTableConfig(String tableNameWithType) {
    TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableNameWithType);
    if (tableDataManager == null) {
      String errorMessage = String.format("Table data manager not found for table: %s", tableNameWithType);
      LOGGER.error(errorMessage);
      throw new RuntimeException(errorMessage);
    }
    return tableDataManager.fetchIndexLoadingConfig().getTableConfig();
  }

  private PageCacheWarmupConfig getPageCacheWarmupConfig(TableConfig tableConfig) {
    String tableName = tableConfig.getTableName();
    try {
      Map<String, String> customConfigs = tableConfig.getCustomConfig().getCustomConfigs();
      if (customConfigs == null || customConfigs.get("pageCacheWarmupConfig") == null) {
        LOGGER.info("Page cache warmup config not found for table: {} customConfigs: {}", tableName, customConfigs);
        return null;
      }
      return OBJECT_MAPPER.readValue(customConfigs.get("pageCacheWarmupConfig"), PageCacheWarmupConfig.class);
    } catch (Exception e) {
      String errorMessage = String.format("Failed to get page cache warmup config for table: %s", tableName);
      LOGGER.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
  }


  private double getQpsPerReplica(TableConfig tableConfig) {
    QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
    assert quotaConfig != null;
    return quotaConfig.getMaxQPS() / tableConfig.getReplication();
  }

  private void handleError(String tableNameWithType, String errorMessage, Exception e) {
    _serverMetrics.addMeteredGlobalValue(ServerMeter.PAGE_CACHE_WARMUP_SERVER_ERRORS, 1L);
    _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.PAGE_CACHE_WARMUP_SERVER_ERRORS, 1L);
    LOGGER.error(errorMessage, e);
    throw new RuntimeException(errorMessage, e);
  }

  private void warmupTable(String tableNameWithType, PageCacheWarmupConfig warmupConfig,
                           @Nullable List<String> warmupQueries, @Nullable List<String> segmentFilter, double warmupQps) throws Exception {
    long startTimeMs = System.currentTimeMillis();
    TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableNameWithType);
    assert tableDataManager != null;
    List<String> segments = getSegments(tableDataManager, segmentFilter);
    if (segments.isEmpty()) {
      LOGGER.info("Skipping warmup, no segments found for table: {}", tableNameWithType);
      return;
    }
    List<String> queries = warmupQueries;
    if (queries == null) {
      // TODO: Find a better way to get the controller base URI
      SegmentZKMetadata segmentZKMetadata = tableDataManager.fetchZKMetadata(segments.get(0));
      URI baseURI = FileUploadDownloadClient.extractBaseURI(new URI(segmentZKMetadata.getDownloadUrl()));
      PageCacheWarmupQueryProvider queryProvider = new PageCacheWarmupQueryProvider(baseURI);
      queries = queryProvider.getWarmupQueries(tableNameWithType);
    }
    executeWarmupQueries(tableNameWithType, queries, segments, warmupConfig, startTimeMs, warmupQps);
  }

  private List<String> getSegments(TableDataManager tableDataManager, List<String> segmentFilter) {
    List<SegmentDataManager> segmentManagers = tableDataManager.acquireAllSegments();
    List<String> selectedSegments = new ArrayList<>();
    try {
      for (SegmentDataManager manager : segmentManagers) {
        if (segmentFilter == null || segmentFilter.contains(manager.getSegment().getSegmentMetadata().getName())) {
          selectedSegments.add(manager.getSegment().getSegmentName());
        }
      }
    } finally {
      for (SegmentDataManager manager : segmentManagers) {
        tableDataManager.releaseSegment(manager);
      }
    }
    return selectedSegments;
  }

  private void executeWarmupQueries(String tableNameWithType, List<String> queries, List<String> segments,
                                    PageCacheWarmupConfig warmupConfig, long tableWarmupStartTimeMs, double warmupQps) {
    long tableWarmupDurationMs = warmupConfig.getMaxWarmupDurationSeconds() * 1000;
    LOGGER.info("Starting warmup for table={}, totalQueries={}, warmupDurationMs={}, warmupQps={}",
        tableNameWithType, queries.size(), tableWarmupDurationMs, warmupQps);

    // Rate limiter for warmup queries
    RateLimiter rateLimiter = warmupQps > 0 ? RateLimiter.create(warmupQps) : null;

    long remainingWarmupTimeMs = tableWarmupDurationMs - (System.currentTimeMillis() - tableWarmupStartTimeMs);
    List<CompletableFuture<byte[]>> futures = new ArrayList<>();

    for (String query : queries) {
      remainingWarmupTimeMs = tableWarmupDurationMs - (System.currentTimeMillis() - tableWarmupStartTimeMs);
      if (remainingWarmupTimeMs <= 0) {
        LOGGER.info("Skipping remaining queries as warmup duration has elapsed for table={}", tableNameWithType);
        break;
      }

      // Wait for the rate limiter to allow the next query
      if (rateLimiter != null) {
        rateLimiter.acquire();
      }

      // Query execution logic
      try {
        QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
        queryContext.setEndTimeMs(System.currentTimeMillis() + remainingWarmupTimeMs);

        InstanceRequest instanceRequest = new InstanceRequest(0L, CalciteSqlCompiler.compileToBrokerRequest(query));
        instanceRequest.setSearchSegments(segments);

        ServerQueryRequest serverQueryRequest = new ServerQueryRequest(instanceRequest, ServerMetrics.get(),
            System.currentTimeMillis());
        _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.PAGE_CACHE_WARMUP_QUERIES, 1L);
        _serverMetrics.addMeteredGlobalValue(ServerMeter.PAGE_CACHE_WARMUP_QUERIES, 1L);

        // Execute query with timeout
        ListenableFuture<byte[]> listenableFuture = _queryScheduler.submit(serverQueryRequest);
        CompletableFuture<byte[]> queryFuture = toCompletableFuture(listenableFuture);
        queryFuture.orTimeout(remainingWarmupTimeMs, TimeUnit.MILLISECONDS)
            .thenAccept(result -> {
              LOGGER.info("Successfully executed warmup query={}", query);
            })
            .exceptionally(ex -> {
              if (ex instanceof TimeoutException) {
                LOGGER.warn("Query execution timed out for table={} query={}", tableNameWithType, query, ex);
                _serverMetrics.addMeteredGlobalValue(ServerMeter.PAGE_CACHE_WARMUP_TIMEOUT_ERRORS, 1L);
                _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.PAGE_CACHE_WARMUP_TIMEOUT_ERRORS, 1L);
              } else {
                LOGGER.error("Failed to execute warmup for table={} query={}", tableNameWithType, query, ex);
                _serverMetrics.addMeteredGlobalValue(ServerMeter.PAGE_CACHE_WARMUP_SERVER_ERRORS, 1L);
                _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.PAGE_CACHE_WARMUP_SERVER_ERRORS, 1L);
              }
              return null;
            });
        futures.add(queryFuture);
      } catch (Exception e) {
        LOGGER.error("Error while warmup query for table={}, query={}", tableNameWithType, query, e);
        _serverMetrics.addMeteredGlobalValue(ServerMeter.PAGE_CACHE_WARMUP_SERVER_ERRORS, 1L);
        _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.PAGE_CACHE_WARMUP_SERVER_ERRORS, 1L);
      }
    }
    try {
      // Wait for all futures to complete within table-level timeout
      CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
      allFutures.get(remainingWarmupTimeMs, TimeUnit.MILLISECONDS);
      LOGGER.info("Warmup completed for table={}, durationMs={}",
          tableNameWithType, System.currentTimeMillis() - tableWarmupStartTimeMs);
    } catch (TimeoutException e) {
      LOGGER.error("Table warmup timeout exceeded for table={}, durationMs={}, configuredWarmupDurationMs={}",
          tableNameWithType, System.currentTimeMillis() - tableWarmupStartTimeMs, tableWarmupDurationMs);
      _serverMetrics.addMeteredGlobalValue(ServerMeter.PAGE_CACHE_WARMUP_TIMEOUT_ERRORS, 1L);
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.PAGE_CACHE_WARMUP_TIMEOUT_ERRORS, 1L);
    } catch (Exception e) {
      LOGGER.error("Error during warmup execution for table={}", tableNameWithType, e);
    }
  }

  // Helper method to convert ListenableFuture to CompletableFuture
  private <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> listenableFuture) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();

    Futures.addCallback(listenableFuture, new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        completableFuture.complete(result);
      }
      @Override
      public void onFailure(Throwable t) {
        completableFuture.completeExceptionally(t);
      }
    }, MoreExecutors.directExecutor());  // TODO: Use the same executor as the query scheduler
    return completableFuture;
  }

}
