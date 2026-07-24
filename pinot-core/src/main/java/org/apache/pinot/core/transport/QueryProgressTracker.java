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
package org.apache.pinot.core.transport;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryProgressStats;
import org.apache.pinot.spi.utils.CommonConstants.Server;


/// Server-wide progress registry shared by every single-stage query transport.
///
/// Active contexts are removed conditionally so a late completion cannot remove a newer execution that reused the
/// same query id. Completed snapshots are retained briefly because one server can finish while the broker is still
/// reducing results or waiting for other servers.
public final class QueryProgressTracker {
  private static final int COMPLETED_PROGRESS_STATS_CACHE_SIZE = 10_000;
  private static final int COMPLETED_PROGRESS_STATS_CACHE_EXPIRATION_MINUTES = 10;

  @Nullable
  private final ConcurrentHashMap<String, QueryExecutionContext> _executionContexts;
  @Nullable
  private final Cache<String, QueryProgressStats> _completedProgressStats;

  public QueryProgressTracker(PinotConfiguration config) {
    this(config.getProperty(Server.CONFIG_OF_QUERY_PROGRESS_ENABLED, Server.DEFAULT_QUERY_PROGRESS_ENABLED));
  }

  public QueryProgressTracker(boolean enabled) {
    if (enabled) {
      _executionContexts = new ConcurrentHashMap<>();
      _completedProgressStats = CacheBuilder.newBuilder().maximumSize(COMPLETED_PROGRESS_STATS_CACHE_SIZE)
          .expireAfterWrite(COMPLETED_PROGRESS_STATS_CACHE_EXPIRATION_MINUTES, TimeUnit.MINUTES).build();
    } else {
      _executionContexts = null;
      _completedProgressStats = null;
    }
  }

  public boolean isEnabled() {
    return _executionContexts != null;
  }

  public void register(String queryId, QueryExecutionContext executionContext) {
    if (_executionContexts == null) {
      return;
    }
    _completedProgressStats.invalidate(queryId);
    _executionContexts.put(queryId, executionContext);
  }

  public void complete(String queryId, QueryExecutionContext executionContext, boolean successful) {
    if (_executionContexts == null) {
      return;
    }
    QueryProgressStats progressStats = executionContext.getProgressStats();
    if (successful && progressStats != null) {
      progressStats = asCompleted(progressStats);
    }
    QueryProgressStats completedProgressStats = progressStats;
    _executionContexts.computeIfPresent(queryId, (ignored, currentExecutionContext) -> {
      if (currentExecutionContext != executionContext) {
        return currentExecutionContext;
      }
      if (completedProgressStats != null) {
        _completedProgressStats.put(queryId, completedProgressStats);
      }
      return null;
    });
  }

  @Nullable
  public QueryProgressStats getProgressStats(String queryId) {
    if (_executionContexts == null) {
      return null;
    }
    QueryExecutionContext executionContext = _executionContexts.get(queryId);
    return executionContext != null ? executionContext.getProgressStats()
        : _completedProgressStats.getIfPresent(queryId);
  }

  private static QueryProgressStats asCompleted(QueryProgressStats progressStats) {
    long totalWorkUnits = progressStats.getTotalWorkUnits();
    long processedWorkUnits = totalWorkUnits >= 0 ? totalWorkUnits : progressStats.getProcessedWorkUnits();
    long totalSegmentsToProcess = progressStats.getTotalSegmentsToProcess();
    long processedSegments = totalSegmentsToProcess >= 0
        ? totalSegmentsToProcess : progressStats.getProcessedSegments();
    return new QueryProgressStats(progressStats.getLabel(), processedWorkUnits, totalWorkUnits, processedSegments,
        totalSegmentsToProcess, progressStats.isEstimated()).withDetails(progressStats.getDetails());
  }
}
