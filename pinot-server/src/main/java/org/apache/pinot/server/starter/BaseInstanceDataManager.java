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
package org.apache.pinot.server.starter;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.spi.SegmentMetadata;


/// Owns table registry, creation ordering and segment executors shared by coordination backends.
/// Backend subclasses retain metadata lookup, deletion policy and their own startup/shutdown hooks.
public abstract class BaseInstanceDataManager implements InstanceDataManager {
  protected final Map<String, TableDataManager> _tableDataManagerMap = new ConcurrentHashMap<>();
  /// Serializes same-name creation with the previous table owner's entire shutdown. Values stay strongly reachable
  /// while held; weak idle entries do not accumulate table names indefinitely.
  protected final LoadingCache<String, Lock> _tableLifecycleLocks =
      CacheBuilder.newBuilder().weakValues().build(CacheLoader.from(() -> new ReentrantLock()));
  /// Shared across successive incarnations of a table so stale operations still serialize with replacement work.
  protected final SegmentLocks _segmentLocks = new SegmentLocks();
  protected Cache<Pair<String, String>, SegmentErrorInfo> _errorCache;
  protected SegmentReloadSemaphore _segmentReloadSemaphore;
  protected ExecutorService _segmentReloadRefreshExecutor;
  @Nullable
  protected ExecutorService _segmentPreloadExecutor;

  protected final void initLifecycleResources(int maxRefreshThreads, int maxPreloadThreads, long errorCacheSize) {
    Preconditions.checkArgument(maxRefreshThreads > 0,
        "'pinot.server.instance.max.parallel.refresh.threads' must be positive, got: %s", maxRefreshThreads);
    _segmentReloadSemaphore = new SegmentReloadSemaphore(maxRefreshThreads);
    _segmentReloadRefreshExecutor = Executors.newFixedThreadPool(maxRefreshThreads,
        new ThreadFactoryBuilder().setNameFormat("segment-reload-refresh-thread-%d").build());
    if (maxPreloadThreads > 0) {
      _segmentPreloadExecutor = Executors.newFixedThreadPool(maxPreloadThreads,
          new ThreadFactoryBuilder().setNameFormat("segment-preload-thread-%d").build());
    }
    _errorCache = CacheBuilder.newBuilder().maximumSize(errorCacheSize).build();
  }

  /// The slow path serializes creation with table deletion. Backend admission controls must be held by callers that
  /// need to prevent new table creation during instance shutdown; the Helix path retains its existing lifecycle.
  protected final TableDataManager getOrCreateManagedTable(String tableName,
      Function<String, TableDataManager> factory) {
    TableDataManager existing = _tableDataManagerMap.get(tableName);
    if (existing != null) {
      return existing;
    }
    Lock lock = _tableLifecycleLocks.getUnchecked(tableName);
    lock.lock();
    try {
      return _tableDataManagerMap.computeIfAbsent(tableName, factory);
    } finally {
      lock.unlock();
    }
  }

  protected final void shutDownManagedTables() {
    List<TableDataManager> tables = new ArrayList<>(_tableDataManagerMap.values());
    if (_segmentReloadRefreshExecutor != null) {
      _segmentReloadRefreshExecutor.shutdownNow();
    }
    if (_segmentPreloadExecutor != null) {
      _segmentPreloadExecutor.shutdownNow();
    }
    if (!tables.isEmpty()) {
      ExecutorService stops = Executors.newFixedThreadPool(
          Math.min(Runtime.getRuntime().availableProcessors(), tables.size()));
      for (TableDataManager table : tables) {
        stops.submit(table::shutDown);
      }
      stops.shutdown();
      try {
        if (!stops.awaitTermination(10, TimeUnit.MINUTES)) {
          stops.shutdownNow();
        }
      } catch (InterruptedException e) {
        stops.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public Set<String> getAllTables() {
    return _tableDataManagerMap.keySet();
  }

  @Nullable
  @Override
  public TableDataManager getTableDataManager(String tableNameWithType) {
    return _tableDataManagerMap.get(tableNameWithType);
  }

  @Nullable
  @Override
  public SegmentMetadata getSegmentMetadata(String tableNameWithType, String segmentName) {
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager != null) {
      SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
      if (segmentDataManager == null) {
        return null;
      }
      try {
        return segmentDataManager.getSegment().getSegmentMetadata();
      } finally {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
    return null;
  }

  @Override
  public List<SegmentMetadata> getAllSegmentsMetadata(String tableNameWithType) {
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager == null) {
      return List.of();
    } else {
      List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
      try {
        List<SegmentMetadata> segmentsMetadata = new ArrayList<>(segmentDataManagers.size());
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          segmentsMetadata.add(segmentDataManager.getSegment().getSegmentMetadata());
        }
        return segmentsMetadata;
      } finally {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
  }
}
