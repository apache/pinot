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
package org.apache.pinot.core.data.manager;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.config.TableDataManagerConfig;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseTableDataManager implements TableDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTableDataManager.class);
  // cache deleted segment names for utmost this duration
  private static final int MAX_CACHE_DURATION_SEC = 6 * 3600; // 6 hours

  protected final ConcurrentHashMap<String, SegmentDataManager> _segmentDataManagerMap = new ConcurrentHashMap<>();

  protected Cache<String, Boolean> _deletedSegmentsCache;

  protected TableDataManagerConfig _tableDataManagerConfig;
  protected String _instanceId;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected ServerMetrics _serverMetrics;
  protected String _tableNameWithType;
  protected String _tableDataDir;
  protected File _indexDir;
  protected Logger _logger;

  @Override
  public void init(@Nonnull TableDataManagerConfig tableDataManagerConfig, @Nonnull String instanceId,
      @Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore, @Nonnull ServerMetrics serverMetrics) {
    LOGGER.info("Initializing table data manager for table: {}", tableDataManagerConfig.getTableName());

    _deletedSegmentsCache = CacheBuilder.newBuilder().expireAfterWrite(MAX_CACHE_DURATION_SEC, TimeUnit.SECONDS).build();
    _tableDataManagerConfig = tableDataManagerConfig;
    _instanceId = instanceId;
    _propertyStore = propertyStore;
    _serverMetrics = serverMetrics;

    _tableNameWithType = tableDataManagerConfig.getTableName();
    _tableDataDir = tableDataManagerConfig.getDataDir();
    _indexDir = new File(_tableDataDir);
    if (!_indexDir.exists()) {
      Preconditions.checkState(_indexDir.mkdirs());
    }
    _logger = LoggerFactory.getLogger(_tableNameWithType + "-" + getClass().getSimpleName());

    doInit();

    _logger.info("Initialized table data manager for table: {} with data directory: {}", _tableNameWithType,
        _tableDataDir);
  }

  protected abstract void doInit();

  @Override
  public void start() {
    _logger.info("Starting table data manager for table: {}", _tableNameWithType);
    doStart();
    _logger.info("Started table data manager for table: {}", _tableNameWithType);
  }

  protected abstract void doStart();

  @Override
  public void shutDown() {
    _logger.info("Shutting down table data manager for table: {}", _tableNameWithType);
    doShutdown();
    _logger.info("Shut down table data manager for table: {}", _tableNameWithType);
  }

  protected abstract void doShutdown();

  /**
   * {@inheritDoc}
   * <p>If one segment already exists with the same name, replaces it with the new one.
   * <p>Ensures that reference count of the old segment (if replaced) is reduced by 1, so that the last user of the old
   * segment (or the calling thread, if there are none) remove the segment.
   * <p>The new segment is added with reference count of 1, so that is never removed until a drop command comes through.
   *
   * @param immutableSegment Immutable segment to add
   */
  @Override
  public void addSegment(@Nonnull ImmutableSegment immutableSegment) {
    String segmentName = immutableSegment.getSegmentName();
    _logger.info("Adding immutable segment: {} to table: {}", segmentName, _tableNameWithType);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        immutableSegment.getSegmentMetadata().getTotalRawDocs());
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);

    ImmutableSegmentDataManager newSegmentManager = new ImmutableSegmentDataManager(immutableSegment);
    SegmentDataManager oldSegmentManager = _segmentDataManagerMap.put(segmentName, newSegmentManager);

    // release old segment if needed
    if (oldSegmentManager == null) {
      _logger.info("Added new immutable segment: {} to table: {}", segmentName, _tableNameWithType);
    } else {
      _logger.info("Replaced immutable segment: {} of table: {}", segmentName, _tableNameWithType);
      releaseSegment(oldSegmentManager);
    }
  }

  @Override
  public void addSegment(@Nonnull File indexDir, @Nonnull IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addSegment(@Nonnull String segmentName, @Nonnull TableConfig tableConfig,
      @Nonnull IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Called when we get a helix transition to go to offline or dropped state.
   * We need to remove it safely, keeping in mind that there may be queries that are
   * using the segment,
   * @param segmentName name of the segment to remove.
   */
  @Override
  public void removeSegment(@Nonnull String segmentName) {
    _logger.info("Removing segment: {} from table: {}", segmentName, _tableNameWithType);
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.remove(segmentName);
    if (segmentDataManager != null) {
      releaseSegment(segmentDataManager);
      _logger.info("Removed segment: {} from table: {}", segmentName, _tableNameWithType);
    } else {
      _logger.info("Failed to find segment: {} in table: {}", segmentName, _tableNameWithType);
    }
  }

  /**
   * Called when a segment is deleted. The actual handling of segment delete is outside of this method.
   * This method provides book-keeping around deleted segments.
   * @param segmentName name of the segment to track.
   */
  public void notifySegmentDeleted(@Nonnull String segmentName) {
    // add segment to the cache
    _deletedSegmentsCache.put(segmentName, true);
  }

  /**
   * Check if a segment is recently deleted.
   *
   * @param segmentName name of the segment to check.
   * @return true if segment is in the cache, false otherwise
   */
  public boolean isRecentlyDeleted(@Nonnull String segmentName) {
    return _deletedSegmentsCache.getIfPresent(segmentName) != null;
  }

  /**
   * Remove a segment from the deleted cache if it is being added back.
   *
   * @param segmentName name of the segment that needs to removed from the cache (if needed)
   */
  public void notifySegmentAdded(@Nonnull String segmentName) {
    _deletedSegmentsCache.invalidate(segmentName);
  }

  @Nonnull
  @Override
  public List<SegmentDataManager> acquireAllSegments() {
    List<SegmentDataManager> segmentDataManagers = new ArrayList<>();
    for (SegmentDataManager segmentDataManager : _segmentDataManagerMap.values()) {
      if (segmentDataManager.increaseReferenceCount()) {
        segmentDataManagers.add(segmentDataManager);
      }
    }
    return segmentDataManagers;
  }

  @Nonnull
  @Override
  public List<SegmentDataManager> acquireSegments(@Nonnull List<String> segmentNames) {
    List<SegmentDataManager> segmentDataManagers = new ArrayList<>();
    for (String segmentName : segmentNames) {
      SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
      if (segmentDataManager != null && segmentDataManager.increaseReferenceCount()) {
        segmentDataManagers.add(segmentDataManager);
      }
    }
    return segmentDataManagers;
  }

  @Override
  public SegmentDataManager acquireSegment(@Nonnull String segmentName) {
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager != null && segmentDataManager.increaseReferenceCount()) {
      return segmentDataManager;
    } else {
      return null;
    }
  }

  @Override
  public void releaseSegment(@Nonnull SegmentDataManager segmentDataManager) {
    if (segmentDataManager.decreaseReferenceCount()) {
      closeSegment(segmentDataManager);
    }
  }

  private void closeSegment(@Nonnull SegmentDataManager segmentDataManager) {
    String segmentName = segmentDataManager.getSegmentName();
    _logger.info("Closing segment: {} of table: {}", segmentName, _tableNameWithType);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, -1L);
    _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.DELETED_SEGMENT_COUNT, 1L);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        -segmentDataManager.getSegment().getSegmentMetadata().getTotalRawDocs());
    segmentDataManager.destroy();
    _logger.info("Closed segment: {} of table: {}", segmentName, _tableNameWithType);
  }

  @Nonnull
  @Override
  public String getTableName() {
    return _tableNameWithType;
  }
}
