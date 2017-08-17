/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.manager.offline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.metrics.ServerGauge;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO: pass a reference to Helix property store during initialization. Both OFFLINE and REALTIME use case need it.
public abstract class AbstractTableDataManager implements TableDataManager {
  protected final List<String> _activeSegments = new ArrayList<String>();
  protected final List<String> _loadingSegments = new ArrayList<String>();
  // This read-write lock protects the _segmentsMap and SegmentDataManager.refCnt
  protected final ReadWriteLock _rwLock = new ReentrantReadWriteLock();
  @VisibleForTesting
  protected final Map<String, SegmentDataManager> _segmentsMap = new HashMap<String, SegmentDataManager>();

  protected Logger LOGGER = LoggerFactory.getLogger(AbstractTableDataManager.class);
  protected volatile boolean _isStarted = false;
  protected String _tableName;

  protected TableDataManagerConfig _tableDataManagerConfig;
  protected String _tableDataDir;
  protected File _indexDir;
  protected ServerMetrics _serverMetrics;
  protected String _serverInstance;

  protected AbstractTableDataManager() {
  }

  @Override
  public void init(@Nonnull TableDataManagerConfig tableDataManagerConfig, @Nonnull ServerMetrics serverMetrics,
      @Nullable String serverInstance) {
    _serverInstance = serverInstance;
    _tableDataManagerConfig = tableDataManagerConfig;
    _serverMetrics = serverMetrics;

    _tableName = _tableDataManagerConfig.getTableName();

    _tableDataDir = _tableDataManagerConfig.getDataDir();
    _indexDir = new File(_tableDataDir);
    if (!_indexDir.exists()) {
      _indexDir.mkdirs();
    }

    doInit();
    LOGGER.info("Initialized table: {} with data directory: {}", _tableName, _tableDataDir);
  }

  protected abstract void doInit();

  @Override
  public void start() {
    LOGGER.info("Trying to start table : " + _tableName);
    if (_isStarted) {
      LOGGER.warn("TableDataManager for table {} is already started.", _tableName);
      return;
    }
    if (_tableDataManagerConfig == null) {
      LOGGER.warn("TableDataManager for table {} is not initialized.", _tableName);
      return;
    }
    _isStarted = true;
  }

  @Override
  public void shutDown() {
    LOGGER.info("Trying to shutdown table : " + _tableName);
    doShutdown();
    if (_isStarted) {
      _tableDataManagerConfig = null;
      _isStarted = false;
    } else {
      LOGGER.warn("Already shutDown table : " + _tableName);
    }
  }

  protected abstract void doShutdown();

  /**
   * Add a segment (or replace it, if one exists with the same name).
   * <p>
   * Ensures that reference count of the old segment (if replaced) is reduced by 1, so that the
   * last user of the old segment (or the calling thread, if there are none) remove the segment.
   * The new segment is added with a refcnt of 1, so that is never removed until a drop command
   * comes through.
   *
   * @param indexSegmentToAdd new segment to add/replace.
   */
  public void addSegment(@Nonnull IndexSegment indexSegmentToAdd) {
    final String segmentName = indexSegmentToAdd.getSegmentName();
    LOGGER.info("Trying to add a new segment {} of table {} with OfflineSegmentDataManager", segmentName, _tableName);
    OfflineSegmentDataManager newSegmentManager = new OfflineSegmentDataManager(indexSegmentToAdd);
    final int newNumDocs = indexSegmentToAdd.getSegmentMetadata().getTotalRawDocs();
    SegmentDataManager oldSegmentManager;
    int refCnt = -1;
    try {
      _rwLock.writeLock().lock();
      oldSegmentManager = _segmentsMap.put(segmentName, newSegmentManager);
      if (oldSegmentManager != null) {
        refCnt = oldSegmentManager.decrementRefCnt();
      }
    } finally {
      _rwLock.writeLock().unlock();
    }
    if (oldSegmentManager == null) {
      LOGGER.info("Added new segment {} for table {}", segmentName, _tableName);
    } else {
      LOGGER.info("Replaced segment {}(refCnt {}) with new segment for table {}", segmentName, refCnt, _tableName);
    }
    if (refCnt == 0) {  // oldSegmentManager must be non-null.
      closeSegment(oldSegmentManager);
    }
    _serverMetrics.addValueToTableGauge(_tableName, ServerGauge.DOCUMENT_COUNT, newNumDocs);
    _serverMetrics.addValueToTableGauge(_tableName, ServerGauge.SEGMENT_COUNT, 1L);
  }

  /**
   * Called when we get a helix transition to go to offline or dropped state.
   * We need to remove it safely, keeping in mind that there may be queries that are
   * using the segment,
   * @param segmentName name of the segment to remove.
   */
  @Override
  public void removeSegment(String segmentName) {
    if (!_isStarted) {
      LOGGER.warn("Could not remove segment {}, Tracker is stopped", segmentName);
      return;
    }

    SegmentDataManager segmentDataManager;
    int refCnt = -1;
    try {
      _rwLock.writeLock().lock();
      segmentDataManager = _segmentsMap.remove(segmentName);
      if (segmentDataManager != null) {
        refCnt = segmentDataManager.decrementRefCnt();
      }
    } finally {
      _rwLock.writeLock().unlock();
    }
    if (refCnt == 0) {  // segmentDataManager must be non-null.
      closeSegment(segmentDataManager);
    }
  }

  protected void closeSegment(SegmentDataManager segmentDataManager) {
    final String segmentName = segmentDataManager.getSegmentName();
    LOGGER.info("Closing segment {} for table {}", segmentName, _tableName);
    _serverMetrics.addValueToTableGauge(_tableName, ServerGauge.SEGMENT_COUNT, -1L);
    _serverMetrics.addMeteredTableValue(_tableName, ServerMeter.DELETED_SEGMENT_COUNT, 1L);
    _serverMetrics.addValueToTableGauge(_tableName, ServerGauge.DOCUMENT_COUNT,
        -segmentDataManager.getSegment().getSegmentMetadata().getTotalRawDocs());
    segmentDataManager.destroy();
    LOGGER.info("Segment {} for table {} has been closed", segmentName, _tableName);
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  @Nonnull
  @Override
  public ImmutableList<SegmentDataManager> acquireAllSegments() {
    ImmutableList.Builder<SegmentDataManager> segmentListBuilder = ImmutableList.builder();
    try {
      _rwLock.readLock().lock();
      for (Map.Entry<String, SegmentDataManager> segmentEntry : _segmentsMap.entrySet()) {
        SegmentDataManager segmentDataManager = segmentEntry.getValue();
        segmentDataManager.incrementRefCnt();
        segmentListBuilder.add(segmentDataManager);
      }
    } finally {
      _rwLock.readLock().unlock();
    }
    return segmentListBuilder.build();
  }

  @Override
  public List<SegmentDataManager> acquireSegments(List<String> segmentList) {
    List<SegmentDataManager> ret = new ArrayList<SegmentDataManager>();
    try {
      _rwLock.readLock().lock();
      for (String segName : segmentList) {
        SegmentDataManager segmentDataManager;
        segmentDataManager = _segmentsMap.get(segName);
        if (segmentDataManager != null) {
          segmentDataManager.incrementRefCnt();
          ret.add(segmentDataManager);
        }
      }
    } finally {
      _rwLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public SegmentDataManager acquireSegment(String segmentName) {
    try {
      _rwLock.readLock().lock();
      SegmentDataManager segmentDataManager = _segmentsMap.get(segmentName);
      if (segmentDataManager != null) {
        segmentDataManager.incrementRefCnt();
      }
      return segmentDataManager;
    } finally {
      _rwLock.readLock().unlock();
    }
  }

  @Override
  public void releaseSegment(SegmentDataManager segmentDataManager) {
    if (segmentDataManager == null) {
      return;
    }
    int refCnt = segmentDataManager.decrementRefCnt();
    // Exactly one thread should find this to be zero, so we can safely drop it.
    // We never remove it from the map here, so no need to synchronize.
    if (refCnt == 0) {
      closeSegment(segmentDataManager);
    }
  }

  @Override
  public String getTableName() {
    return _tableName;
  }
}
