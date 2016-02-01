/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;


/**
 * An implementation of offline TableDataManager.
 * Provide add and remove segment functionality.
 *
 *
 */
public class OfflineTableDataManager implements TableDataManager {
  private Logger LOGGER = LoggerFactory.getLogger(OfflineTableDataManager.class);

  private volatile boolean _isStarted = false;
  private final Object _globalLock = new Object();
  private String _tableName;
  private ReadMode _readMode;

  private ExecutorService _queryExecutorService;

  private TableDataManagerConfig _tableDataManagerConfig;
  private String _tableDataDir;
  private int _numberOfTableQueryExecutorThreads;
  private IndexLoadingConfigMetadata _indexLoadingConfigMetadata;

  // This read-write lock protects the _segmentsMap and OfflineSegmentDataManager.refCnt
  private final ReadWriteLock _rwLock = new ReentrantReadWriteLock();
  private final Map<String, OfflineSegmentDataManager> _segmentsMap = new HashMap<String, OfflineSegmentDataManager>(); // Accessed in test via reflection
  private final List<String> _activeSegments = new ArrayList<String>();
  private final List<String> _loadingSegments = new ArrayList<String>();

  private Counter _currentNumberOfSegments = Metrics.newCounter(OfflineTableDataManager.class,
      CommonConstants.Metric.Server.CURRENT_NUMBER_OF_SEGMENTS);
  private Counter _currentNumberOfDocuments = Metrics.newCounter(OfflineTableDataManager.class,
      CommonConstants.Metric.Server.CURRENT_NUMBER_OF_DOCUMENTS);
  private Counter _numDeletedSegments = Metrics.newCounter(OfflineTableDataManager.class,
      CommonConstants.Metric.Server.NUMBER_OF_DELETED_SEGMENTS);

  public OfflineTableDataManager() {
  }

  @Override
  public void init(TableDataManagerConfig tableDataManagerConfig) {
    _tableDataManagerConfig = tableDataManagerConfig;
    _tableName = _tableDataManagerConfig.getTableName();

    LOGGER = LoggerFactory.getLogger(_tableName + "-OfflineTableDataManager");
    _currentNumberOfSegments =
        Metrics.newCounter(OfflineTableDataManager.class, _tableName + "-"
            + CommonConstants.Metric.Server.CURRENT_NUMBER_OF_SEGMENTS);
    _currentNumberOfDocuments =
        Metrics.newCounter(OfflineTableDataManager.class, _tableName + "-"
            + CommonConstants.Metric.Server.CURRENT_NUMBER_OF_DOCUMENTS);
    _numDeletedSegments =
        Metrics.newCounter(OfflineTableDataManager.class, _tableName + "-"
            + CommonConstants.Metric.Server.NUMBER_OF_DELETED_SEGMENTS);

    _tableDataDir = _tableDataManagerConfig.getDataDir();
    if (!new File(_tableDataDir).exists()) {
      new File(_tableDataDir).mkdirs();
    }
    _numberOfTableQueryExecutorThreads = _tableDataManagerConfig.getNumberOfTableQueryExecutorThreads();
    //_numberOfTableQueryExecutorThreads = 1;
    if (_numberOfTableQueryExecutorThreads > 0) {
      _queryExecutorService =
          Executors.newFixedThreadPool(_numberOfTableQueryExecutorThreads, new NamedThreadFactory(
              "parallel-query-executor-" + _tableName));
    } else {
      _queryExecutorService =
          Executors.newCachedThreadPool(new NamedThreadFactory("parallel-query-executor-" + _tableName));
    }
    _readMode = ReadMode.valueOf(_tableDataManagerConfig.getReadMode());
    _indexLoadingConfigMetadata = _tableDataManagerConfig.getIndexLoadingConfigMetadata();
    LOGGER
        .info("Initialized table : " + _tableName + " with :\n\tData Directory: " + _tableDataDir
            + "\n\tRead Mode : " + _readMode + "\n\tQuery Exeutor with "
            + ((_numberOfTableQueryExecutorThreads > 0) ? _numberOfTableQueryExecutorThreads : "cached")
            + " threads");
  }

  @Override
  public void start() {
    LOGGER.info("Trying to start table : " + _tableName);
    if (_tableDataManagerConfig != null) {
      if (_isStarted) {
        LOGGER.warn("Already start the OfflineTableDataManager for table : " + _tableName);
      } else {
        _isStarted = true;
      }
    } else {
      LOGGER.error("The OfflineTableDataManager hasn't been initialized.");
    }
  }

  @Override
  public void shutDown() {
    LOGGER.info("Trying to shutdown table : " + _tableName);
    if (_isStarted) {
      _queryExecutorService.shutdown();
      _tableDataManagerConfig = null;
      _isStarted = false;
    } else {
      LOGGER.warn("Already shutDown table : " + _tableName);
    }
  }

  @Override
  public void addSegment(SegmentMetadata segmentMetadata) throws Exception {
    IndexSegment indexSegment =
        ColumnarSegmentLoader.loadSegment(segmentMetadata, _readMode, _indexLoadingConfigMetadata);
    addSegment(indexSegment);
  }

  // Add or a segment (or, replace it if it exists with the same name).
  public void addSegment(final IndexSegment indexSegmentToAdd) {
    final String segmentName = indexSegmentToAdd.getSegmentName();
    LOGGER.info("Trying to add a new segment " + segmentName + " to table : " + _tableName);
    OfflineSegmentDataManager newSegmentManager = new OfflineSegmentDataManager(indexSegmentToAdd);
    final int newNumDocs = indexSegmentToAdd.getSegmentMetadata().getTotalRawDocs();
    OfflineSegmentDataManager oldSegmentManager;
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
    _currentNumberOfDocuments.inc(newNumDocs);
    _currentNumberOfSegments.inc();
  }

  @Override
  public void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, SegmentZKMetadata segmentZKMetadata) throws Exception {
    // This call should never happen since it is called for realtime segments only
//    addSegment(segmentZKMetadata);
    throw new UnsupportedOperationException("Not supported for Offline segments");
  }

  // Called when we get a helix transition to go to offline or dropped state.
  // We need to remove it safely, keeping in mind that there may be queries that are
  // using the segment,
  @Override
  public void removeSegment(String segmentName) {
    if (!_isStarted) {
      LOGGER.warn("Could not remove segment {}, Tracker is stopped", segmentName);
      return;
    }

    OfflineSegmentDataManager segmentDataManager;
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

  private void closeSegment(SegmentDataManager segmentDataManager) {
    final String segmentName = segmentDataManager.getSegmentName();
    LOGGER.info("Closing segment {} for table {}", segmentName, _tableName);
    _currentNumberOfSegments.dec();
    _numDeletedSegments.inc();
    _currentNumberOfDocuments.dec(segmentDataManager.getSegment().getSegmentMetadata().getTotalRawDocs());
    segmentDataManager.getSegment().destroy();
    LOGGER.info("Segment {} for table {} has been closed", segmentName, _tableName);
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  @Override
  public ExecutorService getExecutorService() {
    return _queryExecutorService;
  }

  @Override
  public List<SegmentDataManager> acquireSegments(List<String> segmentList) {
    List<SegmentDataManager> ret = new ArrayList<SegmentDataManager>();
    try {
      _rwLock.readLock().lock();
      for (String segName : segmentList) {
        OfflineSegmentDataManager segmentDataManager;
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
  public OfflineSegmentDataManager acquireSegment(String segmentName) {
    try {
      _rwLock.readLock().lock();
      OfflineSegmentDataManager segmentDataManager = _segmentsMap.get(segmentName);
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
    int refCnt = ((OfflineSegmentDataManager)segmentDataManager).decrementRefCnt();
    // Exactly one thread should find this to be zero, so we can safely drop it.
    // We never remove it from the map here, so no need to synchronize.
    if (refCnt == 0) {
      closeSegment(segmentDataManager);
    }
  }
}
