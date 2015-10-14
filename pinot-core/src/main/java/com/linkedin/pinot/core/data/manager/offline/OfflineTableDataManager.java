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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;


/**
 * An implemenation of offline TableDataManager.
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
  private final ExecutorService _segmentAsyncExecutorService = Executors
      .newSingleThreadExecutor(new NamedThreadFactory("SegmentAsyncExecutorService"));
  private String _tableDataDir;
  private int _numberOfTableQueryExecutorThreads;
  private IndexLoadingConfigMetadata _indexLoadingConfigMetadata;

  private final Map<String, OfflineSegmentDataManager> _segmentsMap = new ConcurrentHashMap<String, OfflineSegmentDataManager>();
  private final List<String> _activeSegments = new ArrayList<String>();
  private final List<String> _loadingSegments = new ArrayList<String>();
  private Map<String, AtomicInteger> _referenceCounts = new ConcurrentHashMap<String, AtomicInteger>();

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
      _segmentAsyncExecutorService.shutdown();
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
    LOGGER.info("Added IndexSegment : " + indexSegment.getSegmentName() + " to table : " + _tableName);
    addSegment(indexSegment);
  }

  @Override
  public void addSegment(final IndexSegment indexSegmentToAdd) {
    LOGGER.info("Trying to add a new segment to table : " + _tableName);

    synchronized (getGlobalLock()) {
      if (!_segmentsMap.containsKey(indexSegmentToAdd.getSegmentName())) {
        LOGGER.info("Trying to add segment - " + indexSegmentToAdd.getSegmentName());
        _segmentsMap.put(indexSegmentToAdd.getSegmentName(), new OfflineSegmentDataManager(indexSegmentToAdd));
        markSegmentAsLoaded(indexSegmentToAdd.getSegmentName());
        _referenceCounts.put(indexSegmentToAdd.getSegmentName(), new AtomicInteger(1));
      } else {
        LOGGER.info("Trying to refresh segment - " + indexSegmentToAdd.getSegmentName());
        OfflineSegmentDataManager segment = _segmentsMap.get(indexSegmentToAdd.getSegmentName());
        _segmentsMap.put(indexSegmentToAdd.getSegmentName(), new OfflineSegmentDataManager(indexSegmentToAdd));
        if (segment != null) {
          _currentNumberOfDocuments.dec(segment.getSegment().getTotalDocs());
          _currentNumberOfDocuments.inc(indexSegmentToAdd.getTotalDocs());
          segment.getSegment().destroy();
        }
      }
    }
  }

  @Override
  public void addSegment(SegmentZKMetadata indexSegmentToAdd) throws Exception {
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl((OfflineSegmentZKMetadata) indexSegmentToAdd);
    IndexSegment indexSegment =
        ColumnarSegmentLoader.loadSegment(segmentMetadata, _readMode, _indexLoadingConfigMetadata);
    LOGGER.info("Added IndexSegment : " + indexSegment.getSegmentName() + " to table : " + _tableName);
    addSegment(indexSegment);
  }

  @Override
  public void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, SegmentZKMetadata segmentZKMetadata) throws Exception {
    addSegment(segmentZKMetadata);
  }

  @Override
  public void removeSegment(String indexSegmentToRemove) {
    if (!_isStarted) {
      LOGGER.warn("Could not remove segment, as the tracker is already stopped");
      return;
    }
    decrementCount(indexSegmentToRemove);
  }

  public void decrementCount(final String segmentId) {
    if (!_referenceCounts.containsKey(segmentId)) {
      LOGGER.warn("Received command to delete unexisting segment - " + segmentId);
      return;
    }

    AtomicInteger count = _referenceCounts.get(segmentId);

    if (count.get() == 1) {
      OfflineSegmentDataManager segment = null;
      synchronized (getGlobalLock()) {
        if (count.get() == 1) {
          segment = _segmentsMap.remove(segmentId);
          _activeSegments.remove(segmentId);
          _referenceCounts.remove(segmentId);
        }
      }
      if (segment != null) {
        _currentNumberOfSegments.dec();
        _currentNumberOfDocuments.dec(segment.getSegment().getTotalDocs());
        _numDeletedSegments.inc();
        segment.getSegment().destroy();
      }
      LOGGER.info("Segment " + segmentId + " has been deleted");
      _segmentAsyncExecutorService.execute(new Runnable() {
        @Override
        public void run() {
          FileUtils.deleteQuietly(new File(_tableDataDir, segmentId));
          LOGGER.info("The index directory for the segment " + segmentId + " has been deleted");
        }
      });

    } else {
      count.decrementAndGet();
    }
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  public Object getGlobalLock() {
    return _globalLock;
  }

  private void markSegmentAsLoaded(String segmentId) {
    _currentNumberOfSegments.inc();
    _currentNumberOfDocuments.inc(_segmentsMap.get(segmentId).getSegment().getTotalDocs());

    if (!_activeSegments.contains(segmentId)) {
      _activeSegments.add(segmentId);
    }
    _loadingSegments.remove(segmentId);
    if (!_referenceCounts.containsKey(segmentId)) {
      _referenceCounts.put(segmentId, new AtomicInteger(1));
    }
  }

  public List<String> getActiveSegments() {
    return _activeSegments;
  }

  public List<String> getLoadingSegments() {
    return _loadingSegments;
  }

  @Override
  public List<SegmentDataManager> getAllSegments() {
    List<SegmentDataManager> ret = new ArrayList<SegmentDataManager>();
    for (OfflineSegmentDataManager segment : _segmentsMap.values()) {
      incrementCount(segment.getSegmentName());
      ret.add(segment);
    }
    return ret;
  }

  public void incrementCount(final String segmentId) {
    _referenceCounts.get(segmentId).incrementAndGet();
  }

  @Override
  public ExecutorService getExecutorService() {
    return _queryExecutorService;
  }

  @Override
  public List<SegmentDataManager> getSegments(List<String> segmentList) {
    List<SegmentDataManager> ret = new ArrayList<SegmentDataManager>();
    for (String segmentName : segmentList) {
      if (_segmentsMap.containsKey(segmentName)) {
        incrementCount(segmentName);
        ret.add(_segmentsMap.get(segmentName));
      }
    }
    return ret;
  }

  @Override
  public OfflineSegmentDataManager getSegment(String segmentName) {
    if (_segmentsMap.containsKey(segmentName)) {
      incrementCount(segmentName);
      return _segmentsMap.get(segmentName);
    } else {
      return null;
    }
  }

  @Override
  public void returnSegmentReaders(List<String> segmentList) {
    for (String segmentId : segmentList) {
      returnSegmentReader(segmentId);
    }
  }

  @Override
  public void returnSegmentReader(String segmentId) {
    decrementCount(segmentId);
  }

}
