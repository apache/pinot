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
package com.linkedin.pinot.core.data.manager.realtime;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.helix.PinotHelixPropertyStoreZnRecordProvider;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.OfflineSegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;


public class RealtimeTableDataManager implements TableDataManager {
  private Logger LOGGER = LoggerFactory.getLogger(RealtimeTableDataManager.class);

  private final Object _globalLock = new Object();
  private boolean _isStarted = false;
  private File _indexDir;
  private ReadMode _readMode;
  private TableDataManagerConfig _tableDataManagerConfig;
  private String _tableDataDir;
  private int _numberOfTableQueryExecutorThreads;
  private IndexLoadingConfigMetadata _indexLoadingConfigMetadata;
  private ExecutorService _queryExecutorService;

  private final Map<String, SegmentDataManager> _segmentsMap = new HashMap<String, SegmentDataManager>();

  private final ExecutorService _segmentAsyncExecutorService = Executors
      .newSingleThreadExecutor(new NamedThreadFactory("SegmentAsyncExecutorService"));
  private final List<String> _activeSegments = new ArrayList<String>();
  private final List<String> _loadingSegments = new ArrayList<String>();

  private Map<String, AtomicInteger> _referenceCounts = new HashMap<String, AtomicInteger>();
  private ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;

  private String _tableName;

  private Counter _currentNumberOfSegments = Metrics.newCounter(RealtimeTableDataManager.class,
      CommonConstants.Metric.Server.CURRENT_NUMBER_OF_SEGMENTS);
  private Counter _currentNumberOfDocuments = Metrics.newCounter(RealtimeTableDataManager.class,
      CommonConstants.Metric.Server.CURRENT_NUMBER_OF_DOCUMENTS);
  private Counter _numDeletedSegments = Metrics.newCounter(RealtimeTableDataManager.class,
      CommonConstants.Metric.Server.NUMBER_OF_DELETED_SEGMENTS);

  @Override
  public void init(TableDataManagerConfig tableDataManagerConfig) {
    _tableDataManagerConfig = tableDataManagerConfig;
    _tableName = _tableDataManagerConfig.getTableName();
    LOGGER = LoggerFactory.getLogger(_tableName + "-RealtimeTableDataManager");
    _currentNumberOfSegments =
        Metrics.newCounter(RealtimeTableDataManager.class, _tableName + "-"
            + CommonConstants.Metric.Server.CURRENT_NUMBER_OF_SEGMENTS);
    _currentNumberOfDocuments =
        Metrics.newCounter(RealtimeTableDataManager.class, _tableName + "-"
            + CommonConstants.Metric.Server.CURRENT_NUMBER_OF_DOCUMENTS);
    _numDeletedSegments =
        Metrics.newCounter(RealtimeTableDataManager.class, _tableName + "-"
            + CommonConstants.Metric.Server.NUMBER_OF_DELETED_SEGMENTS);

    _tableDataDir = _tableDataManagerConfig.getDataDir();
    if (!new File(_tableDataDir).exists()) {
      new File(_tableDataDir).mkdirs();
    }
    _numberOfTableQueryExecutorThreads = _tableDataManagerConfig.getNumberOfTableQueryExecutorThreads();
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
        .info("Initialized RealtimeTableDataManager: table : " + _tableName + " with :\n\tData Directory: "
            + _tableDataDir + "\n\tRead Mode : " + _readMode + "\n\tQuery Exeutor with "
            + ((_numberOfTableQueryExecutorThreads > 0) ? _numberOfTableQueryExecutorThreads : "cached")
            + " threads");
  }

  @Override
  public void start() {
    if (_isStarted) {
      LOGGER.warn("RealtimeTableDataManager is already started.");
      return;
    }
    _indexDir = new File(_tableDataManagerConfig.getDataDir());
    _readMode = ReadMode.valueOf(_tableDataManagerConfig.getReadMode());
    if (!_indexDir.exists()) {
      if (!_indexDir.mkdir()) {
        LOGGER.error("could not create data dir");
      }
    }
    _isStarted = true;
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

  public void notify(RealtimeSegmentZKMetadata metadata) {
    ZKMetadataProvider.setRealtimeSegmentZKMetadata(_helixPropertyStore, metadata);
    markSegmentAsLoaded(metadata.getSegmentName());
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  @Override
  public void addSegment(SegmentZKMetadata segmentMetadata) throws Exception {
    throw new UnsupportedOperationException("Cannot add realtime segment with just SegmentZKMetadata");
  }

  @Override
  public void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, SegmentZKMetadata segmentZKMetadata) throws Exception {
    this._helixPropertyStore = propertyStore;
    String segmentId = segmentZKMetadata.getSegmentName();
    if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
      if (new File(_indexDir, segmentId).exists()
          && ((RealtimeSegmentZKMetadata) segmentZKMetadata).getStatus() == Status.DONE) {
        // segment already exists on file, simply load it and add it to the map
        if (!_segmentsMap.containsKey(segmentId)) {
          synchronized (getGlobalLock()) {
            if (!_segmentsMap.containsKey(segmentId)) {
              IndexSegment segment =
                  ColumnarSegmentLoader.load(new File(_indexDir, segmentId), _readMode, _indexLoadingConfigMetadata);
              _segmentsMap.put(segmentId, new OfflineSegmentDataManager(segment));
              markSegmentAsLoaded(segmentId);
              _referenceCounts.put(segmentId, new AtomicInteger(1));
            }
          }
        }
      } else {
        if (!_segmentsMap.containsKey(segmentId)) {
          synchronized (getGlobalLock()) {
            if (!_segmentsMap.containsKey(segmentId)) {
              // this is a new segment, lets create an instance of RealtimeSegmentDataManager
              PinotHelixPropertyStoreZnRecordProvider propertyStoreHelper =
                  PinotHelixPropertyStoreZnRecordProvider.forSchema(propertyStore);
              String path =
                  StringUtil.join("/", propertyStoreHelper.getRelativePath(), tableConfig.getValidationConfig()
                      .getSchemaName());
              List<String> schemas = propertyStore.getChildNames(path, AccessOption.PERSISTENT);
              List<Integer> schemasIds = new ArrayList<Integer>();
              for (String id : schemas) {
                schemasIds.add(Integer.parseInt(id));
              }
              Collections.sort(schemasIds);
              String latest = String.valueOf(schemasIds.get(schemasIds.size() - 1));
              LOGGER.info("found schema {} with version {}", tableConfig.getValidationConfig().getSchemaName(), latest);

              ZNRecord record =
                  propertyStoreHelper.get(tableConfig.getValidationConfig().getSchemaName() + "/" + latest);

              SegmentDataManager manager =
                  new RealtimeSegmentDataManager((RealtimeSegmentZKMetadata) segmentZKMetadata, tableConfig,
                      instanceZKMetadata, this, _indexDir.getAbsolutePath(), _readMode, Schema.fromZNRecord(record));
              LOGGER.info("Initialize RealtimeSegmentDataManager - " + segmentId);
              _segmentsMap.put(segmentId, manager);
              _loadingSegments.add(segmentId);
              _referenceCounts.put(segmentId, new AtomicInteger(1));
            }
          }
        }
      }
    }

  }

  public void updateStatus() {

  }

  @Override
  public void addSegment(IndexSegment indexSegmentToAdd) {
    throw new UnsupportedOperationException("Not supported addSegment(IndexSegment) in RealtimeTableDataManager");
  }

  @Override
  public void addSegment(SegmentMetadata segmentMetaToAdd) throws Exception {
    throw new UnsupportedOperationException("Not supported addSegment(SegmentMetadata) in RealtimeTableDataManager");
  }

  @Override
  public void removeSegment(String segmentToRemove) {
    decrementCount(segmentToRemove);
  }

  public void incrementCount(final String segmentId) {
    _referenceCounts.get(segmentId).incrementAndGet();
  }

  private void markSegmentAsLoaded(String segmentId) {
    _currentNumberOfSegments.inc();
    if (_segmentsMap.containsKey(segmentId)) {
      _currentNumberOfDocuments.inc(_segmentsMap.get(segmentId).getSegment().getTotalDocs());
    }
    _loadingSegments.remove(segmentId);
    if (!_activeSegments.contains(segmentId)) {
      _activeSegments.add(segmentId);
    }
  }

  public void decrementCount(final String segmentId) {
    if (!_referenceCounts.containsKey(segmentId)) {
      LOGGER.warn("Received command to delete unexisting segment - " + segmentId);
      return;
    }

    AtomicInteger count = _referenceCounts.get(segmentId);

    if (count.get() == 1) {
      SegmentDataManager segment = null;
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
  public List<SegmentDataManager> getAllSegments() {
    List<SegmentDataManager> ret = new ArrayList<SegmentDataManager>();
    synchronized (getGlobalLock()) {
      for (SegmentDataManager segment : _segmentsMap.values()) {
        incrementCount(segment.getSegmentName());
        ret.add(segment);
      }
    }
    return ret;
  }

  @Override
  public List<SegmentDataManager> getSegments(List<String> segmentList) {
    List<SegmentDataManager> ret = new ArrayList<SegmentDataManager>();
    synchronized (getGlobalLock()) {
      for (String segmentName : segmentList) {
        if (_segmentsMap.containsKey(segmentName)) {
          incrementCount(segmentName);
          ret.add(_segmentsMap.get(segmentName));
        }
      }
    }
    return ret;
  }

  @Override
  public SegmentDataManager getSegment(String segmentName) {
    if (_segmentsMap.containsKey(segmentName)) {
      incrementCount(segmentName);
      return _segmentsMap.get(segmentName);
    } else {
      return null;
    }
  }

  @Override
  public void returnSegmentReaders(List<String> segmentList) {
    synchronized (getGlobalLock()) {
      for (String segmentId : segmentList) {
        decrementCount(segmentId);
      }
    }
  }

  @Override
  public ExecutorService getExecutorService() {
    return _queryExecutorService;
  }

  public Object getGlobalLock() {
    return _globalLock;
  }
}
