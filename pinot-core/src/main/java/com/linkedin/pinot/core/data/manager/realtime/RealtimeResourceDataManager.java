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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.DataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.core.data.manager.config.ResourceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.OfflineSegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.ResourceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;


public class RealtimeResourceDataManager implements ResourceDataManager {
  private Logger _logger = LoggerFactory.getLogger(RealtimeResourceDataManager.class);

  private final Object _globalLock = new Object();
  private boolean _isStarted = false;
  private File _indexDir;
  private ReadMode _readMode;
  private ResourceDataManagerConfig _resourceDataManagerConfig;
  private String _resourceDataDir;
  private int _numberOfResourceQueryExecutorThreads;
  private ExecutorService _queryExecutorService;

  private final Map<String, SegmentDataManager> _segmentsMap = new HashMap<String, SegmentDataManager>();

  private final ExecutorService _segmentAsyncExecutorService = Executors
      .newSingleThreadExecutor(new NamedThreadFactory("SegmentAsyncExecutorService"));
  private final List<String> _activeSegments = new ArrayList<String>();
  private final List<String> _loadingSegments = new ArrayList<String>();

  private Map<String, AtomicInteger> _referenceCounts = new HashMap<String, AtomicInteger>();
  private ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;

  private String _resourceName;

  private Counter _currentNumberOfSegments = Metrics.newCounter(RealtimeResourceDataManager.class,
      CommonConstants.Metric.Server.CURRENT_NUMBER_OF_SEGMENTS);
  private Counter _currentNumberOfDocuments = Metrics.newCounter(RealtimeResourceDataManager.class,
      CommonConstants.Metric.Server.CURRENT_NUMBER_OF_DOCUMENTS);
  private Counter _numDeletedSegments = Metrics.newCounter(RealtimeResourceDataManager.class,
      CommonConstants.Metric.Server.NUMBER_OF_DELETED_SEGMENTS);

  @Override
  public void init(ResourceDataManagerConfig resourceDataManagerConfig) {
    _resourceDataManagerConfig = resourceDataManagerConfig;
    _resourceName = _resourceDataManagerConfig.getResourceName();
    _logger = LoggerFactory.getLogger(_resourceName + "-RealtimeResourceDataManager");
    _currentNumberOfSegments =
        Metrics.newCounter(RealtimeResourceDataManager.class, _resourceName + "-"
            + CommonConstants.Metric.Server.CURRENT_NUMBER_OF_SEGMENTS);
    _currentNumberOfDocuments =
        Metrics.newCounter(RealtimeResourceDataManager.class, _resourceName + "-"
            + CommonConstants.Metric.Server.CURRENT_NUMBER_OF_DOCUMENTS);
    _numDeletedSegments =
        Metrics.newCounter(RealtimeResourceDataManager.class, _resourceName + "-"
            + CommonConstants.Metric.Server.NUMBER_OF_DELETED_SEGMENTS);

    _resourceDataDir = _resourceDataManagerConfig.getDataDir();
    if (!new File(_resourceDataDir).exists()) {
      new File(_resourceDataDir).mkdirs();
    }
    _numberOfResourceQueryExecutorThreads = _resourceDataManagerConfig.getNumberOfResourceQueryExecutorThreads();
    if (_numberOfResourceQueryExecutorThreads > 0) {
      _queryExecutorService =
          Executors.newFixedThreadPool(_numberOfResourceQueryExecutorThreads, new NamedThreadFactory(
              "parallel-query-executor-" + _resourceName));
    } else {
      _queryExecutorService =
          Executors.newCachedThreadPool(new NamedThreadFactory("parallel-query-executor-" + _resourceName));
    }
    _readMode = ReadMode.valueOf(_resourceDataManagerConfig.getReadMode());
    _logger
        .info("Initialized RealtimeResourceDataManager: resource : " + _resourceName + " with :\n\tData Directory: " + _resourceDataDir
            + "\n\tRead Mode : " + _readMode + "\n\tQuery Exeutor with "
            + ((_numberOfResourceQueryExecutorThreads > 0) ? _numberOfResourceQueryExecutorThreads : "cached")
            + " threads");
  }

  @Override
  public void start() {
    if (_isStarted) {
      _logger.warn("RealtimeResourceDataManager is already started.");
      return;
    }
    _indexDir = new File(_resourceDataManagerConfig.getDataDir());
    _readMode = ReadMode.valueOf(_resourceDataManagerConfig.getReadMode());
    if (!_indexDir.exists()) {
      if (!_indexDir.mkdir()) {
        _logger.error("could not create data dir");
      }
    }
    _isStarted = true;
  }

  @Override
  public void shutDown() {
    _logger.info("Trying to shutdown resource : " + _resourceName);
    if (_isStarted) {
      _queryExecutorService.shutdown();
      _segmentAsyncExecutorService.shutdown();
      _resourceDataManagerConfig = null;
      _isStarted = false;
    } else {
      _logger.warn("Already shutDown resource : " + _resourceName);
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

  public void addSegment(SegmentZKMetadata segmentMetadata) throws Exception {
    throw new UnsupportedOperationException("Cannot add realtime segment with just SegmentZKMetadata");
  }

  @Override
  public void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, DataResourceZKMetadata dataResourceZKMetadata, InstanceZKMetadata instanceZKMetadata,
      SegmentZKMetadata segmentZKMetadata) throws Exception {
    this._helixPropertyStore = propertyStore;
    String segmentId = segmentZKMetadata.getSegmentName();
    if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
      if (new File(_indexDir, segmentId).exists()) {
        // segment already exists on file, simply load it and add it to the map
        if (!_segmentsMap.containsKey(segmentId)) {
          synchronized (getGlobalLock()) {
            if (!_segmentsMap.containsKey(segmentId)) {
              IndexSegment segment = ColumnarSegmentLoader.load(new File(_indexDir, segmentId), _readMode);
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
              SegmentDataManager manager =
                  new RealtimeSegmentDataManager((RealtimeSegmentZKMetadata) segmentZKMetadata, (RealtimeDataResourceZKMetadata) dataResourceZKMetadata,
                      instanceZKMetadata, this, _indexDir.getAbsolutePath(), _readMode);
              _logger.info("Initialize RealtimeSegmentDataManager - " + segmentId);
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
    throw new UnsupportedOperationException("Not supported addSegment(IndexSegment) in RealtimeResourceDataManager");
  }

  @Override
  public void addSegment(SegmentMetadata segmentMetaToAdd) throws Exception {
    throw new UnsupportedOperationException("Not supported addSegment(SegmentMetadata) in RealtimeResourceDataManager");
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
      _logger.warn("Received command to delete unexisting segment - " + segmentId);
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
      _logger.info("Segment " + segmentId + " has been deleted");
      _segmentAsyncExecutorService.execute(new Runnable() {
        @Override
        public void run() {
          FileUtils.deleteQuietly(new File(_resourceDataDir, segmentId));
          _logger.info("The index directory for the segment " + segmentId + " has been deleted");
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
