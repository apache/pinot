package com.linkedin.pinot.core.data.manager;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.core.data.manager.config.ResourceDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;


/**
 * An implemenation of offline ResourceDataManager.
 * Provide add and remove segment functionality.
 * 
 * @author xiafu
 *
 */
public class OfflineResourceDataManager implements ResourceDataManager {
  private Logger _logger = LoggerFactory.getLogger(OfflineResourceDataManager.class);

  private volatile boolean _isStarted = false;
  private final Object _globalLock = new Object();
  private String _resourceName;
  private ReadMode _readMode;

  private ExecutorService _queryExecutorService;

  private ResourceDataManagerConfig _resourceDataManagerConfig;
  private final ExecutorService _segmentAsyncExecutorService = Executors
      .newSingleThreadExecutor(new NamedThreadFactory("SegmentAsyncExecutorService"));;
  private String _resourceDataDir;
  private int _numberOfResourceQueryExecutorThreads;

  private final Map<String, SegmentDataManager> _segmentsMap = new HashMap<String, SegmentDataManager>();
  private final List<String> _activeSegments = new ArrayList<String>();
  private final List<String> _loadingSegments = new ArrayList<String>();
  private Map<String, AtomicInteger> _referenceCounts = new HashMap<String, AtomicInteger>();

  private Counter _currentNumberOfSegments = Metrics.newCounter(OfflineResourceDataManager.class,
      CommonConstants.Metric.Server.CURRENT_NUMBER_OF_SEGMENTS);
  private Counter _currentNumberOfDocuments = Metrics.newCounter(OfflineResourceDataManager.class,
      CommonConstants.Metric.Server.CURRENT_NUMBER_OF_DOCUMENTS);
  private Counter _numDeletedSegments = Metrics.newCounter(OfflineResourceDataManager.class,
      CommonConstants.Metric.Server.NUMBER_OF_DELETED_SEGMENTS);

  public OfflineResourceDataManager() {
  }

  @Override
  public void init(ResourceDataManagerConfig resourceDataManagerConfig) {
    _resourceDataManagerConfig = resourceDataManagerConfig;
    _resourceName = _resourceDataManagerConfig.getResourceName();

    _logger = LoggerFactory.getLogger(_resourceName + "-OfflineResourceDataManager");
    _currentNumberOfSegments = Metrics.newCounter(OfflineResourceDataManager.class, _resourceName + "-" + CommonConstants.Metric.Server.CURRENT_NUMBER_OF_SEGMENTS);
    _currentNumberOfDocuments = Metrics.newCounter(OfflineResourceDataManager.class, _resourceName + "-" + CommonConstants.Metric.Server.CURRENT_NUMBER_OF_DOCUMENTS);
    _numDeletedSegments = Metrics.newCounter(OfflineResourceDataManager.class, _resourceName + "-" + CommonConstants.Metric.Server.NUMBER_OF_DELETED_SEGMENTS);

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
        .info("Initialized resource : " + _resourceName + " with :\n\tData Directory: " + _resourceDataDir
            + "\n\tRead Mode : " + _readMode + "\n\tQuery Exeutor with "
            + ((_numberOfResourceQueryExecutorThreads > 0) ? _numberOfResourceQueryExecutorThreads : "cached")
            + " threads");
  }

  @Override
  public void start() {
    _logger.info("Trying to start resource : " + _resourceName);
    if (_resourceDataManagerConfig != null) {
      if (_isStarted) {
        _logger.warn("Already start the OfflineResourceDataManager for resource : " + _resourceName);
      } else {
        _isStarted = true;
      }
    } else {
      _logger.error("The OfflineResourceDataManager hasn't been initialized.");
    }
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

  @Override
  public void addSegment(SegmentMetadata segmentMetadata) throws Exception {
    IndexSegment indexSegment = ColumnarSegmentLoader.loadSegment(segmentMetadata, _readMode);
    _logger.info("Added IndexSegment : " + indexSegment.getSegmentName() + " to resource : " + _resourceName);
    addSegment(indexSegment);

  }

  @Override
  public void addSegment(final IndexSegment indexSegmentToAdd) {
    _logger.info("Trying to add a new segment to resource : " + _resourceName);

    synchronized (getGlobalLock()) {
      if (!_segmentsMap.containsKey(indexSegmentToAdd.getSegmentName())) {
        _logger.info("Trying to add segment - " + indexSegmentToAdd.getSegmentName());
        _segmentsMap.put(indexSegmentToAdd.getSegmentName(), new SegmentDataManager(indexSegmentToAdd));
        markSegmentAsLoaded(indexSegmentToAdd.getSegmentName());
        _referenceCounts.put(indexSegmentToAdd.getSegmentName(), new AtomicInteger(1));
      } else {
        _logger.info("Trying to refresh segment - " + indexSegmentToAdd.getSegmentName());
        SegmentDataManager segment = _segmentsMap.get(indexSegmentToAdd.getSegmentName());
        _segmentsMap.put(indexSegmentToAdd.getSegmentName(), new SegmentDataManager(indexSegmentToAdd));
        if (segment != null) {
          _currentNumberOfDocuments.dec(segment.getSegment().getSegmentMetadata().getTotalDocs());
          _currentNumberOfDocuments.inc(indexSegmentToAdd.getSegmentMetadata().getTotalDocs());
          segment.getSegment().destroy();
        }
      }
    }
  }

  private void refreshSegment(final IndexSegment segmentToRefresh) {
    synchronized (getGlobalLock()) {
      SegmentDataManager segment = _segmentsMap.get(segmentToRefresh.getSegmentName());
      _segmentsMap.put(segmentToRefresh.getSegmentName(), new SegmentDataManager(segmentToRefresh));
      if (segment != null) {
        _currentNumberOfDocuments.dec(segment.getSegment().getSegmentMetadata().getTotalDocs());
        _currentNumberOfDocuments.inc(segmentToRefresh.getSegmentMetadata().getTotalDocs());
        segment.getSegment().destroy();
      }
    }
  }

  @Override
  public void removeSegment(String indexSegmentToRemove) {
    if (!_isStarted) {
      _logger.warn("Could not remove segment, as the tracker is already stopped");
      return;
    }
    decrementCount(indexSegmentToRemove);
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
        _currentNumberOfDocuments.dec(segment.getSegment().getSegmentMetadata().getTotalDocs());
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
  public boolean isStarted() {
    return _isStarted;
  }

  public Object getGlobalLock() {
    return _globalLock;
  }

  private void markSegmentAsLoaded(String segmentId) {
    _currentNumberOfSegments.inc();
    _currentNumberOfDocuments.inc(_segmentsMap.get(segmentId).getSegment().getSegmentMetadata().getTotalDocs());

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
    synchronized (getGlobalLock()) {
      for (SegmentDataManager segment : _segmentsMap.values()) {
        incrementCount(segment.getSegmentName());
        ret.add(segment);
      }
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
      synchronized (getGlobalLock()) {
        incrementCount(segmentName);
      }
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

}
