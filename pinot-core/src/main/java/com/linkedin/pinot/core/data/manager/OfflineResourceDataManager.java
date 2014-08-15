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
  private static Logger LOGGER = LoggerFactory.getLogger(OfflineResourceDataManager.class);

  private volatile boolean _isStarted = false;
  private Object _globalLock = new Object();
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

  private static final Counter _currentNumberOfSegments = Metrics.newCounter(OfflineResourceDataManager.class,
      "currentNumberOfSegments");
  private static final Counter _currentNumberOfDocuments = Metrics.newCounter(OfflineResourceDataManager.class,
      "currentNumberOfDocuments");
  private static final Counter _numDeletedSegments = Metrics.newCounter(OfflineResourceDataManager.class,
      "numberOfDeletedSegments");

  public OfflineResourceDataManager() {
  }

  @Override
  public void init(ResourceDataManagerConfig resourceDataManagerConfig) {
    _resourceDataManagerConfig = resourceDataManagerConfig;
    _resourceName = _resourceDataManagerConfig.getResourceName();
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
    LOGGER
        .info("Initialized resource : " + _resourceName + " with :\n\tData Directory: " + _resourceDataDir
            + "\n\tRead Mode : " + _readMode + "\n\tQuery Exeutor with "
            + ((_numberOfResourceQueryExecutorThreads > 0) ? _numberOfResourceQueryExecutorThreads : "cached")
            + " threads");
  }

  @Override
  public void start() {
    LOGGER.info("Trying to start resource : " + _resourceName);
    if (_resourceDataManagerConfig != null) {
      if (_isStarted) {
        LOGGER.warn("Already start the OfflineResourceDataManager for resource : " + _resourceName);
      } else {
        bootstrapSegments();
        _isStarted = true;
      }
    } else {
      LOGGER.error("The OfflineResourceDataManager hasn't been initialized.");
    }
  }

  private void bootstrapSegments() {
    File resourceDataDir = new File(_resourceDataDir);
    LOGGER.info("Bootstrap ResourceDataManager data directory - " + resourceDataDir.getAbsolutePath());
    for (File segmentDir : resourceDataDir.listFiles()) {
      try {
        LOGGER.info("Bootstrap segment from directory - " + segmentDir.getAbsolutePath());
        IndexSegment indexSegment = ColumnarSegmentLoader.load(segmentDir, ReadMode.heap);
        addSegment(indexSegment);
      } catch (Exception e) {
        LOGGER.error("Unable to bootstrap segment in dir : " + segmentDir.getAbsolutePath());
      }
    }
  }

  @Override
  public void shutDown() {
    LOGGER.info("Trying to shutdown resource : " + _resourceName);
    if (_isStarted) {
      _queryExecutorService.shutdown();
      _segmentAsyncExecutorService.shutdown();
      _resourceDataManagerConfig = null;
      _isStarted = false;
    } else {
      LOGGER.warn("Already shutDown resource : " + _resourceName);
    }
  }

  public void addSegment(SegmentMetadata segmentMetadata) throws Exception {
    IndexSegment indexSegment = ColumnarSegmentLoader.loadSegment(segmentMetadata, _readMode);
    addSegment(indexSegment);
  }

  @Override
  public void addSegment(final IndexSegment indexSegmentToAdd) {
    LOGGER.info("Trying to add a new segment to resource : " + _resourceName);

    synchronized (getGlobalLock()) {
      if (!_segmentsMap.containsKey(indexSegmentToAdd.getSegmentName())) {
        LOGGER.info("Trying to add segment - " + indexSegmentToAdd.getSegmentName());
        _segmentsMap.put(indexSegmentToAdd.getSegmentName(), new SegmentDataManager(indexSegmentToAdd));
        markSegmentAsLoaded(indexSegmentToAdd.getSegmentName());
        _referenceCounts.put(indexSegmentToAdd.getSegmentName(), new AtomicInteger(1));
      } else {
        LOGGER.info("Trying to refresh segment - " + indexSegmentToAdd.getSegmentName());
        refreshSegment(indexSegmentToAdd);
      }
    }
  }

  private void refreshSegment(final IndexSegment segmentToRefresh) {
    synchronized (getGlobalLock()) {
      SegmentDataManager segment = _segmentsMap.get(segmentToRefresh.getSegmentName());
      if (segment != null) {
        _currentNumberOfDocuments.dec(segment.getSegment().getSegmentMetadata().getTotalDocs());
        _currentNumberOfDocuments.inc(segmentToRefresh.getSegmentMetadata().getTotalDocs());
      }
      _segmentsMap.put(segmentToRefresh.getSegmentName(), new SegmentDataManager(segmentToRefresh));
    }
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
      }
      LOGGER.info("Segment " + segmentId + " has been deleted");
      _segmentAsyncExecutorService.execute(new Runnable() {
        @Override
        public void run() {
          FileUtils.deleteQuietly(new File(_resourceDataDir, segmentId));
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
        incrementCount(segment.getSegment().getSegmentName());
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

}
