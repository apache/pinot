package com.linkedin.pinot.server.partition;

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

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.segments.v1.segment.SegmentLoader.IO_MODE;
import com.linkedin.pinot.server.conf.PartitionDataManagerConfig;
import com.linkedin.pinot.server.utils.NamedThreadFactory;
import com.linkedin.pinot.server.utils.SegmentUtils;
import com.linkedin.pinot.server.zookeeper.SegmentInfo;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;


/**
 * An implemenation of offline parition.
 * Provide add and remove segment functionality.
 * 
 * @author xiafu
 *
 */
public class OfflinePartitionDataManager implements PartitionDataManager {
  private static Logger LOGGER = LoggerFactory.getLogger(OfflinePartitionDataManager.class);

  private volatile boolean _isStarted = false;
  private Object _globalLock = new Object();

  private PartitionDataManagerConfig _partitionDataManagerConfig;
  private final ExecutorService _segmentAsyncExecutorService = Executors
      .newSingleThreadExecutor(new NamedThreadFactory("SegmentAsyncExecutorService"));;
  private String _partitionDir;

  private final Map<String, SegmentDataManager> _segmentsMap = new HashMap<String, SegmentDataManager>();
  private final List<String> _activeSegments = new ArrayList<String>();
  private final List<String> _loadingSegments = new ArrayList<String>();
  private Map<String, AtomicInteger> _referenceCounts = new HashMap<String, AtomicInteger>();

  private static final Counter _currentNumberOfSegments = Metrics.newCounter(OfflinePartitionDataManager.class,
      "currentNumberOfSegments");
  private static final Counter _currentNumberOfDocuments = Metrics.newCounter(OfflinePartitionDataManager.class,
      "currentNumberOfDocuments");
  private static final Counter _numDeletedSegments = Metrics.newCounter(OfflinePartitionDataManager.class,
      "numberOfDeletedSegments");

  public OfflinePartitionDataManager() {
  }

  @Override
  public void init(PartitionDataManagerConfig partitionDataManagerConfig) {
    _partitionDataManagerConfig = partitionDataManagerConfig;
    _partitionDir = _partitionDataManagerConfig.getPartitionDir();
    if (!new File(_partitionDir).exists()) {
      new File(_partitionDir).mkdirs();
    }
  }

  @Override
  public void start() {
    if (_partitionDataManagerConfig != null) {
      bootstrapSegments();
      _isStarted = true;
    } else {
      LOGGER.error("The OfflinePartitionDataManager hasn't been initialized.");
    }
  }

  private void bootstrapSegments() {
    File partitionDir = new File(_partitionDir);
    LOGGER.info("Bootstrap partition directory - " + partitionDir.getAbsolutePath());
    for (File segmentDir : partitionDir.listFiles()) {
      try {
        LOGGER.info("Bootstrap segment from directory - " + segmentDir.getAbsolutePath());
        IndexSegment indexSegment = com.linkedin.pinot.segments.v1.segment.SegmentLoader.load(segmentDir, IO_MODE.heap);
        addSegment(indexSegment);
      } catch (Exception e) {
        LOGGER.error("Unable to bootstrap segment in dir : " + segmentDir.getAbsolutePath());
      }
    }
  }

  @Override
  public void shutDown() {
    _partitionDataManagerConfig = null;
    _isStarted = false;

  }

  @Override
  public void addSegment(final IndexSegment indexSegmentToAdd) {
    System.out.println("Trying to add a new segment to partition");

    synchronized (getGlobalLock()) {
      if (!_segmentsMap.containsKey(indexSegmentToAdd.getSegmentName())) {
        System.out.println("Trying to add segment - " + indexSegmentToAdd.getSegmentName());
        _segmentsMap.put(indexSegmentToAdd.getSegmentName(), new SegmentDataManager(indexSegmentToAdd));
        markSegmentAsLoaded(indexSegmentToAdd.getSegmentName());
        _referenceCounts.put(indexSegmentToAdd.getSegmentName(), new AtomicInteger(1));
      } else {
        System.out.println("Trying to refresh segment - " + indexSegmentToAdd.getSegmentName());
        decrementCount(indexSegmentToAdd.getSegmentName());
        _segmentsMap.put(indexSegmentToAdd.getSegmentName(), new SegmentDataManager(indexSegmentToAdd));
        markSegmentAsLoaded(indexSegmentToAdd.getSegmentName());
        _referenceCounts.put(indexSegmentToAdd.getSegmentName(), new AtomicInteger(1));
      }
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
          FileUtils.deleteQuietly(new File(_partitionDir, segmentId));
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

  public void addSegment(final String segmentId, final SegmentInfo segmentInfo, boolean asynchronous) {
    if (asynchronous) {
      _loadingSegments.add(segmentId);
      _segmentAsyncExecutorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            IndexSegment segment =
                SegmentUtils.instantiateSegment(segmentId, segmentInfo, _partitionDataManagerConfig.getConfig());
            if (segment == null) {
              return;
            }
            addSegment(segment);
          } catch (Exception ex) {
            LOGGER.error("Couldn't instantiate the segment", ex);
          }
        }
      });
    } else {
      IndexSegment segment =
          SegmentUtils.instantiateSegment(segmentId, segmentInfo, _partitionDataManagerConfig.getConfig());
      addSegment(segment);
    }
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
}
