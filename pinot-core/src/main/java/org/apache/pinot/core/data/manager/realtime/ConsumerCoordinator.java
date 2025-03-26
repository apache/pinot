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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ConsumerCoordinator coordinates the offline->consuming helix transitions.
 */
public class ConsumerCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerCoordinator.class);

  private final Semaphore _semaphore;
  private final boolean _enforceConsumptionInOrder;
  private final Condition _condition;
  private final Lock _lock;
  private final ServerMetrics _serverMetrics;
  private volatile int _maxSegmentSeqNumLoaded = -1;
  private final boolean _alwaysRelyOnIdealState;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final AtomicBoolean _isFirstTransitionProcessed;
  private static final long WAIT_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3);

  public ConsumerCoordinator(boolean enforceConsumptionInOrder, RealtimeTableDataManager realtimeTableDataManager) {
    _semaphore = new Semaphore(1);
    _lock = new ReentrantLock();
    _condition = _lock.newCondition();
    _enforceConsumptionInOrder = enforceConsumptionInOrder;
    _realtimeTableDataManager = realtimeTableDataManager;
    StreamIngestionConfig streamIngestionConfig = realtimeTableDataManager.getStreamIngestionConfig();
    if (streamIngestionConfig != null) {
      // if trackSegmentSeqNumber is false, server relies on ideal state to fetch previous segment to a segment for all
      // helix transitions.
      _alwaysRelyOnIdealState = streamIngestionConfig.isUseIdealStateToCalculatePreviousSegment();
    } else {
      _alwaysRelyOnIdealState = false;
    }
    _isFirstTransitionProcessed = new AtomicBoolean(false);
    _serverMetrics = ServerMetrics.get();
  }

  void acquire(LLCSegmentName llcSegmentName)
      throws InterruptedException {
    long startTimeMs;

    if (_enforceConsumptionInOrder) {
      startTimeMs = System.currentTimeMillis();

      waitForPrevSegment(llcSegmentName);

      _serverMetrics.addTimedTableValue(_realtimeTableDataManager.getTableName(),
          ServerTimer.PREV_SEGMENT_WAIT_DURATION_MS, System.currentTimeMillis() - startTimeMs, TimeUnit.MILLISECONDS);
    }

    startTimeMs = System.currentTimeMillis();
    while (!_semaphore.tryAcquire(WAIT_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
      LOGGER.warn("Failed to acquire partitionGroup consumer semaphore in: {} ms. Retrying.",
          System.currentTimeMillis() - startTimeMs);
    }
  }

  void release() {
    _semaphore.release();
  }

  @VisibleForTesting
  Semaphore getSemaphore() {
    return _semaphore;
  }

  void trackSegment(LLCSegmentName llcSegmentName) {
    _lock.lock();
    try {
      if (!_alwaysRelyOnIdealState) {
        _maxSegmentSeqNumLoaded = Math.max(_maxSegmentSeqNumLoaded, llcSegmentName.getSequenceNumber());
      }
      // notify all helix threads waiting for their offline -> consuming segment's prev segment to be loaded
      _condition.signalAll();
    } finally {
      _lock.unlock();
    }
  }

  private void waitForPrevSegment(LLCSegmentName currSegment)
      throws InterruptedException {

    if (_alwaysRelyOnIdealState || !_isFirstTransitionProcessed.get()) {
      // if _alwaysRelyOnIdealState or no offline -> consuming transition has been processed, it means rely on
      // ideal state to fetch previous segment.
      acquireSegmentRelyingOnIdealState(currSegment);

      // the first transition will always be prone to error, consider edge case where segment previous to current
      // helix transition's segment was deleted and this server came alive after successful deletion. the prev
      // segment will not exist, hence first transition is handled using isFirstTransitionSuccessful.
      _isFirstTransitionProcessed.compareAndSet(false, true);
      return;
    }

    // rely on _maxSegmentSeqNumLoaded watermark for previous segment.
    if (awaitForPreviousSegmentSequenceNumber(currSegment, WAIT_INTERVAL_MS)) {
      return;
    }

    // tried using prevSegSeqNumber watermark, but could not acquire the previous segment.
    // fallback to acquire prev segment from ideal state.
    acquireSegmentRelyingOnIdealState(currSegment);
  }

  private void acquireSegmentRelyingOnIdealState(LLCSegmentName currSegment)
      throws InterruptedException {
    long startTimeMs = System.currentTimeMillis();

    // this can be slow. And should not happen within lock.
    LLCSegmentName previousLLCSegmentName = getPreviousSegment(currSegment);
    if (previousLLCSegmentName == null) {
      // previous segment can only be null if either all the previous segments are deleted or this is the starting
      // sequence segment of the partition Group.
      return;
    }
    String previousSegment = previousLLCSegmentName.getSegmentName();

    long duration = System.currentTimeMillis() - startTimeMs;
    LOGGER.info("Fetched previous segment: {} to current segment: {} in: {} ms.", previousSegment,
        currSegment.getSegmentName(), duration);
    _serverMetrics.addTimedTableValue(_realtimeTableDataManager.getTableName(),
        ServerTimer.PREV_SEGMENT_FETCH_IDEAL_STATE_DURATION_MS, duration, TimeUnit.MILLISECONDS);

    SegmentDataManager segmentDataManager = _realtimeTableDataManager.acquireSegment(previousSegment);
    try {
      _lock.lock();
      try {
        while (segmentDataManager == null) {
          // if segmentDataManager == null, it means segment is not loaded in the server.
          // wait until it's loaded.
          while (!_condition.await(WAIT_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
            LOGGER.warn("Semaphore access denied to segment: {}. Waiting on previous segment: {} since: {} ms.",
                currSegment.getSegmentName(), previousSegment, System.currentTimeMillis() - startTimeMs);
          }
          segmentDataManager = _realtimeTableDataManager.acquireSegment(previousSegment);
        }
      } finally {
        _lock.unlock();
      }
    } finally {
      if (segmentDataManager != null) {
        _realtimeTableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }

  /***
   * @param currSegment is the segment of current helix transition.
   * @param timeoutMs is max time to wait in millis
   * @return true if previous Segment was registered to the server, else false.
   * @throws InterruptedException
   */
  @VisibleForTesting
  boolean awaitForPreviousSegmentSequenceNumber(LLCSegmentName currSegment, long timeoutMs)
      throws InterruptedException {
    long startTimeMs = System.currentTimeMillis();
    int prevSeqNum = currSegment.getSequenceNumber() - 1;
    _lock.lock();
    try {
      while (_maxSegmentSeqNumLoaded < prevSeqNum) {
        // it means all segments until _maxSegmentSeqNumLoaded is not loaded in the server. Wait until it's loaded.
        if (!_condition.await(timeoutMs, TimeUnit.MILLISECONDS)) {
          LOGGER.warn("Semaphore access denied to segment: {}."
                  + " Waiting on previous segment with sequence number: {} since: {} ms.", currSegment.getSegmentName(),
              prevSeqNum, System.currentTimeMillis() - startTimeMs);
          // waited until the timeout. Rely on ideal state now.
          return false;
        }
      }

      return (_maxSegmentSeqNumLoaded >= prevSeqNum);
    } finally {
      _lock.unlock();
    }
  }

  @VisibleForTesting
  @Nullable
  LLCSegmentName getPreviousSegment(LLCSegmentName currSegment) {
    // if seq num of current segment is 102, maxSequenceNumBelowCurrentSegment must be highest seq num of any segment
    // created before current segment
    int maxSequenceNumBelowCurrentSegment = -1;
    LLCSegmentName previousSegment = null;
    int currPartitionGroupId = currSegment.getPartitionGroupId();
    int currSequenceNum = currSegment.getSequenceNumber();
    Map<String, Map<String, String>> segmentAssignment = getSegmentAssignment();

    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      if (!instanceStateMap.containsValue(CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE)) {
        // if server is looking for previous segment to current transition's segment, it means the previous segment
        // has to be online in any instance. If all previous segments are not online, we just allow the current helix
        // transition to go ahead.
        continue;
      }

      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName == null) {
        // can't compare with this segment, hence skip.
        continue;
      }

      if (llcSegmentName.getPartitionGroupId() != currPartitionGroupId) {
        // ignore segments of different partitions.
        continue;
      }

      if (llcSegmentName.getSequenceNumber() >= currSequenceNum) {
        // ignore segments with higher sequence number than existing helix transition segment.
        continue;
      }

      if (llcSegmentName.getSequenceNumber() > maxSequenceNumBelowCurrentSegment) {
        maxSequenceNumBelowCurrentSegment = llcSegmentName.getSequenceNumber();
        // also track the name of segment
        previousSegment = llcSegmentName;
      }
    }

    return previousSegment;
  }

  @VisibleForTesting
  Map<String, Map<String, String>> getSegmentAssignment() {
    IdealState idealState = HelixHelper.getTableIdealState(_realtimeTableDataManager.getHelixManager(),
        _realtimeTableDataManager.getTableName());
    Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s",
        _realtimeTableDataManager.getTableName());
    return idealState.getRecord().getMapFields();
  }

  @VisibleForTesting
  Lock getLock() {
    return _lock;
  }

  @VisibleForTesting
  AtomicBoolean getIsFirstTransitionProcessed() {
    return _isFirstTransitionProcessed;
  }

  @VisibleForTesting
  int getMaxSegmentSeqNumLoaded() {
    return _maxSegmentSeqNumLoaded;
  }
}
