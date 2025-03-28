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
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
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
  private static final long WAIT_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3);

  private final Semaphore _semaphore;
  private final boolean _enforceConsumptionInOrder;
  private final Condition _condition;
  private final Lock _lock;
  private final ServerMetrics _serverMetrics;
  private final boolean _alwaysRelyOnIdealState;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final AtomicBoolean _firstTransitionProcessed;

  private volatile int _maxSegmentSeqNumRegistered = -1;

  public ConsumerCoordinator(boolean enforceConsumptionInOrder, RealtimeTableDataManager realtimeTableDataManager) {
    _semaphore = new Semaphore(1);
    _lock = new ReentrantLock();
    _condition = _lock.newCondition();
    _enforceConsumptionInOrder = enforceConsumptionInOrder;
    _realtimeTableDataManager = realtimeTableDataManager;
    StreamIngestionConfig streamIngestionConfig = realtimeTableDataManager.getStreamIngestionConfig();
    if (streamIngestionConfig != null) {
      // if isUseIdealStateToCalculatePreviousSegment is true, server relies on ideal state to fetch previous segment
      // to a segment for all helix transitions.
      _alwaysRelyOnIdealState = streamIngestionConfig.isUseIdealStateToCalculatePreviousSegment();
    } else {
      _alwaysRelyOnIdealState = false;
    }
    _firstTransitionProcessed = new AtomicBoolean(false);
    _serverMetrics = ServerMetrics.get();
  }

  public void acquire(LLCSegmentName llcSegmentName)
      throws InterruptedException {
    if (_enforceConsumptionInOrder) {
      long startTimeMs = System.currentTimeMillis();
      waitForPrevSegment(llcSegmentName);
      _serverMetrics.addTimedTableValue(_realtimeTableDataManager.getTableName(), ServerTimer.PREV_SEGMENT_WAIT_TIME_MS,
          System.currentTimeMillis() - startTimeMs, TimeUnit.MILLISECONDS);
    }

    long startTimeMs = System.currentTimeMillis();
    while (!_semaphore.tryAcquire(WAIT_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
      String currSegmentName = llcSegmentName.getSegmentName();
      LOGGER.warn("Failed to acquire consumer semaphore for segment: {} in: {}ms. Retrying.", currSegmentName,
          System.currentTimeMillis() - startTimeMs);

      if (isSegmentAlreadyConsumed(currSegmentName)) {
        throw new SegmentAlreadyConsumedException(currSegmentName);
      }
    }
  }

  public void release() {
    _semaphore.release();
  }

  @VisibleForTesting
  Semaphore getSemaphore() {
    return _semaphore;
  }

  public void trackSegment(LLCSegmentName llcSegmentName) {
    _lock.lock();
    try {
      if (!_alwaysRelyOnIdealState) {
        _maxSegmentSeqNumRegistered = Math.max(_maxSegmentSeqNumRegistered, llcSegmentName.getSequenceNumber());
      }
      // notify all helix threads waiting for their offline -> consuming segment's prev segment to be loaded
      _condition.signalAll();
    } finally {
      _lock.unlock();
    }
  }

  private void waitForPrevSegment(LLCSegmentName currSegment)
      throws InterruptedException {

    if (_alwaysRelyOnIdealState || !_firstTransitionProcessed.get()) {
      // if _alwaysRelyOnIdealState or no offline -> consuming transition has been processed, it means rely on
      // ideal state to fetch previous segment.
      awaitForPreviousSegmentFromIdealState(currSegment);

      // the first transition will always be prone to error, consider edge case where segment previous to current
      // helix transition's segment was deleted and this server came alive after successful deletion. the prev
      // segment will not exist, hence first transition is handled using isFirstTransitionSuccessful.
      _firstTransitionProcessed.set(true);
      return;
    }

    // rely on _maxSegmentSeqNumRegistered watermark for previous segment.
    if (awaitForPreviousSegmentSequenceNumber(currSegment, WAIT_INTERVAL_MS)) {
      return;
    }

    // tried using prevSegSeqNumber watermark, but could not acquire the previous segment.
    // fallback to acquire prev segment from ideal state.
    awaitForPreviousSegmentFromIdealState(currSegment);
  }

  private void awaitForPreviousSegmentFromIdealState(LLCSegmentName currSegment)
      throws InterruptedException {
    String previousSegment = getPreviousSegmentFromIdealState(currSegment);
    if (previousSegment == null) {
      // previous segment can only be null if either all the previous segments are deleted or this is the starting
      // sequence segment of the partition Group.
      return;
    }

    SegmentDataManager segmentDataManager = _realtimeTableDataManager.acquireSegment(previousSegment);
    try {
      long startTimeMs = System.currentTimeMillis();
      _lock.lock();
      try {
        while (!(segmentDataManager instanceof ImmutableSegmentDataManager)) {

          int prevSeqNum = LLCSegmentName.of(segmentDataManager.getSegmentName()).getSequenceNumber();
          if (_maxSegmentSeqNumRegistered >= prevSeqNum) {
            return;
          }

          // if segmentDataManager == null, it means segment is not loaded in the server.
          // wait until it's loaded.
          if (!_condition.await(WAIT_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
            LOGGER.warn("Semaphore access denied to segment: {}. Waited on previous segment: {} for: {}ms.",
                currSegment.getSegmentName(), previousSegment, System.currentTimeMillis() - startTimeMs);

            if (isSegmentAlreadyConsumed(currSegment.getSegmentName())) {
              // if segment is already consumed, just return from here.
              // NOTE: if segment is deleted, this segment will never be registered and helix thread waiting on
              // watermark for prev segment won't be notified. All such helix threads will fallback to rely on ideal
              // state for previous segment.
              throw new SegmentAlreadyConsumedException(currSegment.getSegmentName());
            }

            // waited until timeout, fetch previous segment again from ideal state as previous segment might be
            // changed in ideal state.
            previousSegment = getPreviousSegmentFromIdealState(currSegment);
            if (previousSegment == null) {
              return;
            }
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
      while (_maxSegmentSeqNumRegistered < prevSeqNum) {
        // it means the previous segment is not loaded in the server. Wait until it's loaded.
        if (!_condition.await(timeoutMs, TimeUnit.MILLISECONDS)) {
          LOGGER.warn(
              "Semaphore access denied to segment: {}. Waited on previous segment with sequence number: {} for: {}ms.",
              currSegment.getSegmentName(), prevSeqNum, System.currentTimeMillis() - startTimeMs);

          if (isSegmentAlreadyConsumed(currSegment.getSegmentName())) {
            // if segment is already consumed, just return from here.
            // NOTE: if segment is deleted, this segment will never be registered and helix thread waiting on
            // watermark for prev segment won't be notified. All such helix threads will fallback to rely on ideal
            // state for previous segment.
            throw new SegmentAlreadyConsumedException(currSegment.getSegmentName());
          }

          // waited until the timeout. Rely on ideal state now.
          return _maxSegmentSeqNumRegistered >= prevSeqNum;
        }
      }
      return true;
    } finally {
      _lock.unlock();
    }
  }

  @VisibleForTesting
  @Nullable
  String getPreviousSegmentFromIdealState(LLCSegmentName currSegment) {
    long startTimeMs = System.currentTimeMillis();
    // if seq num of current segment is 102, maxSequenceNumBelowCurrentSegment must be highest seq num of any segment
    // created before current segment
    int maxSequenceNumBelowCurrentSegment = -1;
    String previousSegment = null;
    int currPartitionGroupId = currSegment.getPartitionGroupId();
    int currSequenceNum = currSegment.getSequenceNumber();
    Map<String, Map<String, String>> segmentAssignment = getSegmentAssignment();
    String currentServerInstanceId = _realtimeTableDataManager.getServerInstance();

    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      String state = instanceStateMap.get(currentServerInstanceId);

      if (!CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE.equals(state)) {
        // if server is looking for previous segment to current transition's segment, it means the previous segment
        // has to be online in the instance. If all previous segments are not online, we just allow the current helix
        // transition to go ahead.
        continue;
      }

      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName == null) {
        // ignore uploaded segments
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
        previousSegment = segmentName;
      }
    }

    long timeSpentMs = System.currentTimeMillis() - startTimeMs;
    LOGGER.info("Fetched previous segment: {} to current segment: {} in: {}ms.", previousSegment,
        currSegment.getSegmentName(), timeSpentMs);
    _serverMetrics.addTimedTableValue(_realtimeTableDataManager.getTableName(),
        ServerTimer.PREV_SEGMENT_FETCH_IDEAL_STATE_TIME_MS, timeSpentMs, TimeUnit.MILLISECONDS);

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
  AtomicBoolean getFirstTransitionProcessed() {
    return _firstTransitionProcessed;
  }

  // this should not be used outside of tests.
  @VisibleForTesting
  int getMaxSegmentSeqNumLoaded() {
    return _maxSegmentSeqNumRegistered;
  }

  @VisibleForTesting
  boolean isSegmentAlreadyConsumed(String currSegmentName) {
    SegmentZKMetadata segmentZKMetadata = _realtimeTableDataManager.fetchZKMetadata(currSegmentName);
    if (segmentZKMetadata == null) {
      // segment is deleted. no need to consume.
      LOGGER.warn("Skipping consumption for segment: {} because ZK metadata does not exists.", currSegmentName);
      return true;
    }
    if (segmentZKMetadata.getStatus().isCompleted()) {
      // if segment is done or uploaded, no need to consume.
      LOGGER.warn("Skipping consumption for segment: {} because ZK status is already marked as completed.",
          currSegmentName);
      return true;
    }
    return false;
  }
}
