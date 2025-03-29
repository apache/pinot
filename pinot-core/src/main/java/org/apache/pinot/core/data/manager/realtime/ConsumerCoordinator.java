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
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ConsumerCoordinator coordinates the offline->consuming helix transitions.
 */
public class ConsumerCoordinator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerCoordinator.class);
  private static final long WAIT_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3);

  private final boolean _enforceConsumptionInOrder;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final boolean _useIdealStateToCalculatePreviousSegment;
  private final ServerMetrics _serverMetrics;

  // We use semaphore of 1 permit instead of lock because the semaphore is shared across multiple threads, and it can be
  // released by a different thread than the one that acquired it. There is no out-of-box Lock implementation that
  // allows releasing the lock from a different thread.
  private final Semaphore _semaphore = new Semaphore(1);
  private final Lock _lock = new ReentrantLock();
  private final Condition _condition = _lock.newCondition();
  private final AtomicBoolean _firstTransitionProcessed = new AtomicBoolean(false);

  private volatile int _maxSequenceNumberRegistered = -1;

  public ConsumerCoordinator(boolean enforceConsumptionInOrder, RealtimeTableDataManager realtimeTableDataManager) {
    _enforceConsumptionInOrder = enforceConsumptionInOrder;
    _realtimeTableDataManager = realtimeTableDataManager;
    StreamIngestionConfig streamIngestionConfig = realtimeTableDataManager.getStreamIngestionConfig();
    _useIdealStateToCalculatePreviousSegment =
        streamIngestionConfig != null && streamIngestionConfig.isUseIdealStateToCalculatePreviousSegment();
    _serverMetrics = ServerMetrics.get();
  }

  public void acquire(LLCSegmentName llcSegmentName)
      throws InterruptedException, ShouldNotConsumeException {
    String segmentName = llcSegmentName.getSegmentName();
    if (_enforceConsumptionInOrder) {
      long startTimeMs = System.currentTimeMillis();
      SegmentZKMetadata segmentZKMetadata = waitForPreviousSegment(llcSegmentName);
      _serverMetrics.addTimedTableValue(_realtimeTableDataManager.getTableName(), ServerTimer.PREV_SEGMENT_WAIT_TIME_MS,
          System.currentTimeMillis() - startTimeMs, TimeUnit.MILLISECONDS);

      // When consumption order is enforced, unless the segment is deleted, we wait until the previous segment is
      // registered regardless of whether ZK metadata status has changed to guarantee the consumption ordering.
      //
      // Prevent the following scenario:
      // - Seg 100 (OFFLINE -> CONSUMING pending)
      //
      // - Seg 101 (OFFLINE -> CONSUMING returned because of status change)
      // - Seg 101 (CONSUMING -> ONLINE processed)
      //
      // - Seg 102 (OFFLINE -> CONSUMING started consuming while 100 is not registered)
      if (segmentZKMetadata != null) {
        checkSegmentStatus(segmentZKMetadata);
      }
    }

    long startTimeMs = System.currentTimeMillis();
    while (!_semaphore.tryAcquire(WAIT_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
      LOGGER.warn("Failed to acquire consumer semaphore for segment: {} in: {}ms. Retrying.", segmentName,
          System.currentTimeMillis() - startTimeMs);
      SegmentZKMetadata segmentZKMetadata = _realtimeTableDataManager.fetchZKMetadataNullable(segmentName);
      if (segmentZKMetadata == null) {
        throw new ShouldNotConsumeException("Segment: " + segmentName + " is deleted");
      }
      checkSegmentStatus(segmentZKMetadata);
    }
  }

  public void release() {
    _semaphore.release();
  }

  @VisibleForTesting
  Semaphore getSemaphore() {
    return _semaphore;
  }

  public void register(LLCSegmentName llcSegmentName) {
    _lock.lock();
    try {
      int sequenceNumber = llcSegmentName.getSequenceNumber();
      if (sequenceNumber > _maxSequenceNumberRegistered) {
        _maxSequenceNumberRegistered = sequenceNumber;
        // notify all helix threads waiting for their offline -> consuming segment's prev segment to be loaded
        _condition.signalAll();
      }
    } finally {
      _lock.unlock();
    }
  }

  /**
   * Waits for the previous segment to be registered to the server. Returns the segment ZK metadata fetched during the
   * wait to reduce unnecessary ZK read.
   */
  @Nullable
  private SegmentZKMetadata waitForPreviousSegment(LLCSegmentName currentSegment)
      throws InterruptedException, ShouldNotConsumeException {
    if (!_firstTransitionProcessed.get() || _useIdealStateToCalculatePreviousSegment) {
      SegmentZKMetadata segmentZKMetadata = null;
      if (_maxSequenceNumberRegistered < currentSegment.getSequenceNumber() - 1) {
        int previousSegmentSequenceNumber = getPreviousSegmentSequenceNumberFromIdealState(currentSegment);
        segmentZKMetadata = waitForPreviousSegment(currentSegment, previousSegmentSequenceNumber);
      }
      _firstTransitionProcessed.set(true);
      return segmentZKMetadata;
    } else {
      return waitForPreviousSegment(currentSegment, currentSegment.getSequenceNumber() - 1);
    }
  }

  /**
   * Waits for the previous segment with the sequence number to be registered to the server. Returns the segment ZK
   * metadata fetched during the wait to reduce unnecessary ZK read..
   */
  @Nullable
  @VisibleForTesting
  SegmentZKMetadata waitForPreviousSegment(LLCSegmentName currentSegment, int previousSegmentSequenceNumber)
      throws InterruptedException, ShouldNotConsumeException {
    if (previousSegmentSequenceNumber <= _maxSequenceNumberRegistered) {
      return null;
    }
    SegmentZKMetadata segmentZKMetadata = null;
    long startTimeMs = System.currentTimeMillis();
    _lock.lock();
    try {
      while (previousSegmentSequenceNumber > _maxSequenceNumberRegistered) {
        // it means the previous segment is not loaded in the server. Wait until it's loaded.
        if (!_condition.await(WAIT_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
          String segmentName = currentSegment.getSegmentName();
          LOGGER.warn("Waited on previous segment with sequence number: {} for: {}ms. "
                  + "Refreshing the previous segment sequence number for current segment: {}",
              previousSegmentSequenceNumber, System.currentTimeMillis() - startTimeMs, segmentName);
          segmentZKMetadata = _realtimeTableDataManager.fetchZKMetadataNullable(segmentName);
          if (segmentZKMetadata == null) {
            throw new ShouldNotConsumeException("Segment: " + segmentName + " is deleted");
          }
          previousSegmentSequenceNumber = getPreviousSegmentSequenceNumberFromIdealState(currentSegment);
        }
      }
      return segmentZKMetadata;
    } finally {
      _lock.unlock();
    }
  }

  @VisibleForTesting
  int getPreviousSegmentSequenceNumberFromIdealState(LLCSegmentName currentSegment) {
    long startTimeMs = System.currentTimeMillis();
    // Track the highest sequence number of any segment created before the current segment. If there is none, return -1
    // so that it can always pass the check.
    int maxSequenceNumberBelowCurrentSegment = -1;
    String instanceId = _realtimeTableDataManager.getServerInstance();
    int partitionId = currentSegment.getPartitionGroupId();
    int currentSequenceNumber = currentSegment.getSequenceNumber();

    for (Map.Entry<String, Map<String, String>> entry : getSegmentAssignment().entrySet()) {
      String state = entry.getValue().get(instanceId);
      if (!SegmentStateModel.ONLINE.equals(state)) {
        // if server is looking for previous segment to current transition's segment, it means the previous segment
        // has to be online in the instance. If all previous segments are not online, we just allow the current helix
        // transition to go ahead.
        continue;
      }

      LLCSegmentName llcSegmentName = LLCSegmentName.of(entry.getKey());
      if (llcSegmentName == null) {
        // ignore uploaded segments
        continue;
      }

      if (llcSegmentName.getPartitionGroupId() != partitionId) {
        // ignore segments of different partitions.
        continue;
      }

      int sequenceNumber = llcSegmentName.getSequenceNumber();
      if (sequenceNumber > maxSequenceNumberBelowCurrentSegment && sequenceNumber < currentSequenceNumber) {
        maxSequenceNumberBelowCurrentSegment = sequenceNumber;
      }
    }

    long timeSpentMs = System.currentTimeMillis() - startTimeMs;
    LOGGER.info("Fetched previous segment sequence number: {} to current segment: {} in: {}ms.",
        maxSequenceNumberBelowCurrentSegment, currentSegment.getSegmentName(), timeSpentMs);
    _serverMetrics.addTimedTableValue(_realtimeTableDataManager.getTableName(),
        ServerTimer.PREV_SEGMENT_FETCH_IDEAL_STATE_TIME_MS, timeSpentMs, TimeUnit.MILLISECONDS);

    return maxSequenceNumberBelowCurrentSegment;
  }

  @VisibleForTesting
  Map<String, Map<String, String>> getSegmentAssignment() {
    String realtimeTableName = _realtimeTableDataManager.getTableName();
    IdealState idealState =
        HelixHelper.getTableIdealState(_realtimeTableDataManager.getHelixManager(), realtimeTableName);
    Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", realtimeTableName);
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

  @VisibleForTesting
  int getMaxSequenceNumberRegistered() {
    return _maxSequenceNumberRegistered;
  }

  private static void checkSegmentStatus(SegmentZKMetadata segmentZKMetadata)
      throws ShouldNotConsumeException {
    if (segmentZKMetadata.getStatus().isCompleted()) {
      throw new ShouldNotConsumeException(
          "Segment: " + segmentZKMetadata.getSegmentName() + " is already completed with status: "
              + segmentZKMetadata.getStatus());
    }
  }

  /**
   * This exception is thrown when attempting to acquire the consumer semaphore for a segment that should not be
   * consumed anymore:
   * - Segment is in completed status (DONE/UPLOADED)
   * - Segment is deleted
   *
   * We allow consumption when segment is COMMITTING (for pauseless consumption) because there is no guarantee that the
   * segment will be committed soon. This way the slow server can still catch up.
   */
  public static class ShouldNotConsumeException extends Exception {
    public ShouldNotConsumeException(String message) {
      super(message);
    }
  }
}
