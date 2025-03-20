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

import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SemaphoreAccessCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SemaphoreAccessCoordinator.class);

  private final Semaphore _semaphore;
  private final boolean _enforceConsumptionInOrder;
  private final Condition _condition;
  private final Lock _lock;
  private final Set<Integer> _segmentSequenceNumSet;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private static final boolean RELY_ON_IDEAL_STATE = false;

  public SemaphoreAccessCoordinator(Semaphore semaphore, boolean enforceConsumptionInOrder,
      RealtimeTableDataManager realtimeTableDataManager) {
    _semaphore = semaphore;
    _lock = new ReentrantLock();
    _condition = _lock.newCondition();
    _enforceConsumptionInOrder = enforceConsumptionInOrder;
    _segmentSequenceNumSet = new HashSet<>();
    _realtimeTableDataManager = realtimeTableDataManager;
  }

  public void acquire(LLCSegmentName llcSegmentName)
      throws InterruptedException {

    String segmentName = llcSegmentName.getSegmentName();

    if (_enforceConsumptionInOrder) {
      if (!_realtimeTableDataManager.getIsTableReadyToConsumeData().getAsBoolean()) {
        LOGGER.warn("Waiting for table to be in ready state for segment: {}", segmentName);
        waitUntilTableIsReady();
      }
      waitForPrevSegment(llcSegmentName);
    }

    long startTimeMs = System.currentTimeMillis();
    while (!_semaphore.tryAcquire(5, TimeUnit.MINUTES)) {
      LOGGER.warn("Failed to acquire partitionGroup consumer semaphore in: {} ms. Retrying.",
          System.currentTimeMillis() - startTimeMs);

      if (!isSegmentInProgress(segmentName)) {
        throw new SegmentAlreadyConsumedException("segment: " + segmentName + " status must be in progress");
      }
    }
  }

  public void release() {
    _semaphore.release();
  }

  public Semaphore getSemaphore() {
    return _semaphore;
  }

  public void trackSegment(LLCSegmentName llcSegmentName) {
    _lock.lock();
    try {
      if (!RELY_ON_IDEAL_STATE) {
        _segmentSequenceNumSet.add(llcSegmentName.getSequenceNumber());
      }
      _condition.signalAll();
    } finally {
      _lock.unlock();
    }
  }

  private void waitUntilTableIsReady()
      throws InterruptedException {
    while (!_realtimeTableDataManager.getIsTableReadyToConsumeData().getAsBoolean()) {
      Thread.sleep(RealtimeTableDataManager.READY_TO_CONSUME_DATA_CHECK_INTERVAL_MS);
    }
  }

  private void waitForPrevSegment(LLCSegmentName currSegment)
      throws InterruptedException {

    long startTimeMs = System.currentTimeMillis();

    if (RELY_ON_IDEAL_STATE) {
      String previousSegment = getPreviousSegment(currSegment);
      if (previousSegment == null) {
        return;
      }

      SegmentDataManager segmentDataManager = _realtimeTableDataManager.acquireSegment(previousSegment);
      _lock.lock();
      try {
        while (segmentDataManager == null) {
          while (!_condition.await(5, TimeUnit.MINUTES)) {
            LOGGER.warn("Semaphore access denied to segment: {}. Waiting on previous segment: {} since: {} ms.",
                currSegment.getSegmentName(), previousSegment, System.currentTimeMillis() - startTimeMs);

            if (!isSegmentInProgress(currSegment.getSegmentName())) {
              throw new SegmentAlreadyConsumedException(
                  "segment: " + currSegment.getSegmentName() + " status must be in progress.");
            }
          }
          segmentDataManager = _realtimeTableDataManager.acquireSegment(previousSegment);
        }
      } finally {
        _lock.unlock();
        if (segmentDataManager != null) {
          _realtimeTableDataManager.releaseSegment(segmentDataManager);
        }
      }

      return;
    }

    int prevSeqNum = currSegment.getSequenceNumber() - 1;
    _lock.lock();
    try {
      while (!_segmentSequenceNumSet.contains(prevSeqNum)) {
        while (!_condition.await(5, TimeUnit.MINUTES)) {
          LOGGER.warn("Semaphore access denied to segment: {}."
                  + " Waiting on previous segment with sequence number: {} since: {} ms.", currSegment.getSegmentName(),
              prevSeqNum, System.currentTimeMillis() - startTimeMs);

          if (!isSegmentInProgress(currSegment.getSegmentName())) {
            throw new SegmentAlreadyConsumedException(
                "segment: " + currSegment.getSegmentName() + " status must be in progress.");
          }
        }
      }
    } finally {
      _lock.unlock();
    }
  }

  @Nullable
  private String getPreviousSegment(LLCSegmentName currSegment) {
    int maxSequenceNumBelowCurrentSegment = -1;
    String previousSegment = null;
    int currPartitionGroupId = currSegment.getPartitionGroupId();
    int currSequenceNum = currSegment.getSequenceNumber();

    Map<String, Map<String, String>> segmentAssignment =
        HelixHelper.getSegmentAssignment(_realtimeTableDataManager.getTableName(),
            _realtimeTableDataManager.getHelixManager());

    if (segmentAssignment == null) {
      return null;
    }

    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      if (!instanceStateMap.containsValue(CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE)) {
        continue;
      }

      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      Preconditions.checkNotNull(llcSegmentName);

      if (llcSegmentName.getPartitionGroupId() != currPartitionGroupId) {
        continue;
      }

      if (llcSegmentName.getSequenceNumber() >= currSequenceNum) {
        continue;
      }

      if (llcSegmentName.getSequenceNumber() > maxSequenceNumBelowCurrentSegment) {
        maxSequenceNumBelowCurrentSegment = llcSegmentName.getSequenceNumber();
        previousSegment = segmentName;
      }
    }

    return previousSegment;
  }

  private boolean isSegmentInProgress(String segmentName) {
    SegmentZKMetadata segmentZKMetadata = _realtimeTableDataManager.fetchZKMetadata(segmentName);

    if (segmentZKMetadata == null) {
      LOGGER.error("segmentZKMetadata does not exists for segment: {}", segmentName);
      throw new RuntimeException("segmentZKMetadata does not exists.");
    }

    if (segmentZKMetadata.getStatus() != CommonConstants.Segment.Realtime.Status.IN_PROGRESS) {
      // it's certain at this point that there is a pending consuming -> online/offline transition message.
      // hence return from here and handle pending message instead.
      LOGGER.warn("segment: {} status must be {}. Skipping creation of RealtimeSegmentDataManager", segmentName,
          CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
      return false;
    }

    return true;
  }
}
