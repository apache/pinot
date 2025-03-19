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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
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

    if (_enforceConsumptionInOrder) {
      int prevSequenceNum = llcSegmentName.getSequenceNumber() - 1;
      if (prevSequenceNum >= 0) {
        waitForPrevSegment(prevSequenceNum, llcSegmentName.getSegmentName());
      }
    }

    long startTimeMs = System.currentTimeMillis();
    while (!_semaphore.tryAcquire(5, TimeUnit.MINUTES)) {
      LOGGER.warn("Failed to acquire partitionGroup consumer semaphore in: {} ms. Retrying.",
          System.currentTimeMillis() - startTimeMs);

      if (!isSegmentInProgress(llcSegmentName.getSegmentName())) {
        throw new SegmentAlreadyExistsException(
            "segment: " + llcSegmentName.getSegmentName() + " status must be in progress");
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
      _segmentSequenceNumSet.add(llcSegmentName.getSequenceNumber());
      _condition.signalAll();
    } finally {
      _lock.unlock();
    }
  }

  private void waitForPrevSegment(int prevSeqNum, String currSegmentName)
      throws InterruptedException {

    if (!_realtimeTableDataManager.getIsTableReadyToConsumeData().getAsBoolean()) {
      LOGGER.warn("Waiting for table to be in ready to consume state for segment: {}", currSegmentName);
      do {
        Thread.sleep(RealtimeTableDataManager.READY_TO_CONSUME_DATA_CHECK_INTERVAL_MS);
      } while (!_realtimeTableDataManager.getIsTableReadyToConsumeData().getAsBoolean());
    }

    long startTimeMs = System.currentTimeMillis();
    _lock.lock();
    try {
      while (!_segmentSequenceNumSet.contains(prevSeqNum)) {
        while (!_condition.await(5, TimeUnit.MINUTES)) {
          LOGGER.warn("Semaphore access denied to segment: {}."
                  + " Waiting on previous segment with sequence number: {} since: {} ms.", currSegmentName, prevSeqNum,
              System.currentTimeMillis() - startTimeMs);

          if (!isSegmentInProgress(currSegmentName)) {
            throw new SegmentAlreadyExistsException("segment: " + currSegmentName + " status must be in progress.");
          }
        }
      }
    } finally {
      _lock.unlock();
    }
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
