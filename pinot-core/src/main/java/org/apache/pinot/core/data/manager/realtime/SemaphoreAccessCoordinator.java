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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.utils.LLCSegmentName;


public class SemaphoreAccessCoordinator {

  private final Semaphore _semaphore;
  private final boolean _enforceConsumptionInOrder;
  private final Condition _condition;
  private final Lock _lock;
  private final ConcurrentHashMap.KeySetView<Integer, Boolean> _segmentSequenceNumSet;
  private final int _partitionGroupId;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private volatile boolean _preloaded;

  public SemaphoreAccessCoordinator(Semaphore semaphore, boolean enforceConsumptionInOrder, int partitionGroupId,
      RealtimeTableDataManager realtimeTableDataManager) {
    _semaphore = semaphore;
    _lock = new ReentrantLock();
    _condition = _lock.newCondition();
    _enforceConsumptionInOrder = enforceConsumptionInOrder;
    _segmentSequenceNumSet = ConcurrentHashMap.newKeySet();
    _partitionGroupId = partitionGroupId;
    _realtimeTableDataManager = realtimeTableDataManager;
    _preloaded = false;
  }

  public void acquire(LLCSegmentName llcSegmentName)
      throws InterruptedException {

    if (_enforceConsumptionInOrder) {
      int prevSequenceNum = llcSegmentName.getSequenceNumber() - 1;
      if (prevSequenceNum >= 0) {
        waitForPrevSegment(prevSequenceNum);
      }
    }

    _semaphore.acquire();
  }

  public void release() {
    _semaphore.release();
    try {
      _lock.lock();
      _condition.signalAll();
    } finally {
      _lock.unlock();
    }
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

  private void waitForPrevSegment(int prevSeqNum)
      throws InterruptedException {
    if (!_preloaded) {
      preload();
    }
    _lock.lock();
    try {
      while (!_segmentSequenceNumSet.contains(prevSeqNum)) {
        _condition.await();
      }
    } finally {
      _lock.unlock();
    }
  }

  private synchronized void preload()
      throws InterruptedException {

    if (_preloaded) {
      return;
    }

    while (!_realtimeTableDataManager.getIsTableReadyToConsumeData().getAsBoolean()) {
      Thread.sleep(RealtimeTableDataManager.READY_TO_CONSUME_DATA_CHECK_INTERVAL_MS);
    }

    for (String segment : _realtimeTableDataManager.getSegments()) {
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segment);
      Preconditions.checkNotNull(llcSegmentName);
      if (llcSegmentName.getPartitionGroupId() == _partitionGroupId) {
        _segmentSequenceNumSet.add(llcSegmentName.getSequenceNumber());
      }
    }

    _preloaded = true;
  }
}
