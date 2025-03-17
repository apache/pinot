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

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;


public class SemaphoreCoordinator {

  private final Semaphore _semaphore;
  private final boolean _enforceConsumptionInOrder;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final Condition _condition;
  private final Lock _lock;

  public SemaphoreCoordinator(Semaphore semaphore, boolean enforceConsumptionInOrder,
      RealtimeTableDataManager realtimeTableDataManager) {
    _semaphore = semaphore;
    _lock = new ReentrantLock();
    _condition = _lock.newCondition();
    _enforceConsumptionInOrder = enforceConsumptionInOrder;
    _realtimeTableDataManager = realtimeTableDataManager;
  }

  public void acquire(SegmentZKMetadata segmentZKMetadata)
      throws InterruptedException {
    String prevSegmentName = segmentZKMetadata.getPreviousSegment();

    if (_enforceConsumptionInOrder) {
      waitForPrevSegment(prevSegmentName);
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

  private void waitForPrevSegment(String prevSegmentName)
      throws InterruptedException {
    SegmentDataManager segmentDataManager = _realtimeTableDataManager.acquireSegment(prevSegmentName);
    _lock.lock();
    try {
      while (segmentDataManager == null) {
        _condition.await();
        segmentDataManager = _realtimeTableDataManager.acquireSegment(prevSegmentName);
      }
    } finally {
      _lock.unlock();
      if (segmentDataManager != null) {
        _realtimeTableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }
}
