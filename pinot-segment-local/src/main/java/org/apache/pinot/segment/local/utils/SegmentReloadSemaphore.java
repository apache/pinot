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
package org.apache.pinot.segment.local.utils;

import java.util.concurrent.Semaphore;
import org.slf4j.Logger;


/**
 * Wrapper class for semaphore used to control concurrent segment reload/refresh.
 */
public class SegmentReloadSemaphore {
  private final Semaphore _semaphore;

  public SegmentReloadSemaphore(int permits) {
    _semaphore = new Semaphore(permits, true);
  }

  public void acquire(String segmentName, Logger logger)
      throws InterruptedException {
    long startTimeMs = System.currentTimeMillis();
    logger.info("Waiting for lock to reload: {}, queue-length: {}", segmentName, _semaphore.getQueueLength());
    _semaphore.acquire();
    logger.info("Acquired lock to reload segment: {} (lock-time={}ms, queue-length={})", segmentName,
        System.currentTimeMillis() - startTimeMs, _semaphore.getQueueLength());
  }

  public void release() {
    _semaphore.release();
  }
}
