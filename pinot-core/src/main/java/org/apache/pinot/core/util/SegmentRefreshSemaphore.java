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

package org.apache.pinot.core.util;

import java.util.concurrent.Semaphore;
import org.slf4j.Logger;


/**
 * Wrapper class for semaphore used to control concurrent segment reload/refresh
 */
public class SegmentRefreshSemaphore {

  private final Semaphore _semaphore;

  public SegmentRefreshSemaphore(int permits, boolean fair) {
    if (permits > 0) {
      _semaphore = new Semaphore(permits, fair);
    } else {
      _semaphore = null;
    }
  }

  public void acquireSema(String segmentName, Logger logger)
      throws InterruptedException {
    if (_semaphore != null) {
      long startTime = System.currentTimeMillis();
      logger.info("Waiting for lock to refresh : {}, queue-length: {}", segmentName,
          _semaphore.getQueueLength());
      _semaphore.acquire();
      logger.info("Acquired lock to refresh segment: {} (lock-time={}ms, queue-length={})", segmentName,
          System.currentTimeMillis() - startTime, _semaphore.getQueueLength());
    } else {
      logger.info("Locking of refresh threads disabled (segment: {})", segmentName);
    }
  }

  public void releaseSema() {
    if (_semaphore != null) {
      _semaphore.release();
    }
  }
}
