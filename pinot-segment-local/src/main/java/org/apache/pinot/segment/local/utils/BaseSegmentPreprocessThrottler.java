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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.pinot.common.concurrency.AdjustableSemaphore;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.slf4j.Logger;


/**
 * Base class for segment preprocess throttlers, contains the common logic for the semaphore. The semaphore cannot
 * be null and must contain > 0 total permits
 */
public abstract class BaseSegmentPreprocessThrottler implements PinotClusterConfigChangeListener {

  protected AdjustableSemaphore _semaphore;

  /**
   * Base segment preprocess throttler constructor
   * @param initialPermits initial permits to use for the semaphore
   * @param logger logger to use
   */
  public BaseSegmentPreprocessThrottler(int initialPermits, Logger logger) {
    Preconditions.checkArgument(initialPermits > 0,
        "Semaphore initialPermits must be > 0, but found to be: " + initialPermits);
    _semaphore = new AdjustableSemaphore(initialPermits, true);
    logger.info("Created semaphore with total permits: {}, available permits: {}", totalPermits(),
        availablePermits());
  }

  /**
   * Block trying to acquire the semaphore to perform the segment StarTree index rebuild steps unless interrupted.
   * <p>
   * {@link #release()} should be called after the segment preprocess completes. It is the responsibility of the caller
   * to ensure that {@link #release()} is called exactly once for each call to this method.
   *
   * @throws InterruptedException if the current thread is interrupted
   */
  public void acquire()
      throws InterruptedException {
    _semaphore.acquire();
  }

  /**
   * Should be called after the segment StarTree index build completes. It is the responsibility of the caller to
   * ensure that this method is called exactly once for each call to {@link #acquire()}.
   */
  public void release() {
    _semaphore.release();
  }

  /**
   * Get the number of available permits
   * @return number of available permits
   */
  @VisibleForTesting
  protected int availablePermits() {
    return _semaphore.availablePermits();
  }

  /**
   * Get the total number of permits
   * @return total number of permits
   */
  @VisibleForTesting
  protected int totalPermits() {
    return _semaphore.getTotalPermits();
  }
}
