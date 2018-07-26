/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.broker.queryquota;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;


/**
 * This hit counter is for counting the number of hits within a range of time. Right now the granularity we use is second.
 * In order to save the space and time, we store the number of hits over the last 100 time buckets. When the method hit
 * gets called, we put the timestamp to the specified bucket. When the method getHitCount gets called, we sum all the number
 * of hits within the last 100 time buckets.
 */
public class HitCounter {
  private static int BUCKET_COUNT = 100;
  private final int _timeBucketWidthMs;
  private final AtomicLongArray _bucketStartTime;
  private final AtomicIntegerArray _bucketHitCount;

  public HitCounter(int timeRangeInSeconds) {
    _timeBucketWidthMs = timeRangeInSeconds * 1000 / BUCKET_COUNT;
    _bucketStartTime = new AtomicLongArray(BUCKET_COUNT);
    _bucketHitCount = new AtomicIntegerArray(BUCKET_COUNT);
  }

  /**
   * Increase the hit count in the current bucket.
   */
  public void hit() {
    hit(System.currentTimeMillis());
  }

  @VisibleForTesting
  void hit(long timestamp) {
    long numTimeUnits = timestamp / _timeBucketWidthMs;
    int index = (int)(numTimeUnits % BUCKET_COUNT);
    if (_bucketStartTime.get(index) == numTimeUnits) {
      _bucketHitCount.incrementAndGet(index);
    } else {
      synchronized (_bucketStartTime) {
        if (_bucketStartTime.get(index) != numTimeUnits) {
          _bucketHitCount.set(index, 1);
          _bucketStartTime.set(index, numTimeUnits);
        } else {
          _bucketHitCount.incrementAndGet(index);
        }
      }
    }
  }

  /**
   * Get the total hit count within a time range.
   */
  public int getHitCount() {
    return getHitCount(System.currentTimeMillis());
  }

  @VisibleForTesting
  int getHitCount(long timestamp) {
    long numTimeUnits = timestamp / _timeBucketWidthMs;
    int count = 0;
    for (int i = 0; i < BUCKET_COUNT; i++) {
      if (numTimeUnits - _bucketStartTime.get(i) < BUCKET_COUNT) {
        count += _bucketHitCount.get(i);
      }
    }
    return count;
  }
}
