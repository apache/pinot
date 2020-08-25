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
package org.apache.pinot.broker.queryquota;

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
  private static int DEFAULT_BUCKET_COUNT = 100;
  private final int _timeBucketWidthMs;
  private final int _bucketCount;
  private final AtomicLongArray _bucketStartTime;
  private final AtomicIntegerArray _bucketHitCount;

  public HitCounter(int timeRangeInSeconds) {
    this(timeRangeInSeconds, DEFAULT_BUCKET_COUNT);
  }

  public HitCounter(int timeRangeInSeconds, int bucketCount) {
    _bucketCount = bucketCount;
    _timeBucketWidthMs = timeRangeInSeconds * 1000 / _bucketCount;
    _bucketStartTime = new AtomicLongArray(_bucketCount);
    _bucketHitCount = new AtomicIntegerArray(_bucketCount);
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
    int index = (int) (numTimeUnits % _bucketCount);
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
    for (int i = 0; i < _bucketCount; i++) {
      if (numTimeUnits - _bucketStartTime.get(i) < _bucketCount) {
        count += _bucketHitCount.get(i);
      }
    }
    return count;
  }

  /**
   * Get the maximum count among the buckets
   */
  public int getMaxCountPerBucket() {
    return getMaxCountPerBucket(System.currentTimeMillis());
  }

  @VisibleForTesting
  int getMaxCountPerBucket(long timestamp) {
    long numTimeUnits = timestamp / _timeBucketWidthMs;
    int maxCount = 0;
    for (int i = 0; i < _bucketCount; i++) {
      if (numTimeUnits - _bucketStartTime.get(i) < _bucketCount) {
        maxCount = Math.max(_bucketHitCount.get(i), maxCount);
      }
    }
    return maxCount;
  }
}
