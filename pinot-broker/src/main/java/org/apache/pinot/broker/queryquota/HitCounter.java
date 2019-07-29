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
 * This hit counter is for counting the number of hits within a range of time.
 * Currently two callers are there
 *
 * (1) QueryQuota Manager {@link HelixExternalViewBasedQueryQuotaManager} which
 * uses the hit counter for 1sec range of time and 100 time buckets thus
 * each bucket covers a time width of 10ms.
 *
 * (2) {@link org.apache.pinot.broker.requesthandler.BrokerPeakQPSMetricsHandler}
 * which uses the hit counter to keep track of peak QPS per minute for broker. The
 * hit counter is configured for a time range of 60secs with 600 buckets thus
 * each bucket covers a time width of 100ms
 *
 * In order to save the space and time, we store the number of hits over the
 * last configured number of time buckets. When the method hit gets called,
 * we put the timestamp to the specified bucket. When the method getHitCount
 * gets called, we sum all the numberof hits within the last 100 time buckets.
 */
public class HitCounter {
  private final int _bucketCount;
  private final int _timeBucketWidthMs;
  private final AtomicLongArray _bucketStartTime;
  private final AtomicIntegerArray _bucketHitCount;

  public HitCounter(final int timeRangeInSeconds, final int bucketCount) {
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

  /*
   * Explanation of algorithm to keep track of
   * max QPS within a minute window
   *
   * Hit counter is configured with fixed number buckets
   * and each bucket covers a fixed time window to
   * cover an overall range of 60secs.
   *
   * Each bucket stores two values -- start time
   * and hit counter.
   *
   * Example:
   * T = timestamp
   * BST = bucket start time
   * B = bucket index
   * WIDTH = bucket window width = 10000ms
   * BUCKETS = 6
   *
   * Every time hitAndUpdateLatestTime() is called,
   * we do the following:
   *
   * T = current timestamp
   * BST = T / WIDTH
   * B = BST % BUCKETS
   *
   * if a timestamp T is ending in a bucket B,
   * T, T + 0, T + 1, ..... T + 9999ms will all
   * end up in the same bucket B
   *
   * T + WIDTHms will end up in the next bucket
   * and so on
   *
   * T + 60000, T + 60000 + 1 ... T + 60000 + 9999
   * will also end up in the same bucket B as T but
   * BST would be T + BUCKETS
   *
   * Now this is how bucket update rules work on every
   * call to hitAndUpdateLatesTime()
   *
   * (1) Get T
   * (2) Compute BST and B
   * (3) CURR = current BST of B
   * (4) update B's start time as BST
   * (5) if BST != CURR, it implies we have gone
   * over a minute and the current hit counter value of the
   * bucket along with CURR can be overwritten --
   * so we set B's hit counter to 1 and B's BST to BST.
   * else, we increment B's hit counter by 1
   *
   * note that BST != CURR also implies that
   * BST - CURR >= BUCKETS which further indicates we have gone
   * over a minute.
   *
   * getMaxHitCount() -- used to update BrokerGauge with peak
   * QPS per minute
   *
   * (1) go over all buckets
   * (2) keep track of 2 max values seen so far in the loop
   * (a) max_BST and (b) max_hits
   * (3) consider each bucket's BST to see if max_BST
   * should be updated
   * (4) consider each bucket's hit counter value
   * as follows to see if max_hits need to be updated
   *
   * if abs(bucket's BST - max_BST) <= BUCKETS, it
   * tell us that start time of this bucket is within
   * the window of 1min, so we should consider it's hit
   * counter value to update max_hits
   *
   * else if bucket's BST > max_BST, it implies
   * that bucket's BST is beyond current max_BST
   * by more than a minute, so we should simply
   * set max_hits as bucket's hit counter value
   *
   * See the unit test for peakQPS in HitCounterTest
   */

  /**
   * Currently invoked by {@link org.apache.pinot.broker.requesthandler.BrokerPeakQPSMetricsHandler}
   * every time a query comes into {@link org.apache.pinot.broker.requesthandler.BaseBrokerRequestHandler}
   */
  public void hitAndUpdateLatestTime() {
    hitAndUpdateLatestTime(System.currentTimeMillis());
  }

  @VisibleForTesting
  void hitAndUpdateLatestTime(final long timestamp) {
    // since we are updating two words, it is not possible
    // to do them in a single atomic instruction
    // unless there is a way to do multi-word CAS.
    // wrapping this under synchronize block will
    // hurt performance. currently the caller of
    // this BrokerPeakQPSMetricsHandler creates 600 buckets
    // so that should avoid extreme contention on a
    // single bucket. it should be noted that without
    // the use of synchronize, we should expect some
    // inconsistency at a bucket level
    final long numTimeUnits = timestamp / _timeBucketWidthMs;
    final int bucketIndex = (int) (numTimeUnits % _bucketCount);
    if (numTimeUnits != _bucketStartTime.get(bucketIndex)) {
      _bucketHitCount.set(bucketIndex, 1);
      _bucketStartTime.set(bucketIndex, numTimeUnits);
    } else {
      _bucketHitCount.incrementAndGet(bucketIndex);
    }

    /* potential alternative approach where we can miss
    some updates by simply returning if CAS fails
    final long numTimeUnits = timestamp / _timeBucketWidthMs;
    final int bucketIndex = (int) (numTimeUnits % _bucketCount);
    final int hitCount = _bucketHitCount.get(bucketIndex);
    final long startTime = _bucketStartTime.get(bucketIndex);
    if (numTimeUnits != startTime) {
      if (_bucketStartTime.compareAndSet(bucketIndex, startTime, numTimeUnits)) {
        _bucketHitCount.set(bucketIndex, 1);
      }
    } else {
      if (_bucketHitCount.compareAndSet(bucketIndex, hitCount, hitCount + 1)) {
        _bucketStartTime.set(bucketIndex, numTimeUnits);
      }
    }*/
  }

  /**
   * Used to update {@link org.apache.pinot.common.metrics.BrokerGauge}
   * @return max hit count in a minute
   */
  public long getMaxHitCount() {
    long maxWindowStartTime = _bucketStartTime.get(0);
    int maxHits = _bucketHitCount.get(0);
    for (int bucket = 1; bucket < _bucketCount; ++bucket) {
      final long startTime = _bucketStartTime.get(bucket);
      final int hits = _bucketHitCount.get(bucket);
      if (Math.abs(startTime - maxWindowStartTime) <= _bucketCount) {
        maxHits = Math.max(maxHits, hits);
      } else if (startTime > maxWindowStartTime) {
        maxHits = hits;
      }
      maxWindowStartTime = Math.max(maxWindowStartTime, startTime);
    }
    return maxHits;
  }
}
