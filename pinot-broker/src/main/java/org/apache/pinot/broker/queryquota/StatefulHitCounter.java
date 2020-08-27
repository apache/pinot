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

/**
 * A stateful version of hit counter. Similar to the default hit counter, it maintains a list of buckets.
 * Whereas it maintains an extra variable called _lastAccessTimestamp which tracks the last access time.
 * If the stateful hit counter gets queried, it firstly compares the current timestamp and the last access timestamp,
 * calculating the start index and end index among the buckets. Then, it traverses through all the valid candidate buckets.
 * If the current timestamp has exceeded the current time range of all the buckets, this hit counter will use
 * the current timestamp minus the default time queried time range to calculate the start time index.
 */
public class StatefulHitCounter extends HitCounter {
  private long _maxTimeRangeMs;
  private long _defaultQueriedTimeRangeMs;
  private long _lastAccessTimestamp;

  public StatefulHitCounter(int timeRangeInSeconds, int bucketCount, int defaultQueriedTimeRangeInSeconds) {
    super(timeRangeInSeconds, bucketCount);
    _maxTimeRangeMs = timeRangeInSeconds * 1000L;
    _defaultQueriedTimeRangeMs = defaultQueriedTimeRangeInSeconds * 1000L;
  }

  /**
   * Get the maximum count among the buckets
   */
  public int getMaxCountPerBucket() {
    return getMaxCountPerBucket(System.currentTimeMillis());
  }

  @VisibleForTesting
  int getMaxCountPerBucket(long timestamp) {
    // If the hit counter didn't get queried for more than _maxTimeRangeMs
    if (timestamp - _lastAccessTimestamp > _maxTimeRangeMs) {
      _lastAccessTimestamp = timestamp - _defaultQueriedTimeRangeMs;
    }
    long startTimeUnits = _lastAccessTimestamp / _timeBucketWidthMs;
    int startIndex = (int) (startTimeUnits % _bucketCount);

    long numTimeUnits = timestamp / _timeBucketWidthMs;
    int endIndex = (int) (numTimeUnits % _bucketCount);

    int maxCount = 0;
    // Since the start index was accessed last time, there is no need to query its bucket this time.
    for (int i = startIndex; i != endIndex; i = (++i % _bucketCount)) {
      if (numTimeUnits - _bucketStartTime.get(i) < _bucketCount) {
        maxCount = Math.max(_bucketHitCount.get(i), maxCount);
      }
    }

    // Update the last access timestamp
    _lastAccessTimestamp = timestamp;
    return maxCount;
  }
}
