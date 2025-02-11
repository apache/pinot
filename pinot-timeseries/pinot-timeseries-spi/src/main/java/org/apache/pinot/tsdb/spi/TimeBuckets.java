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
package org.apache.pinot.tsdb.spi;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;


/**
 * Time buckets used for query execution. Each element (say x) in the {@link #getTimeBuckets()} array represents a
 * time-range which is half open on the left side: (x - bucketSize.getSeconds(), x]. Some query languages allow some
 * operators to mutate the time-buckets on the fly, so it is not guaranteed that the time resolution and/or range
 * will be the same across all operators. For instance, Uber's M3QL supports a "summarize 1h sum" operator which will
 * change the bucket resolution to 1 hour for all subsequent operators.
 */
public class TimeBuckets {
  private final Long[] _timeBuckets;
  private final Duration _bucketSize;

  private TimeBuckets(Long[] timeBuckets, Duration bucketSize) {
    _timeBuckets = timeBuckets;
    _bucketSize = bucketSize;
  }

  public Long[] getTimeBuckets() {
    return _timeBuckets;
  }

  public Duration getBucketSize() {
    return _bucketSize;
  }

  public long getTimeRangeStartExclusive() {
    return _timeBuckets[0] - _bucketSize.getSeconds();
  }

  public long getTimeRangeEndInclusive() {
    return _timeBuckets[_timeBuckets.length - 1];
  }

  public long getRangeSeconds() {
    return getTimeRangeEndInclusive() - getTimeRangeStartExclusive();
  }

  public int getNumBuckets() {
    return _timeBuckets.length;
  }

  public int resolveIndex(long timeValue) {
    if (_timeBuckets.length == 0) {
      return -1;
    }
    if (timeValue <= getTimeRangeStartExclusive() || timeValue > getTimeRangeEndInclusive()) {
      return -1;
    }
    long offsetFromRangeStart = timeValue - getTimeRangeStartExclusive();
    // Subtract 1 from the offset because we have intervals half-open on the left.
    return (int) ((offsetFromRangeStart - 1) / _bucketSize.getSeconds());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TimeBuckets)) {
      return false;
    }
    TimeBuckets other = (TimeBuckets) o;
    return this.getTimeRangeStartExclusive() == other.getTimeRangeStartExclusive()
        && this.getTimeRangeEndInclusive() == other.getTimeRangeEndInclusive()
        && this.getBucketSize().equals(other.getBucketSize());
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(_bucketSize);
    result = 31 * result + Arrays.hashCode(_timeBuckets);
    return result;
  }

  /**
   * Creates time-buckets, with the first value in the bucket being firstBucketValue (FBV). The time range represented
   * by the buckets are:
   * <pre>
   *   (FBV - bucketSize.getSeconds(), FBV + (numElements - 1) * bucketSize.getSeconds()]
   * </pre>
   * The raw Long[] time values are:
   * <pre>
   *   FBV, FBV + bucketSize.getSeconds(), ... , FBV + (numElements - 1) * bucketSize.getSeconds()
   * </pre>
   */
  public static TimeBuckets ofSeconds(long firstBucketValue, Duration bucketSize, int numElements) {
    long stepSize = bucketSize.getSeconds();
    Long[] timeBuckets = new Long[numElements];
    for (int i = 0; i < numElements; i++) {
      timeBuckets[i] = firstBucketValue + i * stepSize;
    }
    return new TimeBuckets(timeBuckets, bucketSize);
  }
}
