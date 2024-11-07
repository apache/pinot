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
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TimeBucketsTest {
  @Test
  public void testTimeBucketsSemantics() {
    /*
     * time-bucket values: [10_000, 10_100, 10_200, ... , 10_900]
     */
    final int firstBucketValue = 10_000;
    final int bucketSize = 100;
    final int numElements = 10;
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(firstBucketValue, Duration.ofSeconds(bucketSize), numElements);
    assertEquals(timeBuckets.getNumBuckets(), numElements);
    assertEquals(timeBuckets.getBucketSize().getSeconds(), bucketSize);
    assertEquals(timeBuckets.getTimeRangeStartExclusive(), firstBucketValue - bucketSize);
    assertEquals(timeBuckets.getTimeRangeEndInclusive(), firstBucketValue + (numElements - 1) * bucketSize);
    assertEquals(timeBuckets.getRangeSeconds(),
        timeBuckets.getTimeRangeEndInclusive() - timeBuckets.getTimeRangeStartExclusive());
    assertEquals(timeBuckets.resolveIndex(10_000), 0);
    assertEquals(timeBuckets.resolveIndex(9_999), 0);
    assertEquals(timeBuckets.resolveIndex(9_901), 0);
    assertEquals(timeBuckets.resolveIndex(10_100), 1);
    assertEquals(timeBuckets.resolveIndex(10_101), 2);
    assertEquals(timeBuckets.resolveIndex(10_900), 9);
    // Test out of bound indexes
    assertEquals(timeBuckets.resolveIndex(9_900), -1);
    assertEquals(timeBuckets.resolveIndex(10_901), -1);
  }
}
