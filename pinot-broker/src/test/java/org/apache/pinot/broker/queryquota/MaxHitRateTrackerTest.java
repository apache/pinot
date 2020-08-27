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

import org.testng.Assert;
import org.testng.annotations.Test;


public class MaxHitRateTrackerTest {

  @Test
  public void testMaxHitRateTracker() {
    int timeInSec = 60;
    MaxHitRateTracker hitCounter = new MaxHitRateTracker(timeInSec);
    long currentTimestamp = System.currentTimeMillis();
    for (int i = 0; i < timeInSec; i++) {
      for (int j = 0; j < 5; j++) {
        hitCounter.hit(currentTimestamp + i * 1000);
      }
    }
    long latestTimeStamp = currentTimestamp + (timeInSec - 1) * 1000;
    Assert.assertNotNull(hitCounter);
    Assert.assertEquals(5, hitCounter.getMaxCountPerBucket(latestTimeStamp));

    // 2 seconds have passed, the hit counter should return 5 as well since the count in the last bucket could increase.
    latestTimeStamp = latestTimeStamp + 2000L;
    Assert.assertEquals(5, hitCounter.getMaxCountPerBucket(latestTimeStamp));

    // This time it should return 0 as the internal lastAccessTimestamp has already been updated and there is no more hits between the gap.
    latestTimeStamp = latestTimeStamp + 2000L;
    Assert.assertEquals(0, hitCounter.getMaxCountPerBucket(latestTimeStamp));

    // Increment the hit in this second and we should see the result becomes 1.
    hitCounter.hit(latestTimeStamp);
    latestTimeStamp = latestTimeStamp + 2000L;
    Assert.assertEquals(1, hitCounter.getMaxCountPerBucket(latestTimeStamp));

    // More than a time range period has passed and the hit counter should return 0 as there is no hits.
    hitCounter.hit(latestTimeStamp);
    latestTimeStamp = latestTimeStamp + timeInSec * 2 * 1000L + 2000L;
    Assert.assertEquals(0, hitCounter.getMaxCountPerBucket(latestTimeStamp));
  }
}
