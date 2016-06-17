/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix.core.retention.strategy;

import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit test for time retention.
 */
public class TimeRetentionStrategyTest {
  @Test
  public void testTimeRetention() throws Exception {
    TimeRetentionStrategy retentionStrategy = new TimeRetentionStrategy("DAYS", "30");

    SegmentZKMetadata metadata = new OfflineSegmentZKMetadata();
    metadata.setTimeUnit(TimeUnit.DAYS);

    // Set end time to Jan 2nd, 1970 (not purgeable due to bogus timestamp)
    metadata.setEndTime(1L);
    assertFalse(retentionStrategy.isPurgeable(metadata));

    // Set end time to today
    final long today = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());
    metadata.setEndTime(today);
    assertFalse(retentionStrategy.isPurgeable(metadata));

    // Set end time to two weeks ago
    metadata.setEndTime(today - 14);
    assertFalse(retentionStrategy.isPurgeable(metadata));

    // Set end time to two months ago (purgeable due to being past the retention period)
    metadata.setEndTime(today - 60);
    assertTrue(retentionStrategy.isPurgeable(metadata));

    // Set end time to 200 years in the future (not purgeable due to bogus timestamp)
    metadata.setEndTime(today + (365 * 200));
    assertFalse(retentionStrategy.isPurgeable(metadata));
  }
}
