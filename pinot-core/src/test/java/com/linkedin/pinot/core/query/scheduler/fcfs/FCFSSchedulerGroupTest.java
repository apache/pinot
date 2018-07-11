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
package com.linkedin.pinot.core.query.scheduler.fcfs;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.query.scheduler.SchedulerQueryContext;
import com.yammer.metrics.core.MetricsRegistry;
import org.testng.annotations.Test;

import static com.linkedin.pinot.core.query.scheduler.TestHelper.*;
import static org.testng.Assert.*;


public class FCFSSchedulerGroupTest {
  static final ServerMetrics metrics = new ServerMetrics(new MetricsRegistry());

  @Test
  public void testCompare() {
    // Both groups are null
    assertEquals(FCFSSchedulerGroup.compare(null, null), 0);

    FCFSSchedulerGroup lhs = new FCFSSchedulerGroup("one");
    FCFSSchedulerGroup rhs = new FCFSSchedulerGroup("two");
    assertEquals(FCFSSchedulerGroup.compare(lhs, lhs), 0);

    // Both groups are empty
    assertNull(lhs.peekFirst());
    assertNull(rhs.peekFirst());
    assertEquals(lhs.compareTo(rhs), 0);
    assertEquals(rhs.compareTo(lhs), 0);

    SchedulerQueryContext firstRequest = createQueryRequest("groupOne", metrics, 2000);
    lhs.addLast(firstRequest);
    assertEquals(lhs.compareTo(rhs), 1);
    assertEquals(rhs.compareTo(lhs), -1);

    SchedulerQueryContext secondRequest = createQueryRequest("groupTwo", metrics, 3000);
    rhs.addLast(secondRequest);
    assertEquals(lhs.compareTo(rhs), 1);
    assertEquals(rhs.compareTo(lhs), -1);
  }
}
