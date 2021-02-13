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
package org.apache.pinot.core.query.scheduler.fcfs;

import org.apache.pinot.common.metrics.base.PinotMetricUtilsFactory;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.query.scheduler.SchedulerQueryContext;
import org.testng.annotations.Test;

import static org.apache.pinot.core.query.scheduler.TestHelper.createQueryRequest;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class FCFSSchedulerGroupTest {
  static ServerMetrics metrics = new ServerMetrics(PinotMetricUtilsFactory.getPinotMetricsRegistry());

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
