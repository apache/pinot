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
package org.apache.pinot.common.coordination;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.coordination.QueryDeadlineGuard.TimeInterval;
import org.apache.pinot.common.request.context.NativeQueryDeadline;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Explicit synthetic shared clocks verify transit budgets and failure isolation without assuming synchronized hosts.
public class QueryDeadlineGuardTest {
  @Test
  public void testTransitAndRetriesPreserveOriginAndBoundedLifetime() {
    AtomicReference<TimeInterval> originClock = new AtomicReference<>(new TimeInterval(1000, 1004, true));
    QueryDeadlineGuard origin = new QueryDeadlineGuard(originClock::get, "test-utc", 80, 4, 999_000);
    NativeQueryDeadline deadline = origin.originate(100);
    assertEquals(deadline, new NativeQueryDeadline("test-utc", 1000, 1080));
    assertEquals(origin.remainingMillis(deadline), 76);

    AtomicReference<TimeInterval> serverClock = new AtomicReference<>(new TimeInterval(1020, 1024, true));
    QueryDeadlineGuard server = new QueryDeadlineGuard(serverClock::get, "test-utc", 80, 4, 999_000);
    assertEquals(server.remainingMillis(deadline), 56);
    serverClock.set(new TimeInterval(1040, 1044, true));
    assertEquals(server.remainingMillis(deadline), 36, "A retry must consume transit and elapsed time");
    expectThrows(IllegalStateException.class,
        () -> server.check(new NativeQueryDeadline("other-domain", 1000, 1080)));
    expectThrows(IllegalStateException.class,
        () -> server.check(new NativeQueryDeadline("test-utc", 1050, 1080)));
    expectThrows(IllegalStateException.class,
        () -> server.check(new NativeQueryDeadline("test-utc", 1000, 1081)));
    assertEquals(server.remainingMillis(deadline), 36, "A malformed request must not invalidate the shared clock");

    serverClock.set(new TimeInterval(1076, 1080, true));
    expectThrows(IllegalStateException.class, () -> server.check(deadline));
    assertEquals(server.originate(20), new NativeQueryDeadline("test-utc", 1076, 1096),
        "One expired query must not block another query");
  }

  @Test
  public void testQualificationLossIsTerminalAndArithmeticFailsClosed() {
    for (TimeInterval invalid : List.of(new TimeInterval(1000, 1001, false),
        new TimeInterval(1000, 1005, true), new TimeInterval(1000, 999, true))) {
      AtomicReference<TimeInterval> clock = new AtomicReference<>(invalid);
      QueryDeadlineGuard guard = new QueryDeadlineGuard(clock::get, "test-utc", 100, 4, 1_000_000);
      expectThrows(IllegalStateException.class, () -> guard.originate(20));
      clock.set(new TimeInterval(1000, 1000, true));
      expectThrows(IllegalStateException.class, () -> guard.originate(20));
    }
    QueryDeadlineGuard overflow = new QueryDeadlineGuard(
        () -> new TimeInterval(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1, true), "test-utc", 100, 0, 1_000_000);
    expectThrows(IllegalStateException.class, () -> overflow.originate(2));
    assertEquals(overflow.originate(1).notAfterMillis(), Long.MAX_VALUE);
    QueryDeadlineGuard unavailable = new QueryDeadlineGuard(() -> {
      throw new IllegalStateException("Synthetic clock unavailable");
    }, "test-utc", 100, 0, 1_000_000);
    expectThrows(IllegalStateException.class, () -> unavailable.originate(20));
    expectThrows(IllegalArgumentException.class, () -> new QueryDeadlineGuard(
        () -> new TimeInterval(1, 1, true), "test-utc", 100, 0, 1_000_001));
  }
}
