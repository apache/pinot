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
package org.apache.pinot.core.query.distinct;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class DistinctEarlyTerminationContextTest {

  @Test
  public void testTimeBudgetStopsAsTimeElapses() {
    AtomicLong now = new AtomicLong(100);
    LongSupplier timeSupplier = now::get;

    DistinctEarlyTerminationContext context = new DistinctEarlyTerminationContext();
    context.setTimeSupplier(timeSupplier);
    context.setRemainingTimeNanos(10);

    assertFalse(context.shouldStopProcessing());
    now.set(109);
    assertFalse(context.shouldStopProcessing());
    now.set(110);
    assertTrue(context.shouldStopProcessing());
  }

  @Test
  public void testUnlimitedTimeBudgetDoesNotStop() {
    AtomicLong now = new AtomicLong(0);

    DistinctEarlyTerminationContext context = new DistinctEarlyTerminationContext();
    context.setTimeSupplier(now::get);
    context.setRemainingTimeNanos(Long.MAX_VALUE);

    assertEquals(context.getRemainingTimeNanos(), Long.MAX_VALUE);
    now.set(Long.MAX_VALUE);
    assertFalse(context.shouldStopProcessing());
  }

  @Test
  public void testExhaustedTimeBudgetStopsImmediately() {
    AtomicLong now = new AtomicLong(100);

    DistinctEarlyTerminationContext context = new DistinctEarlyTerminationContext();
    context.setTimeSupplier(now::get);
    context.setRemainingTimeNanos(0);

    assertEquals(context.getRemainingTimeNanos(), 0);
    assertTrue(context.shouldStopProcessing());
  }
}
