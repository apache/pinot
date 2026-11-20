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
package org.apache.pinot.server.api.resources;

import org.testng.annotations.Test;

import static org.apache.pinot.server.api.resources.ReingestionResource.CHECK_INTERVAL_MS;
import static org.apache.pinot.server.api.resources.ReingestionResource.waitForCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


/// Tests how [ReingestionResource] waits for the re-ingestion consumption to complete.
public class ReingestionResourceTest {
  private static final String DESCRIPTION = "test condition";

  @Test
  public void testWaitForConditionWithMaxTimeoutDoesNotTimeOutEarly() {
    // The condition is false on the first check, so the wait must keep checking instead of timing out
    long completionTimeMs = System.currentTimeMillis() + 50;
    waitForCondition(() -> System.currentTimeMillis() >= completionTimeMs, DESCRIPTION, 10, Long.MAX_VALUE);
  }

  @Test
  public void testWaitForConditionDetectsCompletionWithTimeoutShorterThanCheckInterval() {
    // The condition becomes true after the first check. It must be checked again at the deadline instead of timing out
    // after sleeping a full check interval.
    long completionTimeMs = System.currentTimeMillis() + 100;
    waitForCondition(() -> System.currentTimeMillis() >= completionTimeMs, DESCRIPTION, CHECK_INTERVAL_MS, 4_000);
  }

  @Test
  public void testWaitForConditionDoesNotSleepPastTimeout() {
    long startTimeMs = System.currentTimeMillis();
    assertThatThrownBy(() -> waitForCondition(() -> false, DESCRIPTION, CHECK_INTERVAL_MS, 100)).hasMessage(
        "Timed out after 100ms waiting for test condition");
    assertThat(System.currentTimeMillis() - startTimeMs).isLessThan(CHECK_INTERVAL_MS);
  }

  @Test
  public void testWaitForConditionReportsInterrupt() {
    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> waitForCondition(() -> false, DESCRIPTION, CHECK_INTERVAL_MS, Long.MAX_VALUE))
          .hasMessage("Interrupted while waiting for test condition")
          .hasCauseInstanceOf(InterruptedException.class);
    } finally {
      // Clear the interrupt so that it does not leak into other tests
      Thread.interrupted();
    }
  }
}
