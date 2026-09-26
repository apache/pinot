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
package org.apache.pinot.spi.accounting;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Captured resource samplers have bounded lifetimes and never move accounting ownership to the monitor.
public class ExternalExecutionSamplerTest {
  @Test
  public void testPauseAndSampleThenClose() {
    AtomicInteger calls = new AtomicInteger();
    AtomicBoolean pause = new AtomicBoolean();
    ExternalExecutionSampler sampler = new ExternalExecutionSampler(calls::incrementAndGet, pause::get);
    assertFalse(sampler.isPaused());
    pause.set(true);
    assertTrue(sampler.isPaused());
    sampler.sampleUsage();
    sampler.close();
    sampler.close();
    sampler.sampleUsage();
    assertFalse(sampler.isPaused());
    assertEquals(calls.get(), 1);
  }

  @Test
  public void testUnknownAccountantMustNotSilentlyLosePolicy() {
    ThreadAccountant unsupported = mock(ThreadAccountant.class, CALLS_REAL_METHODS);
    assertNull(unsupported.captureExternalExecutionSampler());
    try (ExternalExecutionSampler sampler =
        ThreadAccountantUtils.getNoOpAccountant().captureExternalExecutionSampler()) {
      sampler.sampleUsage();
      assertFalse(sampler.isPaused());
    }
  }

  @Test(timeOut = 10000)
  public void testCloseDrainsRunningSampleAndOnlyOwnerCanClose() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch closeStarted = new CountDownLatch(1);
    AtomicInteger samples = new AtomicInteger();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    ExternalExecutionSampler sampler = new ExternalExecutionSampler(() -> {
      entered.countDown();
      await(release);
      samples.incrementAndGet();
    }, () -> false);
    try {
      executor.submit(() -> assertThrows(IllegalStateException.class, sampler::close)).get(5, TimeUnit.SECONDS);
      Future<?> sampling = executor.submit(sampler::sampleUsage);
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      Future<?> releasing = executor.submit(() -> {
        await(closeStarted);
        release.countDown();
      });
      closeStarted.countDown();
      sampler.close();
      assertEquals(samples.get(), 1);
      sampling.get(5, TimeUnit.SECONDS);
      releasing.get(5, TimeUnit.SECONDS);
      sampler.sampleUsage();
      assertEquals(samples.get(), 1);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("Timed out waiting for test coordination");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
