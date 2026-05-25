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
package org.apache.pinot.spi.query;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class QueryScanCostContextTest {

  @Test
  public void testInitialValuesAreZero() {
    QueryScanCostContext ctx = new QueryScanCostContext();
    assertEquals(ctx.getNumEntriesScannedInFilter(), 0L);
    assertEquals(ctx.getNumDocsScanned(), 0L);
    assertEquals(ctx.getNumEntriesScannedPostFilter(), 0L);
    assertTrue(ctx.getElapsedTimeMs() >= 0);
  }

  @Test
  public void testSingleThreadIncrements() {
    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(100);
    ctx.addEntriesScannedInFilter(200);
    assertEquals(ctx.getNumEntriesScannedInFilter(), 300L);

    ctx.addDocsScanned(50);
    ctx.addDocsScanned(25);
    assertEquals(ctx.getNumDocsScanned(), 75L);

    ctx.addEntriesScannedPostFilter(1000);
    assertEquals(ctx.getNumEntriesScannedPostFilter(), 1000L);
  }

  @Test
  public void testElapsedTime()
      throws InterruptedException {
    QueryScanCostContext ctx = new QueryScanCostContext();
    Thread.sleep(50);
    assertTrue(ctx.getElapsedTimeMs() >= 40, "Elapsed time should be at least ~50ms");
  }

  @Test
  public void testConcurrentWritesAreCorrect()
      throws InterruptedException {
    QueryScanCostContext ctx = new QueryScanCostContext();
    int numThreads = 16;
    long incrementsPerThread = 100_000;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        for (long j = 0; j < incrementsPerThread; j++) {
          ctx.addEntriesScannedInFilter(1);
          ctx.addDocsScanned(1);
        }
        latch.countDown();
      });
    }
    latch.await();
    executor.shutdown();

    long expected = numThreads * incrementsPerThread;
    assertEquals(ctx.getNumEntriesScannedInFilter(), expected);
    assertEquals(ctx.getNumDocsScanned(), expected);
  }

  @Test
  public void testLargeIncrements() {
    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(500_000_000L);
    ctx.addEntriesScannedInFilter(500_000_000L);
    assertEquals(ctx.getNumEntriesScannedInFilter(), 1_000_000_000L);
  }
}
