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
package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Test the performance on {@link IntOnHeapMutableDictionary}.
 */
public class DictionaryPerfRunner {
  private static final Runtime RUNTIME = Runtime.getRuntime();
  private static final int NUM_VALUES = 5_000_000;
  private static final int NUM_READERS = 5;
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(NUM_READERS);

  /**
   * Test the performance for value indexing, dictId fetching and value fetching.
   * <p>We first index all values into the dictionary, then fetch dictIds and values from it.
   * <p>Test both CPU (including random) and memory usage.
   */
  private static void readWritePerfOnCardinality(int cardinality) {
    IntOnHeapMutableDictionary intOnHeapMutableDictionary = new IntOnHeapMutableDictionary();

    System.out.println("Starting read write perf on cardinality: " + cardinality + " for " + NUM_VALUES + " values");
    RUNTIME.gc();

    long usedMemory = RUNTIME.totalMemory();
    long start = System.currentTimeMillis();
    for (int i = 0; i < NUM_VALUES; i++) {
      intOnHeapMutableDictionary.index(i % cardinality);
    }
    System.out.println("Index time for IntOnHeapMutableDictionary: " + (System.currentTimeMillis() - start));
    start = System.currentTimeMillis();
    for (int i = 0; i < NUM_VALUES; i++) {
      intOnHeapMutableDictionary.indexOf(i % cardinality);
    }
    System.out.println("DictId fetching time for IntOnHeapMutableDictionary: " + (System.currentTimeMillis() - start));
    start = System.currentTimeMillis();
    for (int i = 0; i < NUM_VALUES; i++) {
      intOnHeapMutableDictionary.get(i % cardinality);
    }
    System.out.println("Value fetching time for IntOnHeapMutableDictionary: " + (System.currentTimeMillis() - start));
    System.out.println("Memory usage for IntOnHeapMutableDictionary: " + (RUNTIME.totalMemory() - usedMemory));
  }

  /**
   * Test the performance for multi-readers dictId fetching and value fetching.
   * <p>We first index all values into the dictionary, then fetch dictIds and values from it.
   */
  private static void multiReadersPerfOnCardinality(final int cardinality)
      throws InterruptedException {
    final IntOnHeapMutableDictionary intOnHeapMutableDictionary = new IntOnHeapMutableDictionary();

    System.out.println(
        "Starting multi-readers perf on cardinality: " + cardinality + " for " + NUM_VALUES + " values with "
            + NUM_READERS + " readers");

    // Index all values
    for (int i = 0; i < cardinality; i++) {
      intOnHeapMutableDictionary.index(i);
    }

    // Count dictId and value fetching time
    countMultiReadersFetchingTime(cardinality, intOnHeapMutableDictionary);
  }

  private static void countMultiReadersFetchingTime(final int cardinality, final Dictionary dictionary)
      throws InterruptedException {
    final CountDownLatch countDownLatch = new CountDownLatch(NUM_READERS);
    final AtomicLong dictIdFetchingTime = new AtomicLong();
    final AtomicLong valueFetchingTime = new AtomicLong();
    for (int i = 0; i < NUM_READERS; i++) {
      EXECUTOR_SERVICE.execute(new Runnable() {
        @Override
        public void run() {
          long start = System.currentTimeMillis();
          for (int i = 0; i < NUM_VALUES; i++) {
            dictionary.indexOf(i % cardinality);
          }
          dictIdFetchingTime.addAndGet(System.currentTimeMillis() - start);
          start = System.currentTimeMillis();
          for (int i = 0; i < NUM_VALUES; i++) {
            dictionary.get(i % cardinality);
          }
          valueFetchingTime.addAndGet(System.currentTimeMillis() - start);
          countDownLatch.countDown();
        }
      });
    }
    countDownLatch.await();
    System.out.println(
        "Total dictId fetching time for " + dictionary.getClass().getSimpleName() + ": " + dictIdFetchingTime);
    System.out.println(
        "Total value fetching time for " + dictionary.getClass().getSimpleName() + ": " + valueFetchingTime);
  }

  public static void main(String[] args)
      throws InterruptedException {
    int cardinality = Integer.parseInt(args[0]);

    System.out.println("--------------------------------------------------------------------------------");
    readWritePerfOnCardinality(cardinality);
    System.out.println("--------------------------------------------------------------------------------");
    System.out.println("--------------------------------------------------------------------------------");
    multiReadersPerfOnCardinality(cardinality);
    System.out.println("--------------------------------------------------------------------------------");

    EXECUTOR_SERVICE.shutdown();
  }
}
