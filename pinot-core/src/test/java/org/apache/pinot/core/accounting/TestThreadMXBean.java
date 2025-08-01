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
package org.apache.pinot.core.accounting;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.pinot.spi.accounting.ThreadResourceSnapshot;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.utils.ResourceUsageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestThreadMXBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestThreadMXBean.class);

  @BeforeClass
  public void setup() {
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
  }

  /**
   * simple memory allocation
   */
  @Test
  public void testThreadMXBeanSimpleMemAllocTracking() {
    if (ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled()) {
      ThreadResourceSnapshot threadResourceSnapshot = new ThreadResourceSnapshot();
      long[] ll = new long[10000];
      ll[2] = 4;
      LOGGER.trace(String.valueOf(ll[2]));
      long result = threadResourceSnapshot.getAllocatedBytes();
      Assert.assertTrue(result >= 80000 && result <= 85000);
    }
  }

  /**
   * multithread memory allocation test, do not remove
   */
  @SuppressWarnings("unused")
  public void testThreadMXBeanMultithreadMemAllocTracking() {
    if (ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled()) {
      LogManager.getLogger(TestThreadMXBean.class).setLevel(Level.INFO);
      ConcurrentHashMap<Integer, Integer> concurrentHashMap = new ConcurrentHashMap<>();
      ConcurrentHashMap<Integer, Integer> concurrentHashMap2 = new ConcurrentHashMap<>();
      AtomicLong a = new AtomicLong();
      AtomicLong b = new AtomicLong();
      AtomicLong c = new AtomicLong();
      ExecutorService executor = Executors.newFixedThreadPool(3);
      System.gc();

      long heapPrev = ResourceUsageUtils.getUsedHeapSize();
      ThreadResourceSnapshot threadResourceSnapshot0 = new ThreadResourceSnapshot();
      executor.submit(() -> {
        ThreadResourceSnapshot threadResourceSnapshot = new ThreadResourceSnapshot();
        for (int i = 0; i < 100000; i++) {
          concurrentHashMap.put(i, i);
        }
        a.set(threadResourceSnapshot.getAllocatedBytes());
      });

      executor.submit(() -> {
        ThreadResourceSnapshot threadResourceSnapshot = new ThreadResourceSnapshot();
        for (int i = 100000; i < 200000; i++) {
          concurrentHashMap.put(i, i);
        }
        b.set(threadResourceSnapshot.getAllocatedBytes());
      });

      executor.submit(() -> {
        ThreadResourceSnapshot threadResourceSnapshot = new ThreadResourceSnapshot();
        for (int i = 0; i < 200000; i++) {
          concurrentHashMap2.put(i, i);
        }
        c.set(threadResourceSnapshot.getAllocatedBytes());
      });

      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
      }

      long d = threadResourceSnapshot0.getAllocatedBytes();
      long threadAllocatedBytes = a.get() + b.get() + c.get() + d;
      float heapUsedBytes = (float) ResourceUsageUtils.getUsedHeapSize() - heapPrev;
      float ratio = threadAllocatedBytes / heapUsedBytes;

      LOGGER.info("Measured thread allocated bytes {}, heap used bytes {}, ratio {}",
          threadAllocatedBytes, heapUsedBytes, ratio);
    }
  }

  /**
   * multithreading deep memory allocation test, do not remove
   */
  @SuppressWarnings("unused")
  public void testThreadMXBeanDeepMemAllocTracking() {
    if (ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled()) {
      LogManager.getLogger(TestThreadMXBean.class).setLevel(Level.INFO);
      ConcurrentHashMap<Integer, NestedArray> concurrentHashMap = new ConcurrentHashMap<>();
      ConcurrentHashMap<Integer, NestedArray> concurrentHashMap2 = new ConcurrentHashMap<>();
      AtomicLong a = new AtomicLong();
      AtomicLong b = new AtomicLong();
      AtomicLong c = new AtomicLong();
      ExecutorService executor = Executors.newFixedThreadPool(3);
      System.gc();

      long heapPrev = ResourceUsageUtils.getUsedHeapSize();
      ThreadResourceSnapshot threadResourceSnapshot0 = new ThreadResourceSnapshot();
      executor.submit(() -> {
        ThreadResourceSnapshot threadResourceSnapshot = new ThreadResourceSnapshot();
        for (int i = 0; i < 100; i++) {
          concurrentHashMap.put(i, new NestedArray());
        }
        a.set(threadResourceSnapshot.getAllocatedBytes());
      });

      executor.submit(() -> {
        ThreadResourceSnapshot threadResourceSnapshot = new ThreadResourceSnapshot();
        for (int i = 100; i < 200; i++) {
          concurrentHashMap.put(i, new NestedArray());
        }
        b.set(threadResourceSnapshot.getAllocatedBytes());
      });

      executor.submit(() -> {
        ThreadResourceSnapshot threadResourceSnapshot = new ThreadResourceSnapshot();
        for (int i = 0; i < 200; i++) {
          concurrentHashMap2.put(i, new NestedArray());
        }
        c.set(threadResourceSnapshot.getAllocatedBytes());
      });

      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
      }

      long d = threadResourceSnapshot0.getAllocatedBytes();
      long threadAllocatedBytes = a.get() + b.get() + c.get() + d;
      float heapUsedBytes = (float) ResourceUsageUtils.getUsedHeapSize() - heapPrev;
      float ratio = threadAllocatedBytes / heapUsedBytes;

      LOGGER.info("Measured thread allocated bytes {}, heap used bytes {}, ratio {}",
          threadAllocatedBytes, heapUsedBytes, ratio);
    }
  }

  /**
   * test allocation and gc, getHeapMemoryUsage() tracks realtime usage, while getThreadAllocatedBytes() only tracks
   * allocated bytes, do not remove
   */
  @SuppressWarnings("unused")
  public void testThreadMXBeanMemAllocGCTracking() {
    LogManager.getLogger(TestThreadMXBean.class).setLevel(Level.INFO);
    System.gc();
    ThreadResourceSnapshot threadResourceSnapshot0 = new ThreadResourceSnapshot();
    long heapPrev = ResourceUsageUtils.getUsedHeapSize();
    for (int i = 0; i < 3; i++) {
      long[] ignored = new long[100000000];
    }
    System.gc();
    long heapResult = ResourceUsageUtils.getUsedHeapSize() - heapPrev;
    long result = threadResourceSnapshot0.getAllocatedBytes();
    LOGGER.info("Measured thread allocated bytes {}, heap used bytes {}",
        result, heapResult);
  }

  private static class NestedArray {
    Array _array;

    NestedArray() {
      _array = new Array();
    }
  }

  private static class Array {
    double[] _array;

    Array() {
      _array = new double[10000];
    }
  }
}
