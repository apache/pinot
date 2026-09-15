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
package org.apache.pinot.segment.local.io.writer.impl;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MutableOffHeapByteArrayStoreTest implements PinotBuffersAfterClassCheckRule {

  private PinotDataBufferMemoryManager _memoryManager;
  private static final int ONE_GB = 1024 * 1024 * 1024;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(MutableOffHeapByteArrayStoreTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  @Test
  public void maxValueTest()
      throws Exception {
    int numArrays = 1024;
    int avgArrayLen = 32;
    try (MutableOffHeapByteArrayStore store =
        new MutableOffHeapByteArrayStore(_memoryManager, "stringColumn", numArrays, avgArrayLen)) {
      final int arrSize = MutableOffHeapByteArrayStore.getStartSize(numArrays, avgArrayLen);
      byte[] dataIn = new byte[arrSize - 4];
      for (int i = 0; i < dataIn.length; i++) {
        dataIn[i] = (byte) (i % Byte.MAX_VALUE);
      }
      int index = store.add(dataIn);
      byte[] dataOut = store.get(index);
      Assert.assertTrue(Arrays.equals(dataIn, dataOut));
    }
  }

  @Test
  public void byteBufferTest()
      throws Exception {
    try (MutableOffHeapByteArrayStore store =
        new MutableOffHeapByteArrayStore(_memoryManager, "bytesColumn", 1, 1)) {
      byte[] firstValue = {1};
      byte[] secondValue = {2, 3, 4};
      int firstIndex = store.add(firstValue);
      int secondIndex = store.add(secondValue);

      ByteBuffer firstBuffer = store.getByteBuffer(firstIndex);
      byte[] firstResult = new byte[firstBuffer.remaining()];
      firstBuffer.get(firstResult);
      Assert.assertEquals(firstResult, firstValue);

      ByteBuffer secondBuffer = store.getByteBuffer(secondIndex);
      Assert.assertTrue(secondBuffer.isReadOnly());
      byte[] secondResult = new byte[secondBuffer.remaining()];
      secondBuffer.get(secondResult);
      Assert.assertEquals(secondResult, secondValue);
      Assert.assertThrows(ReadOnlyBufferException.class, () -> store.getByteBuffer(secondIndex).put((byte) 0));
    }
  }

  @Test
  public void getByteBufferDuringConcurrentAppendTest()
      throws Exception {
    int numReaders = 4;
    int numValues = 2_048;
    ExecutorService executor = Executors.newFixedThreadPool(numReaders + 1);
    AtomicInteger publishedCount = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch firstValueRead = new CountDownLatch(numReaders);
    try (MutableOffHeapByteArrayStore store =
        new MutableOffHeapByteArrayStore(_memoryManager, "concurrentBytesColumn", 1, 1)) {
      Future<?> writer = executor.submit(() -> {
        await(start);
        Assert.assertEquals(store.add(valueForIndex(0)), 0);
        publishedCount.set(1);
        await(firstValueRead);
        for (int i = 1; i < numValues; i++) {
          Assert.assertEquals(store.add(valueForIndex(i)), i);
          // Publish only after the value and any expanded buffer are visible to readers.
          publishedCount.set(i + 1);
        }
      });

      Future<?>[] readers = new Future<?>[numReaders];
      for (int i = 0; i < numReaders; i++) {
        readers[i] = executor.submit(() -> {
          await(start);
          int nextIndex = 0;
          while (nextIndex < numValues) {
            int readableCount = publishedCount.get();
            while (nextIndex < readableCount) {
              ByteBuffer byteBuffer = store.getByteBuffer(nextIndex);
              Assert.assertTrue(byteBuffer.isReadOnly());
              byte[] actual = new byte[byteBuffer.remaining()];
              byteBuffer.get(actual);
              Assert.assertEquals(actual, valueForIndex(nextIndex));
              nextIndex++;
              if (nextIndex == 1) {
                firstValueRead.countDown();
              }
            }
            Thread.yield();
          }
        });
      }

      start.countDown();
      writer.get(30, TimeUnit.SECONDS);
      for (Future<?> reader : readers) {
        reader.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private static byte[] valueForIndex(int index) {
    return ByteBuffer.allocate(2 * Integer.BYTES).putInt(index).putInt(~index).array();
  }

  private static void await(CountDownLatch latch) {
    try {
      Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void startSizeTest() {
    Assert.assertEquals(MutableOffHeapByteArrayStore.getStartSize(1, ONE_GB), ONE_GB + 4);
    Assert.assertEquals(MutableOffHeapByteArrayStore.getStartSize(3, ONE_GB), Integer.MAX_VALUE);
    Assert.assertEquals(MutableOffHeapByteArrayStore.getStartSize(5, ONE_GB), Integer.MAX_VALUE);
  }

  @Test
  public void overflowTest()
      throws Exception {
    int numArrays = 1024;
    int avgArrayLen = 32;
    try (MutableOffHeapByteArrayStore store =
        new MutableOffHeapByteArrayStore(_memoryManager, "stringColumn", numArrays, avgArrayLen)) {
      final int maxSize = MutableOffHeapByteArrayStore.getStartSize(numArrays, avgArrayLen) - 4;

      byte[] b1 = new byte[3];
      for (int i = 0; i < b1.length; i++) {
        b1[i] = (byte) i;
      }

      byte[] b2 = new byte[maxSize];
      for (int i = 0; i < b2.length; i++) {
        b2[i] = (byte) (i % Byte.MAX_VALUE);
      }

      // Add small array
      final int i1 = store.add(b1);
      Assert.assertTrue(Arrays.equals(store.get(i1), b1));

      // And now the larger one, should result in a new buffer
      final int i2 = store.add(b2);
      Assert.assertTrue(Arrays.equals(store.get(i2), b2));

      // And now one more, should result in a new buffer but exact fit.
      final int i3 = store.add(b2);
      Assert.assertTrue(Arrays.equals(store.get(i3), b2));

      // One more buffer when we add the small one again.
      final int i4 = store.add(b1);
      Assert.assertTrue(Arrays.equals(store.get(i4), b1));

      // Test with one more 'get' to ensure that things have not changed.
      Assert.assertTrue(Arrays.equals(store.get(i1), b1));
      Assert.assertTrue(Arrays.equals(store.get(i2), b2));
      Assert.assertTrue(Arrays.equals(store.get(i3), b2));
      Assert.assertTrue(Arrays.equals(store.get(i4), b1));

      byte[] b3 = new byte[5];
      for (int i = 0; i < b3.length; i++) {
        b3[i] = (byte) (i + 1);
      }

      int ix = -1;
      final int iters = 1_000_000;

      // Now add the small one multiple times causing many additions.
      for (int i = 0; i < iters; i++) {
        if (ix == -1) {
          ix = store.add(b3);
        }
        store.add(b3);
      }
      for (int i = 0; i < iters; i++) {
        Assert.assertTrue(Arrays.equals(store.get(ix++), b3));
      }

      // Original values should still be good.
      Assert.assertTrue(Arrays.equals(store.get(i1), b1));
      Assert.assertTrue(Arrays.equals(store.get(i2), b2));
      Assert.assertTrue(Arrays.equals(store.get(i3), b2));
      Assert.assertTrue(Arrays.equals(store.get(i4), b1));
    }
  }
}
