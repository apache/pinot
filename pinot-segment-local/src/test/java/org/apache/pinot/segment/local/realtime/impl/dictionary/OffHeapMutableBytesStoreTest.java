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
package org.apache.pinot.segment.local.realtime.impl.dictionary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class OffHeapMutableBytesStoreTest {
  private static final int NUM_VALUES = 100_000;
  private static final int MAX_NUM_BYTES_PER_VALUE = 512;
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);

  private final PinotDataBufferMemoryManager _memoryManager = new DirectMemoryManager("testSegment");
  private final byte[][] _values = new byte[NUM_VALUES][];

  @BeforeClass
  public void setUp() {
    System.out.println("Random seed: " + RANDOM_SEED);

    for (int i = 0; i < NUM_VALUES; i++) {
      int numBytes = RANDOM.nextInt(MAX_NUM_BYTES_PER_VALUE);
      byte[] value = new byte[numBytes];
      RANDOM.nextBytes(value);
      _values[i] = value;
    }
  }

  @Test
  public void testAdd()
      throws Exception {
    try (OffHeapMutableBytesStore offHeapMutableBytesStore = new OffHeapMutableBytesStore(_memoryManager, null)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(offHeapMutableBytesStore.add(_values[i]), i);
      }
    }
  }

  @Test
  public void testGet()
      throws Exception {
    try (OffHeapMutableBytesStore offHeapMutableBytesStore = new OffHeapMutableBytesStore(_memoryManager, null)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        offHeapMutableBytesStore.add(_values[i]);
      }
      for (int i = 0; i < NUM_VALUES; i++) {
        int index = RANDOM.nextInt(NUM_VALUES);
        assertTrue(Arrays.equals(offHeapMutableBytesStore.get(index), _values[index]));
      }
    }
  }

  @Test
  public void testEqualsValueAt()
      throws Exception {
    try (OffHeapMutableBytesStore offHeapMutableBytesStore = new OffHeapMutableBytesStore(_memoryManager, null)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        offHeapMutableBytesStore.add(_values[i]);
      }
      for (int i = 0; i < NUM_VALUES; i++) {
        int index = RANDOM.nextInt(NUM_VALUES);
        assertTrue(offHeapMutableBytesStore.equalsValueAt(index, _values[index]));
        if (!Arrays.equals(_values[index], _values[0])) {
          assertFalse(offHeapMutableBytesStore.equalsValueAt(0, _values[index]));
          assertFalse(offHeapMutableBytesStore.equalsValueAt(index, _values[0]));
        }
      }
    }
  }

  @Test
  public void concurrentReadWrite()
      throws Exception {
    int numReaders = 4;
    ExecutorService executorService = Executors.newFixedThreadPool(numReaders + 1);
    List<Future> futures = new ArrayList<>(numReaders + 1);
    try (OffHeapMutableBytesStore offHeapMutableBytesStore = new OffHeapMutableBytesStore(_memoryManager, null)) {
      // Write one value before starting the reader threads
      offHeapMutableBytesStore.add(_values[0]);

      // Writer thread
      futures.add(executorService.submit(() -> {
        for (int i = 1; i < NUM_VALUES; i++) {
          offHeapMutableBytesStore.add(_values[i]);
        }
      }));

      // Reader threads
      for (int i = 0; i < numReaders; i++) {
        futures.add(executorService.submit(() -> {
          while (true) {
            int numValues = offHeapMutableBytesStore.getNumValues();
            // Read at random index
            int index = RANDOM.nextInt(numValues);
            assertTrue(Arrays.equals(offHeapMutableBytesStore.get(index), _values[index]));
            // Read the last added value
            assertTrue(Arrays.equals(offHeapMutableBytesStore.get(numValues - 1), _values[numValues - 1]));
            // Stop after all values are added
            if (numValues == NUM_VALUES) {
              return;
            }
          }
        }));
      }

      executorService.shutdown();
      for (Future future : futures) {
        future.get();
      }
    }
  }
}
