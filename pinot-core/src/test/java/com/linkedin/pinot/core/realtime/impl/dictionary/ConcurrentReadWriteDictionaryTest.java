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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.FieldSpec;


/**
 * Tests for concurrent read and write against REALTIME dictionary.
 * <p>For now just test against {@link IntOnHeapMutableDictionary}. Index contiguous integers from 1 so that the index
 * for each value is deterministic.
 */
public class ConcurrentReadWriteDictionaryTest {
  private static final int NUM_ENTRIES = 1_000_000;
  private static final int NUM_READERS = 8;
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(NUM_READERS + 1);
  private static final long SEED = new Random().nextLong();
  private static final Random RANDOM = new Random(SEED);

  @Test
  public void testSingleReaderSingleWriter() throws Exception {
    try {
      {
        MutableDictionary dictionary = new IntOnHeapMutableDictionary();
        testSingleReaderSingleWriter(dictionary);
        dictionary.close();
      }
      {
        MutableDictionary dictionary = new IntOffHeapMutableDictionary(NUM_ENTRIES / RANDOM.nextInt(NUM_ENTRIES/3), 2000);
        testSingleReaderSingleWriter(dictionary);
        dictionary.close();
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + SEED);
    }
  }

  private void testSingleReaderSingleWriter(MutableDictionary dictionary) throws Exception {
      Future<Void> readerFuture = EXECUTOR_SERVICE.submit(new Reader(dictionary));
      Future<Void> writerFuture = EXECUTOR_SERVICE.submit(new Writer(dictionary));

      writerFuture.get();
      readerFuture.get();
  }

  @Test
  public void testMultiReadersSingleWriter() throws Exception {
    try {
      {
        MutableDictionary dictionary = new IntOnHeapMutableDictionary();
        testMultiReadersSingleWriter(dictionary);
        dictionary.close();
      }
      {
        MutableDictionary dictionary = new IntOffHeapMutableDictionary(NUM_ENTRIES / RANDOM.nextInt(NUM_ENTRIES/3), 2000);
        testMultiReadersSingleWriter(dictionary);
        dictionary.close();
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + SEED);
    }
  }

  // A test to verify the functionality of off-heap int dictionary without concurrency
  private void testRealtimeDictionary(boolean onHeap) throws Exception {
    final int estCardinality = 943;
    final int numValues = estCardinality * 107;
    FieldSpec.DataType[] dataTypes = new FieldSpec.DataType[] {FieldSpec.DataType.INT, FieldSpec.DataType.LONG, FieldSpec.DataType.FLOAT, FieldSpec.DataType.DOUBLE};
    int numEntries;
    final Map<Object, Integer> valueToDictId = new HashMap<>();
    final int[] overflowSizes = new int[] {0, 2000};

    for (FieldSpec.DataType dataType : dataTypes) {
      for (int overflowSize : overflowSizes) {
        MutableDictionary dictionary = makeDictionary(dataType, estCardinality, overflowSize, onHeap);
        valueToDictId.clear();
        numEntries = 0;
        for (int i = 0; i < numValues; i++) {
          try {
            Object x = makeRandomNumber(dataType);
            if (valueToDictId.containsKey(x)) {
              Assert.assertEquals(Integer.valueOf(dictionary.indexOf(x)), valueToDictId.get(x));
            } else {
              dictionary.index(x);
              int dictId = dictionary.indexOf(x);
              Assert.assertEquals(dictId, numEntries++);
              valueToDictId.put(x, dictId);
            }
          } catch (Throwable t) {
            t.printStackTrace();
            Assert.fail("Failed with seed " + SEED + " iteration " + i + " for dataType " + dataType.toString() + " overflowsize=" + overflowSize);
          }
        }
      }
    }
  }

  @Test
  public void testRealtimeDictionary() throws Exception {
    testRealtimeDictionary(true);
    testRealtimeDictionary(false);
  }

  private MutableDictionary makeDictionary(FieldSpec.DataType dataType, final int estCaridinality, int maxOverflowSize,
      boolean onHeap) {
    switch (dataType) {
      case INT:
        if (onHeap) {
          return new IntOnHeapMutableDictionary();
        }
        return new IntOffHeapMutableDictionary(estCaridinality, maxOverflowSize);
      case LONG:
        if (onHeap) {
          return new LongOnHeapMutableDictionary();
        }
        return new LongOffHeapMutableDictionary(estCaridinality, maxOverflowSize);
      case FLOAT:
        if (onHeap) {
          return new FloatOnHeapMutableDictionary();
        }
        return new FloatOffHeapMutableDictionary(estCaridinality, maxOverflowSize);
      case DOUBLE:
        if (onHeap) {
          return new DoubleOnHeapMutableDictionary();
        }
        return new DoubleOffHeapMutableDictionary(estCaridinality, maxOverflowSize);
    }
    throw new UnsupportedOperationException("Unsupported type " + dataType.toString());
  }

  private Object makeRandomNumber(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return RANDOM.nextInt();
      case LONG:
        return RANDOM.nextLong();
      case FLOAT:
        return RANDOM.nextFloat();
      case DOUBLE:
        return RANDOM.nextDouble();
    }
    throw new UnsupportedOperationException("Unsupported type " + dataType.toString());
  }

  private void testMultiReadersSingleWriter(MutableDictionary dictionary) throws Exception {
      Future[] readerFutures = new Future[NUM_READERS];
      for (int i = 0; i < NUM_READERS; i++) {
        readerFutures[i] = EXECUTOR_SERVICE.submit(new Reader(dictionary));
      }
      Future<Void> writerFuture = EXECUTOR_SERVICE.submit(new Writer(dictionary));

      writerFuture.get();
      for (int i = 0; i < NUM_READERS; i++) {
        readerFutures[i].get();
      }
  }

  @AfterClass
  public void tearDown() {
    EXECUTOR_SERVICE.shutdown();
  }

  /**
   * Reader to read the index of each value after it's indexed into the dictionary, then get the value from the index.
   * <p>We can assume that we always first get the index of a value, then use the index to fetch the value.
   */
  private class Reader implements Callable<Void> {
    private final MutableDictionary _dictionary;
    private Reader(MutableDictionary dictionary) {
      _dictionary = dictionary;
    }

    @Override
    public Void call()
        throws Exception {
      try {
        for (int i = 0; i < NUM_ENTRIES; i++) {
          int dictId;
          do {
            dictId = _dictionary.indexOf(i + 1);
          } while (dictId < 0);
          Assert.assertEquals(dictId, i);
          Assert.assertEquals(_dictionary.getIntValue(dictId), i + 1);

          // Fetch value by a random existing dictId
          int randomDictId = RANDOM.nextInt(i + 1);
          Assert.assertEquals(_dictionary.getIntValue(randomDictId), randomDictId + 1);
        }
        return null;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }

  /**
   * Writer to index value into dictionary, then check the index of the value.
   */
  private class Writer implements Callable<Void> {
    private final MutableDictionary _dictionary;
    private Writer(MutableDictionary dictionary) {
      _dictionary = dictionary;
    }

    @Override
    public Void call()
        throws Exception {
      try {
        for (int i = 0; i < NUM_ENTRIES; i++) {
          _dictionary.index(i + 1);
          Assert.assertEquals(_dictionary.indexOf(i + 1), i);

          // Index a random existing value
          int randomValue = RANDOM.nextInt(i + 1) + 1;
          _dictionary.index(randomValue);
          Assert.assertEquals(_dictionary.indexOf(randomValue), randomValue - 1);
        }
        return null;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }
}
