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
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;


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
        testSingleReaderSingleWriter(dictionary, FieldSpec.DataType.INT);
        dictionary.close();
      }
      {
        MutableDictionary dictionary = new IntOffHeapMutableDictionary(NUM_ENTRIES / RANDOM.nextInt(NUM_ENTRIES/3), 2000,
            new DirectMemoryManager("test"), "intColumn");
        testSingleReaderSingleWriter(dictionary, FieldSpec.DataType.INT);
        dictionary.close();
      }
      {
        MutableDictionary dictionary = new StringOffHeapMutableDictionary(NUM_ENTRIES / RANDOM.nextInt(NUM_ENTRIES/3), 2000,
            new DirectMemoryManager("test"), "stringColumn");
        testSingleReaderSingleWriter(dictionary, FieldSpec.DataType.STRING);
        dictionary.close();
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + SEED);
    }
  }

  private void testSingleReaderSingleWriter(MutableDictionary dictionary, FieldSpec.DataType dataType) throws Exception {
      Future<Void> readerFuture = EXECUTOR_SERVICE.submit(new Reader(dictionary, dataType));
      Future<Void> writerFuture = EXECUTOR_SERVICE.submit(new Writer(dictionary, dataType));

      writerFuture.get();
      readerFuture.get();
  }

  @Test
  public void testMultiReadersSingleWriter() throws Exception {
    try {
      {
        MutableDictionary dictionary = new IntOnHeapMutableDictionary();
        testMultiReadersSingleWriter(dictionary, FieldSpec.DataType.INT);
      }
      {
        MutableDictionary dictionary = new IntOffHeapMutableDictionary(NUM_ENTRIES / RANDOM.nextInt(NUM_ENTRIES/3), 2000,
            new DirectMemoryManager("test"), "intColumn");
        testMultiReadersSingleWriter(dictionary, FieldSpec.DataType.INT);
      }
      {
        MutableDictionary dictionary = new StringOffHeapMutableDictionary(NUM_ENTRIES / RANDOM.nextInt(NUM_ENTRIES/3), 2000,
            new DirectMemoryManager("test"), "stringColumn");
        testMultiReadersSingleWriter(dictionary, FieldSpec.DataType.STRING);
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
    FieldSpec.DataType[] dataTypes = new FieldSpec.DataType[] {FieldSpec.DataType.INT, FieldSpec.DataType.LONG, FieldSpec.DataType.FLOAT, FieldSpec.DataType.DOUBLE, FieldSpec.DataType.STRING};
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
            Object x = makeRandomObjectOfType(dataType);
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
        dictionary.close();
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
        return new IntOffHeapMutableDictionary(estCaridinality, maxOverflowSize, new DirectMemoryManager("test"),
            "intColumn");
      case LONG:
        if (onHeap) {
          return new LongOnHeapMutableDictionary();
        }
        return new LongOffHeapMutableDictionary(estCaridinality, maxOverflowSize, new DirectMemoryManager("test"),
            "longColumn");
      case FLOAT:
        if (onHeap) {
          return new FloatOnHeapMutableDictionary();
        }
        return new FloatOffHeapMutableDictionary(estCaridinality, maxOverflowSize,
            new DirectMemoryManager("floatColumn"), "floatColumn");
      case DOUBLE:
        if (onHeap) {
          return new DoubleOnHeapMutableDictionary();
        }
        return new DoubleOffHeapMutableDictionary(estCaridinality, maxOverflowSize, new DirectMemoryManager("test"),
            "doubleColumn");
      case STRING:
        if (onHeap) {
          return new StringOnHeapMutableDictionary();
        }
        return new StringOffHeapMutableDictionary(estCaridinality, maxOverflowSize, new DirectMemoryManager("test"),
            "stringColumn");
    }
    throw new UnsupportedOperationException("Unsupported type " + dataType.toString());
  }

  private Object makeRandomObjectOfType(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return RANDOM.nextInt();
      case LONG:
        return RANDOM.nextLong();
      case FLOAT:
        return RANDOM.nextFloat();
      case DOUBLE:
        return RANDOM.nextDouble();
      case STRING:
        int len = RANDOM.nextInt(1024);
        return generateRandomString(len);
    }
    throw new UnsupportedOperationException("Unsupported type " + dataType.toString());
  }

  // Generates a ascii displayable string of given length
  private String generateRandomString(final int len) {
    byte[] bytes = new byte[len];
    for (int i = 0; i < len; i++) {
      bytes[i] = (byte)(RANDOM.nextInt(92) + 32);
    }
    return new String(bytes);
  }

  private void testMultiReadersSingleWriter(MutableDictionary dictionary, FieldSpec.DataType dataType) throws Exception {
      Future[] readerFutures = new Future[NUM_READERS];
      for (int i = 0; i < NUM_READERS; i++) {
        readerFutures[i] = EXECUTOR_SERVICE.submit(new Reader(dictionary, dataType));
      }
      Future<Void> writerFuture = EXECUTOR_SERVICE.submit(new Writer(dictionary, dataType));

      writerFuture.get();
      for (int i = 0; i < NUM_READERS; i++) {
        readerFutures[i].get();
      }
    dictionary.close();
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
    private final FieldSpec.DataType _dataType;
    private Reader(MutableDictionary dictionary, FieldSpec.DataType dataType) {
      _dictionary = dictionary;
      _dataType = dataType;
    }

    @Override
    public Void call() throws Exception {
      for (int i = 0; i < NUM_ENTRIES; i++) {
        try {
          int dictId;
          do {
            dictId = _dictionary.indexOf(makeObject(i+1, _dataType));
          } while (dictId < 0);
          Assert.assertEquals(dictId, i);
          Assert.assertTrue(checkEquals(_dictionary, dictId, i + 1, _dataType));

          // Fetch value by a random existing dictId
          int randomDictId = RANDOM.nextInt(i + 1);
          Assert.assertTrue(checkEquals(_dictionary, randomDictId, randomDictId + 1, _dataType));
        } catch (Throwable t) {
          throw new RuntimeException("Exception in iteration " + i, t);
        }
      }
      return null;
    }
  }

  /**
   * Writer to index value into dictionary, then check the index of the value.
   */
  private class Writer implements Callable<Void> {
    private final MutableDictionary _dictionary;
    private final FieldSpec.DataType _dataType;
    private Writer(MutableDictionary dictionary, FieldSpec.DataType dataType) {
      _dictionary = dictionary;
      _dataType = dataType;
    }

    @Override
    public Void call() throws Exception {
      for (int i = 0; i < NUM_ENTRIES; i++) {
        try {
          _dictionary.index(makeObject(i + 1, _dataType));
          Assert.assertEquals(_dictionary.indexOf(makeObject(i + 1, _dataType)), i);

          // Index a random existing value
          int randomValue = RANDOM.nextInt(i + 1) + 1;
          _dictionary.index(makeObject(randomValue, _dataType));
          Assert.assertEquals(_dictionary.indexOf(makeObject(randomValue, _dataType)), randomValue - 1);
        } catch (Throwable t) {
          throw new RuntimeException("Exception in iteration " + i, t);
        }
      }
      return null;
    }
  }

  private static Object makeObject(int i, FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new Integer(i);
      case STRING:
        return Integer.toString(i);
      default:
        throw new UnsupportedOperationException("Unsupported type " + dataType);
    }
  }

  // get the value from dictionary ID, and check that it is equal to 'i'
  private static boolean checkEquals(MutableDictionary dictionary, int dictId, int i, FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return dictionary.getIntValue(dictId) == i;
      case STRING:
        return dictionary.getStringValue(dictId).equals(Integer.toString(i));
      default:
        throw new UnsupportedOperationException("Unsupported type " + dataType);
    }
  }
}
