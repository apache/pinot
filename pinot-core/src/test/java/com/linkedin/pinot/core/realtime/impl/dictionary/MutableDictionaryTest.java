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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


/**
 * Tests for functionality and concurrent read/write against mutable dictionaries.
 * <p>Index contiguous integers from 1 so that the index for each value is deterministic.
 */
public class MutableDictionaryTest {
  private static final int NUM_ENTRIES = 100_000;
  private static final int EST_CARDINALITY = NUM_ENTRIES / 3;
  private static final int NUM_READERS = 3;
  private static final FieldSpec.DataType[] DATA_TYPES =
      {FieldSpec.DataType.INT, FieldSpec.DataType.LONG, FieldSpec.DataType.FLOAT, FieldSpec.DataType.DOUBLE, FieldSpec.DataType.STRING};
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);

  private final ExecutorService _executorService = Executors.newFixedThreadPool(NUM_READERS + 1);
  private final PinotDataBufferMemoryManager _memoryManager =
      new DirectMemoryManager(MutableDictionaryTest.class.getName());

  @Test
  public void testSingleReaderSingleWriter() {
    try {
      {
        MutableDictionary dictionary = new IntOnHeapMutableDictionary();
        testSingleReaderSingleWriter(dictionary, FieldSpec.DataType.INT);
        dictionary.close();
      }
      {
        MutableDictionary dictionary =
            new IntOffHeapMutableDictionary(EST_CARDINALITY, 2000, _memoryManager, "intColumn");
        testSingleReaderSingleWriter(dictionary, FieldSpec.DataType.INT);
        dictionary.close();
      }
      {
        MutableDictionary dictionary =
            new StringOffHeapMutableDictionary(EST_CARDINALITY, 2000, _memoryManager, "stringColumn", 32);
        testSingleReaderSingleWriter(dictionary, FieldSpec.DataType.STRING);
        dictionary.close();
      }
    } catch (Throwable t) {
      Assert.fail("Failed with random seed: " + RANDOM_SEED, t);
    }
  }

  private void testSingleReaderSingleWriter(MutableDictionary dictionary, FieldSpec.DataType dataType)
      throws Exception {
    Future<Void> readerFuture = _executorService.submit(new Reader(dictionary, dataType));
    Future<Void> writerFuture = _executorService.submit(new Writer(dictionary, dataType));

    writerFuture.get();
    readerFuture.get();
  }

  @Test
  public void testMultiReadersSingleWriter() {
    try {
      {
        MutableDictionary dictionary = new IntOnHeapMutableDictionary();
        testMultiReadersSingleWriter(dictionary, FieldSpec.DataType.INT);
        dictionary.close();
      }
      {
        MutableDictionary dictionary =
            new IntOffHeapMutableDictionary(EST_CARDINALITY, 2000, _memoryManager, "intColumn");
        testMultiReadersSingleWriter(dictionary, FieldSpec.DataType.INT);
        dictionary.close();
      }
      {
        MutableDictionary dictionary =
            new StringOffHeapMutableDictionary(EST_CARDINALITY, 2000, _memoryManager, "stringColumn", 32);
        testMultiReadersSingleWriter(dictionary, FieldSpec.DataType.STRING);
        dictionary.close();
      }
    } catch (Throwable t) {
      Assert.fail("Failed with random seed: " + RANDOM_SEED, t);
    }
  }

  private void testMultiReadersSingleWriter(MutableDictionary dictionary, FieldSpec.DataType dataType)
      throws Exception {
    Future[] readerFutures = new Future[NUM_READERS];
    for (int i = 0; i < NUM_READERS; i++) {
      readerFutures[i] = _executorService.submit(new Reader(dictionary, dataType));
    }
    Future<Void> writerFuture = _executorService.submit(new Writer(dictionary, dataType));

    writerFuture.get();
    for (int i = 0; i < NUM_READERS; i++) {
      readerFutures[i].get();
    }
  }

  @Test
  public void testOnHeapMutableDictionary() {
    try {
      for (FieldSpec.DataType dataType : DATA_TYPES) {
        MutableDictionary dictionary = MutableDictionaryFactory.getMutableDictionary(dataType, false, null, 0, 0, null);
        testMutableDictionary(dictionary, dataType);
        dictionary.close();
      }
    } catch (Throwable t) {
      Assert.fail("Failed with random seed: " + RANDOM_SEED, t);
    }
  }

  @Test
  public void testOffHeapMutableDictionary() throws Exception {
    int[] maxOverflowSizes = {0, 2000};

    try {
      for (FieldSpec.DataType dataType : DATA_TYPES) {
        for (int maxOverflowSize : maxOverflowSizes) {
          MutableDictionary dictionary = makeOffHeapDictionary(EST_CARDINALITY, maxOverflowSize, dataType);
          testMutableDictionary(dictionary, dataType);
          dictionary.close();
        }
      }
    } catch (Throwable t) {
      Assert.fail("Failed with random seed: " + RANDOM_SEED, t);
    }
  }

  private void testMutableDictionary(MutableDictionary dictionary, FieldSpec.DataType dataType) {
    Map<Object, Integer> valueToDictId = new HashMap<>();
    int numEntries = 0;
    for (int i = 0; i < NUM_ENTRIES; i++) {
      Object value = makeRandomObjectOfType(dataType);
      if (valueToDictId.containsKey(value)) {
        Assert.assertEquals(dictionary.indexOf(value), (int) valueToDictId.get(value));
      } else {
        dictionary.index(value);
        int dictId = dictionary.indexOf(value);
        Assert.assertEquals(dictId, numEntries++);
        valueToDictId.put(value, dictId);
      }
    }
    if (dataType == FieldSpec.DataType.INT) {
      Object value = new Integer(Integer.MIN_VALUE);
      if (valueToDictId.containsKey(value)) {
        Assert.assertEquals(dictionary.indexOf(value), (int) valueToDictId.get(value));
      } else {
        dictionary.index(value);
        int dictId = dictionary.indexOf(value);
        Assert.assertEquals(dictId, numEntries++);
        valueToDictId.put(value, dictId);
      }
    }
  }

  private MutableDictionary makeOffHeapDictionary(int estCardinality, int maxOverflowSize,
      FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntOffHeapMutableDictionary(estCardinality, maxOverflowSize, _memoryManager, "intColumn");
      case LONG:
        return new LongOffHeapMutableDictionary(estCardinality, maxOverflowSize, _memoryManager, "longColumn");
      case FLOAT:
        return new FloatOffHeapMutableDictionary(estCardinality, maxOverflowSize, _memoryManager, "floatColumn");
      case DOUBLE:
        return new DoubleOffHeapMutableDictionary(estCardinality, maxOverflowSize, _memoryManager, "doubleColumn");
      case STRING:
        return new StringOffHeapMutableDictionary(estCardinality, maxOverflowSize, _memoryManager, "stringColumn", 32);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
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
        return RandomStringUtils.randomAscii(RANDOM.nextInt(1024));
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  @AfterClass
  public void tearDown() throws Exception {
    _executorService.shutdown();
    _memoryManager.close();
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
        int dictId;
        do {
          dictId = _dictionary.indexOf(makeObject(i + 1, _dataType));
        } while (dictId < 0);
        Assert.assertEquals(dictId, i);
        checkEquals(_dictionary, dictId, _dataType);

        // Fetch value by a random existing dictId
        checkEquals(_dictionary, RANDOM.nextInt(i + 1), _dataType);
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
        Object value = makeObject(i + 1, _dataType);
        _dictionary.index(value);

        // Index a random existing value
        int randomDictId = RANDOM.nextInt(i + 1);
        _dictionary.index(makeObject(randomDictId + 1, _dataType));
      }
      return null;
    }
  }

  /**
   * Helper method to return an <code>Integer</code> or <code>String</code> based on the given int value and data type.
   */
  private static Object makeObject(int intValue, FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return intValue;
      case STRING:
        return Integer.toString(intValue);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  /**
   * Helper method to check whether the value of the given dictId is one larger than the dictId.
   */
  private static void checkEquals(MutableDictionary dictionary, int dictId, FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        Assert.assertEquals(dictionary.getIntValue(dictId), dictId + 1);
        return;
      case STRING:
        Assert.assertEquals(Integer.parseInt(dictionary.getStringValue(dictId)), dictId + 1);
        return;
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }
}
