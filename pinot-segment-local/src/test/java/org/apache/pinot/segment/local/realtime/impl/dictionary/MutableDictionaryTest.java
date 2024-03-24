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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;
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
  private static final FieldSpec.DataType[] DATA_TYPES = {
      FieldSpec.DataType.INT, FieldSpec.DataType.LONG, FieldSpec.DataType.FLOAT, FieldSpec.DataType.DOUBLE,
      FieldSpec.DataType.BIG_DECIMAL, FieldSpec.DataType.STRING, FieldSpec.DataType.BYTES
  };
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);

  private final ExecutorService _executorService = Executors.newFixedThreadPool(NUM_READERS + 1);
  private final PinotDataBufferMemoryManager _memoryManager =
      new DirectMemoryManager(MutableDictionaryTest.class.getName());

  @Test
  public void testSingleReaderSingleWriter() {
    try {
      try (MutableDictionary dictionary = new IntOnHeapMutableDictionary()) {
        testSingleReaderSingleWriter(dictionary, FieldSpec.DataType.INT);
      }
      try (MutableDictionary dictionary = new IntOffHeapMutableDictionary(EST_CARDINALITY, 2000, _memoryManager,
          "intColumn")) {
        testSingleReaderSingleWriter(dictionary, FieldSpec.DataType.INT);
      }
      try (MutableDictionary dictionary = new StringOffHeapMutableDictionary(EST_CARDINALITY, 2000, _memoryManager,
          "stringColumn", 32)) {
        testSingleReaderSingleWriter(dictionary, FieldSpec.DataType.STRING);
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
      try (MutableDictionary dictionary = new IntOnHeapMutableDictionary()) {
        testMultiReadersSingleWriter(dictionary, FieldSpec.DataType.INT);
      }
      try (MutableDictionary dictionary = new IntOffHeapMutableDictionary(EST_CARDINALITY, 2000, _memoryManager,
          "intColumn")) {
        testMultiReadersSingleWriter(dictionary, FieldSpec.DataType.INT);
      }
      try (MutableDictionary dictionary = new StringOffHeapMutableDictionary(EST_CARDINALITY, 2000, _memoryManager,
          "stringColumn", 32)) {
        testMultiReadersSingleWriter(dictionary, FieldSpec.DataType.STRING);
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
        try (MutableDictionary dictionary = MutableDictionaryFactory
            .getMutableDictionary(dataType, false, null, 0, 0, null)) {
          testMutableDictionary(dictionary, dataType);
        }
      }
    } catch (Throwable t) {
      Assert.fail("Failed with random seed: " + RANDOM_SEED, t);
    }
  }

  @Test
  public void testOffHeapMutableDictionary() {
    int[] maxOverflowSizes = {0, 2000};

    try {
      for (FieldSpec.DataType dataType : DATA_TYPES) {
        for (int maxOverflowSize : maxOverflowSizes) {
          try (MutableDictionary dictionary = makeOffHeapDictionary(EST_CARDINALITY, maxOverflowSize, dataType)) {
            testMutableDictionary(dictionary, dataType);
          }
        }
      }
    } catch (Throwable t) {
      Assert.fail("Failed with random seed: " + RANDOM_SEED, t);
    }
  }

  private void testMutableDictionary(MutableDictionary dictionary, FieldSpec.DataType dataType) {
    Map<Object, Integer> valueToDictId = new HashMap<>();
    int numEntries = 0;

    Comparable expectedMin = null;
    Comparable expectedMax = null;
    List<Comparable> expectedSortedValues = new ArrayList<>();

    for (int i = 0; i < NUM_ENTRIES; i++) {
      // Special case first 'INT' type to be Integer.MIN_VALUE.
      Comparable value =
          (i == 0 && dataType == FieldSpec.DataType.INT) ? Integer.MIN_VALUE : makeRandomObjectOfType(dataType);

      if (valueToDictId.containsKey(value)) {
        Assert.assertEquals(dictionary.indexOf(value.toString()), (int) valueToDictId.get(value));
      } else {
        int dictId;
        if (dataType != FieldSpec.DataType.BYTES) {
          dictId = dictionary.index(value);
        } else {
          dictId = dictionary.index(((ByteArray) value).getBytes());
        }
        Assert.assertEquals(dictId, numEntries++);
        valueToDictId.put(value, dictId);

        if (expectedMin == null || value.compareTo(expectedMin) < 0) {
          expectedMin = value;
        }
        if (expectedMax == null || value.compareTo(expectedMax) > 0) {
          expectedMax = value;
        }
        expectedSortedValues.add(value);
      }
    }

    // Test min/max values.
    Assert.assertEquals(dictionary.getMinVal(), expectedMin);
    Assert.assertEquals(dictionary.getMaxVal(), expectedMax);

    // Test sorted values.
    Collections.sort(expectedSortedValues);
    Object sortedValues = dictionary.getSortedValues();
    List<Comparable> actualSortedValues = (dataType.equals(FieldSpec.DataType.STRING)
        || dataType.equals(FieldSpec.DataType.BYTES) || dataType.equals(FieldSpec.DataType.BIG_DECIMAL))
        ? Arrays.asList((Comparable[]) dictionary.getSortedValues())
        : primitiveArrayToList(dataType, sortedValues);
    Assert.assertEquals(actualSortedValues, expectedSortedValues);

    Assert.assertEquals(dictionary.getDictIdsInRange(expectedMin.toString(), expectedMax.toString(), true, true).size(),
        dictionary.length());
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
      case BIG_DECIMAL:
        return new BigDecimalOffHeapMutableDictionary(estCardinality, maxOverflowSize, _memoryManager,
            "bigDecimalColumn", 32);
      case STRING:
        return new StringOffHeapMutableDictionary(estCardinality, maxOverflowSize, _memoryManager, "stringColumn", 32);
      case BYTES:
        return new BytesOffHeapMutableDictionary(estCardinality, maxOverflowSize, _memoryManager, "bytesColumn", 32);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  private Comparable makeRandomObjectOfType(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return RANDOM.nextInt();
      case LONG:
        return RANDOM.nextLong();
      case FLOAT:
        return RANDOM.nextFloat();
      case DOUBLE:
        return RANDOM.nextDouble();
      case BIG_DECIMAL:
        return BigDecimal.valueOf(RANDOM.nextDouble());
      case STRING:
        return RandomStringUtils.randomAscii(RANDOM.nextInt(1024));
      case BYTES:
        byte[] bytes = new byte[RANDOM.nextInt(100) + 1];
        RANDOM.nextBytes(bytes);
        return new ByteArray(bytes);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
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
    public Void call() {
      for (int i = 0; i < NUM_ENTRIES; i++) {
        int dictId;
        do {
          dictId = _dictionary.indexOf(Integer.toString(i + 1));
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
    public Void call() {
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

  private List<Comparable> primitiveArrayToList(FieldSpec.DataType dataType, Object sortedValues) {
    List<Comparable> valueList;
    switch (dataType) {
      case INT:
        valueList = Arrays.stream((int[]) sortedValues).boxed().collect(Collectors.toList());
        break;

      case LONG:
        valueList = Arrays.stream((long[]) sortedValues).boxed().collect(Collectors.toList());
        break;

      case FLOAT:
        // Stream not available for float.
        valueList = new ArrayList<>();
        for (float value : ((float[]) sortedValues)) {
          valueList.add(value);
        }
        break;

      case DOUBLE:
        valueList = Arrays.stream((double[]) sortedValues).boxed().collect(Collectors.toList());
        break;

      default:
        throw new IllegalArgumentException("Illegal data type for mutable dictionary: " + dataType);
    }
    return valueList;
  }
}
