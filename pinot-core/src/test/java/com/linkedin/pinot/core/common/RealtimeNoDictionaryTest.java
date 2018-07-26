/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RealtimeNoDictionaryTest {
  private static final String INT_COL_NAME = "intcol";
  private static final String LONG_COL_NAME = "longcol";
  private static final String FLOAT_COL_NAME = "floatcol";
  private static final String DOUBLE_COL_NAME = "doublecol";
  private static final int NUM_ROWS = 1000;
  private int[] _intVals = new int[NUM_ROWS];
  private long[] _longVals = new long[NUM_ROWS];
  private float[] _floatVals = new float[NUM_ROWS];
  private double[] _doubleVals = new double[NUM_ROWS];
  private Random _random;

  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(RealtimeNoDictionaryTest.class.getName());
  }

  @AfterClass
  public void tearDown() throws Exception {
    _memoryManager.close();
  }

  private DataFetcher makeDataFetcher(long seed) {
    FieldSpec intSpec = new MetricFieldSpec(INT_COL_NAME, FieldSpec.DataType.INT);
    FieldSpec longSpec = new MetricFieldSpec(LONG_COL_NAME, FieldSpec.DataType.LONG);
    FieldSpec floatSpec = new MetricFieldSpec(FLOAT_COL_NAME, FieldSpec.DataType.FLOAT);
    FieldSpec doubleSpec = new MetricFieldSpec(DOUBLE_COL_NAME, FieldSpec.DataType.DOUBLE);
    _random = new Random(seed);

    FixedByteSingleColumnSingleValueReaderWriter intRawIndex =
        new FixedByteSingleColumnSingleValueReaderWriter(_random.nextInt(NUM_ROWS) + 1, Integer.BYTES, _memoryManager,
            "int");
    FixedByteSingleColumnSingleValueReaderWriter longRawIndex =
        new FixedByteSingleColumnSingleValueReaderWriter(_random.nextInt(NUM_ROWS) + 1, Long.BYTES, _memoryManager,
            "long");
    FixedByteSingleColumnSingleValueReaderWriter floatRawIndex =
        new FixedByteSingleColumnSingleValueReaderWriter(_random.nextInt(NUM_ROWS) + 1, Float.BYTES, _memoryManager,
            "float");
    FixedByteSingleColumnSingleValueReaderWriter doubleRawIndex =
        new FixedByteSingleColumnSingleValueReaderWriter(_random.nextInt(NUM_ROWS) + 1, Double.BYTES, _memoryManager,
            "double");

    for (int i = 0; i < NUM_ROWS; i++) {
      _intVals[i] = _random.nextInt();
      intRawIndex.setInt(i, _intVals[i]);
      _longVals[i] = _random.nextLong();
      longRawIndex.setLong(i, _longVals[i]);
      _floatVals[i] = _random.nextFloat();
      floatRawIndex.setFloat(i, _floatVals[i]);
      _doubleVals[i] = _random.nextDouble();
      doubleRawIndex.setDouble(i, _doubleVals[i]);
    }

    Map<String, DataSource> dataSourceBlock = new HashMap<>();
    dataSourceBlock.put(INT_COL_NAME, new ColumnDataSource(intSpec, NUM_ROWS, 0, intRawIndex, null, null));
    dataSourceBlock.put(LONG_COL_NAME, new ColumnDataSource(longSpec, NUM_ROWS, 0, longRawIndex, null, null));
    dataSourceBlock.put(FLOAT_COL_NAME, new ColumnDataSource(floatSpec, NUM_ROWS, 0, floatRawIndex, null, null));
    dataSourceBlock.put(DOUBLE_COL_NAME, new ColumnDataSource(doubleSpec, NUM_ROWS, 0, doubleRawIndex, null, null));

    return new DataFetcher(dataSourceBlock);
  }

  @Test
  public void testIntColumn() {
    long seed = new Random().nextLong();
    DataFetcher dataFetcher = makeDataFetcher(seed);
    int[] docIds = new int[NUM_ROWS];
    int numDocIds = _random.nextInt(NUM_ROWS) + 1;
    for (int i = 0; i < numDocIds; i++) {
      docIds[i] = _random.nextInt(NUM_ROWS);
    }
    try {
      int[] intValues = new int[NUM_ROWS];
      dataFetcher.fetchIntValues(INT_COL_NAME, docIds, numDocIds, intValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(intValues[i], _intVals[docIds[i]], " for row " + docIds[i]);
      }

      long[] longValues = new long[NUM_ROWS];
      dataFetcher.fetchLongValues(INT_COL_NAME, docIds, numDocIds, longValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(longValues[i], (long) _intVals[docIds[i]], " for row " + docIds[i]);
      }

      float[] floatValues = new float[NUM_ROWS];
      dataFetcher.fetchFloatValues(INT_COL_NAME, docIds, numDocIds, floatValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(floatValues[i], (float) _intVals[docIds[i]], " for row " + docIds[i]);
      }

      double[] doubleValues = new double[NUM_ROWS];
      dataFetcher.fetchDoubleValues(INT_COL_NAME, docIds, numDocIds, doubleValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(doubleValues[i], (double) _intVals[docIds[i]], " for row " + docIds[i]);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + seed);
    }
  }

  @Test
  public void testLongValues() {
    long seed = new Random().nextLong();
    DataFetcher dataFetcher = makeDataFetcher(seed);
    int[] docIds = new int[NUM_ROWS];
    int numDocIds = _random.nextInt(NUM_ROWS) + 1;
    for (int i = 0; i < numDocIds; i++) {
      docIds[i] = _random.nextInt(NUM_ROWS);
    }
    try {
      try {
        int[] intValues = new int[NUM_ROWS];
        dataFetcher.fetchIntValues(LONG_COL_NAME, docIds, numDocIds, intValues);
        Assert.fail("Expected exception converting long to int");
      } catch (UnsupportedOperationException e) {
        // We should see an exception
      }

      long[] longValues = new long[NUM_ROWS];
      dataFetcher.fetchLongValues(LONG_COL_NAME, docIds, numDocIds, longValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(longValues[i], _longVals[docIds[i]], " for row " + docIds[i]);
      }

      float[] floatValues = new float[NUM_ROWS];
      dataFetcher.fetchFloatValues(LONG_COL_NAME, docIds, numDocIds, floatValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(floatValues[i], (float) _longVals[docIds[i]], " for row " + docIds[i]);
      }

      double[] doubleValues = new double[NUM_ROWS];
      dataFetcher.fetchDoubleValues(LONG_COL_NAME, docIds, numDocIds, doubleValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(doubleValues[i], (double) _longVals[docIds[i]], " for row " + docIds[i]);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + seed);
    }
  }

  @Test
  public void testFloatValues() {
    long seed = new Random().nextLong();
    DataFetcher dataFetcher = makeDataFetcher(seed);
    int[] docIds = new int[NUM_ROWS];
    int numDocIds = _random.nextInt(NUM_ROWS) + 1;
    for (int i = 0; i < numDocIds; i++) {
      docIds[i] = _random.nextInt(NUM_ROWS);
    }
    try {
      try {
        int[] intValues = new int[NUM_ROWS];
        dataFetcher.fetchIntValues(FLOAT_COL_NAME, docIds, numDocIds, intValues);
        Assert.fail("Expected exception converting float to int");
      } catch (UnsupportedOperationException e) {
        // We should see an exception
      }

      long[] longValues = new long[NUM_ROWS];
      try {
        dataFetcher.fetchLongValues(FLOAT_COL_NAME, docIds, numDocIds, longValues);
        Assert.fail("Expected exception converting float to long");
      } catch (UnsupportedOperationException e) {
        // We should see an exception
      }

      float[] floatValues = new float[NUM_ROWS];
      dataFetcher.fetchFloatValues(FLOAT_COL_NAME, docIds, numDocIds, floatValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(floatValues[i], _floatVals[docIds[i]], " for row " + docIds[i]);
      }

      double[] doubleValues = new double[NUM_ROWS];
      dataFetcher.fetchDoubleValues(FLOAT_COL_NAME, docIds, numDocIds, doubleValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(doubleValues[i], (double) _floatVals[docIds[i]], " for row " + docIds[i]);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + seed);
    }
  }

  @Test
  public void testDoubleValues() {
    long seed = new Random().nextLong();
    DataFetcher dataFetcher = makeDataFetcher(seed);
    int[] docIds = new int[NUM_ROWS];
    int numDocIds = _random.nextInt(NUM_ROWS) + 1;
    for (int i = 0; i < numDocIds; i++) {
      docIds[i] = _random.nextInt(NUM_ROWS);
    }
    try {
      try {
        int[] intValues = new int[NUM_ROWS];
        dataFetcher.fetchIntValues(DOUBLE_COL_NAME, docIds, numDocIds, intValues);
        Assert.fail("Expected exception converting double to int");
      } catch (UnsupportedOperationException e) {
        // We should see an exception
      }

      long[] longValues = new long[NUM_ROWS];
      try {
        dataFetcher.fetchLongValues(DOUBLE_COL_NAME, docIds, numDocIds, longValues);
        Assert.fail("Expected exception converting double to long");
      } catch (UnsupportedOperationException e) {
        // We should see an exception
      }

      float[] floatValues = new float[NUM_ROWS];
      try {
        dataFetcher.fetchFloatValues(DOUBLE_COL_NAME, docIds, numDocIds, floatValues);
        Assert.fail("Expected exception converting double to float");
      } catch (UnsupportedOperationException e) {
        // We should see an exception
      }

      double[] doubleValues = new double[NUM_ROWS];
      dataFetcher.fetchDoubleValues(DOUBLE_COL_NAME, docIds, numDocIds, doubleValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(doubleValues[i], _doubleVals[docIds[i]], " for row " + docIds[i]);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + seed);
    }
  }
}
