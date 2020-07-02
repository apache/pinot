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
package org.apache.pinot.core.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import org.apache.pinot.core.io.readerwriter.impl.VarByteSingleColumnSingleValueReaderWriter;
import org.apache.pinot.core.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.core.segment.index.datasource.MutableDataSource;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RealtimeNoDictionaryTest {
  private static final String INT_COL_NAME = "intcol";
  private static final String LONG_COL_NAME = "longcol";
  private static final String FLOAT_COL_NAME = "floatcol";
  private static final String DOUBLE_COL_NAME = "doublecol";
  private static final String STRING_COL_NAME = "stringcol";
  private static final String BYTES_COL_NAME = "bytescol";
  private static final int NUM_ROWS = 1000;
  private int[] _intVals = new int[NUM_ROWS];
  private long[] _longVals = new long[NUM_ROWS];
  private float[] _floatVals = new float[NUM_ROWS];
  private double[] _doubleVals = new double[NUM_ROWS];
  private String[] _stringVals = new String[NUM_ROWS];

  private Random _random;

  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(RealtimeNoDictionaryTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  private DataFetcher makeDataFetcher(long seed) {
    FieldSpec intSpec = new MetricFieldSpec(INT_COL_NAME, FieldSpec.DataType.INT);
    FieldSpec longSpec = new MetricFieldSpec(LONG_COL_NAME, FieldSpec.DataType.LONG);
    FieldSpec floatSpec = new MetricFieldSpec(FLOAT_COL_NAME, FieldSpec.DataType.FLOAT);
    FieldSpec doubleSpec = new MetricFieldSpec(DOUBLE_COL_NAME, FieldSpec.DataType.DOUBLE);
    FieldSpec stringSpec = new DimensionFieldSpec(STRING_COL_NAME, FieldSpec.DataType.STRING, true);
    FieldSpec bytesSpec = new DimensionFieldSpec(BYTES_COL_NAME, FieldSpec.DataType.BYTES, true);
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
    VarByteSingleColumnSingleValueReaderWriter stringRawIndex =
        new VarByteSingleColumnSingleValueReaderWriter(_memoryManager, "StringColumn", 512, 30);
    VarByteSingleColumnSingleValueReaderWriter bytesRawIndex =
        new VarByteSingleColumnSingleValueReaderWriter(_memoryManager, "BytesColumn", 512, 30);

    for (int i = 0; i < NUM_ROWS; i++) {
      _intVals[i] = _random.nextInt();
      intRawIndex.setInt(i, _intVals[i]);
      _longVals[i] = _random.nextLong();
      longRawIndex.setLong(i, _longVals[i]);
      _floatVals[i] = _random.nextFloat();
      floatRawIndex.setFloat(i, _floatVals[i]);
      _doubleVals[i] = _random.nextDouble();
      doubleRawIndex.setDouble(i, _doubleVals[i]);
      // generate a random string of length between 10 and 100
      int length = 10 + _random.nextInt(100 - 10);
      _stringVals[i] = RandomStringUtils.randomAlphanumeric(length);
      stringRawIndex.setString(i, _stringVals[i]);
      bytesRawIndex.setBytes(i, StringUtil.encodeUtf8(_stringVals[i]));
    }

    Map<String, DataSource> dataSourceBlock = new HashMap<>();
    dataSourceBlock.put(INT_COL_NAME,
        new MutableDataSource(intSpec, NUM_ROWS, NUM_ROWS, 0, null, 0, intRawIndex, null, null, null,null, null));
    dataSourceBlock.put(LONG_COL_NAME,
        new MutableDataSource(longSpec, NUM_ROWS, NUM_ROWS, 0, null, 0, longRawIndex, null, null, null,null, null));
    dataSourceBlock.put(FLOAT_COL_NAME,
        new MutableDataSource(floatSpec, NUM_ROWS, NUM_ROWS, 0, null, 0, floatRawIndex, null, null, null,null, null));
    dataSourceBlock.put(DOUBLE_COL_NAME,
        new MutableDataSource(doubleSpec, NUM_ROWS, NUM_ROWS, 0, null, 0, doubleRawIndex, null, null, null,null, null));
    dataSourceBlock.put(STRING_COL_NAME,
        new MutableDataSource(stringSpec, NUM_ROWS, NUM_ROWS, 0, null, 0, stringRawIndex, null, null, null,null, null));
    dataSourceBlock.put(BYTES_COL_NAME,
        new MutableDataSource(bytesSpec, NUM_ROWS, NUM_ROWS, 0, null, 0, bytesRawIndex, null, null, null,null, null));

    return new DataFetcher(dataSourceBlock);
  }

  @Test
  public void testStringAndBytes() {
    long seed = new Random().nextLong();
    DataFetcher dataFetcher = makeDataFetcher(seed);
    int[] docIds = new int[NUM_ROWS];
    int numDocIds = _random.nextInt(NUM_ROWS) + 1;
    for (int i = 0; i < numDocIds; i++) {
      docIds[i] = _random.nextInt(NUM_ROWS);
    }
    try {
      String[] stringValues = new String[NUM_ROWS];
      dataFetcher.fetchStringValues(STRING_COL_NAME, docIds, numDocIds, stringValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(stringValues[i], _stringVals[docIds[i]], " for row " + docIds[i]);
      }
      byte[][] byteValues = new byte[NUM_ROWS][];
      dataFetcher.fetchBytesValues(BYTES_COL_NAME, docIds, numDocIds, byteValues);
      for (int i = 0; i < numDocIds; i++) {
        Assert.assertEquals(StringUtil.decodeUtf8(byteValues[i]), _stringVals[docIds[i]], " for row " + docIds[i]);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + seed);
    }
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
        Assert.assertEquals(longValues[i], _intVals[docIds[i]], " for row " + docIds[i]);
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
