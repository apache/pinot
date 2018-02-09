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

package com.linkedin.pinot.core.realtime.converter.stats;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RealtimeNoDictionaryColStatsTest {
  private static final String INT_COL_NAME = "intcol";
  private static final String LONG_COL_NAME = "longcol";
  private static final String FLOAT_COL_NAME = "floatcol";
  private static final String DOUBLE_COL_NAME = "doublecol";
  private static final int NUM_ROWS = 1000;
  private ColumnDataSource _intDataSource;
  private ColumnDataSource _longDataSource;
  private ColumnDataSource _floatDataSource;
  private ColumnDataSource _doubleDataSource;
  private int _intMinVal;
  private int _intMaxVal;
  private long _longMinVal;
  private long _longMaxVal;
  private float _floatMinVal;
  private float _floatMaxVal;
  private double _doubleMinVal;
  private double _doubleMaxVal;
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(RealtimeNoDictionaryColStatsTest.class.getName());
  }

  @AfterClass
  public void tearDown() throws Exception {
    _memoryManager.close();
  }

  private void makeDataSources(long seed) {
    Random random;
    FieldSpec intSpec = new MetricFieldSpec(INT_COL_NAME, FieldSpec.DataType.INT);
    FieldSpec longSpec = new MetricFieldSpec(LONG_COL_NAME, FieldSpec.DataType.LONG);
    FieldSpec floatSpec = new MetricFieldSpec(FLOAT_COL_NAME, FieldSpec.DataType.FLOAT);
    FieldSpec doubleSpec = new MetricFieldSpec(DOUBLE_COL_NAME, FieldSpec.DataType.DOUBLE);
    random = new Random(seed);

    FixedByteSingleColumnSingleValueReaderWriter intRawIndex = new FixedByteSingleColumnSingleValueReaderWriter(
        random.nextInt(NUM_ROWS)+1, Integer.SIZE/8, _memoryManager, "int");
    FixedByteSingleColumnSingleValueReaderWriter longRawIndex = new FixedByteSingleColumnSingleValueReaderWriter(
        random.nextInt(NUM_ROWS)+1, Long.SIZE/8, _memoryManager, "long");
    FixedByteSingleColumnSingleValueReaderWriter floatRawIndex = new FixedByteSingleColumnSingleValueReaderWriter(
        random.nextInt(NUM_ROWS)+1, Float.SIZE/8, _memoryManager, "float");
    FixedByteSingleColumnSingleValueReaderWriter doubleRawIndex = new FixedByteSingleColumnSingleValueReaderWriter(
        random.nextInt(NUM_ROWS)+1, Double.SIZE/8, _memoryManager, "double");

    _intMinVal = Integer.MAX_VALUE; _intMaxVal = Integer.MIN_VALUE;
    _longMinVal = Long.MAX_VALUE; _longMaxVal = Long.MIN_VALUE;
    _floatMinVal = Float.MAX_VALUE; _floatMaxVal = Float.MIN_VALUE;
    _doubleMinVal = Double.MAX_VALUE; _doubleMaxVal = Double.MIN_VALUE;

    int[] intVals = new int[NUM_ROWS];
    long[] longVals = new long[NUM_ROWS];
    float[] floatVals = new float[NUM_ROWS];
    double[] doubleVals = new double[NUM_ROWS];

    for (int i = 0; i < NUM_ROWS; i++) {
      intVals[i] = random.nextInt();
      intRawIndex.setInt(i, intVals[i]);
      if (intVals[i] > _intMaxVal) {
        _intMaxVal = intVals[i];
      }
      if (intVals[i] < _intMinVal) {
        _intMinVal = intVals[i];
      }
      longVals[i]  = random.nextLong();
      longRawIndex.setLong(i, longVals[i]);
      if (longVals[i] > _longMaxVal) {
        _longMaxVal = longVals[i];
      }
      if (longVals[i] < _longMinVal) {
        _longMinVal = longVals[i];
      }
      floatVals[i] = random.nextFloat();
      floatRawIndex.setFloat(i, floatVals[i]);
      if (floatVals[i] > _floatMaxVal) {
        _floatMaxVal = floatVals[i];
      }
      if (floatVals[i] < _floatMinVal) {
        _floatMinVal = floatVals[i];
      }
      doubleVals[i] = random.nextDouble();
      doubleRawIndex.setDouble(i, doubleVals[i]);
      if (doubleVals[i] > _doubleMaxVal) {
        _doubleMaxVal = doubleVals[i];
      }
      if (doubleVals[i] < _doubleMinVal) {
        _doubleMinVal = doubleVals[i];
      }
    }

    _intDataSource = new ColumnDataSource(intSpec, NUM_ROWS, 0, intRawIndex, null, null);
    _longDataSource = new ColumnDataSource(longSpec, NUM_ROWS, 0, longRawIndex, null, null);
    _floatDataSource = new ColumnDataSource(floatSpec, NUM_ROWS, 0, floatRawIndex, null, null);
    _doubleDataSource = new ColumnDataSource(doubleSpec, NUM_ROWS, 0, doubleRawIndex, null, null);
  }

  @Test
  public void testIntStats() throws Exception {
    final long seed = new Random().nextLong();
    makeDataSources(seed);
    try {
      RealtimeNoDictionaryColStatistics statsContainer = new RealtimeNoDictionaryColStatistics(_intDataSource);
      Assert.assertEquals(statsContainer.getMinValue(), _intMinVal);
      Assert.assertEquals(statsContainer.getMaxValue(), _intMaxVal);
      Assert.assertEquals(statsContainer.getTotalNumberOfEntries(), NUM_ROWS);
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed "  + seed);
    }
  }

  @Test
  public void testLongStats() throws Exception {
    final long seed = new Random().nextLong();
    makeDataSources(seed);
    try {
      RealtimeNoDictionaryColStatistics statsContainer = new RealtimeNoDictionaryColStatistics(_longDataSource);
      Assert.assertEquals(statsContainer.getMinValue(), _longMinVal);
      Assert.assertEquals(statsContainer.getMaxValue(), _longMaxVal);
      Assert.assertEquals(statsContainer.getTotalNumberOfEntries(), NUM_ROWS);
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed "  + seed);
    }
  }

  @Test
  public void testFloatStats() throws Exception {
    final long seed = new Random().nextLong();
    makeDataSources(seed);
    try {
      RealtimeNoDictionaryColStatistics statsContainer = new RealtimeNoDictionaryColStatistics(_floatDataSource);
      Assert.assertEquals(statsContainer.getMinValue(), _floatMinVal);
      Assert.assertEquals(statsContainer.getMaxValue(), _floatMaxVal);
      Assert.assertEquals(statsContainer.getTotalNumberOfEntries(), NUM_ROWS);
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed "  + seed);
    }
  }

  @Test
  public void testDoubleStats() throws Exception {
    final long seed = new Random().nextLong();
    makeDataSources(seed);
    try {
      RealtimeNoDictionaryColStatistics statsContainer = new RealtimeNoDictionaryColStatistics(_doubleDataSource);
      Assert.assertEquals(statsContainer.getMinValue(), _doubleMinVal);
      Assert.assertEquals(statsContainer.getMaxValue(), _doubleMaxVal);
      Assert.assertEquals(statsContainer.getTotalNumberOfEntries(), NUM_ROWS);
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed "  + seed);
    }
  }
}
