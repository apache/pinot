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

package com.linkedin.pinot.core.operator.docvaliterators;

import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.IOException;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RealtimeSingleValueIteratorTest {
  private static final int NUM_ROWS = 1000;
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private int[] _intVals = new int[NUM_ROWS];
  private long[] _longVals = new long[NUM_ROWS];
  private float[] _floatVals = new float[NUM_ROWS];
  private double[] _doubleVals = new double[NUM_ROWS];
  FixedByteSingleColumnSingleValueReaderWriter _intReader;
  FixedByteSingleColumnSingleValueReaderWriter _longReader;
  FixedByteSingleColumnSingleValueReaderWriter _floatReader;
  FixedByteSingleColumnSingleValueReaderWriter _doubleReader;
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(RealtimeSingleValueIteratorTest.class.getName());
    _intReader =
        new FixedByteSingleColumnSingleValueReaderWriter(RANDOM.nextInt(NUM_ROWS) + 1, V1Constants.Numbers.INTEGER_SIZE,
            _memoryManager, "intReader");
    _longReader =
        new FixedByteSingleColumnSingleValueReaderWriter(RANDOM.nextInt(NUM_ROWS) + 1, V1Constants.Numbers.LONG_SIZE,
            _memoryManager, "longReader");
    _floatReader =
        new FixedByteSingleColumnSingleValueReaderWriter(RANDOM.nextInt(NUM_ROWS) + 1, V1Constants.Numbers.FLOAT_SIZE,
            _memoryManager, "floatReader");
    _doubleReader =
        new FixedByteSingleColumnSingleValueReaderWriter(RANDOM.nextInt(NUM_ROWS) + 1, V1Constants.Numbers.DOUBLE_SIZE,
            _memoryManager, "doubleReader");

    for (int i = 0; i < NUM_ROWS; i++) {
      _intVals[i] = RANDOM.nextInt();
      _intReader.setInt(i, _intVals[i]);
      _longVals[i] = RANDOM.nextLong();
      _longReader.setLong(i, _longVals[i]);
      _floatVals[i] = RANDOM.nextFloat();
      _floatReader.setFloat(i, _floatVals[i]);
      _doubleVals[i] = RANDOM.nextDouble();
      _doubleReader.setDouble(i, _doubleVals[i]);
    }
  }

  @Test
  public void testIntReader() throws Exception {
    try {
      SingleValueIterator iterator = new SingleValueIterator(_intReader, NUM_ROWS);
      // Test all values
      iterator.reset();
      for (int i = 0; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextIntVal(), _intVals[i], " at row " + i);
      }

      final int startDocId = RANDOM.nextInt(NUM_ROWS);
      iterator.skipTo(startDocId);
      for (int i = startDocId; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextIntVal(), _intVals[i], " at row " + i);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + RANDOM_SEED);
    }
  }

  @Test
  public void testLongReader() throws Exception {
    try {
      SingleValueIterator iterator = new SingleValueIterator(_longReader, NUM_ROWS);
      // Test all values
      iterator.reset();
      for (int i = 0; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextLongVal(), _longVals[i], " at row " + i);
      }

      final int startDocId = RANDOM.nextInt(NUM_ROWS);
      iterator.skipTo(startDocId);
      for (int i = startDocId; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextLongVal(), _longVals[i], " at row " + i);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + RANDOM_SEED);
    }
  }

  @Test
  public void testFloatReader() throws Exception {
    try {
      SingleValueIterator iterator = new SingleValueIterator(_floatReader, NUM_ROWS);
      // Test all values
      iterator.reset();
      for (int i = 0; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextFloatVal(), _floatVals[i], " at row " + i);
      }

      final int startDocId = RANDOM.nextInt(NUM_ROWS);
      iterator.skipTo(startDocId);
      for (int i = startDocId; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextFloatVal(), _floatVals[i], " at row " + i);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + RANDOM_SEED);
    }
  }

  @Test
  public void testDoubleReader() throws Exception {
    try {
      SingleValueIterator iterator = new SingleValueIterator(_doubleReader, NUM_ROWS);
      // Test all values
      iterator.reset();
      for (int i = 0; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextDoubleVal(), _doubleVals[i], " at row " + i);
      }

      final int startDocId = RANDOM.nextInt(NUM_ROWS);
      iterator.skipTo(startDocId);
      for (int i = startDocId; i < NUM_ROWS; i++) {
        Assert.assertEquals(iterator.nextDoubleVal(), _doubleVals[i], " at row " + i);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      Assert.fail("Failed with seed " + RANDOM_SEED);
    }
  }

  @AfterTest
  public void tearDown() throws IOException {
    _memoryManager.close();
  }
}
