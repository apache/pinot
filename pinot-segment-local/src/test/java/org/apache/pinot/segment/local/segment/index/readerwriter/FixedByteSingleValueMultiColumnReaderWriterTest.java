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
package org.apache.pinot.segment.local.segment.index.readerwriter;

import java.io.IOException;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.io.readerwriter.impl.FixedByteSingleValueMultiColumnReaderWriter;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link FixedByteSingleValueMultiColumnReaderWriter}
 */
public class FixedByteSingleValueMultiColumnReaderWriterTest {
  private static final int NUM_ROWS = 1001;
  private static final int NUM_ROWS_PER_CHUNK = 23;

  private static final int STRING_LENGTH = 11;

  private static final int[] COLUMN_SIZES_IN_BYTES =
      new int[]{Integer.BYTES, Long.BYTES, Float.BYTES, Double.BYTES, STRING_LENGTH};

  private PinotDataBufferMemoryManager _memoryManager;
  private FixedByteSingleValueMultiColumnReaderWriter _readerWriter;
  private Random _random;

  @BeforeClass
  public void setup() {
    _memoryManager = new DirectMemoryManager(FixedByteSingleValueMultiColumnReaderWriterTest.class.getName());
    _readerWriter =
        new FixedByteSingleValueMultiColumnReaderWriter(NUM_ROWS_PER_CHUNK, COLUMN_SIZES_IN_BYTES, _memoryManager,
            "test");
    _random = new Random(System.nanoTime());
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _readerWriter.close();
    _memoryManager.close();
  }

  @Test
  public void test() {
    // Test sequential read/write
    testSequentialReadWrite(0);

    // Test non-contiguous read/write, by starting to write at an offset.
    testSequentialReadWrite(NUM_ROWS);

    // Test mutability
    testMutability();
  }

  private void testMutability() {
    for (int i = 0; i < NUM_ROWS; i++) {
      int row = _random.nextInt(NUM_ROWS);

      int intValue = _random.nextInt();
      _readerWriter.setInt(row, 0, intValue);
      Assert.assertEquals(_readerWriter.getInt(row, 0), intValue);

      long longValue = _random.nextLong();
      _readerWriter.setLong(row, 1, longValue);
      Assert.assertEquals(_readerWriter.getLong(row, 1), longValue);

      float floatValue = _random.nextFloat();
      _readerWriter.setFloat(row, 2, floatValue);
      Assert.assertEquals(_readerWriter.getFloat(row, 2), floatValue);

      double doubleValue = _random.nextDouble();
      _readerWriter.setDouble(row, 3, doubleValue);
      Assert.assertEquals(_readerWriter.getDouble(row, 3), doubleValue);

      String stringValue = RandomStringUtils.randomAlphabetic(STRING_LENGTH);
      _readerWriter.setString(row, 4, stringValue);
      Assert.assertEquals(_readerWriter.getString(row, 4), stringValue);
    }
  }

  private void testSequentialReadWrite(int startOffset) {
    int[] intValues = new int[NUM_ROWS];
    long[] longValues = new long[NUM_ROWS];
    float[] floatValues = new float[NUM_ROWS];
    double[] doubleValues = new double[NUM_ROWS];
    String[] stringValues = new String[NUM_ROWS];

    for (int i = 0; i < NUM_ROWS; i++) {
      int row = i + startOffset;
      intValues[i] = _random.nextInt();
      _readerWriter.setInt(row, 0, intValues[i]);

      longValues[i] = _random.nextLong();
      _readerWriter.setLong(row, 1, longValues[i]);

      floatValues[i] = _random.nextFloat();
      _readerWriter.setFloat(row, 2, floatValues[i]);

      doubleValues[i] = _random.nextDouble();
      _readerWriter.setDouble(row, 3, doubleValues[i]);

      stringValues[i] = RandomStringUtils.randomAlphanumeric(STRING_LENGTH);
      _readerWriter.setString(row, 4, stringValues[i]);
    }

    for (int i = 0; i < NUM_ROWS; i++) {
      int row = i + startOffset;
      Assert.assertEquals(_readerWriter.getInt(row, 0), intValues[i]);
      Assert.assertEquals(_readerWriter.getLong(row, 1), longValues[i]);
      Assert.assertEquals(_readerWriter.getFloat(row, 2), floatValues[i]);
      Assert.assertEquals(_readerWriter.getDouble(row, 3), doubleValues[i]);
      Assert.assertEquals(_readerWriter.getString(row, 4), stringValues[i]);
    }
  }
}
