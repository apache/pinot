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
package org.apache.pinot.spi.utils;

import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ArrayCopyUtilsTest {
  private static final int ARRAY_LENGTH = 100;
  private static final int BUFFER_LENGTH = 95;
  private static final int COPY_LENGTH = 90;
  private static final Random RANDOM = new Random();

  private static final int[] INT_ARRAY = new int[ARRAY_LENGTH];
  private static final long[] LONG_ARRAY = new long[ARRAY_LENGTH];
  private static final float[] FLOAT_ARRAY = new float[ARRAY_LENGTH];
  private static final double[] DOUBLE_ARRAY = new double[ARRAY_LENGTH];
  private static final String[] STRING_ARRAY = new String[ARRAY_LENGTH];
  private static final int[] INT_BUFFER = new int[BUFFER_LENGTH];
  private static final long[] LONG_BUFFER = new long[BUFFER_LENGTH];
  private static final float[] FLOAT_BUFFER = new float[BUFFER_LENGTH];
  private static final double[] DOUBLE_BUFFER = new double[BUFFER_LENGTH];
  private static final String[] STRING_BUFFER = new String[BUFFER_LENGTH];

  @BeforeClass
  public void setUp() {
    for (int i = 0; i < ARRAY_LENGTH; i++) {
      int value = RANDOM.nextInt();
      INT_ARRAY[i] = value;
      LONG_ARRAY[i] = value;
      FLOAT_ARRAY[i] = value;
      DOUBLE_ARRAY[i] = value;
      STRING_ARRAY[i] = Integer.toString(value);
    }
  }

  @Test
  public void testCopyFromIntArray() {
    ArrayCopyUtils.copy(INT_ARRAY, LONG_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(INT_ARRAY, FLOAT_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(INT_ARRAY, DOUBLE_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(INT_ARRAY, STRING_BUFFER, COPY_LENGTH);
    for (int i = 0; i < COPY_LENGTH; i++) {
      Assert.assertEquals(LONG_BUFFER[i], (long) INT_ARRAY[i]);
      Assert.assertEquals(FLOAT_BUFFER[i], (float) INT_ARRAY[i]);
      Assert.assertEquals(DOUBLE_BUFFER[i], (double) INT_ARRAY[i]);
      Assert.assertEquals(STRING_BUFFER[i], Integer.toString(INT_ARRAY[i]));
    }
  }

  @Test
  public void testCopyFromLongArray() {
    ArrayCopyUtils.copy(LONG_ARRAY, INT_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(LONG_ARRAY, FLOAT_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(LONG_ARRAY, DOUBLE_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(LONG_ARRAY, STRING_BUFFER, COPY_LENGTH);
    for (int i = 0; i < COPY_LENGTH; i++) {
      Assert.assertEquals(INT_BUFFER[i], (int) LONG_ARRAY[i]);
      Assert.assertEquals(FLOAT_BUFFER[i], (float) LONG_ARRAY[i]);
      Assert.assertEquals(DOUBLE_BUFFER[i], (double) LONG_ARRAY[i]);
      Assert.assertEquals(STRING_BUFFER[i], Long.toString(LONG_ARRAY[i]));
    }
  }

  @Test
  public void testCopyFromFloatArray() {
    ArrayCopyUtils.copy(FLOAT_ARRAY, INT_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(FLOAT_ARRAY, LONG_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(FLOAT_ARRAY, DOUBLE_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(FLOAT_ARRAY, STRING_BUFFER, COPY_LENGTH);
    for (int i = 0; i < COPY_LENGTH; i++) {
      Assert.assertEquals(INT_BUFFER[i], (int) FLOAT_ARRAY[i]);
      Assert.assertEquals(LONG_BUFFER[i], (long) FLOAT_ARRAY[i]);
      Assert.assertEquals(DOUBLE_BUFFER[i], Double.parseDouble(Float.valueOf(FLOAT_ARRAY[i]).toString()));
      Assert.assertEquals(STRING_BUFFER[i], Float.toString(FLOAT_ARRAY[i]));
    }
  }

  @Test
  public void testCopyFromDoubleArray() {
    ArrayCopyUtils.copy(DOUBLE_ARRAY, INT_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(DOUBLE_ARRAY, LONG_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(DOUBLE_ARRAY, FLOAT_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(DOUBLE_ARRAY, STRING_BUFFER, COPY_LENGTH);
    for (int i = 0; i < COPY_LENGTH; i++) {
      Assert.assertEquals(INT_BUFFER[i], (int) DOUBLE_ARRAY[i]);
      Assert.assertEquals(LONG_BUFFER[i], (long) DOUBLE_ARRAY[i]);
      Assert.assertEquals(FLOAT_BUFFER[i], (float) DOUBLE_ARRAY[i]);
      Assert.assertEquals(STRING_BUFFER[i], Double.toString(DOUBLE_ARRAY[i]));
    }
  }

  @Test
  public void testCopyFromStringArray() {
    ArrayCopyUtils.copy(STRING_ARRAY, INT_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(STRING_ARRAY, LONG_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(STRING_ARRAY, FLOAT_BUFFER, COPY_LENGTH);
    ArrayCopyUtils.copy(STRING_ARRAY, DOUBLE_BUFFER, COPY_LENGTH);
    for (int i = 0; i < COPY_LENGTH; i++) {
      Assert.assertEquals(INT_BUFFER[i], Integer.parseInt(STRING_ARRAY[i]));
      Assert.assertEquals(LONG_BUFFER[i], Long.parseLong(STRING_ARRAY[i]));
      Assert.assertEquals(FLOAT_BUFFER[i], Float.parseFloat(STRING_ARRAY[i]));
      Assert.assertEquals(DOUBLE_BUFFER[i], Double.parseDouble(STRING_ARRAY[i]));
    }
  }
}
