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
package org.apache.pinot.common.utils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ArrayListUtilsTest {
  @Test
  public void testToIntArray() {
    // Test empty list
    IntArrayList intArrayList = new IntArrayList();
    int[] intArray = ArrayListUtils.toIntArray(intArrayList);
    assertEquals(intArray.length, 0);

    // Test list with one element
    intArrayList.add(1);
    intArray = ArrayListUtils.toIntArray(intArrayList);
    assertEquals(intArray.length, 1);
    assertEquals(intArray[0], 1);

    // Test list with multiple elements
    intArrayList.add(2);
    intArrayList.add(3);
    intArray = ArrayListUtils.toIntArray(intArrayList);
    assertEquals(intArray.length, 3);
    assertEquals(intArray[0], 1);
    assertEquals(intArray[1], 2);
    assertEquals(intArray[2], 3);
  }

  @Test
  public void testToLongArray() {
    // Test empty list
    LongArrayList longArrayList = new LongArrayList();
    long[] longArray = ArrayListUtils.toLongArray(longArrayList);
    assertEquals(longArray.length, 0);

    // Test list with one element
    longArrayList.add(1L);
    longArray = ArrayListUtils.toLongArray(longArrayList);
    assertEquals(longArray.length, 1);
    assertEquals(longArray[0], 1L);

    // Test list with multiple elements
    longArrayList.add(2L);
    longArrayList.add(3L);
    longArray = ArrayListUtils.toLongArray(longArrayList);
    assertEquals(longArray.length, 3);
    assertEquals(longArray[0], 1L);
    assertEquals(longArray[1], 2L);
    assertEquals(longArray[2], 3L);
  }

  @Test
  public void testToFloatArray() {
    // Test empty list
    FloatArrayList floatArrayList = new FloatArrayList();
    float[] floatArray = ArrayListUtils.toFloatArray(floatArrayList);
    assertEquals(floatArray.length, 0);

    // Test list with one element
    floatArrayList.add(1.0f);
    floatArray = ArrayListUtils.toFloatArray(floatArrayList);
    assertEquals(floatArray.length, 1);
    assertEquals(floatArray[0], 1.0f);

    // Test list with multiple elements
    floatArrayList.add(2.0f);
    floatArrayList.add(3.0f);
    floatArray = ArrayListUtils.toFloatArray(floatArrayList);
    assertEquals(floatArray.length, 3);
    assertEquals(floatArray[0], 1.0f);
    assertEquals(floatArray[1], 2.0f);
    assertEquals(floatArray[2], 3.0f);
  }

  @Test
  public void testToDoubleArray() {
    // Test empty list
    DoubleArrayList doubleArrayList = new DoubleArrayList();
    double[] doubleArray = ArrayListUtils.toDoubleArray(doubleArrayList);
    assertEquals(doubleArray.length, 0);

    // Test list with one element
    doubleArrayList.add(1.0);
    doubleArray = ArrayListUtils.toDoubleArray(doubleArrayList);
    assertEquals(doubleArray.length, 1);
    assertEquals(doubleArray[0], 1.0);

    // Test list with multiple elements
    doubleArrayList.add(2.0);
    doubleArrayList.add(3.0);
    doubleArray = ArrayListUtils.toDoubleArray(doubleArrayList);
    assertEquals(doubleArray.length, 3);
    assertEquals(doubleArray[0], 1.0);
    assertEquals(doubleArray[1], 2.0);
    assertEquals(doubleArray[2], 3.0);
  }

  @Test
  public void testToStringArray() {
    // Test empty list
    ObjectArrayList<String> stringArrayList = new ObjectArrayList<String>();
    String[] stringArray = ArrayListUtils.toStringArray(stringArrayList);
    assertEquals(stringArray.length, 0);

    // Test list with one element
    stringArrayList.add("1");
    stringArray = ArrayListUtils.toStringArray(stringArrayList);
    assertEquals(stringArray.length, 1);
    assertEquals(stringArray[0], "1");

    // Test list with multiple elements
    stringArrayList.add("2");
    stringArrayList.add("3");
    stringArray = ArrayListUtils.toStringArray(stringArrayList);
    assertEquals(stringArray.length, 3);
    assertEquals(stringArray[0], "1");
    assertEquals(stringArray[1], "2");
    assertEquals(stringArray[2], "3");
  }
}
