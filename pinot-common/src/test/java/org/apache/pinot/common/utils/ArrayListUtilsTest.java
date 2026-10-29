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
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ArrayListUtilsTest {
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
}
