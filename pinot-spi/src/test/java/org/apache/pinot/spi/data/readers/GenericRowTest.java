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
package org.apache.pinot.spi.data.readers;

import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GenericRowTest {

  @Test
  public void testEmptyRowsEqual() {
    GenericRow first = new GenericRow();
    GenericRow second = new GenericRow();
    Assert.assertEquals(first, second);
  }

  @Test
  public void testEmptyRowNotEqualToNonEmptyRow() {
    GenericRow first = new GenericRow();
    GenericRow second = new GenericRow();
    second.putValue("one", 1);
    Assert.assertNotEquals(first, second);
  }

  @Test
  public void testRowDifferentValueNotEqual() {
    GenericRow first = new GenericRow();
    first.putValue("one", 1);
    GenericRow second = new GenericRow();
    second.putValue("one", "one");
    Assert.assertNotEquals(first, second);
  }

  @Test
  public void testDifferentNumberOfKeysWithSomeSameValueNotEqual() {
    GenericRow first = new GenericRow();
    first.putValue("one", 1);
    first.putValue("two", 2);
    GenericRow second = new GenericRow();
    second.putValue("one", 1);
    Assert.assertNotEquals(first, second);
  }

  @Test
  public void testDifferentNumberOfKeysWithNoSameValueNotEqual() {
    GenericRow first = new GenericRow();
    first.putValue("one", 1);
    first.putValue("two", 2);
    GenericRow second = new GenericRow();
    second.putValue("one", "one");
    Assert.assertNotEquals(first, second);
  }

  @Test
  public void testNullAndNonNullValuesNotEqual() {
    GenericRow first = new GenericRow();
    first.putValue("one", null);
    GenericRow second = new GenericRow();
    second.putValue("one", 1);
    Assert.assertNotEquals(first, second);

    first = new GenericRow();
    first.putValue("one", 1);
    second = new GenericRow();
    second.putValue("one", null);
    Assert.assertNotEquals(first, second);
  }

  @Test
  public void testIntValuesEqual() {
    GenericRow first = new GenericRow();
    first.putValue("one", 1);
    GenericRow second = new GenericRow();
    second.putValue("one", 1);
    Assert.assertEquals(first, second);
  }

  @Test
  public void testMapValuesSameSizeNotEqual() {
    GenericRow first = new GenericRow();
    first.putValue("one", 1);
    HashMap<String, Object> firstData = new HashMap<String, Object>();
    firstData.put("two", 2);

    GenericRow second = new GenericRow();
    HashMap<String, Object> secondData = new HashMap<String, Object>();
    secondData.put("two", "two");
    second.putValue("one", secondData);

    Assert.assertNotEquals(first, second);
  }

  @Test
  public void testMapValuesDifferentSizeNotEqual() {
    GenericRow first = new GenericRow();
    first.putValue("one", 1);
    HashMap<String, Object> firstData = new HashMap<String, Object>();
    firstData.put("two", 2);
    firstData.put("three", 3);

    GenericRow second = new GenericRow();
    HashMap<String, Object> secondData = new HashMap<String, Object>();
    secondData.put("two", 2);
    second.putValue("one", secondData);

    Assert.assertNotEquals(first, second);
  }

  @Test
  public void testMapValuesEqual() {
    GenericRow first = new GenericRow();
    HashMap<String, Integer> firstData = new HashMap<String, Integer>();
    firstData.put("two", 2);
    first.putValue("one", 1);
    first.putValue("two", firstData);

    GenericRow second = new GenericRow();
    second.putValue("one", 1);
    second.putValue("two", firstData.clone());

    Assert.assertEquals(first, second);
  }

  @Test
  public void testNullValueFieldsNotEqual() {
    GenericRow first = new GenericRow();
    first.putDefaultNullValue("one", 1);
    GenericRow second = new GenericRow();
    second.putDefaultNullValue("one", 2);
    Assert.assertNotEquals(first, second);

    first = new GenericRow();
    first.putDefaultNullValue("one", 1);
    second = new GenericRow();
    second.putDefaultNullValue("one", null);
    Assert.assertNotEquals(first, second);
  }

  @Test
  public void testNullValueFieldsEqual() {
    GenericRow first = new GenericRow();
    first.putDefaultNullValue("one", 1);
    GenericRow second = new GenericRow();
    second.putDefaultNullValue("one", 1);
    Assert.assertEquals(first, second);

    first = new GenericRow();
    first.putDefaultNullValue("one", null);
    second = new GenericRow();
    second.putDefaultNullValue("one", null);
    Assert.assertEquals(first, second);
  }
}
