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
}