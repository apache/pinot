/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.operator.groupby.result.array;

import com.linkedin.pinot.core.operator.groupby.DoubleResultArray;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


@Test
public class DoubleResultArrayTest {
  Random _random;

  @BeforeSuite
  void init() {
    _random = new Random(System.currentTimeMillis());
  }

  /**
   * This test ensures that all elements have the default value set correctly.
   */
  @Test
  void testDefaultValue() {
    int arraySize = Math.max(10, _random.nextInt() % 100);
    double defaultValue = _random.nextDouble();
    DoubleResultArray doubleResultArray = new DoubleResultArray(arraySize, defaultValue);

    for (int i = 0; i < doubleResultArray.size(); i++) {
      Assert.assertEquals(doubleResultArray.getDoubleResult(i), defaultValue);
    }
  }

  /**
   * This test sets random values at each index, and ensures that values are set correctly.
   */
  @Test
  void testData() {
    int arraySize = Math.max(10, _random.nextInt() % 100);
    double defaultValue = _random.nextDouble();
    DoubleResultArray doubleResultArray = new DoubleResultArray(arraySize, defaultValue);

    for (int i = 0; i < arraySize; i++) {
      double value = _random.nextDouble();
      doubleResultArray.set(i, value);
      Assert.assertEquals(value, doubleResultArray.getDoubleResult(i));
    }
  }

  /**
   * This test sets random values at each index, and then performs an expand on the array.
   * And then tests that the original values are still intact after expanding.
   */
  @Test
  void testExpand() {
    int origSize = Math.max(10, _random.nextInt() % 100);
    double defaultValue = _random.nextDouble();

    DoubleResultArray doubleResultArray = new DoubleResultArray(origSize, defaultValue);
    double[] expected = new double[origSize];

    for (int i = 0; i < origSize; i++) {
      double value = _random.nextDouble();
      doubleResultArray.set(i, value);
      expected[i] = value;
    }

    int expandedSize = origSize * 2;
    doubleResultArray.expand(expandedSize);
    int actualSize = doubleResultArray.size();
    Assert.assertEquals(expandedSize, actualSize,
        ("Size Mis-match: Actual: " + actualSize + " Expected : " + expandedSize));

    for (int i = 0; i < origSize; i++) {
      double actual = doubleResultArray.getDoubleResult(i);
      Assert.assertEquals(expected[i], actual, ("Value Mis-match: Actual: " + actual + " Expected: " + expected[i]));
    }

    for (int i = origSize; i < expandedSize; i++) {
      double actual = doubleResultArray.getDoubleResult(i);
      Assert.assertEquals(defaultValue, actual, ("Value Mis-match: Actual: " + actual + " Expected: " + defaultValue));
    }
  }
}
