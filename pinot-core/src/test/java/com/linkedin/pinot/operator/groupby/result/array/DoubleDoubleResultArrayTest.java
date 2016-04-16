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

import com.linkedin.pinot.core.operator.groupby.DoubleDoubleResultArray;
import com.linkedin.pinot.core.query.utils.Pair;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


@Test
public class DoubleDoubleResultArrayTest {
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
    Pair<Double, Double> defaultValue = new Pair<>(_random.nextDouble(), _random.nextDouble());
    DoubleDoubleResultArray doubleDoubleResultArray = new DoubleDoubleResultArray(arraySize, defaultValue);

    for (int i = 0; i < doubleDoubleResultArray.size(); i++) {
      Pair<Double, Double> result = doubleDoubleResultArray.getResult(i);
      Assert.assertEquals(result.getFirst(), defaultValue.getFirst());
      Assert.assertEquals(result.getSecond(), defaultValue.getSecond());
    }
  }

  /**
   * This test sets random values at each index, and ensures that values are set correctly.
   */
  @Test
  void testData() {
    int arraySize = Math.max(10, _random.nextInt() % 100);
    Pair<Double, Double> defaultValue = new Pair<>(_random.nextDouble(), _random.nextDouble());
    DoubleDoubleResultArray doubleDoubleResultArray = new DoubleDoubleResultArray(arraySize, defaultValue);

    for (int i = 0; i < arraySize; i++) {
      Pair<Double, Double> value = new Pair<>(_random.nextDouble(), _random.nextDouble());
      doubleDoubleResultArray.set(i, value);
      Pair<Double, Double> result = doubleDoubleResultArray.getResult(i);
      Assert.assertEquals(result.getFirst(), value.getFirst());
      Assert.assertEquals(result.getSecond(), value.getSecond());
    }
  }

  /**
   * This test sets random values at each index, and then performs an expand on the array.
   * And then tests that the original values are still intact after expanding.
   */
  @Test
  void testExpand() {
    int origSize = Math.max(10, _random.nextInt() % 100);
    Pair<Double, Double> defaultValue = new Pair<>(_random.nextDouble(), _random.nextDouble());
    DoubleDoubleResultArray doubleDoubleResultArray = new DoubleDoubleResultArray(origSize, defaultValue);
    Pair<Double, Double>[] expected = new Pair[origSize];

    for (int i = 0; i < origSize; i++) {
      Pair<Double, Double> value = new Pair<>(_random.nextDouble(), _random.nextDouble());
      doubleDoubleResultArray.set(i, value);
      expected[i] = value;
    }

    int expandedSize = origSize * 2;
    doubleDoubleResultArray.expand(expandedSize);
    int actualSize = doubleDoubleResultArray.size();
    Assert.assertEquals(expandedSize, actualSize,
        ("Size Mis-match: Actual: " + actualSize + " Expected : " + expandedSize));

    for (int i = 0; i < origSize; i++) {
      Pair<Double, Double> actual = doubleDoubleResultArray.getResult(i);
      Assert.assertEquals(expected[i].getFirst(), actual.getFirst());
      Assert.assertEquals(expected[i].getSecond(), actual.getSecond());
    }

    for (int i = origSize; i < expandedSize; i++) {
      Pair<Double, Double> actual = doubleDoubleResultArray.getResult(i);
      Assert.assertEquals(defaultValue.getFirst(), actual.getFirst());
      Assert.assertEquals(defaultValue.getSecond(), actual.getSecond());
    }
  }
}
