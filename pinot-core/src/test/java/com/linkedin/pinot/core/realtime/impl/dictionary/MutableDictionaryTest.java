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
package com.linkedin.pinot.core.realtime.impl.dictionary;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for mutable dictionary implementations.
 */
public class MutableDictionaryTest {

  private static final String COLUMN_NAME = "testColumn";
  private static final int DICTIONARY_SIZE = 1000;
  private static final Integer MAX_VALUE = 100;
  private Random _random;

  @BeforeClass
  public void setup() {
    _random = new Random();
  }

  @Test
  public void testInt() {
    IntMutableDictionary dictionary = new IntMutableDictionary(COLUMN_NAME);
    Assert.assertTrue(dictionary.isEmpty());

    List<Integer> expected = new ArrayList<>(DICTIONARY_SIZE);
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;

    Set<Integer> valueSet = new HashSet<>();
    for (int i = 0; i < DICTIONARY_SIZE; i++) {
      int value = _random.nextInt() % MAX_VALUE;
      dictionary.index(value);
      valueSet.add(value);

      expected.add(value);
      min = Math.min(value, min);
      max = Math.max(value, max);
    }

    Assert.assertEquals(dictionary.length(), valueSet.size());
    Assert.assertTrue(!dictionary.contains(MAX_VALUE));
    Assert.assertEquals(dictionary.getMinVal(), min);
    Assert.assertEquals(dictionary.getMaxVal(), max);

    for (int i = 0; i < DICTIONARY_SIZE; i++) {
      Assert.assertTrue(dictionary.contains(expected.get(i)));
    }

    for (int i = 0; i < dictionary.length(); i++) {
      String lower = String.valueOf(dictionary.getIntValue(i) - 1);
      String upper = String.valueOf(dictionary.getIntValue(i) + 1);
      Assert.assertTrue(dictionary.inRange(lower, upper, i, true, true));
      Assert.assertTrue(!dictionary.inRange(upper, upper, i, false, false));
    }
  }

  @Test
  public void testLong() {
    LongMutableDictionary dictionary = new LongMutableDictionary(COLUMN_NAME);
    Assert.assertTrue(dictionary.isEmpty());

    List<Long> expected = new ArrayList<>(DICTIONARY_SIZE);
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;

    Set<Long> valueSet = new HashSet<>();
    for (int i = 0; i < DICTIONARY_SIZE; i++) {
      long value = _random.nextLong() % MAX_VALUE;
      dictionary.index(value);
      valueSet.add(value);

      expected.add(value);
      min = Math.min(value, min);
      max = Math.max(value, max);
    }

    Assert.assertEquals(dictionary.length(), valueSet.size());
    Assert.assertTrue(!dictionary.contains(MAX_VALUE.longValue()));
    Assert.assertEquals(dictionary.getMinVal(), min);
    Assert.assertEquals(dictionary.getMaxVal(), max);

    for (int i = 0; i < DICTIONARY_SIZE; i++) {
      Assert.assertTrue(dictionary.contains(expected.get(i)));
    }

    for (int i = 0; i < dictionary.length(); i++) {
      String lower = String.valueOf(dictionary.getLongValue(i) - 1);
      String upper = String.valueOf(dictionary.getLongValue(i) + 1);
      Assert.assertTrue(dictionary.inRange(lower, upper, i, true, true));
      Assert.assertTrue(!dictionary.inRange(upper, upper, i, false, false));
    }
  }

  @Test
  public void testFloat() {
    FloatMutableDictionary dictionary = new FloatMutableDictionary(COLUMN_NAME);
    Assert.assertTrue(dictionary.isEmpty());

    List<Float> expected = new ArrayList<>(DICTIONARY_SIZE);
    float min = Float.MAX_VALUE;
    float max = Float.MIN_VALUE;

    Set<Float> valueSet = new HashSet<>();
    for (int i = 0; i < DICTIONARY_SIZE; i++) {
      float value = _random.nextFloat() % MAX_VALUE;
      dictionary.index(value);
      valueSet.add(value);

      expected.add(value);
      min = Math.min(value, min);
      max = Math.max(value, max);
    }

    Assert.assertEquals(dictionary.length(), valueSet.size());
    Assert.assertTrue(!dictionary.contains(MAX_VALUE.floatValue()));

    Assert.assertEquals(dictionary.getMinVal(), min);
    Assert.assertEquals(dictionary.getMaxVal(), max);

    for (int i = 0; i < DICTIONARY_SIZE; i++) {
      Assert.assertTrue(dictionary.contains(expected.get(i)));
    }

    for (int i = 0; i < dictionary.length(); i++) {
      String lower = String.valueOf(dictionary.getFloatValue(i) - 1);
      String upper = String.valueOf(dictionary.getFloatValue(i) + 1);
      Assert.assertTrue(dictionary.inRange(lower, upper, i, true, true));
      Assert.assertTrue(!dictionary.inRange(upper, upper, i, false, false));
    }
  }

  @Test
  public void testDouble() {
    DoubleMutableDictionary dictionary = new DoubleMutableDictionary(COLUMN_NAME);
    Assert.assertTrue(dictionary.isEmpty());

    List<Double> expected = new ArrayList<>(DICTIONARY_SIZE);
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;

    Set<Double> valueSet = new HashSet<>();
    for (int i = 0; i < DICTIONARY_SIZE; i++) {
      double value = _random.nextDouble() % MAX_VALUE;
      dictionary.index(value);
      valueSet.add(value);

      expected.add(value);
      min = Math.min(value, min);
      max = Math.max(value, max);
    }

    Assert.assertEquals(dictionary.length(), valueSet.size());
    Assert.assertTrue(!dictionary.contains(MAX_VALUE.doubleValue()));
    Assert.assertEquals(dictionary.getMinVal(), min);
    Assert.assertEquals(dictionary.getMaxVal(), max);

    for (int i = 0; i < DICTIONARY_SIZE; i++) {
      Assert.assertTrue(dictionary.contains(expected.get(i)));
    }

    for (int i = 0; i < dictionary.length(); i++) {
      String lower = String.valueOf(dictionary.getDoubleValue(i) - 1);
      String upper = String.valueOf(dictionary.getDoubleValue(i) + 1);
      Assert.assertTrue(dictionary.inRange(lower, upper, i, true, true));
      Assert.assertTrue(!dictionary.inRange(upper, upper, i, false, false));
    }
  }

  @Test
  public void testString() {
    StringMutableDictionary dictionary = new StringMutableDictionary(COLUMN_NAME);
    Assert.assertTrue(dictionary.isEmpty());

    List<String> expected = new ArrayList<>(DICTIONARY_SIZE);
    Set<String> valueSet = new HashSet<>();
    for (int i = 0; i < DICTIONARY_SIZE; i++) {
      String value = RandomStringUtils.random(100);
      dictionary.index(value);
      valueSet.add(value);
      expected.add(value);
    }

    Assert.assertEquals(dictionary.length(), valueSet.size());

    for (int i = 0; i < DICTIONARY_SIZE; i++) {
      Assert.assertTrue(dictionary.contains(expected.get(i)));
    }
  }
}
