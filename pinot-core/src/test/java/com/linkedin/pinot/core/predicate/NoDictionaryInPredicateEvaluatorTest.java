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
package com.linkedin.pinot.core.predicate;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.operator.filter.predicate.InPredicateEvaluatorFactory;
import com.linkedin.pinot.core.operator.filter.predicate.NotInPredicateEvaluatorFactory;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for all implementations of no-dictionary based predicate evaluators.
 */
public class NoDictionaryInPredicateEvaluatorTest {

  private static final String COLUMN_NAME = "column";
  private static final int NUM_PREDICATE_VALUES = 100;
  private static final int NUM_MULTI_VALUES = 10;
  private static final int MAX_STRING_LENGTH = 100;
  private Random _random;

  @BeforeClass
  public void setup() {
    _random = new Random();
  }

  @Test
  public void testIntPredicateEvaluators() {
    String[] stringValues = new String[NUM_PREDICATE_VALUES];
    IntSet valueSet = new IntOpenHashSet();

    for (int i = 0; i < 100; i++) {
      int value = _random.nextInt();
      stringValues[i] = Integer.toString(value);
      valueSet.add(value);
    }

    InPredicate inPredicate =
        new InPredicate(COLUMN_NAME, Collections.singletonList(StringUtil.join(InPredicate.DELIMITER, stringValues)));
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.INT);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_NAME,
        Collections.singletonList(StringUtil.join(InPredicate.DELIMITER, stringValues)));
    PredicateEvaluator notInPredicateEvaluator =
        NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator(notInPredicate, FieldSpec.DataType.INT);

    for (Integer value : valueSet) {
      Assert.assertTrue(inPredicateEvaluator.applySV(value));
      Assert.assertFalse(notInPredicateEvaluator.applySV(value));
    }

    for (int i = 0; i < 100; i++) {
      int value = _random.nextInt();
      Assert.assertEquals(inPredicateEvaluator.applySV(value), valueSet.contains(value));
      Assert.assertEquals(notInPredicateEvaluator.applySV(value), !valueSet.contains(value));
    }

    int[] multiValues = new int[NUM_MULTI_VALUES];
    PredicateEvaluatorTestUtils.fillRandom(multiValues);
    multiValues[_random.nextInt(NUM_MULTI_VALUES)] =
        Integer.parseInt(stringValues[_random.nextInt(NUM_PREDICATE_VALUES)]);
    Assert.assertTrue(inPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
    Assert.assertFalse(notInPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
  }

  @Test
  public void testLongPredicateEvaluators() {
    String[] stringValues = new String[NUM_PREDICATE_VALUES];
    LongSet valueSet = new LongOpenHashSet();

    for (int i = 0; i < 100; i++) {
      long value = _random.nextLong();
      stringValues[i] = Long.toString(value);
      valueSet.add(value);
    }

    InPredicate inPredicate =
        new InPredicate(COLUMN_NAME, Collections.singletonList(StringUtil.join(InPredicate.DELIMITER, stringValues)));
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.LONG);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_NAME,
        Collections.singletonList(StringUtil.join(InPredicate.DELIMITER, stringValues)));
    PredicateEvaluator notInPredicateEvaluator =
        NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator(notInPredicate, FieldSpec.DataType.LONG);

    for (Long value : valueSet) {
      Assert.assertTrue(inPredicateEvaluator.applySV(value));
      Assert.assertFalse(notInPredicateEvaluator.applySV(value));
    }

    for (int i = 0; i < 100; i++) {
      long value = _random.nextLong();
      Assert.assertEquals(inPredicateEvaluator.applySV(value), valueSet.contains(value));
      Assert.assertEquals(notInPredicateEvaluator.applySV(value), !valueSet.contains(value));
    }

    long[] multiValues = new long[NUM_MULTI_VALUES];
    PredicateEvaluatorTestUtils.fillRandom(multiValues);
    multiValues[_random.nextInt(NUM_MULTI_VALUES)] =
        Long.parseLong(stringValues[_random.nextInt(NUM_PREDICATE_VALUES)]);

    Assert.assertTrue(inPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
    Assert.assertFalse(notInPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
  }

  @Test
  public void testFloatPredicateEvaluators() {
    String[] stringValues = new String[NUM_PREDICATE_VALUES];
    FloatSet valueSet = new FloatOpenHashSet();

    for (int i = 0; i < 100; i++) {
      float value = _random.nextFloat();
      stringValues[i] = Float.toString(value);
      valueSet.add(value);
    }

    InPredicate inPredicate =
        new InPredicate(COLUMN_NAME, Collections.singletonList(StringUtil.join(InPredicate.DELIMITER, stringValues)));
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.FLOAT);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_NAME,
        Collections.singletonList(StringUtil.join(InPredicate.DELIMITER, stringValues)));
    PredicateEvaluator notInPredicateEvaluator =
        NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator(notInPredicate, FieldSpec.DataType.FLOAT);

    for (float value : valueSet) {
      Assert.assertTrue(inPredicateEvaluator.applySV(value));
      Assert.assertFalse(notInPredicateEvaluator.applySV(value));
    }

    for (int i = 0; i < 100; i++) {
      float value = _random.nextFloat();
      Assert.assertEquals(inPredicateEvaluator.applySV(value), valueSet.contains(value));
      Assert.assertEquals(notInPredicateEvaluator.applySV(value), !valueSet.contains(value));
    }

    float[] multiValues = new float[NUM_MULTI_VALUES];
    PredicateEvaluatorTestUtils.fillRandom(multiValues);
    multiValues[_random.nextInt(NUM_MULTI_VALUES)] =
        Float.parseFloat(stringValues[_random.nextInt(NUM_PREDICATE_VALUES)]);

    Assert.assertTrue(inPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
    Assert.assertFalse(notInPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
  }

  @Test
  public void testDoublePredicateEvaluators() {
    String[] stringValues = new String[NUM_PREDICATE_VALUES];
    DoubleSet valueSet = new DoubleOpenHashSet();

    for (int i = 0; i < 100; i++) {
      double value = _random.nextDouble();
      stringValues[i] = Double.toString(value);
      valueSet.add(value);
    }

    InPredicate inPredicate =
        new InPredicate(COLUMN_NAME, Collections.singletonList(StringUtil.join(InPredicate.DELIMITER, stringValues)));
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.DOUBLE);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_NAME,
        Collections.singletonList(StringUtil.join(InPredicate.DELIMITER, stringValues)));
    PredicateEvaluator notInPredicateEvaluator =
        NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator(notInPredicate, FieldSpec.DataType.DOUBLE);

    for (double value : valueSet) {
      Assert.assertTrue(inPredicateEvaluator.applySV(value));
      Assert.assertFalse(notInPredicateEvaluator.applySV(value));
    }

    for (int i = 0; i < 100; i++) {
      double value = _random.nextDouble();
      Assert.assertEquals(inPredicateEvaluator.applySV(value), valueSet.contains(value));
      Assert.assertEquals(notInPredicateEvaluator.applySV(value), !valueSet.contains(value));
    }

    double[] multiValues = new double[NUM_MULTI_VALUES];
    PredicateEvaluatorTestUtils.fillRandom(multiValues);
    multiValues[_random.nextInt(NUM_MULTI_VALUES)] =
        Double.parseDouble(stringValues[_random.nextInt(NUM_PREDICATE_VALUES)]);

    Assert.assertTrue(inPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
    Assert.assertFalse(notInPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
  }

  @Test
  public void testStringPredicateEvaluators() {
    String[] stringValues = new String[NUM_PREDICATE_VALUES];
    Set<String> valueSet = new HashSet<>();

    for (int i = 0; i < 100; i++) {
      stringValues[i] = RandomStringUtils.random(MAX_STRING_LENGTH).replace("\t", "");
      valueSet.add(stringValues[i]);
    }

    InPredicate inPredicate =
        new InPredicate(COLUMN_NAME, Collections.singletonList(StringUtil.join(InPredicate.DELIMITER, stringValues)));
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.STRING);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_NAME,
        Collections.singletonList(StringUtil.join(InPredicate.DELIMITER, stringValues)));
    PredicateEvaluator notInPredicateEvaluator =
        NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator(notInPredicate, FieldSpec.DataType.STRING);

    for (String value : valueSet) {
      Assert.assertTrue(inPredicateEvaluator.applySV(value));
      Assert.assertFalse(notInPredicateEvaluator.applySV(value));
    }

    for (int i = 0; i < 100; i++) {
      String value = RandomStringUtils.random(MAX_STRING_LENGTH);
      Assert.assertEquals(inPredicateEvaluator.applySV(value), valueSet.contains(value));
      Assert.assertEquals(notInPredicateEvaluator.applySV(value), !valueSet.contains(value));
    }

    String[] multiValues = new String[NUM_MULTI_VALUES];
    PredicateEvaluatorTestUtils.fillRandom(multiValues, MAX_STRING_LENGTH);
    multiValues[_random.nextInt(NUM_MULTI_VALUES)] = stringValues[_random.nextInt(NUM_PREDICATE_VALUES)];

    Assert.assertTrue(inPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
    Assert.assertFalse(notInPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
  }
}
