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
package org.apache.pinot.core.operator.filter.predicate;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for all implementations of no-dictionary based predicate evaluators.
 */
public class NoDictionaryInPredicateEvaluatorTest {
  private static final ExpressionContext COLUMN_EXPRESSION = ExpressionContext.forIdentifier("column");
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
    List<String> stringValues = new ArrayList<>(NUM_PREDICATE_VALUES);
    IntSet valueSet = new IntOpenHashSet();

    for (int i = 0; i < 100; i++) {
      int value = _random.nextInt();
      stringValues.add(Integer.toString(value));
      valueSet.add(value);
    }

    InPredicate inPredicate = new InPredicate(COLUMN_EXPRESSION, stringValues);
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.INT);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_EXPRESSION, stringValues);
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
        Integer.parseInt(stringValues.get(_random.nextInt(NUM_PREDICATE_VALUES)));
    Assert.assertTrue(inPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
    Assert.assertFalse(notInPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
  }

  @Test
  public void testLongPredicateEvaluators() {
    List<String> stringValues = new ArrayList<>(NUM_PREDICATE_VALUES);
    LongSet valueSet = new LongOpenHashSet();

    for (int i = 0; i < 100; i++) {
      long value = _random.nextLong();
      stringValues.add(Long.toString(value));
      valueSet.add(value);
    }

    InPredicate inPredicate = new InPredicate(COLUMN_EXPRESSION, stringValues);
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.LONG);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_EXPRESSION, stringValues);
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
        Long.parseLong(stringValues.get(_random.nextInt(NUM_PREDICATE_VALUES)));

    Assert.assertTrue(inPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
    Assert.assertFalse(notInPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
  }

  @Test
  public void testFloatPredicateEvaluators() {
    List<String> stringValues = new ArrayList<>(NUM_PREDICATE_VALUES);
    FloatSet valueSet = new FloatOpenHashSet();

    for (int i = 0; i < 100; i++) {
      float value = _random.nextFloat();
      stringValues.add(Float.toString(value));
      valueSet.add(value);
    }

    InPredicate inPredicate = new InPredicate(COLUMN_EXPRESSION, stringValues);
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.FLOAT);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_EXPRESSION, stringValues);
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
        Float.parseFloat(stringValues.get(_random.nextInt(NUM_PREDICATE_VALUES)));

    Assert.assertTrue(inPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
    Assert.assertFalse(notInPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
  }

  @Test
  public void testDoublePredicateEvaluators() {
    List<String> stringValues = new ArrayList<>(NUM_PREDICATE_VALUES);
    DoubleSet valueSet = new DoubleOpenHashSet();

    for (int i = 0; i < 100; i++) {
      double value = _random.nextDouble();
      stringValues.add(Double.toString(value));
      valueSet.add(value);
    }

    InPredicate inPredicate = new InPredicate(COLUMN_EXPRESSION, stringValues);
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.DOUBLE);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_EXPRESSION, stringValues);
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
        Double.parseDouble(stringValues.get(_random.nextInt(NUM_PREDICATE_VALUES)));

    Assert.assertTrue(inPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
    Assert.assertFalse(notInPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
  }

  @Test
  public void testBigDecimalPredicateEvaluators() {
    List<String> stringValues = new ArrayList<>(NUM_PREDICATE_VALUES);
    TreeSet<BigDecimal> valueSet = new TreeSet<>();

    for (int i = 0; i < 100; i++) {
      BigDecimal value = BigDecimal.valueOf(_random.nextDouble());
      stringValues.add(value.toPlainString());
      valueSet.add(value);
    }

    InPredicate inPredicate = new InPredicate(COLUMN_EXPRESSION, stringValues);
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.BIG_DECIMAL);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_EXPRESSION, stringValues);
    PredicateEvaluator notInPredicateEvaluator =
        NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator(notInPredicate, FieldSpec.DataType.BIG_DECIMAL);

    for (BigDecimal value : valueSet) {
      Assert.assertTrue(inPredicateEvaluator.applySV(value));
      Assert.assertFalse(notInPredicateEvaluator.applySV(value));
    }

    for (int i = 0; i < 100; i++) {
      BigDecimal value = BigDecimal.valueOf(_random.nextDouble());
      Assert.assertEquals(inPredicateEvaluator.applySV(value), valueSet.contains(value));
      Assert.assertEquals(notInPredicateEvaluator.applySV(value), !valueSet.contains(value));
    }
  }

  @Test
  public void testStringPredicateEvaluators() {
    List<String> stringValues = new ArrayList<>(NUM_PREDICATE_VALUES);
    Set<String> valueSet = new HashSet<>();

    for (int i = 0; i < 100; i++) {
      String value = RandomStringUtils.random(MAX_STRING_LENGTH);
      stringValues.add(value);
      valueSet.add(value);
    }

    InPredicate inPredicate = new InPredicate(COLUMN_EXPRESSION, stringValues);
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.STRING);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_EXPRESSION, stringValues);
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
    multiValues[_random.nextInt(NUM_MULTI_VALUES)] = stringValues.get(_random.nextInt(NUM_PREDICATE_VALUES));

    Assert.assertTrue(inPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
    Assert.assertFalse(notInPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
  }

  @Test
  public void testBytesPredicateEvaluators() {
    List<String> stringValues = new ArrayList<>(NUM_PREDICATE_VALUES);
    Set<byte[]> valueSet = new HashSet<>();

    for (int i = 0; i < 100; i++) {
      byte[] value = RandomStringUtils.random(MAX_STRING_LENGTH).getBytes();
      valueSet.add(value);
      stringValues.add(BytesUtils.toHexString(value));
    }

    InPredicate inPredicate = new InPredicate(COLUMN_EXPRESSION, stringValues);
    PredicateEvaluator inPredicateEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(inPredicate, FieldSpec.DataType.BYTES);

    NotInPredicate notInPredicate = new NotInPredicate(COLUMN_EXPRESSION, stringValues);
    PredicateEvaluator notInPredicateEvaluator =
        NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator(notInPredicate, FieldSpec.DataType.BYTES);

    for (byte[] value : valueSet) {
      Assert.assertTrue(inPredicateEvaluator.applySV(value));
      Assert.assertFalse(notInPredicateEvaluator.applySV(value));
    }

    for (int i = 0; i < 100; i++) {
      byte[] value = RandomStringUtils.random(MAX_STRING_LENGTH).getBytes();
      Assert.assertEquals(inPredicateEvaluator.applySV(value), valueSet.contains(value));
      Assert.assertEquals(notInPredicateEvaluator.applySV(value), !valueSet.contains(value));
    }

    byte[][] multiValues = new byte[NUM_MULTI_VALUES][];
    PredicateEvaluatorTestUtils.fillRandom(multiValues, MAX_STRING_LENGTH);
    multiValues[_random.nextInt(NUM_MULTI_VALUES)] =
        BytesUtils.toBytes(stringValues.get(_random.nextInt(NUM_PREDICATE_VALUES)));

    Assert.assertTrue(inPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
    Assert.assertFalse(notInPredicateEvaluator.applyMV(multiValues, NUM_MULTI_VALUES));
  }
}
