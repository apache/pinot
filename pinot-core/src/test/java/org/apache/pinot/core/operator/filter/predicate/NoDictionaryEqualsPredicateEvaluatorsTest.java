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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/// Unit test for no-dictionary based Eq and NEq predicate evaluators.
public class NoDictionaryEqualsPredicateEvaluatorsTest {
  private static final ExpressionContext COLUMN_EXPRESSION = ExpressionContext.forIdentifier("column");
  private static final int NUM_MULTI_VALUES = 100;
  private static final int MAX_STRING_LENGTH = 100;
  private Random _random;

  @BeforeClass
  public void setup() {
    _random = new Random();
  }

  @Test
  public void testIntPredicateEvaluators() {
    int intValue = _random.nextInt();
    String stringValue = Integer.toString(intValue);

    EqPredicate eqPredicate = new EqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator eqPredicateEvaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(eqPredicate, FieldSpec.DataType.INT);

    NotEqPredicate notEqPredicate = new NotEqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator neqPredicateEvaluator =
        NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(notEqPredicate, FieldSpec.DataType.INT);

    Assert.assertTrue(eqPredicateEvaluator.applySV(intValue));
    Assert.assertFalse(neqPredicateEvaluator.applySV(intValue));

    int[] randomInts = new int[NUM_MULTI_VALUES];
    PredicateEvaluatorTestUtils.fillRandom(randomInts);
    randomInts[_random.nextInt(NUM_MULTI_VALUES)] = intValue;

    Assert.assertTrue(eqPredicateEvaluator.applyMV(randomInts, NUM_MULTI_VALUES));
    Assert.assertFalse(neqPredicateEvaluator.applyMV(randomInts, NUM_MULTI_VALUES));

    for (int i = 0; i < 100; i++) {
      int random = _random.nextInt();
      Assert.assertEquals(eqPredicateEvaluator.applySV(random), (random == intValue));
      Assert.assertEquals(neqPredicateEvaluator.applySV(random), (random != intValue));

      PredicateEvaluatorTestUtils.fillRandom(randomInts);
      Assert.assertEquals(eqPredicateEvaluator.applyMV(randomInts, NUM_MULTI_VALUES),
          ArrayUtils.contains(randomInts, intValue));
      Assert.assertEquals(neqPredicateEvaluator.applyMV(randomInts, NUM_MULTI_VALUES),
          !ArrayUtils.contains(randomInts, intValue));
    }
  }

  @Test
  public void testLongPredicateEvaluators() {
    long longValue = _random.nextLong();
    String stringValue = Long.toString(longValue);

    EqPredicate eqPredicate = new EqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator eqPredicateEvaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(eqPredicate, FieldSpec.DataType.LONG);

    NotEqPredicate notEqPredicate = new NotEqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator neqPredicateEvaluator =
        NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(notEqPredicate, FieldSpec.DataType.LONG);

    Assert.assertTrue(eqPredicateEvaluator.applySV(longValue));
    Assert.assertFalse(neqPredicateEvaluator.applySV(longValue));

    long[] randomLongs = new long[NUM_MULTI_VALUES];
    PredicateEvaluatorTestUtils.fillRandom(randomLongs);
    randomLongs[_random.nextInt(NUM_MULTI_VALUES)] = longValue;

    Assert.assertTrue(eqPredicateEvaluator.applyMV(randomLongs, NUM_MULTI_VALUES));
    Assert.assertFalse(neqPredicateEvaluator.applyMV(randomLongs, NUM_MULTI_VALUES));

    for (int i = 0; i < 100; i++) {
      long random = _random.nextLong();
      Assert.assertEquals(eqPredicateEvaluator.applySV(random), (random == longValue));
      Assert.assertEquals(neqPredicateEvaluator.applySV(random), (random != longValue));

      PredicateEvaluatorTestUtils.fillRandom(randomLongs);
      Assert.assertEquals(eqPredicateEvaluator.applyMV(randomLongs, NUM_MULTI_VALUES),
          ArrayUtils.contains(randomLongs, longValue));
      Assert.assertEquals(neqPredicateEvaluator.applyMV(randomLongs, NUM_MULTI_VALUES),
          !ArrayUtils.contains(randomLongs, longValue));
    }
  }

  @Test
  public void testFloatPredicateEvaluators() {
    float floatValue = _random.nextFloat();
    String stringValue = Float.toString(floatValue);

    EqPredicate eqPredicate = new EqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator eqPredicateEvaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(eqPredicate, FieldSpec.DataType.FLOAT);

    NotEqPredicate notEqPredicate = new NotEqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator neqPredicateEvaluator =
        NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(notEqPredicate, FieldSpec.DataType.FLOAT);

    Assert.assertTrue(eqPredicateEvaluator.applySV(floatValue));
    Assert.assertFalse(neqPredicateEvaluator.applySV(floatValue));

    float[] randomFloats = new float[NUM_MULTI_VALUES];
    PredicateEvaluatorTestUtils.fillRandom(randomFloats);
    randomFloats[_random.nextInt(NUM_MULTI_VALUES)] = floatValue;

    Assert.assertTrue(eqPredicateEvaluator.applyMV(randomFloats, NUM_MULTI_VALUES));
    Assert.assertFalse(neqPredicateEvaluator.applyMV(randomFloats, NUM_MULTI_VALUES));

    for (int i = 0; i < 100; i++) {
      float random = _random.nextFloat();
      Assert.assertEquals(eqPredicateEvaluator.applySV(random), (random == floatValue));
      Assert.assertEquals(neqPredicateEvaluator.applySV(random), (random != floatValue));

      PredicateEvaluatorTestUtils.fillRandom(randomFloats);
      Assert.assertEquals(eqPredicateEvaluator.applyMV(randomFloats, NUM_MULTI_VALUES),
          ArrayUtils.contains(randomFloats, floatValue));
      Assert.assertEquals(neqPredicateEvaluator.applyMV(randomFloats, NUM_MULTI_VALUES),
          !ArrayUtils.contains(randomFloats, floatValue));
    }
  }

  @Test
  public void testDoublePredicateEvaluators() {
    double doubleValue = _random.nextDouble();
    String stringValue = Double.toString(doubleValue);

    EqPredicate eqPredicate = new EqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator eqPredicateEvaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(eqPredicate, FieldSpec.DataType.DOUBLE);

    NotEqPredicate notEqPredicate = new NotEqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator neqPredicateEvaluator =
        NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(notEqPredicate, FieldSpec.DataType.DOUBLE);

    Assert.assertTrue(eqPredicateEvaluator.applySV(doubleValue));
    Assert.assertFalse(neqPredicateEvaluator.applySV(doubleValue));

    double[] randomDoubles = new double[NUM_MULTI_VALUES];
    PredicateEvaluatorTestUtils.fillRandom(randomDoubles);
    randomDoubles[_random.nextInt(NUM_MULTI_VALUES)] = doubleValue;

    Assert.assertTrue(eqPredicateEvaluator.applyMV(randomDoubles, NUM_MULTI_VALUES));
    Assert.assertFalse(neqPredicateEvaluator.applyMV(randomDoubles, NUM_MULTI_VALUES));

    for (int i = 0; i < 100; i++) {
      double random = _random.nextDouble();
      Assert.assertEquals(eqPredicateEvaluator.applySV(random), (random == doubleValue));
      Assert.assertEquals(neqPredicateEvaluator.applySV(random), (random != doubleValue));

      PredicateEvaluatorTestUtils.fillRandom(randomDoubles);
      Assert.assertEquals(eqPredicateEvaluator.applyMV(randomDoubles, NUM_MULTI_VALUES),
          ArrayUtils.contains(randomDoubles, doubleValue));
      Assert.assertEquals(neqPredicateEvaluator.applyMV(randomDoubles, NUM_MULTI_VALUES),
          !ArrayUtils.contains(randomDoubles, doubleValue));
    }
  }

  @Test
  public void testBigDecimalPredicateEvaluators() {
    BigDecimal bigDecimalValue = BigDecimal.valueOf(_random.nextDouble());
    String stringValue = bigDecimalValue.toPlainString();

    EqPredicate eqPredicate = new EqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator eqPredicateEvaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(eqPredicate, FieldSpec.DataType.BIG_DECIMAL);

    NotEqPredicate notEqPredicate = new NotEqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator neqPredicateEvaluator =
        NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(notEqPredicate, FieldSpec.DataType.BIG_DECIMAL);

    Assert.assertTrue(eqPredicateEvaluator.applySV(bigDecimalValue));
    Assert.assertFalse(neqPredicateEvaluator.applySV(bigDecimalValue));

    for (int i = 0; i < 100; i++) {
      BigDecimal random = BigDecimal.valueOf(_random.nextDouble());
      Assert.assertEquals(eqPredicateEvaluator.applySV(random), (random.compareTo(bigDecimalValue) == 0));
      Assert.assertEquals(neqPredicateEvaluator.applySV(random), (random.compareTo(bigDecimalValue) != 0));
    }
  }

  @Test
  public void testStringPredicateEvaluators() {
    String stringValue = RandomStringUtils.secure().next(MAX_STRING_LENGTH);

    EqPredicate eqPredicate = new EqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator eqPredicateEvaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(eqPredicate, FieldSpec.DataType.STRING);

    NotEqPredicate notEqPredicate = new NotEqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator neqPredicateEvaluator =
        NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(notEqPredicate, FieldSpec.DataType.STRING);

    Assert.assertTrue(eqPredicateEvaluator.applySV(stringValue));
    Assert.assertFalse(neqPredicateEvaluator.applySV(stringValue));

    String[] randomStrings = new String[NUM_MULTI_VALUES];
    PredicateEvaluatorTestUtils.fillRandom(randomStrings, MAX_STRING_LENGTH);
    randomStrings[_random.nextInt(NUM_MULTI_VALUES)] = stringValue;

    Assert.assertTrue(eqPredicateEvaluator.applyMV(randomStrings, NUM_MULTI_VALUES));
    Assert.assertFalse(neqPredicateEvaluator.applyMV(randomStrings, NUM_MULTI_VALUES));

    for (int i = 0; i < 100; i++) {
      String random = RandomStringUtils.secure().next(MAX_STRING_LENGTH);
      Assert.assertEquals(eqPredicateEvaluator.applySV(random), (random.equals(stringValue)));
      Assert.assertEquals(neqPredicateEvaluator.applySV(random), (!random.equals(stringValue)));

      PredicateEvaluatorTestUtils.fillRandom(randomStrings, MAX_STRING_LENGTH);
      Assert.assertEquals(eqPredicateEvaluator.applyMV(randomStrings, NUM_MULTI_VALUES),
          ArrayUtils.contains(randomStrings, stringValue));
      Assert.assertEquals(neqPredicateEvaluator.applyMV(randomStrings, NUM_MULTI_VALUES),
          !ArrayUtils.contains(randomStrings, stringValue));
    }
  }

  @Test
  public void testJsonPredicateEvaluators() {
    String jsonStringTemplate = "{\"id\": %s, \"name\": %s}";
    String jsonString = String.format(jsonStringTemplate,
        RandomStringUtils.secure().nextAlphanumeric(MAX_STRING_LENGTH),
        RandomStringUtils.secure().nextAlphanumeric(MAX_STRING_LENGTH));

    EqPredicate eqPredicate = new EqPredicate(COLUMN_EXPRESSION, jsonString);
    PredicateEvaluator eqPredicateEvaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(eqPredicate, FieldSpec.DataType.JSON);

    NotEqPredicate notEqPredicate = new NotEqPredicate(COLUMN_EXPRESSION, jsonString);
    PredicateEvaluator neqPredicateEvaluator =
        NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(notEqPredicate, FieldSpec.DataType.JSON);

    Assert.assertTrue(eqPredicateEvaluator.applySV(jsonString));
    Assert.assertFalse(neqPredicateEvaluator.applySV(jsonString));

    String[] randomJsonStrings = new String[NUM_MULTI_VALUES];
    PredicateEvaluatorTestUtils.fillRandomJson(randomJsonStrings, jsonStringTemplate, 2, MAX_STRING_LENGTH);
    randomJsonStrings[_random.nextInt(NUM_MULTI_VALUES)] = jsonString;

    Assert.assertTrue(eqPredicateEvaluator.applyMV(randomJsonStrings, NUM_MULTI_VALUES));
    Assert.assertFalse(neqPredicateEvaluator.applyMV(randomJsonStrings, NUM_MULTI_VALUES));

    for (int i = 0; i < 100; i++) {
      String randomJson = String.format(jsonStringTemplate,
          RandomStringUtils.secure().nextAlphanumeric(MAX_STRING_LENGTH),
          RandomStringUtils.secure().nextAlphanumeric(MAX_STRING_LENGTH));
      Assert.assertEquals(eqPredicateEvaluator.applySV(randomJson), (randomJson.equals(jsonString)));
      Assert.assertEquals(neqPredicateEvaluator.applySV(randomJson), (!randomJson.equals(jsonString)));

      PredicateEvaluatorTestUtils.fillRandomJson(randomJsonStrings, jsonStringTemplate, 2, MAX_STRING_LENGTH);
      Assert.assertEquals(eqPredicateEvaluator.applyMV(randomJsonStrings, NUM_MULTI_VALUES),
          ArrayUtils.contains(randomJsonStrings, jsonString));
      Assert.assertEquals(neqPredicateEvaluator.applyMV(randomJsonStrings, NUM_MULTI_VALUES),
          !ArrayUtils.contains(randomJsonStrings, jsonString));
    }
  }

  @Test
  public void testBytesPredicateEvaluators() {
    byte[] bytesValue = RandomStringUtils.secure().next(MAX_STRING_LENGTH).getBytes();
    String stringValue = BytesUtils.toHexString(bytesValue);

    EqPredicate eqPredicate = new EqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator eqPredicateEvaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(eqPredicate, FieldSpec.DataType.BYTES);

    NotEqPredicate notEqPredicate = new NotEqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator neqPredicateEvaluator =
        NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(notEqPredicate, FieldSpec.DataType.BYTES);

    Assert.assertTrue(eqPredicateEvaluator.applySV(bytesValue));
    Assert.assertFalse(neqPredicateEvaluator.applySV(bytesValue));

    byte[][] randomBytesArray = new byte[NUM_MULTI_VALUES][];
    PredicateEvaluatorTestUtils.fillRandom(randomBytesArray, MAX_STRING_LENGTH);
    randomBytesArray[_random.nextInt(NUM_MULTI_VALUES)] = bytesValue;

    Assert.assertTrue(eqPredicateEvaluator.applyMV(randomBytesArray, NUM_MULTI_VALUES));
    Assert.assertFalse(neqPredicateEvaluator.applyMV(randomBytesArray, NUM_MULTI_VALUES));

    for (int i = 0; i < 100; i++) {
      byte[] randomBytes = RandomStringUtils.secure().next(MAX_STRING_LENGTH).getBytes();
      String randomString = BytesUtils.toHexString(randomBytes);
      Assert.assertEquals(eqPredicateEvaluator.applySV(randomBytes), (randomString.equals(stringValue)));
      Assert.assertEquals(neqPredicateEvaluator.applySV(randomBytes), (!randomString.equals(stringValue)));

      PredicateEvaluatorTestUtils.fillRandom(randomBytesArray, MAX_STRING_LENGTH);
      Assert.assertEquals(eqPredicateEvaluator.applyMV(randomBytesArray, NUM_MULTI_VALUES),
          ArrayUtils.contains(randomBytesArray, stringValue));
      Assert.assertEquals(neqPredicateEvaluator.applyMV(randomBytesArray, NUM_MULTI_VALUES),
          !ArrayUtils.contains(randomBytesArray, stringValue));
    }
  }

  @Test
  public void testUuidPredicateEvaluators() {
    UUID uuidValue = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    byte[] uuidBytes = UuidUtils.toBytes(uuidValue);
    // Predicate literals reach the evaluator as UUID strings. Use an upper-cased one to pin down that the hex digits
    // are matched case-insensitively rather than compared as raw strings.
    String stringValue = uuidValue.toString().toUpperCase(Locale.ROOT);

    EqPredicate eqPredicate = new EqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator eqPredicateEvaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(eqPredicate, FieldSpec.DataType.UUID);

    NotEqPredicate notEqPredicate = new NotEqPredicate(COLUMN_EXPRESSION, stringValue);
    PredicateEvaluator neqPredicateEvaluator =
        NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(notEqPredicate, FieldSpec.DataType.UUID);

    // getDataType() reports the type applySV consumes, not the column's logical type -- exactly as a TIMESTAMP
    // column's evaluator reports LONG. UUID literals are converted to their 16-byte stored form up front, so the
    // BYTES raw evaluator is reused as-is and reports BYTES.
    Assert.assertEquals(eqPredicateEvaluator.getDataType(), FieldSpec.DataType.BYTES);
    Assert.assertEquals(neqPredicateEvaluator.getDataType(), FieldSpec.DataType.BYTES);

    Assert.assertTrue(eqPredicateEvaluator.applySV(uuidBytes));
    Assert.assertFalse(neqPredicateEvaluator.applySV(uuidBytes));

    // A UUID differing only in the last byte must not match, guarding against a truncated comparison.
    byte[] nearMissBytes = Arrays.copyOf(uuidBytes, uuidBytes.length);
    nearMissBytes[nearMissBytes.length - 1] ^= 0x01;
    Assert.assertFalse(eqPredicateEvaluator.applySV(nearMissBytes));
    Assert.assertTrue(neqPredicateEvaluator.applySV(nearMissBytes));

    for (int i = 0; i < 100; i++) {
      byte[] randomUuidBytes = UuidUtils.toBytes(UUID.randomUUID());
      boolean matches = Arrays.equals(randomUuidBytes, uuidBytes);
      Assert.assertEquals(eqPredicateEvaluator.applySV(randomUuidBytes), matches);
      Assert.assertEquals(neqPredicateEvaluator.applySV(randomUuidBytes), !matches);
    }
  }
}
