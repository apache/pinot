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

import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.request.context.ExpressionContext;
import org.apache.pinot.spi.request.context.predicate.RangePredicate;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for no-dictionary based range predicate evaluators.
 */
public class NoDictionaryRangePredicateEvaluatorTest {
  private static final ExpressionContext COLUMN_EXPRESSION = ExpressionContext.forIdentifier("column");

  @Test
  public void testIntPredicateEvaluator() {
    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\00010]", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i >= -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\00010]", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i > -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\00010)", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i > -10 && i < 10));
    }

    predicateEvaluator = buildRangePredicate("(*\00010]", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(*\00010)", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i < 10));
    }

    predicateEvaluator = buildRangePredicate("[10\000*]", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i >= 10));
    }

    predicateEvaluator = buildRangePredicate("(10\000*)", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i > 10));
    }

    predicateEvaluator = buildRangePredicate("(*\000*)", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV(i));
    }
    Assert.assertTrue(predicateEvaluator.applySV(Integer.MIN_VALUE));
    Assert.assertTrue(predicateEvaluator.applySV(Integer.MAX_VALUE));

    predicateEvaluator =
        buildRangePredicate(String.format("(%d\000%d)", Integer.MIN_VALUE, Integer.MAX_VALUE), FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV(i));
    }
    Assert.assertFalse(predicateEvaluator.applySV(Integer.MIN_VALUE));
    Assert.assertFalse(predicateEvaluator.applySV(Integer.MAX_VALUE));
  }

  @Test
  public void testLongPredicateEvaluator() {
    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\00010]", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i >= -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\00010]", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i > -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\00010)", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i > -10 && i < 10));
    }

    predicateEvaluator = buildRangePredicate("(*\00010]", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(*\00010)", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i < 10));
    }

    predicateEvaluator = buildRangePredicate("[10\000*)", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i >= 10));
    }

    predicateEvaluator = buildRangePredicate("(10\000*)", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i > 10));
    }

    predicateEvaluator = buildRangePredicate("(*\000*)", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV((long) i));
    }
    Assert.assertTrue(predicateEvaluator.applySV(Long.MIN_VALUE));
    Assert.assertTrue(predicateEvaluator.applySV(Long.MAX_VALUE));

    predicateEvaluator =
        buildRangePredicate(String.format("(%d\000%d)", Long.MIN_VALUE, Long.MAX_VALUE), FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV((long) i));
    }
    Assert.assertFalse(predicateEvaluator.applySV(Long.MIN_VALUE));
    Assert.assertFalse(predicateEvaluator.applySV(Long.MAX_VALUE));
  }

  @Test
  public void testFloatPredicateEvaluator() {
    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\00010]", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i >= -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\00010]", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i > -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\00010)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i > -10 && i < 10));
    }

    predicateEvaluator = buildRangePredicate("(*\00010]", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i <= 10), "Value: " + i);
    }

    predicateEvaluator = buildRangePredicate("(*\00010)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i < 10));
    }

    predicateEvaluator = buildRangePredicate("[10\000*)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i >= 10), "Value: " + i);
    }

    predicateEvaluator = buildRangePredicate("(10\000*)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i > 10));
    }

    predicateEvaluator = buildRangePredicate("(*\000*)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV((float) i));
    }
    Assert.assertTrue(predicateEvaluator.applySV(Float.NEGATIVE_INFINITY));
    Assert.assertTrue(predicateEvaluator.applySV(Float.POSITIVE_INFINITY));

    predicateEvaluator =
        buildRangePredicate(String.format("(%f\000%f)", Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY),
            FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV((float) i));
    }
    Assert.assertFalse(predicateEvaluator.applySV(Float.NEGATIVE_INFINITY));
    Assert.assertFalse(predicateEvaluator.applySV(Float.POSITIVE_INFINITY));
  }

  @Test
  public void testDoublePredicateEvaluator() {
    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\00010]", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((double) i), (i >= -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\00010]", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((double) i), (i > -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\00010)", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((double) i), (i > -10 && i < 10));
    }

    predicateEvaluator = buildRangePredicate("(*\00010]", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((double) i), (i <= 10), "Value: " + i);
    }

    predicateEvaluator = buildRangePredicate("(*\00010)", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((double) i), (i < 10));
    }

    predicateEvaluator = buildRangePredicate("[10\000*)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i >= 10), "Value: " + i);
    }

    predicateEvaluator = buildRangePredicate("(10\000*)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i > 10));
    }

    predicateEvaluator = buildRangePredicate("(*\000*)", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV((double) i));
    }
    Assert.assertTrue(predicateEvaluator.applySV(Double.NEGATIVE_INFINITY));
    Assert.assertTrue(predicateEvaluator.applySV(Double.POSITIVE_INFINITY));

    predicateEvaluator =
        buildRangePredicate(String.format("(%f\000%f)", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY),
            FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV((double) i));
    }
    Assert.assertFalse(predicateEvaluator.applySV(Double.NEGATIVE_INFINITY));
    Assert.assertFalse(predicateEvaluator.applySV(Double.POSITIVE_INFINITY));
  }

  @Test
  public void testStringPredicateEvaluator() {
    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\00010]", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert
          .assertEquals(predicateEvaluator.applySV(value), (value.compareTo("-10") >= 0 && value.compareTo("10") <= 0));
    }

    predicateEvaluator = buildRangePredicate("(-10\00010]", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert
          .assertEquals(predicateEvaluator.applySV(value), (value.compareTo("-10") > 0 && value.compareTo("10") <= 0));
    }

    predicateEvaluator = buildRangePredicate("(-10\00010)", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("-10") > 0 && value.compareTo("10") < 0));
    }

    predicateEvaluator = buildRangePredicate("(*\00010]", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("10") <= 0));
    }

    predicateEvaluator = buildRangePredicate("(*\00010)", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("10") < 0));
    }

    predicateEvaluator = buildRangePredicate("[10\000*)", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("10") >= 0));
    }

    predicateEvaluator = buildRangePredicate("(10\000*)", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("10") > 0));
    }

    predicateEvaluator = buildRangePredicate("(*\000*)", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV(Integer.toString(i)));
    }
  }

  @Test
  public void testBytesPredicateEvaluator() {
    PredicateEvaluator predicateEvaluator = buildRangePredicate("[10\00020]", FieldSpec.DataType.BYTES);
    for (int i = 0x00; i < 0x30; i++) {
      byte[] value = Integer.toString(i).getBytes();
      Assert.assertEquals(predicateEvaluator.applySV(value),
          (ByteArray.compare(value, new byte[]{0x10}) >= 0 && ByteArray.compare(value, new byte[]{0x20}) <= 0));
    }

    predicateEvaluator = buildRangePredicate("(10\00020]", FieldSpec.DataType.BYTES);
    for (int i = 0x00; i < 0x30; i++) {
      byte[] value = Integer.toString(i).getBytes();
      Assert.assertEquals(predicateEvaluator.applySV(value),
          (ByteArray.compare(value, new byte[]{0x10}) > 0 && ByteArray.compare(value, new byte[]{0x20}) <= 0));
    }

    predicateEvaluator = buildRangePredicate("(10\00020)", FieldSpec.DataType.BYTES);
    for (int i = 0x00; i < 0x30; i++) {
      byte[] value = Integer.toString(i).getBytes();
      Assert.assertEquals(predicateEvaluator.applySV(value),
          (ByteArray.compare(value, new byte[]{0x10}) > 0 && ByteArray.compare(value, new byte[]{0x20}) < 0));
    }

    predicateEvaluator = buildRangePredicate("(*\00010]", FieldSpec.DataType.BYTES);
    for (int i = 0x00; i < 0x30; i++) {
      byte[] value = Integer.toString(i).getBytes();
      Assert.assertEquals(predicateEvaluator.applySV(value), ByteArray.compare(value, new byte[]{0x10}) <= 0);
    }

    predicateEvaluator = buildRangePredicate("(*\00010)", FieldSpec.DataType.BYTES);
    for (int i = 0x00; i < 0x30; i++) {
      byte[] value = Integer.toString(i).getBytes();
      Assert.assertEquals(predicateEvaluator.applySV(value), ByteArray.compare(value, new byte[]{0x10}) < 0);
    }

    predicateEvaluator = buildRangePredicate("[10\000*)", FieldSpec.DataType.BYTES);
    for (int i = 0x00; i < 0x30; i++) {
      byte[] value = Integer.toString(i).getBytes();
      Assert.assertEquals(predicateEvaluator.applySV(value), ByteArray.compare(value, new byte[]{0x10}) >= 0);
    }

    predicateEvaluator = buildRangePredicate("(10\000*)", FieldSpec.DataType.BYTES);
    for (int i = 0x00; i < 0x30; i++) {
      byte[] value = Integer.toString(i).getBytes();
      Assert.assertEquals(predicateEvaluator.applySV(value), ByteArray.compare(value, new byte[]{0x10}) > 0);
    }

    predicateEvaluator = buildRangePredicate("(*\000*)", FieldSpec.DataType.BYTES);
    for (int i = 0x00; i < 0x30; i++) {
      byte[] value = Integer.toString(i).getBytes();
      Assert.assertTrue(predicateEvaluator.applySV(value));
    }
  }

  private PredicateEvaluator buildRangePredicate(String rangeString, FieldSpec.DataType dataType) {
    RangePredicate predicate = new RangePredicate(COLUMN_EXPRESSION, rangeString);
    return RangePredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, dataType);
  }
}
