/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for no-dictionary based range predicate evaluators.
 */
public class NoDictionaryRangePredicateEvaluatorTest {
  private static final String COLUMN_NAME = "column";

  @Test
  public void testIntPredicateEvaluator() {
    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\t\t10]", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i >= -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\t\t10]", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i > -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\t\t10)", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i > -10 && i < 10));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t10]", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t10)", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i < 10));
    }

    predicateEvaluator = buildRangePredicate("[10\t\t*]", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i >= 10));
    }

    predicateEvaluator = buildRangePredicate("(10\t\t*)", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV(i), (i > 10));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t*)", FieldSpec.DataType.INT);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV(i));
    }
  }

  @Test
  public void testLongPredicateEvaluator() {

    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\t\t10]", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i >= -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\t\t10]", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i > -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\t\t10)", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i > -10 && i < 10));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t10]", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t10)", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i < 10));
    }

    predicateEvaluator = buildRangePredicate("[10\t\t*)", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i >= 10));
    }

    predicateEvaluator = buildRangePredicate("(10\t\t*)", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((long) i), (i > 10));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t*)", FieldSpec.DataType.LONG);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV((long) i));
    }
  }

  @Test
  public void testFloatPredicateEvaluator() {

    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\t\t10]", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i >= -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\t\t10]", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i > -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\t\t10)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i > -10 && i < 10));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t10]", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i <= 10), "Value: " + i);
    }

    predicateEvaluator = buildRangePredicate("(*\t\t10)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i < 10));
    }

    predicateEvaluator = buildRangePredicate("[10\t\t*)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i >= 10), "Value: " + i);
    }

    predicateEvaluator = buildRangePredicate("(10\t\t*)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i > 10));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t*)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV((float) i));
    }
  }

  @Test
  public void testDoublePredicateEvaluator() {

    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\t\t10]", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((double) i), (i >= -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\t\t10]", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((double) i), (i > -10 && i <= 10));
    }

    predicateEvaluator = buildRangePredicate("(-10\t\t10)", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((double) i), (i > -10 && i < 10));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t10]", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((double) i), (i <= 10), "Value: " + i);
    }

    predicateEvaluator = buildRangePredicate("(*\t\t10)", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((double) i), (i < 10));
    }

    predicateEvaluator = buildRangePredicate("[10\t\t*)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i >= 10), "Value: " + i);
    }

    predicateEvaluator = buildRangePredicate("(10\t\t*)", FieldSpec.DataType.FLOAT);
    for (int i = -20; i < 20; i++) {
      Assert.assertEquals(predicateEvaluator.applySV((float) i), (i > 10));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t*)", FieldSpec.DataType.DOUBLE);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV((double) i));
    }
  }

  @Test
  public void testStringPredicateEvaluator() {

    PredicateEvaluator predicateEvaluator = buildRangePredicate("[-10\t\t10]", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("-10") >= 0 && value.compareTo("10") <= 0));
    }

    predicateEvaluator = buildRangePredicate("(-10\t\t10]", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("-10") > 0 && value.compareTo("10") <= 0));
    }

    predicateEvaluator = buildRangePredicate("(-10\t\t10)", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("-10") > 0 && value.compareTo("10") < 0));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t10]", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("10") <= 0));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t10)", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("10") < 0));
    }

    predicateEvaluator = buildRangePredicate("[10\t\t*)", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("10") >= 0));
    }

    predicateEvaluator = buildRangePredicate("(10\t\t*)", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      String value = Integer.toString(i);
      Assert.assertEquals(predicateEvaluator.applySV(value), (value.compareTo("10") > 0));
    }

    predicateEvaluator = buildRangePredicate("(*\t\t*)", FieldSpec.DataType.STRING);
    for (int i = -20; i < 20; i++) {
      Assert.assertTrue(predicateEvaluator.applySV(Integer.toString(i)));
    }
  }

  private PredicateEvaluator buildRangePredicate(String rangeString, FieldSpec.DataType dataType) {
    RangePredicate predicate = new RangePredicate(COLUMN_NAME, Collections.singletonList(rangeString));
    return RangePredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, dataType);
  }
}
