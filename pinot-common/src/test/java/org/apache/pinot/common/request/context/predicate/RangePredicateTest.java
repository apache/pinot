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
package org.apache.pinot.common.request.context.predicate;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RangePredicateTest {

  @Test
  public void testUnbounded() {
    RangePredicate rangePredicate = new RangePredicate(ExpressionContext.forIdentifier("ts"), "(*" + '\0' + "*)");
    Assert.assertEquals(rangePredicate.getType(), Predicate.Type.RANGE);
    Assert.assertEquals(rangePredicate.getLowerBound(), "*");
    Assert.assertEquals(rangePredicate.getUpperBound(), "*");
    Assert.assertEquals(rangePredicate.getLhs().getIdentifier(), "ts");
    Assert.assertFalse(rangePredicate.isLowerInclusive());
    Assert.assertFalse(rangePredicate.isUpperInclusive());
    Assert.assertEquals(rangePredicate.getRange(), "(*\0*)");
  }

  @Test
  public void testLowUnbounded() {
    String rangeString = "(*" + '\0' + "100]";
    RangePredicate rangePredicate = new RangePredicate(ExpressionContext.forIdentifier("ts"), rangeString);
    Assert.assertEquals(rangePredicate.getType(), Predicate.Type.RANGE);
    Assert.assertEquals(rangePredicate.getLowerBound(), "*");
    Assert.assertEquals(rangePredicate.getUpperBound(), "100");
    Assert.assertEquals(rangePredicate.getLhs().getIdentifier(), "ts");
    Assert.assertFalse(rangePredicate.isLowerInclusive());
    Assert.assertTrue(rangePredicate.isUpperInclusive());
    Assert.assertEquals(rangePredicate.getRange(), rangeString);

    rangeString = "(*" + '\0' + "100)";
    rangePredicate = new RangePredicate(ExpressionContext.forIdentifier("ts"), rangeString);
    Assert.assertEquals(rangePredicate.getType(), Predicate.Type.RANGE);
    Assert.assertEquals(rangePredicate.getLowerBound(), "*");
    Assert.assertEquals(rangePredicate.getUpperBound(), "100");
    Assert.assertEquals(rangePredicate.getLhs().getIdentifier(), "ts");
    Assert.assertFalse(rangePredicate.isLowerInclusive());
    Assert.assertFalse(rangePredicate.isUpperInclusive());
    Assert.assertEquals(rangePredicate.getRange(), rangeString);
  }

  @Test
  public void testUpperUnbounded() {
    String rangeString = "[100" + '\0' + "*)";
    RangePredicate rangePredicate = new RangePredicate(ExpressionContext.forIdentifier("ts"), rangeString);
    Assert.assertEquals(rangePredicate.getType(), Predicate.Type.RANGE);
    Assert.assertEquals(rangePredicate.getLowerBound(), "100");
    Assert.assertEquals(rangePredicate.getUpperBound(), "*");
    Assert.assertEquals(rangePredicate.getLhs().getIdentifier(), "ts");
    Assert.assertTrue(rangePredicate.isLowerInclusive());
    Assert.assertFalse(rangePredicate.isUpperInclusive());
    Assert.assertEquals(rangePredicate.getRange(), rangeString);

    rangeString = "(100" + '\0' + "*)";
    rangePredicate = new RangePredicate(ExpressionContext.forIdentifier("ts"), rangeString);
    Assert.assertEquals(rangePredicate.getType(), Predicate.Type.RANGE);
    Assert.assertEquals(rangePredicate.getLowerBound(), "100");
    Assert.assertEquals(rangePredicate.getUpperBound(), "*");
    Assert.assertEquals(rangePredicate.getLhs().getIdentifier(), "ts");
    Assert.assertFalse(rangePredicate.isLowerInclusive());
    Assert.assertFalse(rangePredicate.isUpperInclusive());
    Assert.assertEquals(rangePredicate.getRange(), rangeString);
  }
}
