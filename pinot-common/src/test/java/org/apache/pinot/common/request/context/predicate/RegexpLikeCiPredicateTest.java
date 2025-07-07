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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


public class RegexpLikeCiPredicateTest {

  @Test
  public void testRegexpLikeCiPredicate() {
    ExpressionContext lhs = ExpressionContext.forIdentifier("testColumn");
    String value = ".*hello.*";
    RegexpLikeCiPredicate predicate = new RegexpLikeCiPredicate(lhs, value);

    assertEquals(predicate.getType(), Predicate.Type.REGEXP_LIKE_CI);
    assertEquals(predicate.getLhs(), lhs);
    assertEquals(predicate.getValue(), value);
    assertEquals(predicate.toString(), "regexp_like_ci(testColumn,'.*hello.*')");

    // Test case-insensitive matching
    assertTrue(predicate.getPattern().matcher("Hello World").find());
    assertTrue(predicate.getPattern().matcher("HELLO WORLD").find());
    assertTrue(predicate.getPattern().matcher("hello world").find());
    assertFalse(predicate.getPattern().matcher("Goodbye World").find());
  }

  @Test
  public void testEqualsAndHashCode() {
    ExpressionContext lhs1 = ExpressionContext.forIdentifier("testColumn");
    ExpressionContext lhs2 = ExpressionContext.forIdentifier("testColumn");
    ExpressionContext lhs3 = ExpressionContext.forIdentifier("otherColumn");
    String value1 = ".*hello.*";
    String value2 = ".*hello.*";
    String value3 = ".*world.*";

    RegexpLikeCiPredicate predicate1 = new RegexpLikeCiPredicate(lhs1, value1);
    RegexpLikeCiPredicate predicate2 = new RegexpLikeCiPredicate(lhs2, value2);
    RegexpLikeCiPredicate predicate3 = new RegexpLikeCiPredicate(lhs3, value1);
    RegexpLikeCiPredicate predicate4 = new RegexpLikeCiPredicate(lhs1, value3);

    assertEquals(predicate1, predicate2);
    assertEquals(predicate1.hashCode(), predicate2.hashCode());

    assertNotEquals(predicate1, predicate3);
    assertNotEquals(predicate1, predicate4);
    assertNotEquals(predicate1.hashCode(), predicate3.hashCode());
    assertNotEquals(predicate1.hashCode(), predicate4.hashCode());
  }
}
