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
package org.apache.pinot.query.planner.physical.v2.opt.rules;

import org.apache.calcite.rex.RexNode;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SortPushdownRuleTest {
  @Test
  public void testComputeEffectiveFetchWhenFetchIsNull() {
    assertNull(SortPushdownRule.computeEffectiveFetch(null, null));
    assertNull(SortPushdownRule.computeEffectiveFetch(null, SortPushdownRule.createLiteral(100)));
  }

  @Test
  public void testComputeEffectiveFetchWhenOnlyFetch() {
    RexNode fetch = SortPushdownRule.createLiteral(100);
    assertEquals(RexExpressionUtils.getValueAsInt(SortPushdownRule.computeEffectiveFetch(fetch, null)), 100);
  }

  @Test
  public void testComputeEffectiveFetchWhenFetchAndOffset() {
    RexNode fetch = SortPushdownRule.createLiteral(100);
    RexNode offset = SortPushdownRule.createLiteral(11);
    assertEquals(RexExpressionUtils.getValueAsInt(SortPushdownRule.computeEffectiveFetch(fetch, offset)), 111);
  }
}
