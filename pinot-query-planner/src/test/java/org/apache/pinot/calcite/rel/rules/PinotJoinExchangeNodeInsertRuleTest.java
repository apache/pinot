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
package org.apache.pinot.calcite.rel.rules;

import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions.DistributionType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.calcite.rel.hint.PinotHintOptions.DistributionType.BROADCAST;
import static org.apache.pinot.calcite.rel.hint.PinotHintOptions.DistributionType.HASH;
import static org.apache.pinot.calcite.rel.hint.PinotHintOptions.DistributionType.LOCAL;
import static org.apache.pinot.calcite.rel.hint.PinotHintOptions.DistributionType.RANDOM;
import static org.testng.Assert.assertThrows;


/// Covers the distribution type pairs a join can and cannot be answered with, including join types that SQL hints do
/// not reach today (ANTI, ASOF and LEFT_ASOF are produced or hinted outside the path the query tests exercise).
public class PinotJoinExchangeNodeInsertRuleTest {
  private static final JoinInfo EQUI = JoinInfo.of(ImmutableIntList.of(0), ImmutableIntList.of(0));
  private static final JoinInfo NON_EQUI = JoinInfo.of(ImmutableIntList.of(), ImmutableIntList.of());

  /// {join type, join info, left, right, left replicated, right replicated}
  @DataProvider
  public static Object[][] accepted() {
    return new Object[][]{
        {JoinRelType.INNER, EQUI, HASH, HASH, false, false},
        {JoinRelType.INNER, EQUI, LOCAL, BROADCAST, false, false},
        {JoinRelType.LEFT, EQUI, LOCAL, BROADCAST, false, false},
        {JoinRelType.SEMI, EQUI, LOCAL, BROADCAST, false, false},
        {JoinRelType.ANTI, EQUI, RANDOM, BROADCAST, false, false},
        {JoinRelType.ASOF, EQUI, HASH, BROADCAST, false, false},
        {JoinRelType.LEFT_ASOF, EQUI, LOCAL, BROADCAST, false, false},
        {JoinRelType.RIGHT, EQUI, BROADCAST, LOCAL, false, false},
        // A LOCAL scan of a replicated table gives every worker the whole table, like a broadcast.
        {JoinRelType.INNER, NON_EQUI, LOCAL, LOCAL, false, true},
        {JoinRelType.LEFT, NON_EQUI, LOCAL, LOCAL, false, true},
        {JoinRelType.INNER, EQUI, HASH, LOCAL, false, true},
        {JoinRelType.RIGHT, EQUI, LOCAL, HASH, true, false},
        // A FULL join without equi keys runs on a single worker, which RANDOM and BROADCAST both deliver every row to.
        {JoinRelType.FULL, NON_EQUI, RANDOM, BROADCAST, false, false},
        {JoinRelType.FULL, NON_EQUI, RANDOM, RANDOM, false, false},
        {JoinRelType.FULL, NON_EQUI, BROADCAST, BROADCAST, false, false},
    };
  }

  @Test(dataProvider = "accepted")
  public void testAccepted(JoinRelType joinType, JoinInfo joinInfo, DistributionType left, DistributionType right,
      boolean leftReplicated, boolean rightReplicated) {
    PinotJoinExchangeNodeInsertRule.validateDistributionTypes(left, right, leftReplicated, rightReplicated, joinInfo,
        joinType);
  }

  /// {join type, join info, left, right, left replicated, right replicated}
  @DataProvider
  public static Object[][] rejected() {
    return new Object[][]{
        // Each of these decides a left row from the right rows a worker holds, so the left cannot be the whole side.
        {JoinRelType.LEFT, EQUI, BROADCAST, LOCAL, false, false},
        {JoinRelType.SEMI, EQUI, BROADCAST, LOCAL, false, false},
        {JoinRelType.ANTI, EQUI, BROADCAST, HASH, false, false},
        {JoinRelType.ASOF, EQUI, BROADCAST, HASH, false, false},
        {JoinRelType.LEFT_ASOF, EQUI, BROADCAST, HASH, false, false},
        {JoinRelType.LEFT, EQUI, LOCAL, LOCAL, true, false},
        {JoinRelType.RIGHT, EQUI, LOCAL, BROADCAST, false, false},
        {JoinRelType.FULL, EQUI, LOCAL, BROADCAST, false, false},
        {JoinRelType.INNER, EQUI, BROADCAST, BROADCAST, false, false},
        {JoinRelType.INNER, EQUI, RANDOM, RANDOM, false, false},
        {JoinRelType.INNER, EQUI, HASH, RANDOM, false, false},
        {JoinRelType.INNER, NON_EQUI, LOCAL, LOCAL, false, false},
        {JoinRelType.FULL, NON_EQUI, LOCAL, LOCAL, false, false},
        {JoinRelType.FULL, NON_EQUI, RANDOM, LOCAL, false, false},
    };
  }

  @Test(dataProvider = "rejected")
  public void testRejected(JoinRelType joinType, JoinInfo joinInfo, DistributionType left, DistributionType right,
      boolean leftReplicated, boolean rightReplicated) {
    assertThrows(IllegalArgumentException.class,
        () -> PinotJoinExchangeNodeInsertRule.validateDistributionTypes(left, right, leftReplicated, rightReplicated,
            joinInfo, joinType));
  }
}
