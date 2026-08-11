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
package org.apache.pinot.query.runtime.operator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests [LookupJoinOperator#compileKeyPlan], which decides where each value of the dimension table lookup key comes
/// from.
///
/// The dimension table is a hash map keyed by the primary key values, so a key is only usable when it holds one value
/// per primary key column, in the order the dimension table schema declares them, and with the type of each column.
/// These tests cover the cases that a query alone cannot reach, such as a null constant and a constant of the wrong
/// type. End-to-end coverage is in `LookupJoin.json`.
public class LookupJoinOperatorTest {
  private static final String TABLE_NAME = "dim_tbl_OFFLINE";

  /// Fact table columns. The dimension table columns of the joined row start after these.
  private static final int FACT_CURRENCY = 0;
  private static final int FACT_RATE_START_DATE = 1;
  private static final int LEFT_COLUMN_SIZE = 2;

  /// Dimension table columns, in the order that the leaf stage reports them. The order is not the primary key order,
  /// which is what makes the key positions worth testing.
  private static final int DIM_CURRENCY = 0;
  private static final int DIM_RATE = 1;
  private static final int DIM_RATE_START_DATE = 2;
  private static final DataSchema DIM_SCHEMA =
      new DataSchema(new String[]{"currency", "rate", "rate_start_date"}, new ColumnDataType[]{
          ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG
      });
  private static final List<String> PRIMARY_KEY_COLUMNS = List.of("currency", "rate_start_date");

  /// Key positions follow the primary key, not the order of the join condition.
  private static final int KEY_CURRENCY = 0;
  private static final int KEY_RATE_START_DATE = 1;

  @Test
  public void testKeyPositionsFollowPrimaryKeyOrderNotConditionOrder() {
    // ON dim.rate_start_date = fact.rate_start_date AND dim.currency = fact.currency
    LookupJoinOperator.KeyPlan keyPlan = compileKeyPlan(
        List.of(joinKey(FACT_RATE_START_DATE, DIM_RATE_START_DATE), joinKey(FACT_CURRENCY, DIM_CURRENCY)), List.of());

    assertEquals(keyPlan._sources[KEY_CURRENCY], FACT_CURRENCY);
    assertEquals(keyPlan._sources[KEY_RATE_START_DATE], FACT_RATE_START_DATE);
    assertFalse(keyPlan._neverMatches);
  }

  @Test
  public void testConstantBindsPrimaryKeyColumn() {
    // ON dim.currency = 'gbp' AND dim.rate_start_date = fact.rate_start_date
    LookupJoinOperator.KeyPlan keyPlan =
        compileKeyPlan(List.of(joinKey(FACT_RATE_START_DATE, DIM_RATE_START_DATE)),
            List.of(dimEqualsConstant(DIM_CURRENCY, ColumnDataType.STRING, "gbp")));

    assertEquals(keyPlan._sources[KEY_RATE_START_DATE], FACT_RATE_START_DATE);
    assertEquals(keyPlan._constants[KEY_CURRENCY], "gbp");
    assertFalse(keyPlan._neverMatches);
  }

  @Test
  public void testConstantDoesNotReplaceJoinKey() {
    // ON dim.currency = fact.currency AND dim.rate_start_date = fact.rate_start_date AND dim.currency = 'gbp'
    // The join key is not kept anywhere else, so replacing it would silently widen the join. The constant stays a
    // filter that runs after the lookup.
    LookupJoinOperator.KeyPlan keyPlan = compileKeyPlan(
        List.of(joinKey(FACT_CURRENCY, DIM_CURRENCY), joinKey(FACT_RATE_START_DATE, DIM_RATE_START_DATE)),
        List.of(dimEqualsConstant(DIM_CURRENCY, ColumnDataType.STRING, "gbp")));

    assertEquals(keyPlan._sources[KEY_CURRENCY], FACT_CURRENCY);
    assertEquals(keyPlan._sources[KEY_RATE_START_DATE], FACT_RATE_START_DATE);
    assertNull(keyPlan._constants[KEY_CURRENCY]);
  }

  @Test
  public void testConstantOfTheColumnTypeIsAccepted() {
    // ON dim.currency = fact.currency AND dim.rate_start_date = 1
    // The planner coerces the operands of a comparison, so the constant already carries the type of the column.
    LookupJoinOperator.KeyPlan keyPlan = compileKeyPlan(List.of(joinKey(FACT_CURRENCY, DIM_CURRENCY)),
        List.of(dimEqualsConstant(DIM_RATE_START_DATE, ColumnDataType.LONG, 1L)));

    assertEquals(keyPlan._constants[KEY_RATE_START_DATE], 1L);
  }

  @Test
  public void testConstantOfAnotherTypeIsRejected() {
    // PrimaryKey compares values with equals, where an Integer never equals a Long, so a constant of the wrong type
    // misses every row. The planner is expected to have coerced it already.
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> compileKeyPlan(List.of(joinKey(FACT_CURRENCY, DIM_CURRENCY)),
            List.of(dimEqualsConstant(DIM_RATE_START_DATE, ColumnDataType.INT, 1))));
    assertTrue(exception.getMessage().contains("got a constant of stored type: INT"), exception.getMessage());
  }

  @Test
  public void testBigDecimalConstantIsAccepted() {
    // BigDecimal#equals compares the scale, so 1.5 does not match a stored 1.50. A hash join carries the same hazard,
    // so the lookup join accepts the constant rather than singling out one arm of it.
    LookupJoinOperator.KeyPlan keyPlan = compileKeyPlan(DECIMAL_DIM_SCHEMA, List.of("amount"), List.of(),
        List.of(dimEqualsConstant(0, ColumnDataType.BIG_DECIMAL, new BigDecimal("1.5"))));

    assertEquals(keyPlan._constants[0], new BigDecimal("1.5"));
  }

  @Test
  public void testBytesConstantIsRejected() {
    // A dimension table keys on a raw byte[], whose equals and hashCode are identity, so no constant can match it.
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> compileKeyPlan(BYTES_DIM_SCHEMA, List.of("id"), List.of(),
            List.of(dimEqualsConstant(0, ColumnDataType.BYTES, new ByteArray(new byte[]{1, 2})))));
    assertTrue(exception.getMessage().contains("of type BYTES"), exception.getMessage());
  }

  @Test
  public void testNullConstantMakesEveryLookupMiss() {
    // A null never matches a primary key value, so the operator must not run the lookup at all.
    LookupJoinOperator.KeyPlan keyPlan =
        compileKeyPlan(List.of(joinKey(FACT_RATE_START_DATE, DIM_RATE_START_DATE)),
            List.of(dimEqualsConstant(DIM_CURRENCY, ColumnDataType.STRING, null)));

    assertTrue(keyPlan._neverMatches);
  }

  @Test
  public void testSetPredicateDoesNotBindPrimaryKeyColumn() {
    // dim.currency IN ('gbp', 'usd') reaches the operator as a disjunction. A hash lookup cannot read a set of keys,
    // so the primary key column stays open and the join is rejected.
    RexExpression inList = new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.OR.name(),
        List.of(dimEqualsConstant(DIM_CURRENCY, ColumnDataType.STRING, "gbp"),
            dimEqualsConstant(DIM_CURRENCY, ColumnDataType.STRING, "usd")));

    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> compileKeyPlan(List.of(joinKey(FACT_RATE_START_DATE, DIM_RATE_START_DATE)), List.of(inList)));
    assertTrue(exception.getMessage().contains("cannot determine primary key columns: [currency]"),
        exception.getMessage());
  }

  @Test
  public void testOpenPrimaryKeyColumnIsRejected() {
    // ON dim.rate_start_date = fact.rate_start_date only. Nothing gives a value for currency.
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> compileKeyPlan(List.of(joinKey(FACT_RATE_START_DATE, DIM_RATE_START_DATE)), List.of()));
    assertTrue(exception.getMessage().contains("cannot determine primary key columns: [currency]"),
        exception.getMessage());
  }

  @Test
  public void testJoinKeyOnNonPrimaryKeyColumnIsRejected() {
    // The condition on "rate" is a join key, so it is not in the non-equi conditions and no filter applies it.
    // Dropping it would return rows that do not match the join condition.
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> compileKeyPlan(List.of(joinKey(FACT_CURRENCY, DIM_CURRENCY),
            joinKey(FACT_RATE_START_DATE, DIM_RATE_START_DATE), joinKey(FACT_RATE_START_DATE, DIM_RATE)), List.of()));
    assertTrue(exception.getMessage().contains("join key on column: rate, which is not a primary key column"),
        exception.getMessage());
  }

  @Test
  public void testDuplicateJoinKeysOnSamePrimaryKeyColumnAreRejected() {
    // Only one of the two conditions on currency can build the key, and the other one has nowhere to run.
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> compileKeyPlan(List.of(joinKey(FACT_CURRENCY, DIM_CURRENCY), joinKey(FACT_RATE_START_DATE, DIM_CURRENCY),
            joinKey(FACT_RATE_START_DATE, DIM_RATE_START_DATE)), List.of()));
    assertTrue(exception.getMessage().contains("multiple join keys on primary key column: currency"),
        exception.getMessage());
  }

  @Test
  public void testMissingPrimaryKeyColumnsAreRejected() {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> compileKeyPlan(DIM_SCHEMA, List.of(), List.of(joinKey(FACT_RATE_START_DATE, DIM_RATE_START_DATE)),
            List.of()));
    assertTrue(exception.getMessage().contains("Failed to find primary key columns"), exception.getMessage());
  }

  private static final DataSchema DECIMAL_DIM_SCHEMA =
      new DataSchema(new String[]{"amount"}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL});
  private static final DataSchema BYTES_DIM_SCHEMA =
      new DataSchema(new String[]{"id"}, new ColumnDataType[]{ColumnDataType.BYTES});

  /// A join key, as the pair of column ids that Calcite splits a `fact_column = dim_column` condition into.
  private static Pair<Integer, Integer> joinKey(int factColumnId, int dimColumnId) {
    return Pair.of(factColumnId, dimColumnId);
  }

  private static LookupJoinOperator.KeyPlan compileKeyPlan(List<Pair<Integer, Integer>> joinKeys,
      List<RexExpression> nonEquiConditions) {
    return compileKeyPlan(DIM_SCHEMA, PRIMARY_KEY_COLUMNS, joinKeys, nonEquiConditions);
  }

  private static LookupJoinOperator.KeyPlan compileKeyPlan(DataSchema dimSchema, List<String> primaryKeyColumns,
      List<Pair<Integer, Integer>> joinKeys, List<RexExpression> nonEquiConditions) {
    List<Integer> leftKeys = new ArrayList<>(joinKeys.size());
    List<Integer> rightKeys = new ArrayList<>(joinKeys.size());
    for (Pair<Integer, Integer> joinKey : joinKeys) {
      leftKeys.add(joinKey.getLeft());
      rightKeys.add(joinKey.getRight());
    }
    JoinNode node =
        new JoinNode(0, dimSchema, PlanNode.NodeHint.EMPTY, List.of(), JoinRelType.INNER, leftKeys, rightKeys,
            nonEquiConditions, JoinNode.JoinStrategy.LOOKUP);
    return LookupJoinOperator.compileKeyPlan(node, TABLE_NAME, primaryKeyColumns, dimSchema, LEFT_COLUMN_SIZE);
  }

  /// Builds a `dim_column = constant` condition. Non-equi conditions index the joined row, so the dimension table
  /// columns start at [#LEFT_COLUMN_SIZE].
  private static RexExpression dimEqualsConstant(int dimColumnId, ColumnDataType dataType, @Nullable Object value) {
    return new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.EQUALS.name(),
        List.of(new RexExpression.InputRef(LEFT_COLUMN_SIZE + dimColumnId),
            new RexExpression.Literal(dataType, value)));
  }
}
