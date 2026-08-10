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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.testng.annotations.DataProvider;
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
/// per primary key column, in the order the dimension table schema declares them, and with the stored type of each
/// column. These tests cover the cases that a query alone cannot reach, such as a null literal and a literal of the
/// wrong numeric width. End-to-end coverage is in `LookupJoin.json`.
public class LookupJoinOperatorTest {
  private static final String TABLE_NAME = "dim_tbl_OFFLINE";

  /// Dimension table columns, in the order that the leaf stage reports them. The order is not the primary key order,
  /// which is what makes the key positions worth testing.
  private static final String[] RIGHT_COLUMNS = {"currency", "rate", "rate_start_date"};
  private static final DataSchema RIGHT_SCHEMA = new DataSchema(RIGHT_COLUMNS,
      new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG});
  private static final List<String> PRIMARY_KEY_COLUMNS = List.of("currency", "rate_start_date");

  /// The fact table has two columns, so the dimension table columns start at index 2 of the joined row.
  private static final int LEFT_COLUMN_SIZE = 2;

  @Test
  public void testKeyPositionsFollowPrimaryKeyOrderNotConditionOrder() {
    // ON dim.rate_start_date = fact.col1 AND dim.currency = fact.col0
    // The conditions are in reverse primary key order, so the key values must still land in primary key order.
    LookupJoinOperator.KeyPlan keyPlan =
        compileKeyPlan(List.of(1, 0), List.of(2, 0), List.of());

    assertEquals(keyPlan._sources, new int[]{0, 1});
    assertFalse(keyPlan._neverMatches);
  }

  @Test
  public void testLiteralBindsPrimaryKeyColumn() {
    // ON dim.currency = 'gbp' AND dim.rate_start_date = fact.col1
    LookupJoinOperator.KeyPlan keyPlan =
        compileKeyPlan(List.of(1), List.of(2), List.of(eq(rightColumnRef(0), literal(ColumnDataType.STRING,
            "gbp"))));

    assertEquals(keyPlan._sources[1], 1);
    assertEquals(keyPlan._constants[0], "gbp");
    assertFalse(keyPlan._neverMatches);
  }

  @Test
  public void testLiteralDoesNotReplaceEquiJoinKey() {
    // ON dim.currency = fact.col0 AND dim.rate_start_date = fact.col1 AND dim.currency = 'gbp'
    // The equi-join key is not kept anywhere else, so replacing it would silently widen the join. The literal stays a
    // filter that runs after the lookup.
    LookupJoinOperator.KeyPlan keyPlan =
        compileKeyPlan(List.of(0, 1), List.of(0, 2), List.of(eq(rightColumnRef(0), literal(ColumnDataType.STRING,
            "gbp"))));

    assertEquals(keyPlan._sources, new int[]{0, 1});
    assertNull(keyPlan._constants[0]);
  }

  @Test
  public void testLiteralIsConvertedToStoredTypeOfColumn() {
    // ON dim.currency = fact.col0 AND dim.rate_start_date = 1
    // The primary key column is LONG. A key that holds an Integer misses every row, because PrimaryKey compares
    // values with equals.
    LookupJoinOperator.KeyPlan keyPlan =
        compileKeyPlan(List.of(0), List.of(0), List.of(eq(rightColumnRef(2), literal(ColumnDataType.INT, 1))));

    assertEquals(keyPlan._constants[1], 1L);
  }

  @Test
  public void testNullConstantMakesEveryLookupMiss() {
    // A null never matches a primary key value, so the operator must not run the lookup at all.
    LookupJoinOperator.KeyPlan keyPlan =
        compileKeyPlan(List.of(1), List.of(2), List.of(eq(rightColumnRef(0), literal(ColumnDataType.STRING,
            null))));

    assertTrue(keyPlan._neverMatches);
  }

  @Test
  public void testSetPredicateDoesNotBindPrimaryKeyColumn() {
    // dim.currency IN ('gbp', 'usd') reaches the operator as a disjunction. A hash lookup cannot read a set of keys,
    // so the primary key column stays open and the join is rejected.
    RexExpression inList = new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.OR.name(),
        List.of(eq(rightColumnRef(0), literal(ColumnDataType.STRING, "gbp")),
            eq(rightColumnRef(0), literal(ColumnDataType.STRING, "usd"))));

    IllegalStateException exception =
        expectThrows(IllegalStateException.class, () -> compileKeyPlan(List.of(1), List.of(2), List.of(inList)));
    assertTrue(exception.getMessage().contains("cannot determine primary key columns: [currency]"),
        exception.getMessage());
  }

  @Test
  public void testOpenPrimaryKeyColumnIsRejected() {
    // ON dim.rate_start_date = fact.col1 only. Nothing gives a value for currency.
    IllegalStateException exception =
        expectThrows(IllegalStateException.class, () -> compileKeyPlan(List.of(1), List.of(2), List.of()));
    assertTrue(exception.getMessage().contains("cannot determine primary key columns: [currency]"),
        exception.getMessage());
  }

  @Test
  public void testJoinKeyOnNonPrimaryKeyColumnIsRejected() {
    // ON dim.currency = fact.col0 AND dim.rate_start_date = fact.col1 AND dim.rate = fact.col1
    // The condition on "rate" is an equi-join key, so it is not in the non-equi conditions and no filter applies it.
    // Dropping it would return rows that do not match the join condition.
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> compileKeyPlan(List.of(0, 1, 1), List.of(0, 2, 1), List.of()));
    assertTrue(exception.getMessage().contains("join key on column: rate, which is not a primary key column"),
        exception.getMessage());
  }

  @Test
  public void testDuplicateJoinKeysOnSamePrimaryKeyColumnAreRejected() {
    // ON dim.currency = fact.col0 AND dim.currency = fact.col1 AND dim.rate_start_date = fact.col1
    // Only one of the two conditions on currency can build the key, and the other one has nowhere to run.
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> compileKeyPlan(List.of(0, 1, 1), List.of(0, 0, 2), List.of()));
    assertTrue(exception.getMessage().contains("multiple join keys on primary key column: currency"),
        exception.getMessage());
  }

  @Test
  public void testMissingPrimaryKeyColumnsAreRejected() {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> LookupJoinOperator.compileKeyPlan(joinNode(List.of(1), List.of(2), List.of()), TABLE_NAME, List.of(),
            RIGHT_SCHEMA, LEFT_COLUMN_SIZE));
    assertTrue(exception.getMessage().contains("Failed to find primary key columns"), exception.getMessage());
  }

  @DataProvider
  public Object[][] storedTypes() {
    return new Object[][]{
        // A literal of any numeric width converts to the width that the column stores.
        {ColumnDataType.INT, 1L, 1},
        {ColumnDataType.LONG, 1, 1L},
        {ColumnDataType.FLOAT, 1.5d, 1.5f},
        {ColumnDataType.DOUBLE, 1.5f, 1.5d},
        {ColumnDataType.STRING, "gbp", "gbp"},
        // BOOLEAN stores an int and TIMESTAMP stores a long, which is what a literal already holds.
        {ColumnDataType.BOOLEAN, 1, 1},
        {ColumnDataType.TIMESTAMP, 1000L, 1000L}
    };
  }

  @Test(dataProvider = "storedTypes")
  public void testConstantConvertsToStoredType(ColumnDataType columnDataType, Object literal, Object expected) {
    Object stored = LookupJoinOperator.toStoredValue(literal, columnDataType, TABLE_NAME, "col");

    assertEquals(stored, expected);
    assertEquals(stored.getClass(), expected.getClass());
  }

  @Test
  public void testConstantOnUnsupportedStoredTypeIsRejected() {
    // BigDecimal compares its scale, and a BYTES literal is a ByteArray while the dimension table stores byte[]. A
    // constant of either type misses every row, so the operator rejects it.
    for (ColumnDataType columnDataType : List.of(ColumnDataType.BIG_DECIMAL, ColumnDataType.BYTES)) {
      IllegalStateException exception = expectThrows(IllegalStateException.class,
          () -> LookupJoinOperator.toStoredValue(BigDecimal.ONE, columnDataType, TABLE_NAME, "col"));
      assertTrue(exception.getMessage().contains("does not support a constant on primary key column: col"),
          exception.getMessage());
    }
  }

  @Test
  public void testNonNumericConstantOnNumericColumnIsRejected() {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> LookupJoinOperator.toStoredValue("gbp", ColumnDataType.LONG, TABLE_NAME, "col"));
    assertTrue(exception.getMessage().contains("cannot use the constant: gbp"), exception.getMessage());
  }

  private static LookupJoinOperator.KeyPlan compileKeyPlan(List<Integer> leftKeys, List<Integer> rightKeys,
      List<RexExpression> nonEquiConditions) {
    return LookupJoinOperator.compileKeyPlan(joinNode(leftKeys, rightKeys, nonEquiConditions), TABLE_NAME,
        PRIMARY_KEY_COLUMNS, RIGHT_SCHEMA, LEFT_COLUMN_SIZE);
  }

  private static JoinNode joinNode(List<Integer> leftKeys, List<Integer> rightKeys,
      List<RexExpression> nonEquiConditions) {
    return new JoinNode(0, RIGHT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), JoinRelType.INNER, leftKeys, rightKeys,
        nonEquiConditions, JoinNode.JoinStrategy.LOOKUP);
  }

  /// Builds a reference to a dimension table column. Non-equi conditions index the joined row, so the dimension table
  /// columns start at [#LEFT_COLUMN_SIZE].
  private static RexExpression rightColumnRef(int rightColumnId) {
    return new RexExpression.InputRef(LEFT_COLUMN_SIZE + rightColumnId);
  }

  private static RexExpression literal(ColumnDataType dataType, @Nullable Object value) {
    return new RexExpression.Literal(dataType, value);
  }

  private static RexExpression eq(RexExpression left, RexExpression right) {
    return new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.EQUALS.name(), List.of(left, right));
  }
}
