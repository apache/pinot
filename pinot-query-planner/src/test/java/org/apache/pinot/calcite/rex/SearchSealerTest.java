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
package org.apache.pinot.calcite.rex;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimestampString;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.query.type.TypeFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Unit tests for [SearchSealer] and [PinotSealedSearchOperator] on single `SEARCH` calls. Planning tests are in
/// `SealedInListPlanningTest`.
public class SearchSealerTest {
  private static final TypeFactory TYPE_FACTORY = new TypeFactory();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  enum Shape {
    POINTS, COMPLEMENTED_POINTS, RANGES, POINTS_AND_RANGES
  }

  @DataProvider(name = "searches")
  public Object[][] searches() {
    List<Object[]> cases = new ArrayList<>();
    for (SqlTypeName type : new SqlTypeName[]{
        SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.DOUBLE, SqlTypeName.DECIMAL, SqlTypeName.VARCHAR,
        SqlTypeName.VARBINARY, SqlTypeName.TIMESTAMP, SqlTypeName.BOOLEAN
    }) {
      for (Shape shape : Shape.values()) {
        if (type == SqlTypeName.BOOLEAN && shape != Shape.POINTS && shape != Shape.COMPLEMENTED_POINTS) {
          continue;
        }
        for (RexUnknownAs nullAs : RexUnknownAs.values()) {
          for (boolean nullable : new boolean[]{false, true}) {
            cases.add(new Object[]{type, shape, nullAs, nullable});
          }
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  /// Sealing and un-sealing gives back the same `SEARCH`, and `RexExpressionUtils` converts the sealed call, the
  /// un-sealed call and the original call to the same expression.
  @Test(dataProvider = "searches")
  public void testRoundTrip(SqlTypeName typeName, Shape shape, RexUnknownAs nullAs, boolean nullable) {
    RexCall search = search(typeName, shape, nullAs, nullable);
    SearchSealer sealer = new SearchSealer(1);
    RexNode sealed = sealer.seal(REX_BUILDER, search);
    assertTrue(sealed instanceof RexCall);
    RexCall sealedCall = (RexCall) sealed;
    assertTrue(sealedCall.getOperator() instanceof PinotSealedSearchOperator, sealed.toString());
    assertEquals(sealedCall.getKind(), SqlKind.OTHER_FUNCTION);
    assertEquals(sealedCall.getOperands(), List.of(search.getOperands().get(0)));
    assertEquals(sealed.getType(), search.getType());
    // The digest does not hold the values.
    assertEquals(sealed.toString(), "$SEARCH#0($0)");

    RexNode unsealed = SearchSealer.unsealCall(REX_BUILDER, sealedCall);
    assertEquals(unsealed, search);
    RexExpression expected = RexExpressionUtils.fromRexNode(search);
    assertEquals(RexExpressionUtils.fromRexNode(unsealed), expected);
    assertEquals(RexExpressionUtils.fromRexNode(sealed), expected);
  }

  /// The operator derives the same return type as `SEARCH` and gives the same answers to [Strong].
  @Test(dataProvider = "searches")
  public void testSameTypeAndNullSemanticsAsSearch(SqlTypeName typeName, Shape shape, RexUnknownAs nullAs,
      boolean nullable) {
    RexCall search = search(typeName, shape, nullAs, nullable);
    RexCall sealed = (RexCall) new SearchSealer(1).seal(REX_BUILDER, search);
    // Type derived from the operands, as RexBuilder#makeCall does when a rule rebuilds the call.
    RexNode rebuilt = REX_BUILDER.makeCall(sealed.getOperator(), sealed.getOperands());
    RexNode searchRebuilt = REX_BUILDER.makeCall(SqlStdOperatorTable.SEARCH, search.getOperands());
    assertEquals(rebuilt.getType(), searchRebuilt.getType());

    ImmutableBitSet nullColumns = ImmutableBitSet.of(0);
    assertEquals(Strong.isNull(sealed, nullColumns), Strong.isNull(search, nullColumns));
    assertEquals(Strong.isNotTrue(sealed, nullColumns), Strong.isNotTrue(search, nullColumns));
    assertEquals(Strong.isStrong(sealed), Strong.isStrong(search));
    assertTrue(RexUtil.isDeterministic(sealed));
    assertFalse(sealed.getOperator().isDynamicFunction());
    assertTrue(sealed.getOperator().isSafeOperator());
  }

  @Test
  public void testThreshold() {
    RexCall search = search(SqlTypeName.INTEGER, Shape.POINTS, RexUnknownAs.UNKNOWN, true);
    int numRanges = search.getOperands().get(1).accept(new RexVisitorImpl<Integer>(false) {
      @Override
      public Integer visitLiteral(RexLiteral literal) {
        return literal.getValueAs(Sarg.class).rangeSet.asRanges().size();
      }
    });
    assertSame(new SearchSealer(numRanges + 1).seal(REX_BUILDER, search), search);
    assertSame(new SearchSealer(0).seal(REX_BUILDER, search), search);
    assertSame(new SearchSealer(-1).seal(REX_BUILDER, search), search);
    assertNotSame(new SearchSealer(numRanges).seal(REX_BUILDER, search), search);
  }

  /// One query shares one operator per distinct Sarg (so equal lists stay equal), different Sargs get different
  /// operators and names (so the digests differ), and operators of different queries are never equal.
  @Test
  public void testOperatorIdentity() {
    RexCall points = search(SqlTypeName.INTEGER, Shape.POINTS, RexUnknownAs.UNKNOWN, true);
    RexCall other = search(SqlTypeName.INTEGER, Shape.COMPLEMENTED_POINTS, RexUnknownAs.UNKNOWN, true);
    SearchSealer sealer = new SearchSealer(1);
    RexCall sealed1 = (RexCall) sealer.seal(REX_BUILDER, points);
    RexCall sealed2 = (RexCall) sealer.seal(REX_BUILDER, copy(points));
    RexCall sealed3 = (RexCall) sealer.seal(REX_BUILDER, other);
    assertSame(sealed1.getOperator(), sealed2.getOperator());
    assertEquals(sealed1, sealed2);
    assertNotEquals(sealed1.getOperator(), sealed3.getOperator());
    assertNotEquals(sealed1.toString(), sealed3.toString());

    RexCall otherQuery = (RexCall) new SearchSealer(1).seal(REX_BUILDER, points);
    assertEquals(otherQuery.getOperator().getName(), sealed1.getOperator().getName());
    assertNotEquals(otherQuery.getOperator(), sealed1.getOperator());
    assertNotEquals(otherQuery, sealed1);
  }

  /// When the operand becomes a literal (for example after a filter is pushed through a project of constants),
  /// simplification keeps the call and `RexExpressionUtils` evaluates it like `SEARCH(literal, sarg)`.
  @Test
  public void testLiteralOperand() {
    RexCall search = search(SqlTypeName.INTEGER, Shape.POINTS, RexUnknownAs.UNKNOWN, true);
    RexCall sealed = (RexCall) new SearchSealer(1).seal(REX_BUILDER, search);
    for (RexLiteral literal : new RexLiteral[]{
        REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(3)), REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(4)),
        REX_BUILDER.makeNullLiteral(search.getOperands().get(0).getType())
    }) {
      RexCall withLiteral = sealed.clone(sealed.getType(), List.of(literal));
      RexNode simplified = new RexSimplify(REX_BUILDER, RelOptPredicateList.EMPTY, RexUtil.EXECUTOR)
          .simplifyUnknownAs(withLiteral, RexUnknownAs.UNKNOWN);
      RexCall searchWithLiteral = search.clone(search.getType(), List.of(literal, search.getOperands().get(1)));
      if (literal.isNull()) {
        // Like SEARCH with NULL AS UNKNOWN, the call is null when its operand is null.
        assertTrue(RexUtil.isNullLiteral(simplified, true), simplified.toString());
      } else {
        assertSame(simplified, withLiteral);
      }
      assertEquals(RexExpressionUtils.fromRexNode(withLiteral), RexExpressionUtils.fromRexNode(searchWithLiteral));
    }
  }

  private static RexCall copy(RexCall search) {
    RexLiteral literal = (RexLiteral) search.getOperands().get(1);
    return (RexCall) REX_BUILDER.makeCall(SqlStdOperatorTable.SEARCH, search.getOperands().get(0),
        REX_BUILDER.makeSearchArgumentLiteral(literal.getValueAs(Sarg.class), literal.getType()));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static RexCall search(SqlTypeName typeName, Shape shape, RexUnknownAs nullAs, boolean nullable) {
    List<RexLiteral> literals = literals(typeName);
    RelDataType literalType = literals.get(0).getType();
    RelDataType operandType =
        TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(typeName), nullable);
    RexNode operand = REX_BUILDER.makeInputRef(operandType, 0);
    RangeSet rangeSet = TreeRangeSet.create();
    List<Comparable> values = new ArrayList<>();
    for (RexLiteral literal : literals) {
      values.add(literal.getValueAs(Comparable.class));
    }
    switch (shape) {
      case POINTS:
        values.forEach(v -> rangeSet.add(Range.singleton(v)));
        break;
      case COMPLEMENTED_POINTS:
        values.forEach(v -> rangeSet.add(Range.singleton(v)));
        break;
      case RANGES:
        for (int i = 0; i + 1 < values.size(); i += 2) {
          rangeSet.add(Range.open(values.get(i), values.get(i + 1)));
        }
        break;
      case POINTS_AND_RANGES:
        rangeSet.add(Range.singleton(values.get(0)));
        rangeSet.add(Range.closedOpen(values.get(1), values.get(2)));
        rangeSet.add(Range.singleton(values.get(3)));
        rangeSet.add(Range.greaterThan(values.get(values.size() - 1)));
        break;
      default:
        throw new IllegalStateException();
    }
    RangeSet finalRangeSet = shape == Shape.COMPLEMENTED_POINTS ? rangeSet.complement() : rangeSet;
    Sarg sarg = Sarg.of(nullAs, ImmutableRangeSet.copyOf(finalRangeSet));
    return (RexCall) REX_BUILDER.makeCall(SqlStdOperatorTable.SEARCH, operand,
        REX_BUILDER.makeSearchArgumentLiteral(sarg, literalType));
  }

  private static List<RexLiteral> literals(SqlTypeName typeName) {
    List<RexLiteral> literals = new ArrayList<>();
    switch (typeName) {
      case BOOLEAN:
        literals.add(REX_BUILDER.makeLiteral(false));
        literals.add(REX_BUILDER.makeLiteral(true));
        return literals;
      default:
        break;
    }
    for (int i = 1; i <= 6; i++) {
      switch (typeName) {
        case INTEGER:
          literals.add(REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(i * 3),
              TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
          break;
        case BIGINT:
          literals.add(REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(10_000_000_000L + i),
              TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)));
          break;
        case DOUBLE:
          literals.add(REX_BUILDER.makeApproxLiteral(BigDecimal.valueOf(i + 0.5),
              TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE)));
          break;
        case DECIMAL:
          literals.add(REX_BUILDER.makeExactLiteral(new BigDecimal(i + ".25")));
          break;
        case VARCHAR:
          literals.add(REX_BUILDER.makeLiteral("value-" + i));
          break;
        case VARBINARY:
          literals.add(REX_BUILDER.makeBinaryLiteral(new ByteString(new byte[]{(byte) i, 0x7f})));
          break;
        case TIMESTAMP:
          long millis = 1_700_000_000_000L + i * 60_000L;
          literals.add(REX_BUILDER.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(millis), 1));
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return literals;
  }
}
