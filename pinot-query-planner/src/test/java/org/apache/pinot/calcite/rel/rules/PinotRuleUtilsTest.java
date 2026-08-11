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

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests [PinotRuleUtils#isStageInvariant], which decides whether an expression may be relocated across a stage
/// boundary. It must reject both of Pinot's variability axes: `isDeterministic = false` and
/// `FunctionVolatility.VOLATILE`.
public class PinotRuleUtilsTest {

  private final RelDataTypeFactory _typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder _rexBuilder = new RexBuilder(_typeFactory);

  private PinotSqlFunction sqlFunction(String name) {
    PinotScalarFunction scalarFunction = FunctionRegistry.getFunctions().get(FunctionRegistry.canonicalize(name));
    assertNotNull(scalarFunction, "Failed to find function: " + name);
    PinotSqlFunction sqlFunction = scalarFunction.toPinotSqlFunction();
    assertNotNull(sqlFunction, "Function is not registered as a PinotSqlFunction: " + name);
    return sqlFunction;
  }

  private RexNode call(SqlOperator operator, RexNode... operands) {
    RelDataType returnType = _typeFactory.createSqlType(SqlTypeName.BIGINT);
    return _rexBuilder.makeCall(returnType, operator, List.of(operands));
  }

  private RexNode literal(int value) {
    return _rexBuilder.makeLiteral(value, _typeFactory.createSqlType(SqlTypeName.INTEGER));
  }

  @Test
  public void testLiteralAndInputRefAreStageInvariant() {
    assertTrue(PinotRuleUtils.isStageInvariant(literal(1)));
    assertTrue(PinotRuleUtils.isStageInvariant(
        _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0)));
  }

  @Test
  public void testDeterministicCallIsStageInvariant() {
    assertTrue(PinotRuleUtils.isStageInvariant(call(SqlStdOperatorTable.PLUS, literal(1), literal(2))));
  }

  @Test
  public void testNonDeterministicFunctionIsNotStageInvariant() {
    // rand() is @ScalarFunction(isDeterministic = false); the operator-level flag is shared with the seeded overload.
    PinotSqlFunction rand = sqlFunction("rand");
    assertFalse(rand.isDeterministic());
    assertFalse(PinotRuleUtils.isStageInvariant(call(rand)));
  }

  @Test
  public void testVolatileFunctionIsNotStageInvariant() {
    // stageId() stays deterministic so it can be constant-folded, but is VOLATILE, so it must not be relocated.
    PinotSqlFunction stageId = sqlFunction("stageId");
    assertTrue(stageId.isDeterministic(), "stageId should stay deterministic for compile-time evaluation");
    assertTrue(stageId.isVolatile());
    assertFalse(PinotRuleUtils.isStageInvariant(call(stageId, literal(0))));
  }

  @Test
  public void testNowIsVolatileButDeterministic() {
    // The registry entry for now() must stay deterministic so PinotEvaluateLiteralRule can fold it once at plan time,
    // while being volatile so it is never re-evaluated at a different point in the plan.
    // Note: SQL NOW() resolves to the hard-coded PinotOperatorTable entry rather than this one (registerScalarFunctions
    // skips names already present), and is niladic so it is always folded before the filter-join rules run. See
    // QueryCompilationTest#testVolatileNowFilterIsStillPushedBelowJoin.
    PinotSqlFunction now = sqlFunction("now");
    assertTrue(now.isDeterministic());
    assertTrue(now.isVolatile());
    assertFalse(PinotRuleUtils.isStageInvariant(call(now)));
  }

  @Test
  public void testNestedVolatileOperandIsDetected() {
    // The visitor must recurse into operands, not just inspect the top-level operator.
    RexNode nested = call(SqlStdOperatorTable.PLUS, literal(1), call(sqlFunction("stageId"), literal(0)));
    assertFalse(PinotRuleUtils.isStageInvariant(nested));
  }

  @Test
  public void testImmutableFunctionIsStageInvariant() {
    PinotSqlFunction upper = sqlFunction("upper");
    assertTrue(upper.isDeterministic());
    assertFalse(upper.isVolatile());
    assertTrue(PinotRuleUtils.isStageInvariant(
        call(upper, _rexBuilder.makeLiteral("x"))));
  }
}
