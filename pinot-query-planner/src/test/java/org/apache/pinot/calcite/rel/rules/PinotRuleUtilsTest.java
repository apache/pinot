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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests [PinotRuleUtils#isRelocatable], which decides whether an expression may be moved to a different position in
/// the plan. It must reject all three variability axes: `isDeterministic = false`, Calcite's `isDynamicFunction()`,
/// and Pinot's `FunctionVolatility.VOLATILE`.
public class PinotRuleUtilsTest {

  private final RelDataTypeFactory _typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder _rexBuilder = new RexBuilder(_typeFactory);

  private PinotSqlFunction registryFunction(String name) {
    PinotScalarFunction scalarFunction = FunctionRegistry.getFunctions().get(FunctionRegistry.canonicalize(name));
    assertNotNull(scalarFunction, "Failed to find function: " + name);
    PinotSqlFunction sqlFunction = scalarFunction.toPinotSqlFunction();
    assertNotNull(sqlFunction, "Function is not registered as a PinotSqlFunction: " + name);
    return sqlFunction;
  }

  /// Resolves the operator a query would actually bind to, which is not always the [FunctionRegistry] entry --
  /// `PinotOperatorTable#registerScalarFunctions` skips names already present in its hard-coded list.
  private SqlOperator resolvedOperator(String name) {
    List<SqlOperator> matches = new ArrayList<>();
    PinotOperatorTable.instance(false).lookupOperatorOverloads(new SqlIdentifier(name, SqlParserPos.ZERO),
        SqlFunctionCategory.USER_DEFINED_FUNCTION, SqlSyntax.FUNCTION, matches, null);
    assertFalse(matches.isEmpty(), "Failed to resolve operator: " + name);
    return matches.get(0);
  }

  private RexNode call(SqlOperator operator, RexNode... operands) {
    RelDataType returnType = _typeFactory.createSqlType(SqlTypeName.BIGINT);
    return _rexBuilder.makeCall(returnType, operator, List.of(operands));
  }

  private RexNode literal(int value) {
    return _rexBuilder.makeLiteral(value, _typeFactory.createSqlType(SqlTypeName.INTEGER));
  }

  @Test
  public void testLiteralAndInputRefAreRelocatable() {
    assertTrue(PinotRuleUtils.isRelocatable(literal(1)));
    assertTrue(PinotRuleUtils.isRelocatable(
        _rexBuilder.makeInputRef(_typeFactory.createSqlType(SqlTypeName.INTEGER), 0)));
  }

  @Test
  public void testDeterministicCallIsRelocatable() {
    assertTrue(PinotRuleUtils.isRelocatable(call(SqlStdOperatorTable.PLUS, literal(1), literal(2))));
  }

  @Test
  public void testImmutableFunctionIsRelocatable() {
    PinotSqlFunction upper = registryFunction("upper");
    assertTrue(upper.isDeterministic());
    assertFalse(upper.isVolatile());
    assertTrue(PinotRuleUtils.isRelocatable(call(upper, _rexBuilder.makeLiteral("x"))));
  }

  @Test
  public void testNonDeterministicFunctionIsNotRelocatable() {
    // rand() is @ScalarFunction(isDeterministic = false); the operator-level flag is shared with the seeded overload.
    PinotSqlFunction rand = registryFunction("rand");
    assertFalse(rand.isDeterministic());
    assertFalse(PinotRuleUtils.isRelocatable(call(rand)));
  }

  @Test
  public void testVolatileFunctionIsNotRelocatable() {
    // stageId() stays deterministic so it can be constant-folded, but is VOLATILE, so it must not be relocated.
    PinotSqlFunction stageId = registryFunction("stageId");
    assertTrue(stageId.isDeterministic(), "stageId should stay deterministic for compile-time evaluation");
    assertTrue(stageId.isVolatile());
    assertFalse(PinotRuleUtils.isRelocatable(call(stageId, literal(0))));
  }

  /// `FunctionVolatility.STABLE` is constant within a single query, so it is safe to relocate. `reqId` gets STABLE
  /// from the class-level annotation on `InternalFunctions`, which also covers annotation inheritance.
  @Test
  public void testStableFunctionIsRelocatable() {
    PinotSqlFunction reqId = registryFunction("reqId");
    assertTrue(reqId.isDeterministic());
    assertFalse(reqId.isVolatile(), "STABLE must not be reported as volatile");
    assertTrue(PinotRuleUtils.isRelocatable(call(reqId, literal(0))));
  }

  /// Calcite's own dynamic functions carry the same "fold once, never re-evaluate" contract via a different flag.
  @Test
  public void testCalciteDynamicFunctionIsNotRelocatable() {
    assertTrue(SqlStdOperatorTable.CURRENT_TIMESTAMP.isDynamicFunction());
    assertTrue(SqlStdOperatorTable.CURRENT_TIMESTAMP.isDeterministic(),
        "guarding on isDeterministic alone would miss this");
    assertFalse(PinotRuleUtils.isRelocatable(call(SqlStdOperatorTable.CURRENT_TIMESTAMP)));
  }

  /// Both the registry entry and the hard-coded [PinotOperatorTable] entry that shadows it must agree that `now()` is
  /// volatile, otherwise the guard's answer depends on which one a query happens to bind to.
  @Test
  public void testNowIsVolatileOnBothRegistrations() {
    PinotSqlFunction registryNow = registryFunction("now");
    assertTrue(registryNow.isDeterministic(), "now() must stay deterministic so it is folded once at plan time");
    assertTrue(registryNow.isVolatile());
    assertFalse(PinotRuleUtils.isRelocatable(call(registryNow)));

    SqlOperator resolvedNow = resolvedOperator("NOW");
    assertTrue(resolvedNow instanceof PinotSqlFunction, "expected a PinotSqlFunction, got: " + resolvedNow.getClass());
    assertTrue(((PinotSqlFunction) resolvedNow).isVolatile(),
        "the operator NOW() actually binds to must also report volatile");
    assertFalse(PinotRuleUtils.isRelocatable(call(resolvedNow)));
  }

  @Test
  public void testNestedVolatileOperandIsDetected() {
    // The visitor must recurse into operands, not just inspect the top-level operator.
    RexNode nested = call(SqlStdOperatorTable.PLUS, literal(1), call(registryFunction("stageId"), literal(0)));
    assertFalse(PinotRuleUtils.isRelocatable(nested));
  }
}
