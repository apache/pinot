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
package org.apache.pinot.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.type.OpenStructSqlType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// OPEN_STRUCT columns in the multi-stage engine: a key read (`v['n']`) is typed by the key's declared child field
/// spec and falls back to VARCHAR for an undeclared key, matching what the leaf stage returns for it; selecting the
/// whole column is JSON text, as in the single-stage engine; and queries that never touch the column still plan.
public class OpenStructMultiStagePlanningTest extends QueryEnvironmentTestBase {
  private QueryEnvironment _env;

  @BeforeClass
  public void setUpOpenStructTable() {
    Map<String, Schema> schemas = new HashMap<>(TABLE_SCHEMAS);
    Schema schema = getSchemaBuilder("d").build();
    Map<String, FieldSpec> declared = Map.of("n", new DimensionFieldSpec("n", DataType.LONG, true),
        "price", new DimensionFieldSpec("price", DataType.DOUBLE, true),
        "inner", new ComplexFieldSpec("inner", DataType.OPEN_STRUCT, true, Map.of()));
    schema.addField(new ComplexFieldSpec("v", DataType.OPEN_STRUCT, true, declared));
    schemas.put("d", schema);
    // Same column, declaring `n` with the same type and `price` with another.
    Schema other = getSchemaBuilder("d2").build();
    other.addField(new ComplexFieldSpec("v", DataType.OPEN_STRUCT, true,
        Map.of("n", new DimensionFieldSpec("n", DataType.LONG, true),
            "price", new DimensionFieldSpec("price", DataType.STRING, true))));
    schemas.put("d2", other);
    _env = getQueryEnvironment(3, 1, 2, schemas, SERVER1_SEGMENTS, SERVER2_SEGMENTS, PARTITIONED_SEGMENTS_MAP);
  }

  private RelDataType validatedRowType(String sql) {
    try (QueryEnvironment.CompiledQuery query = _env.compile(sql)) {
      return query.getRelRoot().validatedRowType;
    }
  }

  private DispatchableSubPlan planQuery(String sql) {
    try (QueryEnvironment.CompiledQuery query = _env.compile(sql)) {
      return query.planQuery(0).getQueryPlan();
    }
  }

  @Test
  public void testDeclaredKeyIsTypedByItsChildFieldSpec() {
    RelDataType rowType = validatedRowType("SELECT v['n'], v['price'] FROM d");
    assertEquals(rowType.getFieldList().get(0).getType().getSqlTypeName(), SqlTypeName.BIGINT);
    assertEquals(rowType.getFieldList().get(1).getType().getSqlTypeName(), SqlTypeName.DOUBLE);
    assertTrue(rowType.getFieldList().get(0).getType().isNullable());
  }

  @Test
  public void testUndeclaredKeyAndNestedPathAreVarchar() {
    RelDataType rowType = validatedRowType("SELECT v['name'], v['o.z'] FROM d");
    assertEquals(rowType.getFieldList().get(0).getType().getSqlTypeName(), SqlTypeName.VARCHAR);
    assertEquals(rowType.getFieldList().get(1).getType().getSqlTypeName(), SqlTypeName.VARCHAR);
  }

  @Test
  public void testNumericFilterAndAggregationOnDeclaredKeyPlan() {
    RelDataType rowType = validatedRowType("SELECT SUM(v['n']), COUNT(*) FROM d WHERE v['n'] > 50");
    assertEquals(rowType.getFieldList().get(0).getType().getSqlTypeName(), SqlTypeName.BIGINT);
    assertNotNull(planQuery("SELECT SUM(v['n']), COUNT(*) FROM d WHERE v['n'] > 50"));
    assertNotNull(planQuery("SELECT CAST(v['name'] AS BIGINT) FROM d"));
  }

  @Test
  public void testWholeColumnIsJsonText() {
    DispatchableSubPlan plan = planQuery("SELECT col1, v FROM d");
    DataSchema schema = plan.getQueryStageMap().get(0).getPlanFragment().getFragmentRoot().getDataSchema();
    assertEquals(schema.getColumnDataTypes()[1], ColumnDataType.JSON);
  }

  @Test
  public void testComplexDeclaredChildIsVarchar() {
    RelDataType rowType = validatedRowType("SELECT v['inner'] FROM d");
    assertEquals(rowType.getFieldList().get(0).getType().getSqlTypeName(), SqlTypeName.VARCHAR);
  }

  /// A UNION ALL of two OPEN_STRUCT columns stays OPEN_STRUCT: the whole column is still JSON, and a key both sides
  /// declare with the same type keeps it while a key they declare differently is VARCHAR.
  @Test
  public void testUnionAllKeepsOpenStruct() {
    DispatchableSubPlan plan = planQuery("SELECT v FROM d UNION ALL SELECT v FROM d WHERE col3 > 0");
    DataSchema schema = plan.getQueryStageMap().get(0).getPlanFragment().getFragmentRoot().getDataSchema();
    assertEquals(schema.getColumnDataTypes()[0], ColumnDataType.JSON);
    // d2 has no segments, so only validate across the two declarations.
    String union = "(SELECT v FROM d UNION ALL SELECT v FROM d2)";
    assertTrue(validatedRowType("SELECT v FROM " + union).getFieldList().get(0).getType()
        instanceof OpenStructSqlType);
    RelDataType rowType = validatedRowType("SELECT u.v['n'], u.v['price'] FROM " + union + " AS u");
    assertEquals(rowType.getFieldList().get(0).getType().getSqlTypeName(), SqlTypeName.BIGINT);
    assertEquals(rowType.getFieldList().get(1).getType().getSqlTypeName(), SqlTypeName.VARCHAR);
  }

  @Test
  public void testCaseAndCoalesceWithNullKeepJson() {
    for (String sql : List.of("SELECT CASE WHEN col3 > 0 THEN v ELSE NULL END FROM d",
        "SELECT COALESCE(v, NULL) FROM d")) {
      DataSchema schema =
          planQuery(sql).getQueryStageMap().get(0).getPlanFragment().getFragmentRoot().getDataSchema();
      assertEquals(schema.getColumnDataTypes()[0], ColumnDataType.JSON, sql);
    }
  }

  @Test
  public void testNonLiteralKeyIsRejected() {
    Exception e = expectThrows(Exception.class, () -> planQuery("SELECT v[col1] FROM d"));
    assertTrue(e.getMessage().contains("must be a literal"), e.getMessage());
  }

  @Test
  public void testComputedOpenStructKeyLookupIsRejectedDuringPlanning() {
    String computed = "CASE WHEN col3 > 0 THEN v ELSE NULL END";
    for (String sql : List.of("SELECT (" + computed + ")['n'] FROM d",
        "SELECT x['n'] FROM (SELECT " + computed + " AS x FROM d)")) {
      Exception e = expectThrows(Exception.class, () -> planQuery(sql));
      assertTrue(e.getMessage().contains("OPEN_STRUCT key lookup requires a column reference"), e.getMessage());
    }
  }

  /// The leaf receives the planned key type, so it can convert a segment value stored as another type (for example an
  /// undeclared INT key that the plan types VARCHAR) before a pushed-down comparison or sort. The type is the same
  /// for a declared key, whose matching segments keep their index.
  @Test
  public void testPushedKeyReadCarriesThePlannedType() {
    assertEquals(plannedKeyTypes("SELECT col1 FROM d WHERE v['k'] > '2'"), List.of("STRING"));
    assertEquals(plannedKeyTypes("SELECT v['k'] FROM d ORDER BY v['k'] LIMIT 1"), List.of("STRING"));
    assertEquals(plannedKeyTypes("SELECT col1 FROM d WHERE v['n'] > 2"), List.of("LONG"));
  }

  private List<String> plannedKeyTypes(String sql) {
    List<RexExpression> expressions = new ArrayList<>();
    for (DispatchablePlanFragment fragment : planQuery(sql).getQueryStageMap().values()) {
      if (fragment.getTableName() != null) {
        collectExpressions(fragment.getPlanFragment().getFragmentRoot(), expressions);
      }
    }
    List<String> types = new ArrayList<>();
    expressions.forEach(expression -> collectItemTypes(expression, types));
    return types.stream().distinct().toList();
  }

  private static void collectExpressions(PlanNode node, List<RexExpression> expressions) {
    if (node instanceof FilterNode filter) {
      expressions.add(filter.getCondition());
    } else if (node instanceof ProjectNode project) {
      expressions.addAll(project.getProjects());
    }
    for (PlanNode input : node.getInputs()) {
      collectExpressions(input, expressions);
    }
  }

  private static void collectItemTypes(RexExpression expression, List<String> types) {
    if (expression instanceof RexExpression.FunctionCall call) {
      List<RexExpression> operands = call.getFunctionOperands();
      if (call.getFunctionName().equals("ITEM")) {
        assertEquals(operands.size(), 3, "OPEN_STRUCT key lookup must carry its planned type");
        types.add(String.valueOf(((RexExpression.Literal) operands.get(2)).getValue()));
      }
      operands.forEach(operand -> collectItemTypes(operand, types));
    }
  }

  /// The multi-stage engine casts an undeclared VARCHAR key when comparing it to a numeric literal.
  @Test
  public void testUndeclaredKeyComparedToNumberIsCast() {
    String sql = "EXPLAIN PLAN FOR SELECT COUNT(*) FROM d WHERE v['x'] > 5";
    try (QueryEnvironment.CompiledQuery query = _env.compile(sql)) {
      String plan = query.explain(0, null).getExplainPlan();
      assertTrue(plan.contains("CAST(ITEM("), plan);
    }
  }

  /// The physical optimizer converts the table scan's row type through its own converter; the column is JSON there
  /// too, and key reads are typed as with the default planner.
  @Test
  public void testPhysicalOptimizerPlansTheColumnAsJson() {
    String options = "SET usePhysicalOptimizer=true; ";
    DataSchema schema = planQuery(options + "SELECT col1, v FROM d").getQueryStageMap().get(0).getPlanFragment()
        .getFragmentRoot().getDataSchema();
    assertEquals(schema.getColumnDataTypes()[1], ColumnDataType.JSON);
    for (String sql : List.of("SELECT SUM(v['n']), COUNT(*) FROM d WHERE v['n'] > 50", "SELECT v['name'] FROM d",
        "SELECT v['n'], COUNT(*) FROM d GROUP BY v['n']")) {
      assertNotNull(planQuery(options + sql), sql);
    }
  }

  /// `ITEM` on an array (a multi-value column) keeps its standard typing and one-based indexing; only an OPEN_STRUCT
  /// operand takes the declared-key typing.
  @Test
  public void testItemOnNonOpenStructOperandsIsUnchanged() {
    RelDataType rowType = validatedRowType("SELECT mcol1[1], mcol2[1] FROM e");
    assertEquals(rowType.getFieldList().get(0).getType().getSqlTypeName(), SqlTypeName.VARCHAR);
    assertEquals(rowType.getFieldList().get(1).getType().getSqlTypeName(), SqlTypeName.BIGINT);
    assertNotNull(planQuery("SELECT mcol1[1], mcol2[1] FROM e WHERE mcol2[1] > 5"));
  }

  /// A key is looked up in the operand's own declared keys, so the same key name on another OPEN_STRUCT column with a
  /// different declaration is typed by that column's declaration.
  @Test
  public void testDeclaredKeyTypeFollowsTheColumnItIsReadFrom() {
    RelDataType rowType = validatedRowType("SELECT x.v['price'], u.v['price'] FROM d AS x CROSS JOIN d2 AS u LIMIT 1");
    assertEquals(rowType.getFieldList().get(0).getType().getSqlTypeName(), SqlTypeName.DOUBLE);
    assertEquals(rowType.getFieldList().get(1).getType().getSqlTypeName(), SqlTypeName.VARCHAR);
  }

  @Test
  public void testQueriesThatDoNotReadTheColumnPlan() {
    for (String sql : List.of("SELECT COUNT(*) FROM d", "SELECT SUM(col3) FROM d", "SELECT * FROM d LIMIT 1")) {
      assertNotNull(planQuery(sql), sql);
    }
  }
}
