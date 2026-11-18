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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
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
    assertNotNull(_env.planQuery("SELECT SUM(v['n']), COUNT(*) FROM d WHERE v['n'] > 50"));
    assertNotNull(_env.planQuery("SELECT CAST(v['name'] AS BIGINT) FROM d"));
  }

  @Test
  public void testWholeColumnIsJsonText() {
    DispatchableSubPlan plan = _env.planQuery("SELECT col1, v FROM d");
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
    DispatchableSubPlan plan = _env.planQuery("SELECT v FROM d UNION ALL SELECT v FROM d WHERE col3 > 0");
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
          _env.planQuery(sql).getQueryStageMap().get(0).getPlanFragment().getFragmentRoot().getDataSchema();
      assertEquals(schema.getColumnDataTypes()[0], ColumnDataType.JSON, sql);
    }
  }

  @Test
  public void testNonLiteralKeyIsRejected() {
    Exception e = expectThrows(Exception.class, () -> _env.planQuery("SELECT v[col1] FROM d"));
    assertTrue(e.getMessage().contains("must be a literal"), e.getMessage());
  }

  /// Unlike the single-stage engine, which compares an undeclared (STRING) key to a number as text, the multi-stage
  /// engine applies SQL coercion: the key is cast to the literal's type and compared numerically.
  @Test
  public void testUndeclaredKeyComparedToNumberIsCast() {
    String plan = _env.explainQuery("EXPLAIN PLAN FOR SELECT COUNT(*) FROM d WHERE v['x'] > 5", 0L);
    assertTrue(plan.contains("CAST(ITEM("), plan);
  }

  @Test
  public void testQueriesThatDoNotReadTheColumnPlan() {
    for (String sql : List.of("SELECT COUNT(*) FROM d", "SELECT SUM(col3) FROM d", "SELECT * FROM d LIMIT 1")) {
      assertNotNull(_env.planQuery(sql), sql);
    }
  }
}
