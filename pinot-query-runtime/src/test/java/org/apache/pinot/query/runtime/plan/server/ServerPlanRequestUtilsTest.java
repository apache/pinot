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
package org.apache.pinot.query.runtime.plan.server;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.planner.plannode.RuntimeFilterNode;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.sql.FilterKind;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for the probe-side runtime-filter construction in {@link ServerPlanRequestUtils}: the
 * tiering decision (exact IN vs bloom), empty/multi-key handling, type fallbacks, and that an existing
 * leaf filter is preserved. These exercise the no-false-negative reducer in isolation.
 */
public class ServerPlanRequestUtilsTest {

  private static PinotQuery queryWithProbeColumns(String... columns) {
    PinotQuery pinotQuery = new PinotQuery();
    List<Expression> selectList = new ArrayList<>();
    for (String column : columns) {
      selectList.add(RequestUtils.getIdentifierExpression(column));
    }
    pinotQuery.setSelectList(selectList);
    return pinotQuery;
  }

  private static List<Object[]> intRows(int... values) {
    List<Object[]> rows = new ArrayList<>();
    for (int value : values) {
      rows.add(new Object[]{value});
    }
    return rows;
  }

  /** Wraps each scalar build-key value as a single-column build row. */
  private static List<Object[]> rowsOf(Object... values) {
    List<Object[]> rows = new ArrayList<>();
    for (Object value : values) {
      rows.add(new Object[]{value});
    }
    return rows;
  }

  /** Returns the first function (DFS) whose operator equals {@code operator}, or null. */
  private static Function findFunction(Expression expression, String operator) {
    if (expression == null || !expression.isSetFunctionCall()) {
      return null;
    }
    Function function = expression.getFunctionCall();
    if (function.getOperator().equalsIgnoreCase(operator)) {
      return function;
    }
    for (Expression operand : function.getOperands()) {
      Function found = findFunction(operand, operator);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  @Test
  public void testExactInSingleKey() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.INT});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), intRows(5, 1, 3), buildSchema,
        RuntimeFilterNode.Type.IN);

    Expression filter = pinotQuery.getFilterExpression();
    assertNotNull(filter);
    Function in = filter.getFunctionCall();
    assertEquals(in.getOperator(), FilterKind.IN.name());
    List<Expression> operands = in.getOperands();
    // First operand is the probe column; the rest are the sorted build keys.
    assertEquals(operands.get(0), RequestUtils.getIdentifierExpression("fk"));
    assertEquals(operands.size(), 4);
    assertEquals(operands.get(1).getLiteral().getIntValue(), 1);
    assertEquals(operands.get(2).getLiteral().getIntValue(), 3);
    assertEquals(operands.get(3).getLiteral().getIntValue(), 5);
  }

  @Test
  public void testAutoSmallUsesExactIn() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.INT});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), intRows(7, 8, 9), buildSchema,
        RuntimeFilterNode.Type.AUTO);
    // Few build-key rows -> AUTO chooses an exact IN (index-accelerated).
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), FilterKind.IN.name());
  }

  @Test
  public void testEmptyBuildYieldsConstantFalse() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.INT});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), new ArrayList<>(), buildSchema,
        RuntimeFilterNode.Type.AUTO);
    // Empty build side -> nothing can match -> constant false predicate prunes the whole probe.
    Expression filter = pinotQuery.getFilterExpression();
    assertNotNull(filter);
    assertTrue(filter.isSetLiteral(), "empty build should produce a constant literal filter");
    assertEquals(filter.getLiteral().getBoolValue(), false);
  }

  @Test
  public void testBloomSingleKeyEmitsInIdSetAndRange() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.INT});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), intRows(10, 20, 30), buildSchema,
        RuntimeFilterNode.Type.BLOOM);

    Expression filter = pinotQuery.getFilterExpression();
    assertNotNull(filter);
    // Bloom predicate: AND(EQUALS(inIdSet(fk, '<base64>'), 1), BETWEEN(fk, 10, 30)).
    assertNotNull(findFunction(filter, "inIdSet"), "bloom path must emit an inIdSet transform");
    Function between = findFunction(filter, FilterKind.BETWEEN.name());
    assertNotNull(between, "bloom path must emit a BETWEEN range predicate for numeric keys");
    assertEquals(between.getOperands().get(1).getLiteral().getIntValue(), 10);
    assertEquals(between.getOperands().get(2).getLiteral().getIntValue(), 30);
  }

  @Test
  public void testBigDecimalBloomFallsBackToExactIn() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL});
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{new java.math.BigDecimal("1.5")});
    rows.add(new Object[]{new java.math.BigDecimal("2.5")});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), rows, buildSchema,
        RuntimeFilterNode.Type.BLOOM);
    // BIG_DECIMAL is unsupported by IdSet -> falls back to an exact IN (no inIdSet).
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), FilterKind.IN.name());
  }

  @Test
  public void testMultiKeyEmitsExactInPerKey() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk1", "fk2");
    DataSchema buildSchema =
        new DataSchema(new String[]{"k1", "k2"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, 100L});
    rows.add(new Object[]{2, 200L});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0, 1), List.of(0, 1), rows, buildSchema,
        RuntimeFilterNode.Type.AUTO);
    // Multi-key -> exact IN per key, ANDed together (no bloom for composite keys).
    Expression filter = pinotQuery.getFilterExpression();
    assertEquals(filter.getFunctionCall().getOperator(), FilterKind.AND.name());
    assertEquals(filter.getFunctionCall().getOperands().size(), 2);
    for (Expression operand : filter.getFunctionCall().getOperands()) {
      assertEquals(operand.getFunctionCall().getOperator(), FilterKind.IN.name());
    }
  }

  @Test
  public void testAutoTiersToBloomAboveThreshold() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.INT});
    // maxInSize = 2, 3 build-key rows -> AUTO crosses the exact-IN threshold and uses a bloom.
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), intRows(1, 2, 3), buildSchema,
        RuntimeFilterNode.Type.AUTO, 2, 0.01, 16 * 1024 * 1024, 1000);
    assertNotNull(findFunction(pinotQuery.getFilterExpression(), "inIdSet"),
        "AUTO above maxInSize build-key rows must switch to a bloom");
  }

  @Test
  public void testAutoBelowThresholdStaysExactIn() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.INT});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), intRows(1, 2, 3), buildSchema,
        RuntimeFilterNode.Type.AUTO, 10, 0.01, 16 * 1024 * 1024, 1000);
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), FilterKind.IN.name());
  }

  @Test
  public void testAutoMultiKeyStaysExactInAboveThreshold() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk1", "fk2");
    DataSchema buildSchema =
        new DataSchema(new String[]{"k1", "k2"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, 10L});
    rows.add(new Object[]{2, 20L});
    rows.add(new Object[]{3, 30L});
    // Even above the threshold, multi-key stays exact-IN per key (the bloom tier is single-key only).
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0, 1), List.of(0, 1), rows, buildSchema,
        RuntimeFilterNode.Type.AUTO, 1, 0.01, 16 * 1024 * 1024, 1000);
    Expression filter = pinotQuery.getFilterExpression();
    assertNull(findFunction(filter, "inIdSet"), "multi-key must not use a bloom");
    assertEquals(filter.getFunctionCall().getOperator(), FilterKind.AND.name());
    for (Expression operand : filter.getFunctionCall().getOperands()) {
      assertEquals(operand.getFunctionCall().getOperator(), FilterKind.IN.name());
    }
  }

  @Test
  public void testBuildOverMaxRowsAbandonsFilter() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.INT});
    // rowCount (3) > maxBuildRows (2): the planner cap was hit, so the key set may be truncated/incomplete
    // -> the filter MUST be abandoned (no false negatives), leaving the query unfiltered.
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), intRows(1, 2, 3), buildSchema,
        RuntimeFilterNode.Type.IN, 10000, 0.01, 16 * 1024 * 1024, 2);
    assertNull(pinotQuery.getFilterExpression(), "a truncated (over-cap) build must abandon the filter");
  }

  @Test
  public void testFloatBloomEmitsRangeWhenNoNaN() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.FLOAT});
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1.5f});
    rows.add(new Object[]{3.5f});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), rows, buildSchema,
        RuntimeFilterNode.Type.BLOOM, 10000, 0.01, 16 * 1024 * 1024, 1000);
    Expression filter = pinotQuery.getFilterExpression();
    assertNotNull(findFunction(filter, "inIdSet"));
    assertNotNull(findFunction(filter, FilterKind.BETWEEN.name()), "finite float bloom must include a range");
  }

  @Test
  public void testFloatNaNBuildKeyOmitsRangePredicate() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.FLOAT});
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1.5f});
    rows.add(new Object[]{Float.NaN});
    rows.add(new Object[]{3.5f});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), rows, buildSchema,
        RuntimeFilterNode.Type.BLOOM, 10000, 0.01, 16 * 1024 * 1024, 1000);
    Expression filter = pinotQuery.getFilterExpression();
    // A NaN build key must keep the bloom but DROP the BETWEEN range — a finite range would exclude probe
    // NaN rows that should match (false negative), and BETWEEN(NaN, NaN) would drop everything.
    assertNotNull(findFunction(filter, "inIdSet"), "bloom must still be emitted with a NaN build key");
    assertNull(findFunction(filter, FilterKind.BETWEEN.name()),
        "a NaN build key must omit the BETWEEN range predicate to avoid false negatives");
  }

  @Test
  public void testNullBuildKeysAreSkipped() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.LONG});
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1L});
    rows.add(new Object[]{null});  // null key must be skipped (cannot match an INNER join)
    rows.add(new Object[]{3L});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), rows, buildSchema,
        RuntimeFilterNode.Type.IN);
    Function in = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(in.getOperator(), FilterKind.IN.name());
    // fk + the two non-null keys (1, 3); the null is dropped, no NPE.
    assertEquals(in.getOperands().size(), 3);
    assertEquals(in.getOperands().get(1).getLiteral().getLongValue(), 1L);
    assertEquals(in.getOperands().get(2).getLiteral().getLongValue(), 3L);
  }

  @Test
  public void testAllNullBuildKeysYieldConstantFalse() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.LONG});
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{null});
    rows.add(new Object[]{null});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), rows, buildSchema,
        RuntimeFilterNode.Type.AUTO);
    Expression filter = pinotQuery.getFilterExpression();
    assertTrue(filter.isSetLiteral(), "an all-null build side cannot match anything");
    assertEquals(filter.getLiteral().getBoolValue(), false);
  }

  @Test
  public void testBloomOverMaxBytesAbandonsFilter() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.INT});
    // maxBytes = 1 forces the serialized bloom over the cap -> the filter is abandoned (no exception,
    // no predicate); the real hash join remains the source of truth.
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), intRows(1, 2, 3), buildSchema,
        RuntimeFilterNode.Type.BLOOM, 10000, 0.01, 1, 1000);
    assertNull(pinotQuery.getFilterExpression(), "a bloom exceeding maxBytes must abandon the filter");
  }

  /** Every storable single join-key type, with a small build set, for the exact-IN footprint cap. */
  @DataProvider(name = "exactInCapTypes")
  public static Object[][] exactInCapTypes() {
    return new Object[][]{
        {ColumnDataType.INT, rowsOf(1, 2, 3)},
        {ColumnDataType.LONG, rowsOf(1L, 2L, 3L)},
        {ColumnDataType.FLOAT, rowsOf(1.0f, 2.0f)},
        {ColumnDataType.DOUBLE, rowsOf(1.0, 2.0)},
        {ColumnDataType.STRING, rowsOf("alpha", "beta")},
        {ColumnDataType.BYTES, rowsOf(new ByteArray(new byte[]{1}), new ByteArray(new byte[]{2}))},
        {ColumnDataType.BIG_DECIMAL, rowsOf(new BigDecimal("1.5"), new BigDecimal("2.5"))}
    };
  }

  @Test(dataProvider = "exactInCapTypes")
  public void testExactInCapHonoredForEachKeyType(ColumnDataType keyType, List<Object[]> buildRows) {
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{keyType});
    // A 1-byte ceiling forces every non-empty build set over the cap; this drives estimateExactInBytes
    // through each per-type branch (STRING/BYTES/BIG_DECIMAL walk every row) and must abandon the filter.
    PinotQuery abandon = queryWithProbeColumns("fk");
    ServerPlanRequestUtils.attachRuntimeFilter(abandon, List.of(0), List.of(0), buildRows, buildSchema,
        RuntimeFilterNode.Type.IN, 10000, 0.01, 1, 1000);
    assertNull(abandon.getFilterExpression(), "exact IN over maxBytes must abandon the filter for " + keyType);
    // A generous ceiling keeps the same exact IN for that type.
    PinotQuery keep = queryWithProbeColumns("fk");
    ServerPlanRequestUtils.attachRuntimeFilter(keep, List.of(0), List.of(0), buildRows, buildSchema,
        RuntimeFilterNode.Type.IN, 10000, 0.01, 1 << 20, 1000);
    assertEquals(keep.getFilterExpression().getFunctionCall().getOperator(), FilterKind.IN.name(),
        "exact IN under maxBytes must be kept for " + keyType);
  }

  @Test
  public void testExactInCapBoundaryIsStrict() {
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.INT});
    // Three INT keys estimate to 3 * (IN_LITERAL_OVERHEAD_BYTES + 4) = 36 bytes. The guard is a strict
    // '>' so a ceiling equal to the footprint keeps the filter and one byte under abandons it.
    PinotQuery atLimit = queryWithProbeColumns("fk");
    ServerPlanRequestUtils.attachRuntimeFilter(atLimit, List.of(0), List.of(0), intRows(1, 2, 3), buildSchema,
        RuntimeFilterNode.Type.IN, 10000, 0.01, 36, 1000);
    assertEquals(atLimit.getFilterExpression().getFunctionCall().getOperator(), FilterKind.IN.name(),
        "footprint == maxBytes must be kept (strict > boundary)");
    PinotQuery overLimit = queryWithProbeColumns("fk");
    ServerPlanRequestUtils.attachRuntimeFilter(overLimit, List.of(0), List.of(0), intRows(1, 2, 3), buildSchema,
        RuntimeFilterNode.Type.IN, 10000, 0.01, 35, 1000);
    assertNull(overLimit.getFilterExpression(), "footprint one byte over maxBytes must abandon");
  }

  @Test
  public void testMultiKeyExactInOverMaxBytesAbandonsFilter() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk1", "fk2");
    DataSchema buildSchema =
        new DataSchema(new String[]{"k1", "k2"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, 10L});
    rows.add(new Object[]{2, 20L});
    // Multi-key always uses exact IN (per key, AND'd); a tiny maxBytes must abandon the whole filter.
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0, 1), List.of(0, 1), rows, buildSchema,
        RuntimeFilterNode.Type.AUTO, 10000, 0.01, 1, 1000);
    assertNull(pinotQuery.getFilterExpression(), "a multi-key exact IN exceeding maxBytes must abandon the filter");
  }

  @Test
  public void testExistingFilterPreserved() {
    PinotQuery pinotQuery = queryWithProbeColumns("fk");
    Expression existing = RequestUtils.getFunctionExpression(FilterKind.GREATER_THAN.name(),
        RequestUtils.getIdentifierExpression("fk"), RequestUtils.getLiteralExpression(0));
    pinotQuery.setFilterExpression(existing);
    DataSchema buildSchema = new DataSchema(new String[]{"k"}, new ColumnDataType[]{ColumnDataType.INT});
    ServerPlanRequestUtils.attachRuntimeFilter(pinotQuery, List.of(0), List.of(0), intRows(1, 2), buildSchema,
        RuntimeFilterNode.Type.IN);
    // The runtime filter is ANDed with the pre-existing WHERE predicate.
    Expression filter = pinotQuery.getFilterExpression();
    assertEquals(filter.getFunctionCall().getOperator(), FilterKind.AND.name());
    assertNotNull(findFunction(filter, FilterKind.GREATER_THAN.name()));
    assertNotNull(findFunction(filter, FilterKind.IN.name()));
  }
}
