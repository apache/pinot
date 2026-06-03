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
package org.apache.pinot.sql.parsers;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.GroupingSets;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TTupleProtocol;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;


/**
 * Verifies that {@link CalciteSqlParser} normalizes GROUP BY ROLLUP / CUBE / GROUPING SETS into the union
 * group-by column list plus the per-set participation masks (see {@link GroupingSets}).
 */
public class GroupingSetsParserTest {

  private static List<String> groupByColumns(PinotQuery pinotQuery) {
    List<String> columns = new ArrayList<>();
    for (Expression expression : pinotQuery.getGroupByList()) {
      columns.add(expression.getIdentifier().getName());
    }
    return columns;
  }

  @Test
  public void testOrdinaryGroupByHasNoMasks() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, b, SUM(c) FROM t GROUP BY a, b");
    assertEquals(groupByColumns(query), List.of("a", "b"));
    assertNull(query.getGroupingSetsMasks());
  }

  @Test
  public void testRollup() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, b, SUM(c) FROM t GROUP BY ROLLUP(a, b)");
    assertEquals(groupByColumns(query), List.of("a", "b"));
    // (a,b), (a), () => 0b11, 0b01, 0b00.
    assertEquals(query.getGroupingSetsMasks(),
        List.of(GroupingSets.maskOf(0, 1), GroupingSets.maskOf(0), GroupingSets.maskOf()));
  }

  @Test
  public void testCube() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, b, SUM(c) FROM t GROUP BY CUBE(a, b)");
    assertEquals(groupByColumns(query), List.of("a", "b"));
    // (a,b), (b), (a), () per the power-set enumeration.
    assertEquals(query.getGroupingSetsMasks(), List.of(
        GroupingSets.maskOf(0, 1), GroupingSets.maskOf(1), GroupingSets.maskOf(0), GroupingSets.maskOf()));
  }

  @Test
  public void testExplicitGroupingSets() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT a, b, SUM(c) FROM t GROUP BY GROUPING SETS ((a, b), (a), ())");
    assertEquals(groupByColumns(query), List.of("a", "b"));
    assertEquals(query.getGroupingSetsMasks(),
        List.of(GroupingSets.maskOf(0, 1), GroupingSets.maskOf(0), GroupingSets.maskOf()));
  }

  @Test
  public void testMixedColumnAndRollup() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, b, c, SUM(d) FROM t GROUP BY a, ROLLUP(b, c)");
    assertEquals(groupByColumns(query), List.of("a", "b", "c"));
    // a always present, crossed with ROLLUP(b,c): (a,b,c), (a,b), (a).
    assertEquals(query.getGroupingSetsMasks(),
        List.of(GroupingSets.maskOf(0, 1, 2), GroupingSets.maskOf(0, 1), GroupingSets.maskOf(0)));
  }

  @Test
  public void testGroupingSetsDeduplicatesUnionColumns() {
    // 'a' appears in both the explicit column and the ROLLUP => union stays [a, b].
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, b, SUM(c) FROM t GROUP BY a, ROLLUP(a, b)");
    assertEquals(groupByColumns(query), List.of("a", "b"));
    // a crossed with ROLLUP(a,b): a|ab=ab, a|a=a, a|()=a => (a,b),(a),(a) - duplicates kept.
    assertEquals(query.getGroupingSetsMasks(),
        List.of(GroupingSets.maskOf(0, 1), GroupingSets.maskOf(0), GroupingSets.maskOf(0)));
  }

  @Test
  public void testGrandTotalEmptyGroupingSet() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT SUM(c) FROM t GROUP BY ()");
    assertEquals(query.getGroupByList(), List.of());
    assertEquals(query.getGroupingSetsMasks(), List.of(GroupingSets.maskOf()));
  }

  @Test
  public void testGroupByDistinctDeduplicatesSets() {
    // a crossed with ROLLUP(a) => (a), (a); GROUP BY DISTINCT collapses to a single (a).
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, SUM(c) FROM t GROUP BY DISTINCT a, ROLLUP(a)");
    assertEquals(groupByColumns(query), List.of("a"));
    assertEquals(query.getGroupingSetsMasks(), List.of(GroupingSets.maskOf(0)));
  }

  @Test
  public void testTooManyGroupingSetsRejected() {
    // CUBE over 14 columns => 2^14 = 16384 > MAX_GROUPING_SETS (8192): must fail fast, not OOM.
    StringBuilder columns = new StringBuilder();
    for (int i = 0; i < 14; i++) {
      columns.append(i == 0 ? "" : ", ").append("c").append(i);
    }
    String sql = "SELECT SUM(x) FROM t GROUP BY CUBE(" + columns + ")";
    assertThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(sql));
  }

  @Test
  public void testGroupingSetsMasksSurviveSerialization()
      throws Exception {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, b, SUM(c) FROM t GROUP BY ROLLUP(a, b)");
    // TCompactProtocol is the production wire format; TTupleProtocol exercises the (separate) tuple scheme.
    for (TProtocolFactory factory : List.of(new TCompactProtocol.Factory(), new TTupleProtocol.Factory())) {
      byte[] bytes = new TSerializer(factory).serialize(query);
      PinotQuery roundTripped = new PinotQuery();
      new TDeserializer(factory).deserialize(roundTripped, bytes);
      assertEquals(roundTripped.getGroupingSetsMasks(), query.getGroupingSetsMasks());
      assertEquals(roundTripped.getGroupByList(), query.getGroupByList());
    }
  }

  @Test
  public void testUnsetMasksSurviveSerialization()
      throws Exception {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, SUM(c) FROM t GROUP BY a");
    byte[] bytes = new TSerializer(new TCompactProtocol.Factory()).serialize(query);
    PinotQuery roundTripped = new PinotQuery();
    new TDeserializer(new TCompactProtocol.Factory()).deserialize(roundTripped, bytes);
    assertNull(roundTripped.getGroupingSetsMasks());
  }

  @Test
  public void testGroupingRewrite() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT a, b, GROUPING(a), GROUPING(b), SUM(d) FROM t GROUP BY ROLLUP(a, b)");
    // union [a, b], N=2: GROUPING(a) => grouping($groupingId, N-1-0=1); GROUPING(b) => grouping($groupingId, 0).
    Function groupingA = query.getSelectList().get(2).getFunctionCall();
    assertEquals(groupingA.getOperator(), "grouping");
    assertEquals(groupingA.getOperands().get(0).getIdentifier().getName(), GroupingSets.GROUPING_ID_COLUMN);
    assertEquals(groupingA.getOperands().get(1).getLiteral().getIntValue(), 1);
    Function groupingB = query.getSelectList().get(3).getFunctionCall();
    assertEquals(groupingB.getOperands().get(1).getLiteral().getIntValue(), 0);
  }

  @Test
  public void testGroupingIdRewrite() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT a, b, GROUPING_ID(a, b), SUM(d) FROM t GROUP BY CUBE(a, b)");
    Function groupingId = query.getSelectList().get(2).getFunctionCall();
    assertEquals(groupingId.getOperator(), "groupingid");
    assertEquals(groupingId.getOperands().get(0).getIdentifier().getName(), GroupingSets.GROUPING_ID_COLUMN);
    // shifts for (a, b) with N=2: a => 1, b => 0.
    assertEquals(groupingId.getOperands().get(1).getLiteral().getIntArrayValue(), List.of(1, 0));
  }

  @Test
  public void testGroupingInHavingRewrite() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT a, b, SUM(d) FROM t GROUP BY ROLLUP(a, b) HAVING GROUPING(b) = 0");
    // HAVING GROUPING(b) = 0 => equals(grouping($groupingId, 0), 0).
    Function equals = query.getHavingExpression().getFunctionCall();
    Function grouping = equals.getOperands().get(0).getFunctionCall();
    assertEquals(grouping.getOperator(), "grouping");
    assertEquals(grouping.getOperands().get(0).getIdentifier().getName(), GroupingSets.GROUPING_ID_COLUMN);
  }

  @Test
  public void testGroupingWithoutGroupingSetsRejected() {
    assertThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT a, GROUPING(a) FROM t GROUP BY a"));
  }

  @Test
  public void testGroupingOnNonGroupByColumnRejected() {
    assertThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(
        "SELECT a, GROUPING(b), SUM(d) FROM t GROUP BY ROLLUP(a)"));
  }

  @Test
  public void testPlainGroupByDistinctHasNoMasks() {
    // GROUP BY DISTINCT without ROLLUP/CUBE/GROUPING SETS normalizes to a flat list with no masks.
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, b, SUM(c) FROM t GROUP BY DISTINCT a, b");
    assertEquals(groupByColumns(query), List.of("a", "b"));
    assertNull(query.getGroupingSetsMasks());
  }
}
