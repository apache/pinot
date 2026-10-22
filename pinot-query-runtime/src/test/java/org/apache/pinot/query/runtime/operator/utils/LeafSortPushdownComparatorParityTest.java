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
package org.apache.pinot.query.runtime.operator.utils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.query.parser.CalciteRexExpressionParser;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies that the order in which the single-stage (V1) leaf engine sorts rows for an ORDER BY that was derived from
/// an MSE sender-side collation list (via [CalciteRexExpressionParser#convertOrderByList]) is the same order that the
/// MSE receiver ([SortUtils.SortComparator], as built by `SortedMailboxMergeReceiveOperator`) expects.
///
/// The V1 side is built exactly like the leaf does: collations -> order-by expressions -> [QueryContext] ->
/// [OrderByComparatorFactory]. Rows hold the Java objects that V1 produces per stored type (Integer, Long, Float,
/// Double, BigDecimal, String, ByteArray; BOOLEAN is stored as Integer, TIMESTAMP as Long, UUID as ByteArray).
///
/// Parity is checked pairwise: for every pair of rows, `signum(v1.compare)` must equal
/// `signum(sortComparator.compare)`.
/// This is stronger than "a V1-sorted list is non-decreasing under SortComparator" because it also pins down ties.
///
/// Known behaviors (documented rather than hidden):
/// - With null handling disabled V1 never produces nulls and ignores NULLS FIRST/LAST; rows carry default values, so
///   there is nothing to disagree on. The V1 comparator would NPE if it ever saw a null (see
///   [#testNullHandlingDisabledComparatorRejectsNulls()]).
/// - MV columns cannot be in a V1 selection order-by (BadQueryRequestException) and array values are not
///   [Comparable], so the SortComparator cannot handle them either. A sort on an array column fails with or without
///   the pushdown; only the error changes.
///
/// Thread-safety: stateless, safe to run in parallel.
public class LeafSortPushdownComparatorParityTest {
  private static final String[] DIRECTIONS = {"ASC", "DESC"};
  private static final NullDirection[] NULL_DIRECTIONS = NullDirection.values();

  private static Object[] strictSamples(String type) {
    switch (type) {
      case "INT":
        return new Object[]{Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE};
      case "LONG":
        return new Object[]{Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE};
      case "TIMESTAMP":
        return new Object[]{-1L, 0L, 1_700_000_000_000L, Long.MAX_VALUE};
      case "BOOLEAN":
        return new Object[]{0, 1};
      case "FLOAT":
        return new Object[]{Float.NEGATIVE_INFINITY, -1.5f, -0.0f, 0.0f, Float.MIN_VALUE, 1.5f,
            Float.POSITIVE_INFINITY, Float.NaN};
      case "DOUBLE":
        return new Object[]{Double.NEGATIVE_INFINITY, -1.5, -0.0, 0.0, Double.MIN_VALUE, 1.5,
            Double.POSITIVE_INFINITY, Double.NaN};
      case "BIG_DECIMAL":
        // 1, 1.0 and 1.00 compare equal (BigDecimal#compareTo) in both engines
        return new Object[]{new BigDecimal("-5"), BigDecimal.ZERO, new BigDecimal("1"), new BigDecimal("1.0"),
            new BigDecimal("1.00"), new BigDecimal("10"), new BigDecimal("1E+3")};
      case "STRING":
        // Includes a supplementary code point (UTF-16 surrogate pair sorts before U+FFFF but after 'z' in
        // String#compareTo, while UTF-8 byte order would put it last) and non-ASCII
        return new Object[]{"", "B", "a", "ab", "z", "é", "😀", "￿"};
      case "BYTES":
        // High bytes must compare as unsigned in both engines
        return new Object[]{new ByteArray(new byte[0]), new ByteArray(new byte[]{0}), new ByteArray(new byte[]{0, 0}),
            new ByteArray(new byte[]{1}), new ByteArray(new byte[]{(byte) 0x80}),
            new ByteArray(new byte[]{(byte) 0xFF})};
      case "UUID":
        return new Object[]{uuid(0), uuid(1), uuid(0x7F), uuid(0x80), uuid(0xFF)};
      default:
        throw new IllegalArgumentException(type);
    }
  }

  private static ByteArray uuid(int lastByte) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) lastByte;
    if (lastByte >= 0x80) {
      bytes[0] = (byte) 0x80;
    }
    return new ByteArray(bytes);
  }

  @DataProvider(name = "cases")
  public Object[][] cases() {
    List<Object[]> cases = new ArrayList<>();
    for (String type : new String[]{"INT", "LONG", "TIMESTAMP", "BOOLEAN", "FLOAT", "DOUBLE", "BIG_DECIMAL", "STRING",
        "BYTES", "UUID"}) {
      for (String dir : DIRECTIONS) {
        for (NullDirection nullDirection : NULL_DIRECTIONS) {
          cases.add(new Object[]{type, dir, nullDirection});
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "cases")
  public void testSingleColumnParity(String type, String dir, NullDirection nullDirection) {
    List<RelFieldCollation> collations = List.of(collation(0, dir, nullDirection));
    Object[] samples = strictSamples(type);
    List<Object[]> rows = new ArrayList<>();
    for (Object sample : samples) {
      rows.add(new Object[]{sample});
    }
    rows.add(new Object[]{null});
    rows.add(new Object[]{null});

    Comparator<Object[]> v1 = v1Comparator(collations, 1, true);
    Comparator<Object[]> mse = new SortUtils.SortComparator(List.copyOf(collations), false);
    assertPairwiseParity(rows, v1, mse, type + " " + dir + " " + nullDirection);
    assertSortedByV1IsSortedByMse(rows, v1, mse, type + " " + dir + " " + nullDirection);
  }

  @Test(dataProvider = "cases")
  public void testNullHandlingDisabledParity(String type, String dir, NullDirection nullDirection) {
    // With null handling disabled V1 never sees nulls (they are default values) and ignores NULLS FIRST/LAST
    List<RelFieldCollation> collations = List.of(collation(0, dir, nullDirection));
    List<Object[]> rows = new ArrayList<>();
    for (Object sample : strictSamples(type)) {
      rows.add(new Object[]{sample});
    }
    Comparator<Object[]> v1 = v1Comparator(collations, 1, false);
    Comparator<Object[]> mse = new SortUtils.SortComparator(List.copyOf(collations), false);
    assertPairwiseParity(rows, v1, mse, type + " " + dir + " " + nullDirection + " (null handling off)");
  }

  @Test
  public void testNullHandlingDisabledComparatorRejectsNulls() {
    // Documents that pushing down is only safe with null handling off when the rows really carry no nulls: the V1
    // comparator does not handle nulls in this mode.
    List<RelFieldCollation> collations = List.of(collation(0, "ASC", NullDirection.UNSPECIFIED));
    Comparator<Object[]> v1 = v1Comparator(collations, 1, false);
    assertThrows(NullPointerException.class, () -> v1.compare(new Object[]{null}, new Object[]{1}));
  }

  @Test
  public void testOrderByExpressionTranslation() {
    // UNSPECIFIED/LAST ASC -> plain asc (V1 default NULLS LAST); FIRST ASC -> explicit nullsfirst
    // UNSPECIFIED/FIRST DESC -> plain desc (V1 default NULLS FIRST); LAST DESC -> explicit nullslast
    assertEquals(orderBy("ASC", NullDirection.UNSPECIFIED), "asc(c0)");
    assertEquals(orderBy("ASC", NullDirection.LAST), "asc(c0)");
    assertEquals(orderBy("ASC", NullDirection.FIRST), "nullsfirst(asc(c0))");
    assertEquals(orderBy("DESC", NullDirection.UNSPECIFIED), "desc(c0)");
    assertEquals(orderBy("DESC", NullDirection.FIRST), "desc(c0)");
    assertEquals(orderBy("DESC", NullDirection.LAST), "nullslast(desc(c0))");
  }

  @Test
  public void testMultiColumnParity() {
    Random random = new Random(42);
    String[] types = {"INT", "STRING", "DOUBLE", "BYTES", "BIG_DECIMAL"};
    for (int iteration = 0; iteration < 50; iteration++) {
      int numColumns = 3;
      List<RelFieldCollation> collations = new ArrayList<>();
      Object[][] pools = new Object[numColumns][];
      for (int c = 0; c < numColumns; c++) {
        collations.add(collation(c, DIRECTIONS[random.nextInt(2)], NULL_DIRECTIONS[random.nextInt(3)]));
        pools[c] = strictSamples(types[random.nextInt(types.length)]);
      }
      List<Object[]> rows = new ArrayList<>();
      for (int i = 0; i < 60; i++) {
        Object[] row = new Object[numColumns];
        for (int c = 0; c < numColumns; c++) {
          row[c] = random.nextInt(5) == 0 ? null : pools[c][random.nextInt(pools[c].length)];
        }
        rows.add(row);
      }
      Comparator<Object[]> v1 = v1Comparator(collations, numColumns, true);
      Comparator<Object[]> mse = new SortUtils.SortComparator(List.copyOf(collations), false);
      assertPairwiseParity(rows, v1, mse, "iteration " + iteration + " " + collations);
      assertSortedByV1IsSortedByMse(rows, v1, mse, "iteration " + iteration + " " + collations);
    }
  }

  private static RelFieldCollation collation(int index, String dir, NullDirection nullDirection) {
    return new RelFieldCollation(index, dir.equals("ASC") ? Direction.ASCENDING : Direction.DESCENDING, nullDirection);
  }

  private static PinotQuery pinotQuery(int numColumns) {
    PinotQuery pinotQuery = new PinotQuery();
    DataSource dataSource = new DataSource();
    dataSource.setTableName("t");
    pinotQuery.setDataSource(dataSource);
    List<Expression> selectList = new ArrayList<>();
    for (int i = 0; i < numColumns; i++) {
      selectList.add(RequestUtils.getIdentifierExpression("c" + i));
    }
    pinotQuery.setSelectList(selectList);
    return pinotQuery;
  }

  private static String orderBy(String dir, NullDirection nullDirection) {
    PinotQuery pinotQuery = pinotQuery(1);
    List<Expression> orderByList =
        CalciteRexExpressionParser.convertOrderByList(List.of(collation(0, dir, nullDirection)), pinotQuery);
    return RequestUtils.prettyPrint(orderByList.get(0));
  }

  /// Builds the comparator the leaf would use, going through the same collation -> PinotQuery -> QueryContext path.
  private static Comparator<Object[]> v1Comparator(List<RelFieldCollation> collations, int numColumns,
      boolean nullHandlingEnabled) {
    PinotQuery pinotQuery = pinotQuery(numColumns);
    pinotQuery.setOrderByList(CalciteRexExpressionParser.convertOrderByList(collations, pinotQuery));
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    queryContext.setNullHandlingEnabled(nullHandlingEnabled);
    // Rows are laid out by select-list position; the order-by list is in collation order with no dedup here, so map
    // each order-by expression to its column index.
    List<Integer> columnIndices = new ArrayList<>();
    for (RelFieldCollation collation : collations) {
      columnIndices.add(collation.getFieldIndex());
    }
    Comparator<Object[]> comparator =
        OrderByComparatorFactory.getComparator(queryContext.getOrderByExpressions(), nullHandlingEnabled);
    return (o1, o2) -> {
      // V1 materializes order-by values in order-by-list order at positions [0, n)
      Object[] k1 = new Object[columnIndices.size()];
      Object[] k2 = new Object[columnIndices.size()];
      for (int i = 0; i < k1.length; i++) {
        k1[i] = o1[columnIndices.get(i)];
        k2[i] = o2[columnIndices.get(i)];
      }
      return comparator.compare(k1, k2);
    };
  }

  private static void assertPairwiseParity(List<Object[]> rows, Comparator<Object[]> v1, Comparator<Object[]> mse,
      String description) {
    for (Object[] a : rows) {
      for (Object[] b : rows) {
        assertEquals(Integer.signum(v1.compare(a, b)), Integer.signum(mse.compare(a, b)),
            description + ": " + Arrays.toString(a) + " vs " + Arrays.toString(b));
      }
    }
  }

  private static void assertSortedByV1IsSortedByMse(List<Object[]> rows, Comparator<Object[]> v1,
      Comparator<Object[]> mse, String description) {
    List<Object[]> sorted = new ArrayList<>(rows);
    Collections.shuffle(sorted, new Random(7));
    sorted.sort(v1);
    for (int i = 1; i < sorted.size(); i++) {
      assertTrue(mse.compare(sorted.get(i - 1), sorted.get(i)) <= 0, description + ": out of order at " + i);
    }
  }
}
