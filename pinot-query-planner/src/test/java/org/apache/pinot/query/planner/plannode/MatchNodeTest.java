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
package org.apache.pinot.query.planner.plannode;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;


public class MatchNodeTest {
  private static final DataSchema DATA_SCHEMA = new DataSchema(new String[]{"sym", "startPrice"},
      new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
  private static final List<PatternSymbol> SYMBOLS =
      List.of(new PatternSymbol("A", null), new PatternSymbol("B", null), new PatternSymbol("C", null));

  @Test
  public void testExplain() {
    assertEquals(buildNode(new RowPattern.Symbol(0)).explain(), "MATCH_RECOGNIZE");
  }

  @Test
  public void testPatternStringRendersQuantifiers() {
    // A* A+ A? A{3} A{3,} A{3,5}, and the reluctant form of each.
    assertPattern("A*",
        new RowPattern.Quantifier(new RowPattern.Symbol(0), 0, RowPattern.Quantifier.UNBOUNDED, true));
    assertPattern("A*?",
        new RowPattern.Quantifier(new RowPattern.Symbol(0), 0, RowPattern.Quantifier.UNBOUNDED, false));
    assertPattern("A+",
        new RowPattern.Quantifier(new RowPattern.Symbol(0), 1, RowPattern.Quantifier.UNBOUNDED, true));
    assertPattern("A+?",
        new RowPattern.Quantifier(new RowPattern.Symbol(0), 1, RowPattern.Quantifier.UNBOUNDED, false));
    assertPattern("A?", new RowPattern.Quantifier(new RowPattern.Symbol(0), 0, 1, true));
    assertPattern("A??", new RowPattern.Quantifier(new RowPattern.Symbol(0), 0, 1, false));
    assertPattern("A{3}", new RowPattern.Quantifier(new RowPattern.Symbol(0), 3, 3, true));
    assertPattern("A{3,}",
        new RowPattern.Quantifier(new RowPattern.Symbol(0), 3, RowPattern.Quantifier.UNBOUNDED, true));
    assertPattern("A{3,5}", new RowPattern.Quantifier(new RowPattern.Symbol(0), 3, 5, true));
    assertPattern("A{3,5}?", new RowPattern.Quantifier(new RowPattern.Symbol(0), 3, 5, false));
  }

  @Test
  public void testPatternStringRendersNestedGrouping() {
    // ^ (A B)* (B | C{2}) $ - a quantified concatenation must be parenthesized, an alternation parenthesizes itself.
    RowPattern pattern = new RowPattern.Concat(List.of(
        RowPattern.AnchorStart.INSTANCE,
        new RowPattern.Quantifier(new RowPattern.Concat(List.of(new RowPattern.Symbol(0), new RowPattern.Symbol(1))), 0,
            RowPattern.Quantifier.UNBOUNDED, true),
        new RowPattern.Alternate(List.of(new RowPattern.Symbol(1),
            new RowPattern.Quantifier(new RowPattern.Symbol(2), 2, 2, true))),
        RowPattern.AnchorEnd.INSTANCE));
    assertPattern("^ (A B)* (B | C{2}) $", pattern);
  }

  @Test
  public void testWithInputs() {
    MatchNode node = buildNode(new RowPattern.Symbol(0));
    PlanNode input = new TableScanNode(1, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, new ArrayList<>(), "testTable",
        List.of("sym", "price"));

    MatchNode withInput = (MatchNode) node.withInputs(List.of(input));
    assertEquals(withInput.getInputs(), List.of(input));
    assertSame(withInput.getPattern(), node.getPattern());
    assertEquals(withInput.getPatternSymbols(), node.getPatternSymbols());
    assertEquals(withInput.getMeasures(), node.getMeasures());
    assertEquals(withInput.getAfterMatchSkipMode(), node.getAfterMatchSkipMode());
  }

  @Test
  public void testEqualsAndHashCode() {
    MatchNode node1 = buildNode(new RowPattern.Symbol(0));
    MatchNode node2 = buildNode(new RowPattern.Symbol(0));
    assertEquals(node1, node2);
    assertEquals(node1.hashCode(), node2.hashCode());

    // A different pattern is a different node, even though everything else matches.
    assertNotEquals(node1, buildNode(new RowPattern.Symbol(1)));
    // A greedy and a reluctant quantifier select different matches and must never compare equal.
    assertNotEquals(new RowPattern.Quantifier(new RowPattern.Symbol(0), 1, 2, true),
        new RowPattern.Quantifier(new RowPattern.Symbol(0), 1, 2, false));
    // Alternation is ordered: the leftmost alternative wins, so a reordering is a different pattern.
    assertNotEquals(new RowPattern.Alternate(List.of(new RowPattern.Symbol(0), new RowPattern.Symbol(1))),
        new RowPattern.Alternate(List.of(new RowPattern.Symbol(1), new RowPattern.Symbol(0))));
    // Concatenation and alternation of the same children mean different things.
    assertNotEquals(new RowPattern.Concat(List.of(new RowPattern.Symbol(0), new RowPattern.Symbol(1))),
        new RowPattern.Alternate(List.of(new RowPattern.Symbol(0), new RowPattern.Symbol(1))));
  }

  /// A pattern field reference must never compare equal to the input ref with the same index: they read different
  /// rows, and conflating them is exactly the degradation this class exists to prevent.
  @Test
  public void testPatternFieldRefIsNotAnInputRef() {
    RexExpression.PatternFieldRef patternFieldRef = new RexExpression.PatternFieldRef(1, 0, "A");
    assertNotEquals(patternFieldRef, new RexExpression.InputRef(1));
    assertNotEquals(patternFieldRef, new RexExpression.PatternFieldRef(1, 1, "B"));
    assertEquals(patternFieldRef.withSymbolOrdinal(1), new RexExpression.PatternFieldRef(1, 1, "A"));
  }

  private static void assertPattern(String expected, RowPattern pattern) {
    assertEquals(buildNode(pattern).getPatternString(), expected);
  }

  private static MatchNode buildNode(RowPattern pattern) {
    return new MatchNode(1, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, new ArrayList<>(), SYMBOLS, pattern,
        List.of(new MatchNode.Measure("startPrice", new RexExpression.PatternFieldRef(1, 0, "A"))), List.of(0),
        List.of(new RelFieldCollation(1)), MatchNode.AfterMatchSkipMode.PAST_LAST_ROW, MatchNode.NO_SKIP_TO_SYMBOL,
        MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH);
  }
}
