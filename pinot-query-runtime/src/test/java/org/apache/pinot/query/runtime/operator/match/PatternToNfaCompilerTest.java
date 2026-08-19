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
package org.apache.pinot.query.runtime.operator.match;

import java.util.List;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.planner.plannode.RowPattern;
import org.apache.pinot.query.runtime.operator.match.PatternNfa.Transition;
import org.apache.pinot.query.runtime.operator.match.PatternNfa.TransitionKind;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.Test;

import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.INPUT_SCHEMA;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.alternate;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.anySymbol;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.concat;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.labelSymbols;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.quantify;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.rows;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.symbol;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the shape of the compiled automaton and, through [PartitionMatcher], the matching semantics that the
/// transition ordering is supposed to produce.
///
/// Matching is asserted as `(endPosition, classification)` where the classification spells out which pattern
/// variable each matched row was mapped to, so a preference bug shows up as a different string rather than as a
/// different row count only.
public class PatternToNfaCompilerTest {
  private static final long UNLIMITED_STEPS = Long.MAX_VALUE;

  @Test
  public void testGreedyQuantifierPrefersTheLongestMatch() {
    // PATTERN (A*) over AAA: greedy takes all three rows.
    assertMatch(quantify(symbol(0), 0, RowPattern.Quantify.UNBOUNDED, true), labelSymbols("A"), "AAA", "AAA");
  }

  @Test
  public void testReluctantQuantifierPrefersTheShortestMatch() {
    // PATTERN (A*?) over AAA: reluctant leaves the loop immediately, which is a legal empty match.
    assertMatch(quantify(symbol(0), 0, RowPattern.Quantify.UNBOUNDED, false), labelSymbols("A"), "AAA", "");
  }

  @Test
  public void testReluctantQuantifierStillHonoursItsMinimum() {
    // PATTERN (A+?) over AAA: reluctant, but one repetition is mandatory.
    assertMatch(quantify(symbol(0), 1, RowPattern.Quantify.UNBOUNDED, false), labelSymbols("A"), "AAA", "A");
  }

  @Test
  public void testGreedyQuantifierBacktracksWhenTheRestOfThePatternNeedsRows() {
    // PATTERN (A* A) over AAA: the greedy loop must give one row back so the trailing A can match.
    RowPattern pattern =
        concat(quantify(symbol(0), 0, RowPattern.Quantify.UNBOUNDED, true), symbol(1));
    // Two symbols, both matching the label A, so the classification shows where the loop stopped.
    List<PatternSymbol> symbols = List.of(MatchTestFixtures.labelSymbol("A", 0), aliasOfA());
    assertMatch(pattern, symbols, "AAA", "AAX");
  }

  @Test
  public void testAlternationPrefersTheLeftmostBranch() {
    // Both X and Y match the label A, so only the branch order decides.
    List<PatternSymbol> symbols = List.of(labelledAs("X", "A", 0), labelledAs("Y", "A", 1));
    assertMatch(alternate(symbol(0), symbol(1)), symbols, "A", "X");
    assertMatch(alternate(symbol(1), symbol(0)), symbols, "A", "Y");
  }

  @Test
  public void testAlternationFallsThroughToALaterBranch() {
    // PATTERN (A | B) over B: the first branch cannot match, the second one can.
    assertMatch(alternate(symbol(0), symbol(1)), labelSymbols("A", "B"), "B", "B");
  }

  @Test
  public void testAlternationBacktracksOutOfAPreferredBranchThatBlocksTheRestOfThePattern() {
    // PATTERN ((A B | A) B): the leftmost branch matches the first two rows on its own, but then the trailing B has
    // nothing left. Preference must not win over completeness, so the search has to unwind the preferred branch and
    // come back through the second one. A preference bug that forgets to backtrack reports no match at all here.
    RowPattern pattern = concat(alternate(concat(symbol(0), symbol(1)), symbol(0)), symbol(1));
    assertMatch(pattern, labelSymbols("A", "B"), "AB", "AB");
    // With one more B the preferred branch does complete, and it must be the one that wins.
    assertMatch(pattern, labelSymbols("A", "B"), "ABB", "ABB");
  }

  @Test
  public void testReluctantQuantifierStopsAtTheFirstRowThatLetsTheRestOfThePatternMatch() {
    // PATTERN (A*? B) over A A B: reluctant prefers to leave the loop at once, but the trailing B forces it to take
    // exactly as many rows as it must - no more.
    RowPattern pattern = concat(quantify(symbol(0), 0, RowPattern.Quantify.UNBOUNDED, false), symbol(1));
    assertMatch(pattern, labelSymbols("A", "B"), "AAB", "AAB");
  }

  @Test
  public void testGreedyAndReluctantDifferOnlyInWhichMatchTheyPrefer() {
    // PATTERN (A{1,3} A) over AAAA, with a second variable that also matches an A so the split is visible.
    // Greedy gives the loop as many rows as it can while still letting the trailing A match; reluctant gives it the
    // fewest. Both are legal matches of the same pattern, which is exactly why the preference order decides.
    List<PatternSymbol> symbols = List.of(MatchTestFixtures.labelSymbol("A", 0), aliasOfA());
    assertMatch(concat(quantify(symbol(0), 1, 3, true), symbol(1)), symbols, "AAAA", "AAAX");
    assertMatch(concat(quantify(symbol(0), 1, 3, false), symbol(1)), symbols, "AAAA", "AX");
  }

  @Test
  public void testBoundedQuantifierRespectsItsUpperBound() {
    // PATTERN (A{2,3}) over AAAA: greedy stops at the upper bound rather than consuming everything.
    assertMatch(quantify(symbol(0), 2, 3, true), labelSymbols("A"), "AAAA", "AAA");
  }

  @Test
  public void testBoundedQuantifierRespectsItsLowerBound() {
    // PATTERN (A{2,3}) over A: a single row is not enough, so there is no match at all.
    assertNoMatch(quantify(symbol(0), 2, 3, true), labelSymbols("A"), "A");
  }

  @Test
  public void testExactQuantifier() {
    assertMatch(quantify(symbol(0), 2, 2, true), labelSymbols("A"), "AAAA", "AA");
  }

  @Test
  public void testReluctantBoundedQuantifierStopsAtItsLowerBound() {
    assertMatch(quantify(symbol(0), 2, 3, false), labelSymbols("A"), "AAAA", "AA");
  }

  @Test
  public void testBoundedQuantifierUsesCounterRegistersInsteadOfUnrollingStates() {
    // The whole point of counter registers: a huge repetition bound must not blow up the automaton.
    PatternNfa small = PatternToNfaCompiler.compile(quantify(symbol(0), 1, 3, true));
    PatternNfa huge = PatternToNfaCompiler.compile(quantify(symbol(0), 1, 100_000, true));
    assertEquals(huge.getNumStates(), small.getNumStates());
    assertEquals(huge.getNumCounters(), 1);
    assertEquals(huge.getNumStates(), PatternToNfaCompiler.compile(
        quantify(symbol(0), 1, RowPattern.Quantify.UNBOUNDED, true)).getNumStates());
  }

  @Test
  public void testNestedQuantifiersGetIndependentCounterRegisters() {
    PatternNfa nfa = PatternToNfaCompiler.compile(quantify(quantify(symbol(0), 2, 2, true), 3, 3, true));
    assertEquals(nfa.getNumCounters(), 2);
    // (A{2}){3} consumes exactly six rows: the inner register must be reset by each outer iteration.
    assertMatch(quantify(quantify(symbol(0), 2, 2, true), 3, 3, true), labelSymbols("A"), "AAAAAAAA", "AAAAAA");
  }

  @Test
  public void testGreedyLoopHeadListsRepeatBeforeExit() {
    assertLoopHeadOrder(quantify(symbol(0), 0, RowPattern.Quantify.UNBOUNDED, true), TransitionKind.REPEAT,
        TransitionKind.EXIT_LOOP);
  }

  @Test
  public void testReluctantLoopHeadListsExitBeforeRepeat() {
    assertLoopHeadOrder(quantify(symbol(0), 0, RowPattern.Quantify.UNBOUNDED, false), TransitionKind.EXIT_LOOP,
        TransitionKind.REPEAT);
  }

  @Test
  public void testEmptyCycleGuardTerminatesOnAStarStar() {
    // PATTERN ((A*)*) is the canonical non terminating pattern: the outer loop can iterate forever over an inner loop
    // that consumes nothing. It must terminate, and still consume everything it can.
    RowPattern pattern = quantify(quantify(symbol(0), 0, RowPattern.Quantify.UNBOUNDED, true), 0,
        RowPattern.Quantify.UNBOUNDED, true);
    assertMatch(pattern, labelSymbols("A"), "AAA", "AAA");
    // With nothing to consume the same pattern still terminates, with an empty match.
    assertMatch(pattern, labelSymbols("A"), "BBB", "");
  }

  @Test
  public void testEmptyCycleGuardTerminatesOnAnOptionalBody() {
    // PATTERN ((A?)+): the body may match zero rows, so the loop could spin forever.
    RowPattern pattern =
        quantify(quantify(symbol(0), 0, 1, true), 1, RowPattern.Quantify.UNBOUNDED, true);
    assertMatch(pattern, labelSymbols("A"), "AAB", "AA");
    assertMatch(pattern, labelSymbols("A"), "BBB", "");
  }

  @Test
  public void testAnchorsAreRelativeToThePartition() {
    // PATTERN (^ A): matches only when the scan starts at the first row of the partition.
    RowPattern anchoredStart = concat(RowPattern.AnchorStart.INSTANCE, symbol(0));
    assertEquals(matchAt(anchoredStart, labelSymbols("A"), "AA", 0), 1);
    assertEquals(matchAt(anchoredStart, labelSymbols("A"), "AA", 1), PartitionMatcher.NO_MATCH);

    // PATTERN (A $): matches only when the match ends at the last row of the partition.
    RowPattern anchoredEnd = concat(symbol(0), RowPattern.AnchorEnd.INSTANCE);
    assertEquals(matchAt(anchoredEnd, labelSymbols("A"), "AA", 0), PartitionMatcher.NO_MATCH);
    assertEquals(matchAt(anchoredEnd, labelSymbols("A"), "AA", 1), 2);
  }

  @Test
  public void testSymbolWithoutDefinitionMatchesEveryRow() {
    // SQL:2016: a PATTERN variable with no DEFINE entry has the condition TRUE.
    assertMatch(quantify(symbol(0), 1, RowPattern.Quantify.UNBOUNDED, true), List.of(anySymbol("A")), "XYZ", "AAA");
  }

  @Test
  public void testStepBudgetThrowsInsteadOfGivingUp() {
    // An ambiguous pattern over a long partition: cut it off at a tiny budget and it must fail loudly, because
    // returning the matches it happened to find would silently drop the rest.
    RowPattern pattern = concat(quantify(symbol(0), 0, RowPattern.Quantify.UNBOUNDED, true),
        quantify(symbol(0), 0, RowPattern.Quantify.UNBOUNDED, true), symbol(1));
    List<PatternSymbol> symbols = List.of(MatchTestFixtures.labelSymbol("A", 0), labelledAs("Z", "Z", 1));
    PatternNfa nfa = PatternToNfaCompiler.compile(pattern);
    PartitionMatcher matcher = new PartitionMatcher(nfa, symbols, INPUT_SCHEMA, 20);
    QueryException exception =
        expectThrows(QueryException.class, () -> matcher.match(rows("AAAAAAAAAAAAAAAAAAAA"), 0, 1));
    assertTrue(exception.getMessage().contains("maxStepsPerMatchAttempt"), exception.getMessage());
  }

  /// A second pattern variable that also matches the label `A`, used to observe where a greedy loop stopped.
  private static PatternSymbol aliasOfA() {
    return labelledAs("X", "A", 1);
  }

  /// A pattern variable named `name` whose DEFINE matches rows whose label column equals `label`.
  private static PatternSymbol labelledAs(String name, String label, int ordinal) {
    PatternSymbol delegate = MatchTestFixtures.labelSymbol(label, ordinal);
    return new PatternSymbol(name, delegate.getDefinition());
  }

  private static void assertLoopHeadOrder(RowPattern pattern, TransitionKind first, TransitionKind second) {
    PatternNfa nfa = PatternToNfaCompiler.compile(pattern);
    List<Transition> loopHead = null;
    for (PatternNfa.State state : nfa.getStates()) {
      if (state.getTransitions().size() == 2 && state.getTransitions().stream()
          .allMatch(t -> t.getKind() == TransitionKind.REPEAT || t.getKind() == TransitionKind.EXIT_LOOP)) {
        loopHead = state.getTransitions();
      }
    }
    assertNotNull(loopHead, "No loop head found in " + nfa);
    assertEquals(loopHead.get(0).getKind(), first);
    assertEquals(loopHead.get(1).getKind(), second);
  }

  /// Asserts that the preferred match starting at row 0 maps exactly the rows spelled out by
  /// `expectedClassification`, whose i-th character is the name of the pattern variable the i-th row of the
  /// match was mapped to.
  private static void assertMatch(RowPattern pattern, List<PatternSymbol> symbols, String input,
      String expectedClassification) {
    PartitionMatcher matcher = newMatcher(pattern, symbols);
    List<Object[]> partitionRows = rows(input);
    int endPos = matcher.match(partitionRows, 0, 1);
    assertTrue(endPos != PartitionMatcher.NO_MATCH, "Expecting a match of '" + expectedClassification + "'");
    assertEquals(endPos, expectedClassification.length());
    StringBuilder classification = new StringBuilder();
    for (int i = 0; i < endPos; i++) {
      classification.append(matcher.getTape().classifierAt(i));
    }
    assertEquals(classification.toString(), expectedClassification);
  }

  private static void assertNoMatch(RowPattern pattern, List<PatternSymbol> symbols, String input) {
    assertEquals(matchAt(pattern, symbols, input, 0), PartitionMatcher.NO_MATCH);
  }

  private static int matchAt(RowPattern pattern, List<PatternSymbol> symbols, String input, int startPos) {
    return newMatcher(pattern, symbols).match(rows(input), startPos, 1);
  }

  private static PartitionMatcher newMatcher(RowPattern pattern, List<PatternSymbol> symbols) {
    return new PartitionMatcher(PatternToNfaCompiler.compile(pattern), symbols, INPUT_SCHEMA, UNLIMITED_STEPS);
  }
}
