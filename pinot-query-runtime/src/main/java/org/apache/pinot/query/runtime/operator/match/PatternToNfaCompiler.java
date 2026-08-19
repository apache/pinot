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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.query.planner.plannode.RowPattern;
import org.apache.pinot.query.runtime.operator.match.PatternNfa.State;
import org.apache.pinot.query.runtime.operator.match.PatternNfa.Transition;
import org.apache.pinot.query.runtime.operator.match.PatternNfa.TransitionKind;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * Compiles a MATCH_RECOGNIZE {@code PATTERN} tree into a {@link PatternNfa} whose transitions are ordered by
 * preference.
 *
 * <h2>The invariant this class exists to establish</h2>
 * <b>Every state lists its outgoing transitions in SQL:2016 preference order, so a depth first search that always
 * takes the first not-yet-tried transition of the current state enumerates candidate matches in preferment order, and
 * the <i>first</i> complete match it reaches is the preferred match.</b> {@link PartitionMatcher} relies on exactly
 * that: it never enumerates all matches and picks a best one, it stops at the first one.
 *
 * <p>The three constructs that carry a preference are encoded as follows.
 * <ul>
 *   <li><b>Alternation.</b> {@code A | B | C} emits one epsilon transition per branch, in source order, so the
 *       leftmost branch that can complete the whole pattern wins.</li>
 *   <li><b>Greedy quantifier.</b> The loop head lists the {@code REPEAT} edge before the {@code EXIT_LOOP} edge, so
 *       one more repetition is preferred over leaving the loop.</li>
 *   <li><b>Reluctant quantifier</b> ({@code *?}, {@code +?}, {@code ??}, {@code {n,m}?}). The same two edges in the
 *       opposite order, so leaving the loop is preferred over one more repetition.</li>
 * </ul>
 *
 * <h2>Quantifiers use counter registers, never state unrolling</h2>
 * A quantifier compiles to a single cycle:
 * <pre>
 *   from --START_LOOP(c)--&gt; loopHead
 *   loopHead --REPEAT(c, max)--&gt; bodyStart ... bodyEnd --EPSILON--&gt; loopHead
 *   loopHead --EXIT_LOOP(c, min)--&gt; exit
 * </pre>
 * {@code START_LOOP} resets register {@code c}, {@code REPEAT} is allowed only while {@code c &lt; max} and
 * increments it, and {@code EXIT_LOOP} is allowed only once {@code c &gt;= min}. The state count is therefore
 * independent of {@code min} and {@code max}: {@code A{1,10000}} compiles to the same automaton shape as {@code A+}.
 * Registers are per quantifier <i>instance</i> and are reset by {@code START_LOOP}, so nested and repeated
 * quantifiers such as {@code (A{2}){3}} count independently.
 *
 * <h2>Empty cycle guard</h2>
 * A quantifier whose body can match zero rows, such as {@code (A*)*} or {@code (A?)+}, would otherwise let the
 * matcher take the {@code REPEAT} edge forever without ever consuming a row. {@code REPEAT} therefore also requires
 * that the previous iteration of the same register consumed at least one row, and {@code EXIT_LOOP} is permitted
 * unconditionally right after an empty iteration - an unbounded number of empty iterations trivially satisfies any
 * minimum. Termination is guaranteed without rejecting any legal pattern.
 *
 * <h2>Anchors</h2>
 * {@code ^} and {@code $} become guard transitions that consume no row and are satisfied only at the first row of the
 * partition, and past its last row, respectively. They are relative to the partition, as SQL:2016 requires, not to
 * the input as a whole.
 *
 * <p>This class is stateless; {@link #compile} allocates its own builder state.
 */
public class PatternToNfaCompiler {
  private PatternToNfaCompiler() {
  }

  /**
   * Compiles {@code pattern} into an automaton with prioritized transitions.
   *
   * @throws org.apache.pinot.spi.exception.QueryException if the pattern contains a construct that is not supported
   *         yet. Every such construct is rejected here rather than approximated, because an approximated pattern
   *         returns wrong rows rather than an error.
   */
  public static PatternNfa compile(RowPattern pattern) {
    Builder builder = new Builder();
    int start = builder.newState();
    int accept = builder.compileInto(pattern, start);
    return new PatternNfa(builder._states, start, accept, builder._numCounters);
  }

  /**
   * Mutable automaton under construction. {@link #compileInto} appends the states of one sub-pattern and returns the
   * state the automaton is in after that sub-pattern matched.
   */
  private static class Builder {
    private final List<State> _states = new ArrayList<>();
    private int _numCounters;

    private int newState() {
      _states.add(new State());
      return _states.size() - 1;
    }

    private void addTransition(int from, TransitionKind kind, int target, int operand, int bound) {
      _states.get(from).addTransition(new Transition(kind, target, operand, bound));
    }

    private int compileInto(RowPattern pattern, int from) {
      switch (pattern.getKind()) {
        case SYMBOL:
          return compileSymbol((RowPattern.Symbol) pattern, from);
        case CONCAT:
          return compileConcat((RowPattern.Concat) pattern, from);
        case ALTERNATE:
          return compileAlternate((RowPattern.Alternate) pattern, from);
        case QUANTIFY:
          return compileQuantify((RowPattern.Quantify) pattern, from);
        case ANCHOR_START:
          return compileAnchor(TransitionKind.ANCHOR_START, from);
        case ANCHOR_END:
          return compileAnchor(TransitionKind.ANCHOR_END, from);
        default:
          throw QueryErrorCode.QUERY_EXECUTION.asException(
              "Unsupported MATCH_RECOGNIZE PATTERN construct: " + pattern.getKind());
      }
    }

    private int compileSymbol(RowPattern.Symbol symbol, int from) {
      int to = newState();
      addTransition(from, TransitionKind.MATCH, to, symbol.getSymbolOrdinal(), 0);
      return to;
    }

    private int compileConcat(RowPattern.Concat concat, int from) {
      int current = from;
      for (RowPattern child : concat.getChildren()) {
        current = compileInto(child, current);
      }
      return current;
    }

    /**
     * Emits one epsilon transition per branch, in source order. The transition list order is the preference order, so
     * the leftmost branch that lets the whole pattern complete wins, as SQL:2016 requires.
     */
    private int compileAlternate(RowPattern.Alternate alternate, int from) {
      int to = newState();
      for (RowPattern child : alternate.getChildren()) {
        int branchStart = newState();
        addTransition(from, TransitionKind.EPSILON, branchStart, 0, 0);
        int branchEnd = compileInto(child, branchStart);
        addTransition(branchEnd, TransitionKind.EPSILON, to, 0, 0);
      }
      return to;
    }

    /**
     * Emits the counter guarded cycle described in the class javadoc. The two loop head transitions are ordered
     * {@code REPEAT} first for a greedy quantifier and {@code EXIT_LOOP} first for a reluctant one; that ordering is
     * the only difference between the two.
     */
    private int compileQuantify(RowPattern.Quantify quantify, int from) {
      int counterId = _numCounters++;
      int loopHead = newState();
      int exit = newState();
      int bodyStart = newState();
      addTransition(from, TransitionKind.START_LOOP, loopHead, counterId, 0);
      int bodyEnd = compileInto(quantify.getChild(), bodyStart);
      addTransition(bodyEnd, TransitionKind.EPSILON, loopHead, 0, 0);

      int maxRepeat = quantify.getMaxRepeat() == RowPattern.Quantify.UNBOUNDED ? PatternNfa.UNBOUNDED
          : quantify.getMaxRepeat();
      if (quantify.isGreedy()) {
        addTransition(loopHead, TransitionKind.REPEAT, bodyStart, counterId, maxRepeat);
        addTransition(loopHead, TransitionKind.EXIT_LOOP, exit, counterId, quantify.getMinRepeat());
      } else {
        addTransition(loopHead, TransitionKind.EXIT_LOOP, exit, counterId, quantify.getMinRepeat());
        addTransition(loopHead, TransitionKind.REPEAT, bodyStart, counterId, maxRepeat);
      }
      return exit;
    }

    private int compileAnchor(TransitionKind kind, int from) {
      int to = newState();
      addTransition(from, kind, to, 0, 0);
      return to;
    }
  }
}
