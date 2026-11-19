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
package org.apache.pinot.segment.local.utils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.RegExp.Kind;


/// Bounds Lucene regular expression construction and compiles deterministic automata for the FST matchers.
/// All compilation state is local to the caller.
public final class LuceneRegexpAutomaton {
  private static final int MAX_CONSTRUCTION_WORK = Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

  private LuceneRegexpAutomaton() {
  }

  @SuppressWarnings("deprecation") // Preserve the complement syntax supported by existing FST queries.
  public static Automaton compile(String expression) {
    RegExp regex = new RegExp(expression, RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT);
    Automaton automaton = build(regex, MAX_CONSTRUCTION_WORK).automaton();
    return Operations.determinize(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
  }

  /// Checks Lucene text query syntax before the query parser constructs its automaton.
  public static void validateConstructionBudget(String expression) {
    validateConstructionBudget(expression, MAX_CONSTRUCTION_WORK);
  }

  static void validateConstructionBudget(String expression, int workLimit) {
    build(new RegExp(expression, RegExp.ALL), workLimit);
  }

  private static Construction build(RegExp regex, int limit) {
    return switch (regex.kind) {
      case REGEXP_CONCATENATION, REGEXP_UNION -> buildJoin(regex, limit);
      case REGEXP_INTERSECTION -> intersect(build(regex.exp1, limit), build(regex.exp2, limit), limit);
      case REGEXP_OPTIONAL -> optional(build(regex.exp1, limit), limit);
      case REGEXP_REPEAT -> star(build(regex.exp1, limit), limit);
      case REGEXP_REPEAT_MIN, REGEXP_REPEAT_MINMAX -> repeat(regex, limit);
      case REGEXP_COMPLEMENT, REGEXP_DEPRECATED_COMPLEMENT -> complement(regex, limit);
      case REGEXP_STRING -> leaf(regex, Math.max(1, regex.s.length()), limit);
      case REGEXP_CHAR_CLASS -> leaf(regex, Math.max(1, regex.from.length), limit);
      case REGEXP_INTERVAL -> leaf(regex, Math.max(regex.digits, Integer.toString(regex.max).length()), limit);
      case REGEXP_AUTOMATON, REGEXP_CHAR, REGEXP_CHAR_RANGE, REGEXP_ANYCHAR, REGEXP_EMPTY, REGEXP_ANYSTRING ->
          leaf(regex, 1, limit);
    };
  }

  private static Construction leaf(RegExp regex, long work, int limit) {
    checkBudget(work, limit);
    return checked(regex.toAutomaton(), work, limit);
  }

  private static Construction buildJoin(RegExp regex, int limit) {
    // Lucene flattens consecutive union or concatenation nodes before building their shared automaton.
    // Keep the same operand order and check the accumulated allocation as operands are built.
    JoinBuilder join = new JoinBuilder(regex.kind == Kind.REGEXP_CONCATENATION, limit);
    ArrayDeque<RegExp> pending = new ArrayDeque<>();
    pending.push(regex);
    while (!pending.isEmpty()) {
      RegExp operand = pending.pop();
      if (operand.kind == regex.kind) {
        checkBudget(pending.size() + 2L, limit);
        pending.push(operand.exp2);
        pending.push(operand.exp1);
      } else {
        join.add(build(operand, limit));
      }
    }
    return join.finish();
  }

  private static Construction intersect(Construction left, Construction right, int limit) {
    long work = left.work() * right.work();
    checkBudget(Math.max(work, (long) left.states() * right.states()),
        (long) left.transitions() * right.transitions(), limit);
    return checked(Operations.intersection(left.automaton(), right.automaton()), work, limit);
  }

  private static Construction optional(Construction child, int limit) {
    long work = child.work() + 1;
    if (child.acceptsEmpty()) {
      return checked(child.automaton(), work, limit);
    }
    checkBudget(Math.max(work, child.states() + 1L), (long) child.transitions() + child.initialTransitions(), limit);
    return checked(Operations.optional(child.automaton()), work, limit);
  }

  private static Construction star(Construction child, int limit) {
    long work = child.work() + 1;
    int acceptingStates = child.acceptingStates();
    if (child.states() == 0 || (child.acceptsEmpty() && acceptingStates == 1)) {
      return checked(child.automaton(), work, limit);
    }

    // Lucene merges terminal accepting states into the new initial state. Every other accepting state
    // receives a copy of the operand's initial transitions, as does the new initial state.
    Automaton automaton = child.automaton();
    int nonterminalAccepts = 0;
    for (int state = 0; state < child.states(); state++) {
      if (automaton.isAccept(state) && automaton.getNumTransitions(state) > 0) {
        nonterminalAccepts++;
      }
    }
    long states = (long) child.states() + 1 - (acceptingStates - nonterminalAccepts);
    long transitions = child.transitions() + (1L + nonterminalAccepts) * child.initialTransitions();
    checkBudget(Math.max(work, states), transitions, limit);
    return checked(Operations.repeat(automaton), work, limit);
  }

  private static Construction repeat(RegExp regex, int limit) {
    boolean unbounded = regex.kind == Kind.REGEXP_REPEAT_MIN;
    checkBudget(unbounded ? regex.min + 1L : regex.max, limit);
    Construction child = build(regex.exp1, limit);
    if (unbounded) {
      // Lucene implements both + and {min,} by concatenating the required copies with one starred operand.
      Construction tail = star(child, limit);
      if (regex.min == 0) {
        return tail;
      }
      JoinBuilder join = new JoinBuilder(true, limit);
      for (int copy = 0; copy < regex.min; copy++) {
        join.add(child);
      }
      join.add(tail);
      return join.finish();
    }

    int statesPerCopy = child.states();
    long work = child.work() * Math.max(1, regex.max);
    long stateCount = (long) regex.max * statesPerCopy + (regex.min == 0 ? 1 : 0);
    long requiredPairs = child.acceptsEmpty()
        ? (long) regex.min * (regex.min - 1) / 2 : Math.max(0, regex.min - 1);
    long linkSources = requiredPairs * child.acceptingStates();
    if (regex.max > regex.min) {
      // The optional tail links from the required prefix once, then from only the previous copy.
      long prefixAccepts = regex.min == 0 ? 1
          : (child.acceptsEmpty() ? regex.min : 1L) * child.acceptingStates();
      linkSources += prefixAccepts + (long) (regex.max - regex.min - 1) * child.acceptingStates();
    }
    long transitionWork = (long) regex.max * child.transitions()
        + linkSources * Math.max(1, child.initialTransitions());
    checkBudget(Math.max(work, stateCount), transitionWork, limit);
    return checked(Operations.repeat(child.automaton(), regex.min, regex.max), work, limit);
  }

  private static Construction complement(RegExp regex, int limit) {
    int count = 0;
    RegExp operand = regex;
    do {
      count++;
      checkBudget(count, limit);
      operand = operand.exp1;
    } while (operand.kind == regex.kind);

    Construction child = build(operand, limit);
    int determinizeLimit =
        regex.kind == Kind.REGEXP_COMPLEMENT ? Integer.MAX_VALUE : Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;
    for (int i = 0; i < count; i++) {
      Automaton automaton = Operations.complement(child.automaton(), determinizeLimit);
      long footprint = (long) automaton.getNumStates() + automaton.getNumTransitions();
      child = checked(automaton, Math.max(child.work() + 1, footprint), limit);
    }
    return child;
  }

  private static Construction checked(Automaton automaton, long work, int limit) {
    checkBudget(Math.max(work, automaton.getNumStates()), automaton.getNumTransitions(), limit);
    return new Construction(automaton, work);
  }

  private static void checkBudget(long stateWork, long transitionWork, int limit) {
    checkBudget(Math.max(stateWork, transitionWork), limit);
  }

  private static void checkBudget(long work, int limit) {
    if (work > limit) {
      throw new IllegalArgumentException("Regular expression exceeds the automaton construction budget");
    }
  }

  /// Checks each join before Lucene allocates its state and transition arrays.
  private static final class JoinBuilder {
    private final boolean _concatenate;
    private final int _limit;
    private final List<Automaton> _automata = new ArrayList<>();
    private long _work;
    private long _states;
    private long _transitionWork;
    private long _linkSources;

    private JoinBuilder(boolean concatenate, int limit) {
      _concatenate = concatenate;
      _limit = limit;
      _states = concatenate ? 0 : 1;
    }

    private void add(Construction child) {
      _work += child.work();
      _states += child.states();
      _transitionWork += child.transitions();
      if (_concatenate) {
        // Earlier accepting states also link to this operand when every intervening operand was nullable.
        _transitionWork += _linkSources * Math.max(1, child.initialTransitions());
        _linkSources = child.acceptingStates() + (child.acceptsEmpty() ? _linkSources : 0);
      } else {
        _transitionWork += child.initialTransitions();
      }
      checkBudget(Math.max(_work, _states), _transitionWork, _limit);
      _automata.add(child.automaton());
    }

    private Construction finish() {
      Automaton automaton = _concatenate ? Operations.concatenate(_automata) : Operations.union(_automata);
      return checked(automaton, _work, _limit);
    }
  }

  private record Construction(Automaton automaton, long work) {
    private int states() {
      return automaton.getNumStates();
    }

    private int transitions() {
      return automaton.getNumTransitions();
    }

    private int acceptingStates() {
      return automaton.getAcceptStates().cardinality();
    }

    private boolean acceptsEmpty() {
      return states() > 0 && automaton.isAccept(0);
    }

    private int initialTransitions() {
      return states() > 0 ? automaton.getNumTransitions(0) : 0;
    }
  }
}
