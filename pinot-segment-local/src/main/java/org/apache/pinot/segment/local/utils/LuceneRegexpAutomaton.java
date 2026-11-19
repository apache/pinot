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

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;


/// Bounds Lucene regular expression construction and compiles deterministic automata for the FST matchers.
/// All compilation state is local to the caller.
public final class LuceneRegexpAutomaton {
  private static final int MAX_CONSTRUCTION_WORK = Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

  private LuceneRegexpAutomaton() {
  }

  @SuppressWarnings("deprecation") // Preserve the complement syntax supported by existing FST queries.
  public static Automaton compile(String expression) {
    RegExp regex = new RegExp(expression, RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT);
    estimate(regex, MAX_CONSTRUCTION_WORK);
    return Operations.determinize(regex.toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
  }

  /// Checks a pattern using Lucene text query syntax before building its automaton.
  public static void validateConstructionBudget(String expression) {
    validateConstructionBudget(expression, MAX_CONSTRUCTION_WORK);
  }

  static void validateConstructionBudget(String expression, int workLimit) {
    // Lucene expands repetitions before determinizing. Estimate that work on the parsed tree first.
    estimate(new RegExp(expression, RegExp.ALL), workLimit);
  }

  private static ConstructionCost estimate(RegExp regex, int workLimit) {
    return switch (regex.kind) {
      case REGEXP_REPEAT_MIN, REGEXP_REPEAT_MINMAX -> estimateRepetition(regex, workLimit);
      case REGEXP_CONCATENATION, REGEXP_UNION, REGEXP_INTERSECTION -> {
        ConstructionCost left = estimate(regex.exp1, workLimit);
        ConstructionCost right = estimate(regex.exp2, workLimit);
        long work = regex.kind == RegExp.Kind.REGEXP_INTERSECTION
            ? left.work() * right.work() : left.work() + right.work();
        boolean acceptsEmpty = regex.kind == RegExp.Kind.REGEXP_UNION
            ? left.acceptsEmpty() || right.acceptsEmpty()
            : left.acceptsEmpty() && right.acceptsEmpty();
        yield checkedCost(work, acceptsEmpty, workLimit);
      }
      case REGEXP_OPTIONAL -> checkedCost(estimate(regex.exp1, workLimit).work() + 1, true, workLimit);
      case REGEXP_REPEAT -> estimateStar(regex.exp1, estimate(regex.exp1, workLimit), workLimit);
      case REGEXP_COMPLEMENT -> {
        ConstructionCost child = estimate(regex.exp1, workLimit);
        yield checkedCost(child.work() + 1, !child.acceptsEmpty(), workLimit);
      }
      case REGEXP_DEPRECATED_COMPLEMENT -> estimateComplement(regex, workLimit);
      case REGEXP_STRING -> checkedCost(Math.max(1, regex.s.length()), regex.s.isEmpty(), workLimit);
      case REGEXP_ANYSTRING -> checkedCost(1, true, workLimit);
      case REGEXP_CHAR_CLASS -> checkedCost(Math.max(1, regex.from.length), false, workLimit);
      case REGEXP_INTERVAL ->
          checkedCost(Math.max(regex.digits, Integer.toString(regex.max).length()), false, workLimit);
      default -> checkedCost(1, false, workLimit);
    };
  }

  private static ConstructionCost estimateComplement(RegExp regex, int workLimit) {
    int complements = 0;
    RegExp child = regex;
    do {
      complements++;
      child = child.exp1;
    } while (child.kind == regex.kind);

    ConstructionCost cost = estimate(child, workLimit);
    Automaton automaton = child.toAutomaton();
    for (int i = 0; i < complements; i++) {
      // Complement can determinize a small child into a much larger automaton. Measure it before any outer
      // repetition can copy it. As in Lucene, determinization itself is subject to the default work limit.
      automaton = Operations.complement(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
      long footprint = (long) automaton.getNumStates() + automaton.getNumTransitions();
      boolean acceptsEmpty = automaton.getNumStates() > 0 && automaton.isAccept(0);
      cost = checkedCost(Math.max(cost.work() + 1, footprint), acceptsEmpty, workLimit);
    }
    return cost;
  }

  private static ConstructionCost estimateStar(RegExp operand, ConstructionCost child, int workLimit) {
    long work = child.work() + 1;
    return switch (operand.kind) {
      // These operands have one initial transition and no accepting state with outgoing transitions,
      // or already loop at their only state. Their repetition adds at most one unit of work.
      case REGEXP_CHAR, REGEXP_CHAR_RANGE, REGEXP_ANYCHAR, REGEXP_STRING, REGEXP_ANYSTRING ->
          checkedCost(work, true, workLimit);
      case REGEXP_EMPTY -> checkedCost(work, false, workLimit);
      default -> {
        // The child has passed validation, including any nested stars. Measure it before Lucene copies its
        // initial transitions, once for the new initial state and once for each nonterminal accepting state.
        Automaton automaton = operand.toAutomaton();
        int numStates = automaton.getNumStates();
        long numTransitions = automaton.getNumTransitions();
        if (numStates > 0 && (!automaton.isAccept(0) || automaton.getAcceptStates().cardinality() != 1)) {
          int copies = 1;
          for (int state = 0; state < numStates; state++) {
            if (automaton.isAccept(state) && automaton.getNumTransitions(state) > 0) {
              copies++;
            }
          }
          numTransitions += (long) copies * automaton.getNumTransitions(0);
        }
        yield checkedCost(Math.max(work, Math.max(numStates + 1L, numTransitions)), numStates > 0, workLimit);
      }
    };
  }

  private static ConstructionCost estimateRepetition(RegExp regex, int workLimit) {
    boolean unbounded = regex.kind == RegExp.Kind.REGEXP_REPEAT_MIN;
    long concatenatedCopies = unbounded ? (long) regex.min + 1 : regex.min;
    long copies = unbounded ? concatenatedCopies : regex.max;
    checkBudget(copies, workLimit);
    ConstructionCost child = estimate(regex.exp1, workLimit);
    // Lucene constructs the child even for {0}. The mandatory copies can link to every later copy in their
    // concatenation if the child accepts the empty string. Lucene adds a bounded repeat's optional tail linearly.
    long work = child.work() * Math.max(1, copies);
    if (child.acceptsEmpty()) {
      work += child.work() * concatenatedCopies * (concatenatedCopies - 1) / 2;
    }
    if (unbounded) {
      // Lucene implements both + and {min,} by appending a star of the same operand.
      ConstructionCost star = estimateStar(regex.exp1, child, workLimit);
      work += star.work() - child.work();
      return checkedCost(work, (regex.min == 0 || child.acceptsEmpty()) && star.acceptsEmpty(), workLimit);
    }
    return checkedCost(work, regex.min == 0 || child.acceptsEmpty(), workLimit);
  }

  private static ConstructionCost checkedCost(long work, boolean acceptsEmpty, int workLimit) {
    checkBudget(work, workLimit);
    return new ConstructionCost(work, acceptsEmpty);
  }

  private static void checkBudget(long work, int workLimit) {
    if (work > workLimit) {
      throw new IllegalArgumentException("Regular expression exceeds the automaton construction budget");
    }
  }

  private record ConstructionCost(long work, boolean acceptsEmpty) {
  }
}
