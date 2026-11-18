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
    validateConstructionBudget(regex);
    return Operations.determinize(regex.toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
  }

  /// Checks a pattern using Lucene text query syntax before building its automaton.
  public static void validateConstructionBudget(String expression) {
    validateConstructionBudget(new RegExp(expression, RegExp.ALL));
  }

  private static void validateConstructionBudget(RegExp regex) {
    // Lucene expands counted repetitions before determinizing. Estimate that work on the parsed tree first.
    estimate(regex);
  }

  private static ConstructionCost estimate(RegExp regex) {
    return switch (regex.kind) {
      case REGEXP_REPEAT_MIN, REGEXP_REPEAT_MINMAX -> estimateRepetition(regex);
      case REGEXP_CONCATENATION, REGEXP_UNION, REGEXP_INTERSECTION -> {
        ConstructionCost left = estimate(regex.exp1);
        ConstructionCost right = estimate(regex.exp2);
        long work = regex.kind == RegExp.Kind.REGEXP_INTERSECTION
            ? left.work() * right.work() : left.work() + right.work();
        boolean acceptsEmpty = regex.kind == RegExp.Kind.REGEXP_UNION
            ? left.acceptsEmpty() || right.acceptsEmpty()
            : left.acceptsEmpty() && right.acceptsEmpty();
        yield new ConstructionCost(work, acceptsEmpty);
      }
      case REGEXP_OPTIONAL, REGEXP_REPEAT -> new ConstructionCost(estimate(regex.exp1).work() + 1, true);
      case REGEXP_COMPLEMENT, REGEXP_DEPRECATED_COMPLEMENT -> {
        ConstructionCost child = estimate(regex.exp1);
        yield new ConstructionCost(child.work() + 1, !child.acceptsEmpty());
      }
      case REGEXP_STRING -> new ConstructionCost(Math.max(1, regex.s.length()), regex.s.isEmpty());
      case REGEXP_ANYSTRING -> new ConstructionCost(1, true);
      case REGEXP_CHAR_CLASS -> new ConstructionCost(Math.max(1, regex.from.length), false);
      case REGEXP_INTERVAL ->
          new ConstructionCost(Math.max(regex.digits, Integer.toString(regex.max).length()), false);
      default -> new ConstructionCost(1, false);
    };
  }

  private static ConstructionCost estimateRepetition(RegExp regex) {
    boolean unbounded = regex.kind == RegExp.Kind.REGEXP_REPEAT_MIN;
    long concatenatedCopies = unbounded ? (long) regex.min + 1 : regex.min;
    long copies = unbounded ? concatenatedCopies : regex.max;
    checkBudget(copies);
    ConstructionCost child = estimate(regex.exp1);
    // Lucene constructs the child even for {0}. The mandatory copies can link to every later copy in their
    // concatenation if the child accepts the empty string. Lucene adds a bounded repeat's optional tail linearly.
    long work = child.work() * Math.max(1, copies);
    if (child.acceptsEmpty()) {
      work += child.work() * concatenatedCopies * (concatenatedCopies - 1) / 2;
    }
    return new ConstructionCost(work, regex.min == 0 || child.acceptsEmpty());
  }

  private static void checkBudget(long work) {
    if (work > MAX_CONSTRUCTION_WORK) {
      throw new IllegalArgumentException("Regular expression exceeds the automaton construction budget");
    }
  }

  private record ConstructionCost(long work, boolean acceptsEmpty) {
    private ConstructionCost {
      checkBudget(work);
    }
  }
}
