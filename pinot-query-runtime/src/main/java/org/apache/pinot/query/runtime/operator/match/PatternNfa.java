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


/// A non-deterministic finite automaton compiled from a MATCH_RECOGNIZE `PATTERN` clause by
/// [PatternToNfaCompiler], and executed by [PartitionMatcher].
///
/// ## Prioritized transitions
///
/// The transitions of a state are stored in **preference order**, highest preference first. A depth first search
/// that always tries the transitions of a state in list order therefore enumerates candidate matches in the SQL:2016
/// "preferment order", and the first complete match it reaches is the preferred one. See
/// [PatternToNfaCompiler] for how each pattern construct maps onto that ordering.
///
/// ## Counter registers
///
/// Bounded quantifiers such as `A{2,5}` are **not** unrolled into repeated states. Every quantifier node
/// allocates one counter register; the loop is a single cycle in the automaton whose entry and exit edges are guarded
/// by that counter. The number of states is therefore linear in the size of the pattern text and independent of the
/// repetition bounds, so `A{1,10000}` costs the same to compile as `A+`.
///
/// Instances are immutable and safe to share between the threads that execute different partitions.
public class PatternNfa {
  /// Sentinel for [Transition#getBound()] of an unbounded [TransitionKind#REPEAT].
  public static final int UNBOUNDED = -1;

  private final List<State> _states;
  private final int _startState;
  private final int _acceptState;
  private final int _numCounters;

  PatternNfa(List<State> states, int startState, int acceptState, int numCounters) {
    _states = List.copyOf(states);
    _startState = startState;
    _acceptState = acceptState;
    _numCounters = numCounters;
  }

  public List<State> getStates() {
    return _states;
  }

  public State getState(int stateId) {
    return _states.get(stateId);
  }

  public int getNumStates() {
    return _states.size();
  }

  public int getStartState() {
    return _startState;
  }

  /// The single accepting state. Reaching it means the whole pattern has been matched.
  public int getAcceptState() {
    return _acceptState;
  }

  /// Number of counter registers the automaton uses, i.e. the number of quantifiers in the pattern.
  public int getNumCounters() {
    return _numCounters;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("PatternNfa{start=").append(_startState).append(", accept=")
        .append(_acceptState).append(", counters=").append(_numCounters).append('}');
    for (int stateId = 0; stateId < _states.size(); stateId++) {
      builder.append('\n').append(stateId).append(':');
      for (Transition transition : _states.get(stateId).getTransitions()) {
        builder.append(' ').append(transition);
      }
    }
    return builder.toString();
  }

  /// What a transition does when it is taken.
  public enum TransitionKind {
    /// Consumes the current row if the DEFINE predicate of [Transition#getOperand()] holds for it.
    MATCH,
    /// Consumes nothing and has no side effect.
    EPSILON,
    /// Enters a quantifier: resets the counter register [Transition#getOperand()].
    START_LOOP,
    /// Runs one more iteration of a quantifier. Allowed while the counter is below
    /// [Transition#getBound()] repetitions and the previous iteration consumed at least one row; increments the
    /// counter.
    REPEAT,
    /// Leaves a quantifier. Allowed once the counter reached [Transition#getBound()] repetitions, or once an
    /// iteration turned out to be empty.
    EXIT_LOOP,
    /// The `^` anchor: allowed only at the first row of the partition.
    ANCHOR_START,
    /// The `$` anchor: allowed only past the last row of the partition.
    ANCHOR_END
  }

  /// One outgoing edge of a [State]. Its position in [State#getTransitions()] is its preference.
  public static final class Transition {
    private final TransitionKind _kind;
    private final int _target;
    private final int _operand;
    private final int _bound;

    Transition(TransitionKind kind, int target, int operand, int bound) {
      _kind = kind;
      _target = target;
      _operand = operand;
      _bound = bound;
    }

    public TransitionKind getKind() {
      return _kind;
    }

    /// The state this transition leads to.
    public int getTarget() {
      return _target;
    }

    /// Pattern symbol ordinal for [TransitionKind#MATCH], counter register id for the loop kinds, unused
    /// otherwise.
    public int getOperand() {
      return _operand;
    }

    /// Maximum repetition count for [TransitionKind#REPEAT] ([#UNBOUNDED] if there is no upper bound), and
    /// the minimum repetition count for [TransitionKind#EXIT_LOOP]. Unused otherwise.
    public int getBound() {
      return _bound;
    }

    @Override
    public String toString() {
      switch (_kind) {
        case MATCH:
          return "MATCH(s" + _operand + ")->" + _target;
        case EPSILON:
          return "EPS->" + _target;
        case START_LOOP:
          return "START(c" + _operand + ")->" + _target;
        case REPEAT:
          return "REPEAT(c" + _operand + ",max=" + (_bound == UNBOUNDED ? "*" : _bound) + ")->" + _target;
        case EXIT_LOOP:
          return "EXIT(c" + _operand + ",min=" + _bound + ")->" + _target;
        default:
          return _kind + "->" + _target;
      }
    }
  }

  /// A state of the automaton. Mutable only while [PatternToNfaCompiler] builds it.
  public static final class State {
    private final List<Transition> _transitions = new ArrayList<>(2);

    /// The outgoing transitions in preference order, highest preference first.
    public List<Transition> getTransitions() {
      return _transitions;
    }

    void addTransition(Transition transition) {
      _transitions.add(transition);
    }
  }
}
