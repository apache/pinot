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
package org.apache.pinot.segment.local.utils.nativefst.utils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.apache.pinot.segment.local.utils.nativefst.automaton.Automaton;
import org.apache.pinot.segment.local.utils.nativefst.automaton.CharacterRunAutomaton;
import org.apache.pinot.segment.local.utils.nativefst.automaton.RegExp;
import org.apache.pinot.segment.local.utils.nativefst.automaton.State;
import org.apache.pinot.segment.local.utils.nativefst.automaton.Transition;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableArc;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFST;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableState;
import org.roaringbitmap.IntConsumer;

public class RealTimeRegexpMatcher {
  private final String _regexQuery;
  private final MutableFST _fst;
  private final Automaton _automaton;
  private final IntConsumer _dest;

  public RealTimeRegexpMatcher(String regexQuery, MutableFST fst, IntConsumer dest) {
    _regexQuery = regexQuery;
    _fst = fst;
    _dest = dest;
    _automaton = new RegExp(_regexQuery).toAutomaton();
  }

  public static void regexMatch(String regexQuery, MutableFST fst, IntConsumer dest) {
    RealTimeRegexpMatcher matcher = new RealTimeRegexpMatcher(regexQuery, fst, dest);
    matcher.regexMatchOnFST();
  }

  // Matches "input" string with _regexQuery Automaton.
  public boolean match(String input) {
    CharacterRunAutomaton characterRunAutomaton = new CharacterRunAutomaton(_automaton);
    return characterRunAutomaton.run(input);
  }

  /**
   * This function runs matching on automaton built from regexQuery and the FST.
   * FST stores key (string) to a value (Long). Both are state machines and state transition is based on
   * a input character.
   *
   * This algorithm starts with Queue containing (Automaton Start Node, FST Start Node).
   * Each step an entry is popped from the queue:
   *    1) if the automaton state is accept and the FST Node is final (i.e. end node) then the value stored for that FST
   *       is added to the set of result.
   *    2) Else next set of transitions on automaton are gathered and for each transition target node for that character
   *       is figured out in FST Node, resulting pair of (automaton state, fst node) are added to the queue.
   *    3) This process is bound to complete since we are making progression on the FST (which is a DAG) towards final
   *       nodes.
   */
  public void regexMatchOnFST() {
    final Queue<Path> queue = new ArrayDeque();

    if (_automaton.getNumberOfStates() == 0) {
      return;
    }
    // Automaton start state and FST start node is added to the queue.
    queue.add(new Path(_automaton.getInitialState(), _fst.getStartState(), null, new ArrayList<>()));
    Set<State> acceptStates = _automaton.getAcceptStates();
    while (!queue.isEmpty()) {
      final Path path = queue.remove();
      // If automaton is in accept state and the fstNode is final (i.e. end node) then add the entry to endNodes which
      // contains the result set.
      if (acceptStates.contains(path._state)) {
        if (path._node.isTerminal()) {
          //endNodes.add((long) _fst.getOutputSymbol(path._fstArc));
          _dest.accept(path._fstArc.getOutputSymbol());
        }
      }

      Set<Transition> stateTransitions = path._state.getTransitionSet();
      for (Transition t : stateTransitions) {
        final int min = t._min;
        final int max = t._max;
        if (min == max) {
          MutableArc arc = getArcForLabel(path._node, t._min);
          if (arc != null) {
            queue.add(new Path(t._to, arc.getNextState(), arc, path._pathState));
          }
        } else {
          List<MutableArc> arcs = path._node.getArcs();
          for (MutableArc arc : arcs) {
            char label = arc.getNextState().getLabel();
            if (label >= min && label <= max) {
              queue.add(new Path(t._to, arc.getNextState(), arc, path._pathState));
            }
          }
        }
      }
    }
  }

  private MutableArc getArcForLabel(MutableState mutableState, char label) {
    List<MutableArc> arcs = mutableState.getArcs();
    for (MutableArc arc : arcs) {
      if (arc.getNextState().getLabel() == label) {
        return arc;
      }
    }
    return null;
  }

  /**
   * Represents a path in the FST traversal directed by the automaton
   */
  public final class Path {
    public final State _state;
    public final MutableState _node;
    public final MutableArc _fstArc;
    // Used for capturing the path taken till the point (for debugging)
    public List<Character> _pathState;

    public Path(State state, MutableState node, MutableArc fstArc, List<Character> pathState) {
      _state = state;
      _node = node;
      _fstArc = fstArc;
      _pathState = pathState;
      _pathState.add(node.getLabel());
    }
  }
}
