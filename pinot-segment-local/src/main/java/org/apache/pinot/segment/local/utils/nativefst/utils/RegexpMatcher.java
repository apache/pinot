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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.pinot.segment.local.utils.nativefst.FST;
import org.apache.pinot.segment.local.utils.nativefst.automaton.Automaton;
import org.apache.pinot.segment.local.utils.nativefst.automaton.CharacterRunAutomaton;
import org.apache.pinot.segment.local.utils.nativefst.automaton.RegExp;
import org.apache.pinot.segment.local.utils.nativefst.automaton.State;
import org.apache.pinot.segment.local.utils.nativefst.automaton.Transition;
import org.roaringbitmap.IntConsumer;


/**
 * RegexpMatcher is a helper to retrieve matching values for a given regexp query.
 * Regexp query is converted into an automaton and we run the matching algorithm on FST.
 *
 * Two main functions of this class are
 *   regexMatchOnFST() Function runs matching on FST (See function comments for more details)
 *   match(input) Function builds the automaton and matches given input.
 */
public class RegexpMatcher {
  private final String _regexQuery;
  private final FST _fst;
  private final Automaton _automaton;
  private final IntConsumer _dest;

  public RegexpMatcher(String regexQuery, FST fst, IntConsumer dest) {
    _regexQuery = regexQuery;
    _fst = fst;
    _dest = dest;

    _automaton = new RegExp(_regexQuery).toAutomaton();
  }

  public static void regexMatch(String regexQuery, FST fst, IntConsumer dest) {
    RegexpMatcher matcher = new RegexpMatcher(regexQuery, fst, dest);
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
    final List<Path> queue = new ArrayList<>();
    final List<Long> endNodes = new ArrayList<>();

    if (_automaton.getNumberOfStates() == 0) {
      return;
    }

    // Automaton start state and FST start node is added to the queue.
    queue.add(new Path(_automaton.getInitialState(), _fst.getRootNode(), 0, new ArrayList<>()));

    Set<State> acceptStates = _automaton.getAcceptStates();
    while (queue.size() != 0) {
      final Path path = queue.remove(queue.size() - 1);

      // If automaton is in accept state and the fstNode is final (i.e. end node) then add the entry to endNodes which
      // contains the result set.
      if (acceptStates.contains(path._state)) {
        if (_fst.isArcFinal(path._fstArc)) {
          //endNodes.add((long) _fst.getOutputSymbol(path._fstArc));
          _dest.accept(_fst.getOutputSymbol(path._fstArc));
        }
      }

      Set<Transition> stateTransitions = path._state.getTransitionSet();
      Iterator<Transition> iterator = stateTransitions.iterator();

      while (iterator.hasNext()) {
        Transition t = iterator.next();

        final int min = t._min;
        final int max = t._max;

        if (min == max) {
          int arc = _fst.getArc(path._node, (byte) t._min);

          if (arc != 0) {
            queue.add(new Path(t._to, _fst.getEndNode(arc), arc, path._pathState));
          }
        } else {
          if (path._fstArc > 0 && _fst.isArcTerminal(path._fstArc)) {
            continue;
          }

          int node;
          int arc;

          if (path._fstArc == 0) {
            // First (dummy) arc, get the actual arc
            arc = _fst.getFirstArc(path._node);
          } else {
            node = _fst.getEndNode(path._fstArc);
            arc = _fst.getFirstArc(node);
          }

          while (arc != 0) {
            byte label = _fst.getArcLabel(arc);

            if (label >= min && label <= max) {
              queue.add(new Path(t._to, _fst.getEndNode(arc), arc, path._pathState));
            }

            arc = _fst.isArcLast(arc) ? 0 : _fst.getNextArc(arc);
          }
        }
      }
    }
  }

  /**
   * Represents a path in the FST traversal directed by the automaton
   */
  public final class Path {
    public final State _state;
    public final int _node;
    public final int _fstArc;
    // Used for capturing the path taken till the point (for debugging)
    public List<Character> _pathState;

    public Path(State state, int node, int fstArc, List<Character> pathState) {
      this._state = state;
      this._node = node;
      this._fstArc = fstArc;

      this._pathState = pathState;

      this._pathState.add((char) _fst.getArcLabel(fstArc));
    }
  }
}