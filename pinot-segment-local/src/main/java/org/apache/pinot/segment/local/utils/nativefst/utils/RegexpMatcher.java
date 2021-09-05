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
import org.apache.pinot.segment.local.utils.nativefst.FSA;
import org.apache.pinot.segment.local.utils.nativefst.automaton.Automaton;
import org.apache.pinot.segment.local.utils.nativefst.automaton.RegExp;
import org.apache.pinot.segment.local.utils.nativefst.automaton.State;
import org.apache.pinot.segment.local.utils.nativefst.automaton.CharacterRunAutomaton;
import org.apache.pinot.segment.local.utils.nativefst.automaton.Transition;


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
  private final FSA _fsa;
  private final Automaton _automaton;

  public RegexpMatcher(String regexQuery, FSA fsa) {
    _regexQuery = regexQuery;
    _fsa = fsa;

    _automaton = new RegExp(_regexQuery).toAutomaton();
  }

  public static List<Long> regexMatch(String regexQuery, FSA fst) {
    RegexpMatcher matcher = new RegexpMatcher(regexQuery, fst);
    return matcher.regexMatchOnFST();
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
   * @return
   */
  public List<Long> regexMatchOnFST() {
    final List<Path> queue = new ArrayList<>();
    final List<Long> endNodes = new ArrayList<>();

    if (_automaton.getNumberOfStates() == 0) {
      return Collections.emptyList();
    }

    // Automaton start state and FST start node is added to the queue.
    queue.add(new Path( _automaton.getInitialState(), _fsa.getRootNode(), 0, -1, new ArrayList<>()));

    Set<State> acceptStates = _automaton.getAcceptStates();
    while (queue.size() != 0) {
      final Path path = queue.remove(queue.size() - 1);

      // If automaton is in accept state and the fstNode is final (i.e. end node) then add the entry to endNodes which
      // contains the result set.
      if (acceptStates.contains(path.state)) {
        if (_fsa.isArcFinal(path.fstArc)) {
          endNodes.add((long) _fsa.getOutputSymbol(path.fstArc));
        }
      }

      Set<Transition> stateTransitions = path.state.getTransitionSet();
      Iterator<Transition> iterator = stateTransitions.iterator();

      while (iterator.hasNext()){
        Transition t = iterator.next();

        final int min = t._min;
        final int max = t._max;

        if (min == max) {
          int arc = _fsa.getArc(path.node, (byte) t._min);

          if (arc != 0) {
            queue.add(new Path(t._to, _fsa.getEndNode(arc), arc, -1, path.pathState));
          }
        } else {
          if (path.fstArc > 0 && _fsa.isArcTerminal(path.fstArc)) {
            continue;
          }

          int node;
          int arc;

          if (path.fstArc == 0) {
            // First (dummy) arc, get the actual arc
            arc = _fsa.getFirstArc(path.node);
          } else {
            node = _fsa.getEndNode(path.fstArc);
            arc = _fsa.getFirstArc(node);
          }

          while (arc != 0) {
            byte label = _fsa.getArcLabel(arc);

            if (label >= min && label <= max) {
              queue.add(new Path(t._to, _fsa.getEndNode(arc), arc, -1, path.pathState));

            }

            arc = _fsa.isArcLast(arc) ? 0 : _fsa.getNextArc(arc);
          }
        }
      }
    }

    return endNodes;
  }

  public final class Path {
    public final State state;
    public final int node;
    public final int fstArc;
    public final int output;
    public List<Character> pathState;

    public Path(State state, int node, int fstArc, int output, List<Character> pathState) {
      this.state = state;
      this.node = node;
      this.fstArc = fstArc;
      this.output = output;

      this.pathState = pathState;

      this.pathState.add((char)_fsa.getArcLabel(fstArc));
    }
  }
}