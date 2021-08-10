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
package org.apache.pinot.fsa.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.fsa.FSA;
import org.apache.pinot.fsa.FSATraversal;
import org.apache.pinot.fsa.automaton.Automaton;
import org.apache.pinot.fsa.automaton.CharacterRunAutomaton;
import org.apache.pinot.fsa.automaton.RegExp;
import org.apache.pinot.fsa.automaton.State;
import org.apache.pinot.fsa.automaton.Transition;


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
  private final FSA _fst;
  private final Automaton _automaton;

  public RegexpMatcher(String regexQuery, FSA fst) {
    _regexQuery = regexQuery;
    _fst = fst;

    _automaton = new RegExp(_regexQuery).toAutomaton();
  }

  public static List<Long> regexMatch(String regexQuery, FSA fst)
      throws IOException {
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
   * @throws IOException
   */
  public List<Long> regexMatchOnFST()
      throws IOException {
    final List<Path> queue = new ArrayList<>();
    final List<Path> endNodes = new ArrayList<>();
    final FSATraversal matcher = new FSATraversal(_fst);

    if (_automaton.getNumberOfStates() == 0) {
      return Collections.emptyList();
    }

    // Automaton start state and FST start node is added to the queue.
    queue.add(new Path( _automaton.getInitialState(), _fst.getRootNode(), 0, -1));

/*
    final FSA.Arc<Long> scratchArc = new FST.Arc<>();
    final FST.BytesReader fstReader = _fst.getBytesReader();

    Transition t = new Transition(); */

    Set<State> acceptStates = _automaton.getAcceptStates();
    while (queue.size() != 0) {
      final Path path = queue.remove(queue.size() - 1);

      // If automaton is in accept state and the fstNode is final (i.e. end node) then add the entry to endNodes which
      // contains the result set.
      if (acceptStates.contains(path.state)) {
        if (_fst.isArcFinal(path.fstArc)) {
          //TODO: atri
          System.out.println("DOING IT " + path.fstArc + " " + path.node + " " + path.state);

          endNodes.add(path);
        }
      }

      Set<Transition> stateTransitions = path.state.getTransitions();
      Iterator<Transition> iterator = stateTransitions.iterator();

      while (iterator.hasNext()){
        Transition t = iterator.next();

        final int min = t.min;
        final int max = t.max;

        if (min == max) {
          int arc = _fst.getArc(path.node, (byte) t.min);

          //TODO: atri
          System.out.println("ARC IS " + arc + " FOR ARC " + path.fstArc);

          if (arc != 0) {
            //TODO: atri -- see why output symbols are missing and fix it
            queue.add(new Path(t.to, _fst.getEndNode(arc), arc,-1));
          }
        } else {
          int arc = _fst.getArc(path.node, (byte) min);
          while (arc != 0 && _fst.getArcLabel(arc) <= max) {
            //TODO: atri -- see why output symbols are missing and fix it
            queue.add(new Path(t.to, _fst.getEndNode(arc), arc, -1));
            arc = _fst.getNextArc(arc);
          }
        }
      }
    }

    // From the result set of matched entries gather the values stored and return.
    ArrayList<Long> matchedIds = new ArrayList<>();
    for (Path path : endNodes) {
      matchedIds.add(new Long(path.output));
    }

    return matchedIds;
  }

  /**
  private Map<Integer, State> getStateMap(Set<State> states) {
    Map<Integer, State> stateMap = new HashMap<>();
    Iterator iterator = states.iterator();

    while (iterator.hasNext()) {
      State state = (State) iterator.next();

      stateMap.put(state.)
    }
  }*/

  public static final class Path {
    public final State state;
    public final int node;
    public final int fstArc;
    public final int output;

    public Path(State state, int node, int fstArc, int output) {
      this.state = state;
      this.node = node;
      this.fstArc = fstArc;
      this.output = output;
    }
  }
}