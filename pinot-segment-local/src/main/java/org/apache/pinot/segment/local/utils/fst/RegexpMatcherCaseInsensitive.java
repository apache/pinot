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
package org.apache.pinot.segment.local.utils.fst;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.FST;


/**
 * RegexpMatcherCaseInsensitive is a specialized helper to retrieve matching values for case-insensitive regexp queries.
 * This class handles only case-insensitive matching using FST<BytesRef> type.
 *
 * The regexp query is converted to lowercase and then into an automaton for matching against the FST.
 * This class is separate from RegexpMatcher because case-insensitive logic is fundamentally different
 * from case-sensitive logic and doesn't share code.
 */
public class RegexpMatcherCaseInsensitive {
  private final FST<BytesRef> _ifst;
  private final Automaton _automaton;

  public RegexpMatcherCaseInsensitive(String regexQuery, FST<BytesRef> ifst) {
    _ifst = ifst;
    // For case-insensitive FSTs, convert regex to lowercase since we only store lowercase keys
    _automaton = (new RegExp(regexQuery.toLowerCase())).toAutomaton();
  }

  public static List<Long> regexMatch(String regexQuery, FST<BytesRef> ifst)
      throws IOException {
    return new RegexpMatcherCaseInsensitive(regexQuery, ifst).regexMatchOnFST();
  }

  // Matches "input" string with _regexQuery Automaton (case-insensitive).
  public boolean match(String input) {
    CharacterRunAutomaton characterRunAutomaton = new CharacterRunAutomaton(_automaton);
    return characterRunAutomaton.run(input.toLowerCase());
  }

  /**
   * This function runs case-insensitive matching on automaton built from regexQuery and the FST.
   * FST stores key (string) to a value (BytesRef). Both are state machines and state transition is based on
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
   * @return List of matched IDs
   * @throws IOException
   */
  public List<Long> regexMatchOnFST()
      throws IOException {
    final List<Path<BytesRef>> queue = new ArrayList<>();
    final List<Path<BytesRef>> endNodes = new ArrayList<>();
    if (_automaton.getNumStates() == 0) {
      return Collections.emptyList();
    }

    // Automaton start state and FST start node is added to the queue.
    queue.add(
        new Path<>(0, _ifst.getFirstArc(new FST.Arc<BytesRef>()), _ifst.outputs.getNoOutput(), new IntsRefBuilder()));

    final FST.Arc<BytesRef> scratchArc = new FST.Arc<>();
    final FST.BytesReader fstReader = _ifst.getBytesReader();

    Transition t = new Transition();
    while (!queue.isEmpty()) {
      final Path<BytesRef> path = queue.remove(queue.size() - 1);

      // If automaton is in accept state and the fstNode is final (i.e. end node) then add the entry to endNodes which
      // contains the result set.
      if (_automaton.isAccept(path._state)) {
        if (path._fstNode.isFinal()) {
          // For final nodes, we need to combine the accumulated output with the final output
          BytesRef finalOutput = path._fstNode.nextFinalOutput();
          BytesRef completeOutput;
          if (finalOutput != null && finalOutput.length > 0) {
            // Combine accumulated output with final output
            completeOutput = _ifst.outputs.add(path._output, finalOutput);
          } else {
            // Use the accumulated output if no final output
            completeOutput = path._output;
          }
          // Create a new path with the complete output
          endNodes.add(new Path<>(path._state, path._fstNode, completeOutput, path._input));
        }
      }

      // Gather next set of transitions on automaton and find target nodes in FST.
      IntsRefBuilder currentInput = path._input;
      int count = _automaton.initTransition(path._state, t);
      for (int i = 0; i < count; i++) {
        _automaton.getNextTransition(t);
        final int min = t.min;
        final int max = t.max;
        if (min == max) {
          final FST.Arc<BytesRef> nextArc = _ifst.findTargetArc(t.min, path._fstNode, scratchArc, fstReader);
          if (nextArc != null) {
            final IntsRefBuilder newInput = new IntsRefBuilder();
            newInput.copyInts(currentInput.get());
            newInput.append(t.min);
            queue.add(new Path<BytesRef>(t.dest, new FST.Arc<BytesRef>().copyFrom(nextArc),
                _ifst.outputs.add(path._output, nextArc.output()), newInput));
          }
        } else {
          FST.Arc<BytesRef> nextArc =
              org.apache.lucene.util.fst.Util.readCeilArc(min, _ifst, path._fstNode, scratchArc, fstReader);
          while (nextArc != null && nextArc.label() <= max) {
            final IntsRefBuilder newInput = new IntsRefBuilder();
            newInput.copyInts(currentInput.get());
            newInput.append(nextArc.label());
            queue.add(new Path<>(t.dest, new FST.Arc<BytesRef>().copyFrom(nextArc),
                _ifst.outputs.add(path._output, nextArc.output()), newInput));
            nextArc = nextArc.isLast() ? null : _ifst.readNextRealArc(nextArc, fstReader);
          }
        }
      }
    }

    // From the result set of matched entries gather the values stored and return.
    ArrayList<Long> matchedIds = new ArrayList<>();
    for (Path<BytesRef> path : endNodes) {
      // Deserialize BytesRef to List<Integer> and convert to List<Long>
      List<Integer> intValues = IFSTBuilder.deserializeBytesRefToIntegerList(path._output);
      for (Integer value : intValues) {
        matchedIds.add(value.longValue());
      }
    }
    return matchedIds;
  }

  public static final class Path<T> {
    public final int _state;
    public final FST.Arc<T> _fstNode;
    public final T _output;
    public final IntsRefBuilder _input;

    public Path(int state, FST.Arc<T> fstNode, T output, IntsRefBuilder input) {
      _state = state;
      _fstNode = fstNode;
      _output = output;
      _input = input;
    }
  }
}
