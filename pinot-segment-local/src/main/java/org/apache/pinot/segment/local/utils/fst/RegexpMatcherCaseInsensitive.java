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
import java.util.List;
import java.util.function.IntConsumer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;
import org.apache.pinot.spi.query.QueryThreadContext;


/// RegexpMatcherCaseInsensitive is a specialized helper to retrieve matching values for case-insensitive regexp
/// queries. This class handles only case-insensitive matching using `FST<BytesRef>` type.
///
/// The regexp query is converted to lowercase and then into an automaton for matching against the FST.
/// This class is separate from RegexpMatcher because case-insensitive logic is fundamentally different
/// from case-sensitive logic and doesn't share code.
///
/// This class is not thread-safe. Create a new instance (or use the static helper) per query.
public class RegexpMatcherCaseInsensitive {
  private final FST<BytesRef> _ifst;
  private final Automaton _automaton;

  public RegexpMatcherCaseInsensitive(String regexQuery, FST<BytesRef> ifst) {
    _ifst = ifst;
    // For case-insensitive FSTs, convert regex to lowercase since we only store lowercase keys
    _automaton = (new RegExp(regexQuery.toLowerCase())).toAutomaton();
  }

  /// Runs case-insensitive matching of the given regexp query on the FST, emitting the values (dict ids) of each
  /// matched entry to the given consumer. Values are emitted as the traversal hits final states, so the caller can
  /// collect them without this class materializing the full result set in memory.
  public static void regexMatch(String regexQuery, FST<BytesRef> ifst, IntConsumer dictIdConsumer)
      throws IOException {
    new RegexpMatcherCaseInsensitive(regexQuery, ifst).regexMatchOnFST(dictIdConsumer);
  }

  // Matches "input" string with _regexQuery Automaton (case-insensitive).
  public boolean match(String input) {
    CharacterRunAutomaton characterRunAutomaton = new CharacterRunAutomaton(_automaton);
    return characterRunAutomaton.run(input.toLowerCase());
  }

  /// This function runs case-insensitive matching on automaton built from regexQuery and the FST.
  /// FST stores key (string) to a value (BytesRef). Both are state machines and state transition is based on an input
  /// character.
  ///
  /// This algorithm starts with a stack containing (Automaton Start Node, FST Start Node).
  /// Each step an entry is popped from the stack (DFS order to bound the frontier size):
  ///   1) if the automaton state is accept and the FST Node is final (i.e. end node) then the values stored for that
  ///      FST node are emitted to the consumer.
  ///   2) Else next set of transitions on automaton are gathered and for each transition target node for that
  ///      character is figured out in FST Node, resulting pair of (automaton state, fst node) are added to the stack.
  ///   3) This process is bound to complete since we are making progression on the FST (which is a DAG) towards final
  ///      nodes.
  ///
  /// A broad regexp on a high-cardinality column can visit a huge number of paths, so the loop periodically checks
  /// for query termination (timeout or OOM-protection kill) to remain interruptible.
  public void regexMatchOnFST(IntConsumer dictIdConsumer)
      throws IOException {
    if (_automaton.getNumStates() == 0) {
      return;
    }

    // Automaton start state and FST start node is added to the stack.
    List<Path<BytesRef>> stack = new ArrayList<>();
    stack.add(new Path<>(0, _ifst.getFirstArc(new FST.Arc<>()), _ifst.outputs.getNoOutput()));

    FST.Arc<BytesRef> scratchArc = new FST.Arc<>();
    FST.BytesReader fstReader = _ifst.getBytesReader();

    Transition t = new Transition();
    int numPathsProcessed = 0;
    while (!stack.isEmpty()) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numPathsProcessed++,
          "RegexpMatcherCaseInsensitive#regexMatchOnFST");
      Path<BytesRef> path = stack.remove(stack.size() - 1);

      // If automaton is in accept state and the fstNode is final (i.e. end node) then emit the matched values.
      if (_automaton.isAccept(path._state) && path._fstNode.isFinal()) {
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
        // Deserialize BytesRef to the list of matched dict ids
        for (Integer value : IFSTBuilder.deserializeBytesRefToIntegerList(completeOutput)) {
          dictIdConsumer.accept(value);
        }
      }

      // Gather next set of transitions on automaton and find target nodes in FST.
      int count = _automaton.initTransition(path._state, t);
      for (int i = 0; i < count; i++) {
        _automaton.getNextTransition(t);
        int min = t.min;
        int max = t.max;
        if (min == max) {
          FST.Arc<BytesRef> nextArc = _ifst.findTargetArc(t.min, path._fstNode, scratchArc, fstReader);
          if (nextArc != null) {
            stack.add(new Path<>(t.dest, new FST.Arc<BytesRef>().copyFrom(nextArc),
                _ifst.outputs.add(path._output, nextArc.output())));
          }
        } else {
          FST.Arc<BytesRef> nextArc = Util.readCeilArc(min, _ifst, path._fstNode, scratchArc, fstReader);
          while (nextArc != null && nextArc.label() <= max) {
            stack.add(new Path<>(t.dest, new FST.Arc<BytesRef>().copyFrom(nextArc),
                _ifst.outputs.add(path._output, nextArc.output())));
            nextArc = nextArc.isLast() ? null : _ifst.readNextRealArc(nextArc, fstReader);
          }
        }
      }
    }
  }

  public static final class Path<T> {
    public final int _state;
    public final FST.Arc<T> _fstNode;
    public final T _output;

    public Path(int state, FST.Arc<T> fstNode, T output) {
      _state = state;
      _fstNode = fstNode;
      _output = output;
    }
  }
}
