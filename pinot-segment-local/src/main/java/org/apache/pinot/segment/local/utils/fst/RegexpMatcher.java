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
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;
import org.apache.pinot.spi.query.QueryThreadContext;


/// RegexpMatcher is a helper to retrieve matching values for a given regexp query.
/// Regexp query is converted into an automaton, and we run the matching algorithm on FST.
///
/// Two main functions of this class are
///   regexMatchOnFST() Function runs matching on FST (See function comments for more details)
///   match(input) Function builds the automaton and matches given input.
///
/// This class is not thread-safe. Create a new instance (or use the static helper) per query.
public class RegexpMatcher {
  private final FST<Long> _fst;
  private final Automaton _automaton;

  public RegexpMatcher(String regexQuery, FST<Long> fst) {
    _fst = fst;
    _automaton = (new RegExp(regexQuery)).toAutomaton();
  }

  /// Runs matching of the given regexp query on the FST, emitting the value (dict id) of each matched entry to the
  /// given consumer. Values are emitted as the traversal hits final states, so the caller can collect them without
  /// this class materializing the full result set in memory.
  public static void regexMatch(String regexQuery, FST<Long> fst, IntConsumer dictIdConsumer)
      throws IOException {
    new RegexpMatcher(regexQuery, fst).regexMatchOnFST(dictIdConsumer);
  }

  // Matches "input" string with _regexQuery Automaton.
  public boolean match(String input) {
    CharacterRunAutomaton characterRunAutomaton = new CharacterRunAutomaton(_automaton);
    return characterRunAutomaton.run(input);
  }

  /// This function runs matching on automaton built from regexQuery and the FST.
  /// FST stores key (string) to a value (Long). Both are state machines and state transition is based on an input
  /// character.
  ///
  /// This algorithm starts with a stack containing (Automaton Start Node, FST Start Node).
  /// Each step an entry is popped from the stack (DFS order to bound the frontier size):
  ///   1) if the automaton state is accept and the FST Node is final (i.e. end node) then the value stored for that
  ///      FST node is emitted to the consumer.
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
    List<Path<Long>> stack = new ArrayList<>();
    stack.add(new Path<>(0, _fst.getFirstArc(new FST.Arc<>()), _fst.outputs.getNoOutput()));

    FST.Arc<Long> scratchArc = new FST.Arc<>();
    FST.BytesReader fstReader = _fst.getBytesReader();

    Transition t = new Transition();
    int numPathsProcessed = 0;
    while (!stack.isEmpty()) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numPathsProcessed++,
          "RegexpMatcher#regexMatchOnFST");
      Path<Long> path = stack.remove(stack.size() - 1);

      // If automaton is in accept state and the fstNode is final (i.e. end node) then emit the matched value.
      if (_automaton.isAccept(path._state) && path._fstNode.isFinal()) {
        dictIdConsumer.accept(path._output.intValue());
      }

      // Gather next set of transitions on automaton and find target nodes in FST.
      int count = _automaton.initTransition(path._state, t);
      for (int i = 0; i < count; i++) {
        _automaton.getNextTransition(t);
        int min = t.min;
        int max = t.max;
        if (min == max) {
          FST.Arc<Long> nextArc = _fst.findTargetArc(t.min, path._fstNode, scratchArc, fstReader);
          if (nextArc != null) {
            stack.add(new Path<>(t.dest, new FST.Arc<Long>().copyFrom(nextArc),
                _fst.outputs.add(path._output, nextArc.output())));
          }
        } else {
          FST.Arc<Long> nextArc = Util.readCeilArc(min, _fst, path._fstNode, scratchArc, fstReader);
          while (nextArc != null && nextArc.label() <= max) {
            stack.add(new Path<>(t.dest, new FST.Arc<Long>().copyFrom(nextArc),
                _fst.outputs.add(path._output, nextArc.output())));
            nextArc = nextArc.isLast() ? null : _fst.readNextRealArc(nextArc, fstReader);
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
