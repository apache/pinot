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

package org.apache.pinot.segment.local.utils.nativefst.automaton;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;


/**
 * Automata operations involving shuffling.
 */
final public class ShuffleOperations {

  private ShuffleOperations() {
  }

  /**
   * Returns an automaton that accepts the shuffle (interleaving) of
   * the languages of the given automata.
   * As a side-effect, both automata are determinized, if not already deterministic.
   * Never modifies the input automata languages.
   * <p>
   * Complexity: quadratic in number of states (if already deterministic).
   * <p>
   * <dl><dt><b>Author:</b></dt><dd>Torben Ruby
   * &lt;<a href="mailto:ruby@daimi.au.dk">ruby@daimi.au.dk</a>&gt;</dd></dl>
   */
  public static Automaton shuffle(Automaton a1, Automaton a2) {
    a1.determinize();
    a2.determinize();
    Transition[][] transitions1 = Automaton.getSortedTransitions(a1.getStates());
    Transition[][] transitions2 = Automaton.getSortedTransitions(a2.getStates());
    Automaton c = new Automaton();
    LinkedList<StatePair> worklist = new LinkedList<StatePair>();
    HashMap<StatePair, StatePair> newstates = new HashMap<StatePair, StatePair>();
    State s = new State();
    c._initial = s;
    StatePair p = new StatePair(s, a1._initial, a2._initial);
    worklist.add(p);
    newstates.put(p, p);
    while (worklist.size() > 0) {
      p = worklist.removeFirst();
      p._parentState._accept = p._firstState._accept && p._secondState._accept;
      Transition[] t1 = transitions1[p._firstState._number];
      for (int n1 = 0; n1 < t1.length; n1++) {
        StatePair q = new StatePair(t1[n1]._to, p._secondState);
        StatePair r = newstates.get(q);
        if (r == null) {
          q._parentState = new State();
          worklist.add(q);
          newstates.put(q, q);
          r = q;
        }
        p._parentState._transitionSet.add(new Transition(t1[n1]._min, t1[n1]._max, r._parentState));
      }
      Transition[] t2 = transitions2[p._secondState._number];
      for (int n2 = 0; n2 < t2.length; n2++) {
        StatePair q = new StatePair(p._firstState, t2[n2]._to);
        StatePair r = newstates.get(q);
        if (r == null) {
          q._parentState = new State();
          worklist.add(q);
          newstates.put(q, q);
          r = q;
        }
        p._parentState._transitionSet.add(new Transition(t2[n2]._min, t2[n2]._max, r._parentState));
      }
    }
    c._deterministic = false;
    c.removeDeadTransitions();
    c.checkMinimizeAlways();
    return c;
  }

  private static void add(Character suspend_shuffle, Character resume_shuffle, LinkedList<ShuffleConfiguration> pending,
      Set<ShuffleConfiguration> visited, ShuffleConfiguration c, int i1, Transition t1, Transition t2, char min,
      char max) {
    final char HIGH_SURROGATE_BEGIN = '\uD800';
    final char HIGH_SURROGATE_END = '\uDBFF';
    if (suspend_shuffle != null && min <= suspend_shuffle && suspend_shuffle <= max && min != max) {
      if (min < suspend_shuffle) {
        add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, min, (char) (suspend_shuffle - 1));
      }
      add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, suspend_shuffle, suspend_shuffle);
      if (suspend_shuffle < max) {
        add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, (char) (suspend_shuffle + 1), max);
      }
    } else if (resume_shuffle != null && min <= resume_shuffle && resume_shuffle <= max && min != max) {
      if (min < resume_shuffle) {
        add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, min, (char) (resume_shuffle - 1));
      }
      add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, resume_shuffle, resume_shuffle);
      if (resume_shuffle < max) {
        add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, (char) (resume_shuffle + 1), max);
      }
    } else if (min < HIGH_SURROGATE_BEGIN && max >= HIGH_SURROGATE_BEGIN) {
      add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, min, (char) (HIGH_SURROGATE_BEGIN - 1));
      add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, HIGH_SURROGATE_BEGIN, max);
    } else if (min <= HIGH_SURROGATE_END && max > HIGH_SURROGATE_END) {
      add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, min, HIGH_SURROGATE_END);
      add(suspend_shuffle, resume_shuffle, pending, visited, c, i1, t1, t2, (char) (HIGH_SURROGATE_END + 1), max);
    } else {
      ShuffleConfiguration nc = new ShuffleConfiguration(c, i1, t1._to, t2._to, min);
      if (suspend_shuffle != null && min == suspend_shuffle) {
        nc._shuffleSuspended = true;
        nc._suspended = i1;
      } else if (resume_shuffle != null && min == resume_shuffle) {
        nc._shuffleSuspended = false;
      }
      if (min >= HIGH_SURROGATE_BEGIN && min <= HIGH_SURROGATE_BEGIN) {
        nc._shuffleSuspended = true;
        nc._suspended = i1;
        nc._surrogate = true;
      }
      if (!visited.contains(nc)) {
        pending.add(nc);
        visited.add(nc);
      }
    }
  }

  static class ShuffleConfiguration {

    ShuffleConfiguration _prev;
    State[] _caStates;
    State _aState;
    char _min;
    int _hash;
    boolean _shuffleSuspended;
    boolean _surrogate;
    int _suspended;

    @SuppressWarnings("unused")
    private ShuffleConfiguration() {
    }

    ShuffleConfiguration(ShuffleConfiguration c, int i1, State s1, State s2, char min) {
      _prev = c;
      _caStates = c._caStates.clone();
      _aState = c._aState;
      _caStates[i1] = s1;
      _aState = s2;
      this._min = min;
      if (!_surrogate) {
        _shuffleSuspended = c._shuffleSuspended;
        _suspended = c._suspended;
      }
      computeHash();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ShuffleConfiguration) {
        ShuffleConfiguration c = (ShuffleConfiguration) obj;
        return _shuffleSuspended == c._shuffleSuspended && _surrogate == c._surrogate && _suspended == c._suspended
            && Arrays.equals(_caStates, c._caStates) && _aState == c._aState;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return _hash;
    }

    private void computeHash() {
      _hash = 0;
      for (int i = 0; i < _caStates.length; i++) {
        _hash ^= _caStates[i].hashCode();
      }
      _hash ^= _aState.hashCode() * 100;
      if (_shuffleSuspended || _surrogate) {
        _hash += _suspended;
      }
    }
  }
}
