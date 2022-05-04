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
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;


/**
 * Operations for building minimal deterministic automata from sets of strings.
 * The algorithm requires sorted input data, but is very fast (nearly linear with the input size).
 *
 * NOTE: This class is not used today but is a fast way of building automata so is kept for future uses
 * of the library
 */
public final class StringUnionOperations {

  /**
   * Lexicographic order of input sequences.
   */
  public final static Comparator<CharSequence> LEXICOGRAPHIC_ORDER = (s1, s2) -> {
    final int lens1 = s1.length();
    final int lens2 = s2.length();
    final int max = Math.min(lens1, lens2);

    for (int i = 0; i < max; i++) {
      final char c1 = s1.charAt(i);
      final char c2 = s2.charAt(i);
      if (c1 != c2) {
        return c1 - c2;
      }
    }
    return lens1 - lens2;
  };
  /**
   * "register" for state interning.
   */
  private HashMap<StateWithTransitionLabels, StateWithTransitionLabels> _register = new HashMap<>();
  /**
   * Root automaton state.
   */
  private StateWithTransitionLabels _root = new StateWithTransitionLabels();
  /**
   * Previous sequence added to the automaton in {@link #add(CharSequence)}.
   */
  private StringBuilder _previous;

  /**
   * Internal recursive traversal for conversion.
   */
  private static State convert(StateWithTransitionLabels s, IdentityHashMap<StateWithTransitionLabels, State> visited) {
    State converted = visited.get(s);
    if (converted != null) {
      return converted;
    }

    converted = new State();
    converted.setAccept(s._isFinal);

    visited.put(s, converted);
    int i = 0;
    char[] labels = s._labels;
    for (StateWithTransitionLabels target : s._stateWithTransitionLables) {
      converted.addTransition(new Transition(labels[i++], convert(target, visited)));
    }

    return converted;
  }

  /**
   * Build a minimal, deterministic automaton from a sorted list of strings.
   */
  public static State build(CharSequence[] input) {
    final StringUnionOperations builder = new StringUnionOperations();

    for (CharSequence chs : input) {
      builder.add(chs);
    }

    return convert(builder.complete(), new IdentityHashMap<>());
  }

  /**
   * Add another character sequence to this automaton. The sequence must be
   * lexicographically larger or equal compared to any previous sequences
   * added to this automaton (the input must be sorted).
   */
  public void add(CharSequence current) {
    assert _register != null : "Automaton already built.";
    assert current.length() > 0 : "Input sequences must not be empty.";
    assert
        _previous == null || LEXICOGRAPHIC_ORDER.compare(_previous, current) <= 0
        : "Input must be sorted: " + _previous + " >= " + current;
    assert setPrevious(current);

    // Descend in the automaton (find matching prefix).
    int pos = 0;
    int max = current.length();
    StateWithTransitionLabels next;
    StateWithTransitionLabels stateWithTransitionLabels = _root;
    while (pos < max && (next = stateWithTransitionLabels.lastChild(current.charAt(pos))) != null) {
      stateWithTransitionLabels = next;
      pos++;
    }

    if (stateWithTransitionLabels.hasChildren()) {
      replaceOrRegister(stateWithTransitionLabels);
    }

    addSuffix(stateWithTransitionLabels, current, pos);
  }

  /**
   * Finalize the automaton and return the root state. No more strings can be
   * added to the builder after this call.
   *
   * @return Root automaton state.
   */
  public StateWithTransitionLabels complete() {
    if (_register == null) {
      throw new IllegalStateException();
    }

    if (_root.hasChildren()) {
      replaceOrRegister(_root);
    }

    _register = null;
    return _root;
  }

  /**
   * Copy <code>current</code> into an internal buffer.
   */
  private boolean setPrevious(CharSequence current) {
    if (_previous == null) {
      _previous = new StringBuilder();
    }

    _previous.setLength(0);
    _previous.append(current);

    return true;
  }

  /**
   * Replace last child of <code>state</code> with an already registered
   * state or register the last child state.
   */
  private void replaceOrRegister(StateWithTransitionLabels stateWithTransitionLabels) {
    final StateWithTransitionLabels child = stateWithTransitionLabels.lastChild();

    if (child.hasChildren()) {
      replaceOrRegister(child);
    }

    final StateWithTransitionLabels registered = _register.get(child);
    if (registered != null) {
      stateWithTransitionLabels.replaceLastChild(registered);
    } else {
      _register.put(child, child);
    }
  }

  /**
   * Add a suffix of <code>current</code> starting at <code>fromIndex</code>
   * (inclusive) to state <code>state</code>.
   */
  private void addSuffix(StateWithTransitionLabels stateWithTransitionLabels, CharSequence current, int fromIndex) {
    final int len = current.length();
    for (int i = fromIndex; i < len; i++) {
      stateWithTransitionLabels = stateWithTransitionLabels.newState(current.charAt(i));
    }
    stateWithTransitionLabels._isFinal = true;
  }

  /**
   * State with <code>char</code> labels on transitions.
   */
  final static class StateWithTransitionLabels {

    /** An empty set of labels. */
    private final static char[] NO_LABELS = new char[0];

    /** An empty set of states. */
    private final static StateWithTransitionLabels[] NO_STATE_WITH_TRANSITION_LABLES = new StateWithTransitionLabels[0];

    /**
     * Labels of outgoing transitions. Indexed identically to {@link #_stateWithTransitionLables}.
     * Labels must be sorted lexicographically.
     */
    char[] _labels = NO_LABELS;

    /**
     * States reachable from outgoing transitions. Indexed identically to
     * {@link #_labels}.
     */
    StateWithTransitionLabels[] _stateWithTransitionLables = NO_STATE_WITH_TRANSITION_LABLES;

    /**
     * <code>true</code> if this state corresponds to the end of at least one
     * input sequence.
     */
    boolean _isFinal;

    /**
     * JDK1.5-replacement of {@link Arrays#copyOf(char[], int)}
     */
    private static char[] copyOf(char[] original, int newLength) {
      char[] copy = new char[newLength];
      System.arraycopy(original, 0, copy, 0, Math.min(original.length, newLength));
      return copy;
    }

    /**
     * JDK1.5-replacement of {@link Arrays#copyOf(char[], int)}
     */
    public static StateWithTransitionLabels[] copyOf(StateWithTransitionLabels[] original, int newLength) {
      StateWithTransitionLabels[] copy = new StateWithTransitionLabels[newLength];
      System.arraycopy(original, 0, copy, 0, Math.min(original.length, newLength));
      return copy;
    }

    /**
     * Compare two lists of objects for reference-equality.
     */
    private static boolean referenceEquals(Object[] a1, Object[] a2) {
      if (a1.length != a2.length) {
        return false;
      }

      for (int i = 0; i < a1.length; i++) {
        if (a1[i] != a2[i]) {
          return false;
        }
      }

      return true;
    }

    /**
     * Returns the target state of a transition leaving this state and labeled
     * with <code>label</code>. If no such transition exists, returns
     * <code>null</code>.
     */
    public StateWithTransitionLabels getState(char label) {
      final int index = Arrays.binarySearch(_labels, label);
      return index >= 0 ? _stateWithTransitionLables[index] : null;
    }

    /**
     * Two states are equal if:
     * <ul>
     * <li>they have an identical number of outgoing transitions, labeled with
     * the same labels</li>
     * <li>corresponding outgoing transitions lead to the same states (to states
     * with an identical right-language).
     * </ul>
     */
    @Override
    public boolean equals(Object obj) {
      final StateWithTransitionLabels other = (StateWithTransitionLabels) obj;
      return _isFinal == other._isFinal && Arrays.equals(_labels, other._labels) && referenceEquals(
          _stateWithTransitionLables, other._stateWithTransitionLables);
    }

    /**
     * Return <code>true</code> if this state has any children (outgoing
     * transitions).
     */
    public boolean hasChildren() {
      return _labels.length > 0;
    }

    /**
     * Is this state a final state in the automaton?
     */
    public boolean isFinal() {
      return _isFinal;
    }

    /**
     * Compute the hash code of the <i>current</i> status of this state.
     */
    @Override
    public int hashCode() {
      int hash = _isFinal ? 1 : 0;

      hash ^= hash * 31 + _labels.length;
      for (char c : _labels) {
        hash ^= hash * 31 + c;
      }

      /*
       * Compare the right-language of this state using reference-identity of
       * outgoing states. This is possible because states are interned (stored
       * in registry) and traversed in post-order, so any outgoing transitions
       * are already interned.
       */
      for (StateWithTransitionLabels s : _stateWithTransitionLables) {
        hash ^= System.identityHashCode(s);
      }

      return hash;
    }

    /**
     * Create a new outgoing transition labeled <code>label</code> and return
     * the newly created target state for this transition.
     */
    StateWithTransitionLabels newState(char label) {
      assert Arrays.binarySearch(_labels, label) < 0 : "State already has transition labeled: " + label;

      _labels = copyOf(_labels, _labels.length + 1);
      _stateWithTransitionLables = copyOf(_stateWithTransitionLables, _stateWithTransitionLables.length + 1);

      _labels[_labels.length - 1] = label;
      _stateWithTransitionLables[_stateWithTransitionLables.length - 1] = new StateWithTransitionLabels();

      return _stateWithTransitionLables[_stateWithTransitionLables.length - 1];
    }

    /**
     * Return the most recent transitions's target state.
     */
    StateWithTransitionLabels lastChild() {
      assert hasChildren() : "No outgoing transitions.";
      return _stateWithTransitionLables[_stateWithTransitionLables.length - 1];
    }

    /**
     * Return the associated state if the most recent transition
     * is labeled with <code>label</code>.
     */
    StateWithTransitionLabels lastChild(char label) {
      final int index = _labels.length - 1;
      StateWithTransitionLabels s = null;
      if (index >= 0 && _labels[index] == label) {
        s = _stateWithTransitionLables[index];
      }
      assert s == getState(label);
      return s;
    }

    /**
     * Replace the last added outgoing transition's target state with the given
     * state.
     */
    void replaceLastChild(StateWithTransitionLabels stateWithTransitionLabels) {
      assert hasChildren() : "No outgoing transitions.";
      _stateWithTransitionLables[_stateWithTransitionLables.length - 1] = stateWithTransitionLabels;
    }
  }
}
