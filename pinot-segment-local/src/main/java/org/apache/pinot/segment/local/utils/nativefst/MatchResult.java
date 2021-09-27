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
package org.apache.pinot.segment.local.utils.nativefst;

/**
 * A matching result returned from {@link FSTTraversal}.
 *
 * @see FSTTraversal
 */
public final class MatchResult {
  /**
   * The automaton has exactly one match for the input sequence.
   */
  public static final int EXACT_MATCH = 0;

  /**
   * The automaton has no match for the input sequence and no sequence
   * in the automaton is a prefix of the input.
   *
   * Note that to check for a general "input does not exist in the automaton"
   * you have to check for both {@link #NO_MATCH} and
   * {@link #AUTOMATON_HAS_PREFIX}.
   */
  public static final int NO_MATCH = -1;

  /**
   * The automaton contains a prefix of the input sequence (but the
   * full sequence does not exist). This translates to: one of the input sequences
   * used to build the automaton is a prefix of the input sequence, but the
   * input sequence contains a non-existent suffix.
   *
   * <p>{@link MatchResult#_index} will contain an index of the
   * first character of the input sequence not present in the
   * dictionary.</p>
   */
  public static final int AUTOMATON_HAS_PREFIX = -3;

  /**
   * The sequence is a prefix of at least one sequence in the automaton.
   * {@link MatchResult#_node} returns the node from which all sequences
   * with the given prefix start in the automaton.
   */
  public static final int SEQUENCE_IS_A_PREFIX = -4;

  /**
   * One of the match types defined in this class.
   *
   * @see #NO_MATCH
   * @see #EXACT_MATCH
   * @see #AUTOMATON_HAS_PREFIX
   * @see #SEQUENCE_IS_A_PREFIX
   */
  public int _kind;

  /**
   * Input sequence's index, interpretation depends on {@link #_kind}.
   */
  public int _index;

  /**
   * Automaton node, interpretation depends on the {@link #_kind}.
   */
  public int _node;

  public MatchResult() {
    reset(NO_MATCH, 0, 0);
  }

  final void reset(int kind, int index, int node) {
    _kind = kind;
    _index = index;
    _node = node;
  }
}
