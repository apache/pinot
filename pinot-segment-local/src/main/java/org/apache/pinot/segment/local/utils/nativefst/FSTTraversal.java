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
 * This class implements some common matching and scanning operations on a
 * generic FST.
 */
public final class FSTTraversal {
  /**
   * Target automaton.
   */
  private final FST _fst;

  /**
   * Traversals of the given FST.
   *
   * @param fst The target automaton for traversals.
   */
  public FSTTraversal(FST fst) {
    _fst = fst;
  }

  /**
   * Same as {@link #match(byte[], int, int, int)}, but allows passing
   * a reusable {@link MatchResult} object so that no intermediate garbage is
   * produced.
   *
   * @param reuse The {@link MatchResult} to reuse.
   * @param sequence Input sequence to look for in the automaton.
   * @param start Start index in the sequence array. 
   * @param length Length of the byte sequence, must be at least 1.
   * @param node The node to start traversal from, typically the {@linkplain FST#getRootNode() root node}.
   *
   * @return The same object as <code>reuse</code>, but with updated match {@link MatchResult#_kind}
   *        and other relevant fields.
   */
  public MatchResult match(MatchResult reuse, byte[] sequence, int start, int length, int node) {
    if (node == 0) {
      reuse.reset(MatchResult.NO_MATCH, start, node);
      return reuse;
    }

    final FST fst = _fst;
    final int end = start + length;
    for (int i = start; i < end; i++) {
      final int arc = fst.getArc(node, sequence[i]);
      if (arc != 0) {
        if (i + 1 == end && fst.isArcFinal(arc)) {
          /* The automaton has an exact match of the input sequence. */
          reuse.reset(MatchResult.EXACT_MATCH, i, node);
          return reuse;
        }

        if (fst.isArcTerminal(arc)) {
          /* The automaton contains a prefix of the input sequence. */
          reuse.reset(MatchResult.AUTOMATON_HAS_PREFIX, i + 1, node);
          return reuse;
        }

        // Make a transition along the arc.
        node = fst.getEndNode(arc);
      } else {
        if (i > start) {
          reuse.reset(MatchResult.AUTOMATON_HAS_PREFIX, i, node);
        } else {
          reuse.reset(MatchResult.NO_MATCH, i, node);
        }
        return reuse;
      }
    }

    /* The sequence is a prefix of at least one sequence in the automaton. */
    reuse.reset(MatchResult.SEQUENCE_IS_A_PREFIX, 0, node);
    return reuse;
  }

  /**
   * Finds a matching path in the dictionary for a given sequence of labels from
   * <code>sequence</code> and starting at node <code>node</code>.
   *
   * @param sequence Input sequence to look for in the automaton.
   * @param start Start index in the sequence array. 
   * @param length Length of the byte sequence, must be at least 1.
   * @param node The node to start traversal from, typically the {@linkplain FST#getRootNode() root node}.
   *
   * @return {@link MatchResult} with updated match {@link MatchResult#_kind}.
   */
  public MatchResult match(byte[] sequence, int start, int length, int node) {
    return match(new MatchResult(), sequence, start, length, node);
  }

  /**
   * @param sequence Input sequence to look for in the automaton.
   * @param node The node to start traversal from, typically the {@linkplain FST#getRootNode() root node}.
   *
   * @return {@link MatchResult} with updated match {@link MatchResult#_kind}.
   */
  public MatchResult match(byte[] sequence, int node) {
    return match(sequence, 0, sequence.length, node);
  }

  /**
   * @param sequence Input sequence to look for in the automaton.
   *
   * @return {@link MatchResult} with updated match {@link MatchResult#_kind}.
   */
  public MatchResult match(byte[] sequence) {
    return match(sequence, _fst.getRootNode());
  }
}
