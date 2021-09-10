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

import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.AUTOMATON_HAS_PREFIX;
import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.EXACT_MATCH;
import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.NO_MATCH;
import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.SEQUENCE_IS_A_PREFIX;


/**
 * This class implements some common matching and scanning operations on a
 * generic FST.
 */
public final class FSTTraversal {
  /**
   * Target automaton.
   */
  private final FST _FST;

  /**
   * Traversals of the given FST.
   *
   * @param FST The target automaton for traversals.
   */
  public FSTTraversal(FST FST) {
    this._FST = FST;
  }

  /**
   * Calculate perfect hash for a given input sequence of bytes. The perfect hash requires
   * that {@link FST} is built with {@link FSTFlags#NUMBERS} and corresponds to the sequential
   * order of input sequences used at automaton construction time.
   * @param sequence The byte sequence to calculate perfect hash for.
   * @param start Start index in the sequence array.
   * @param length Length of the byte sequence, must be at least 1.
   * @param node The node to start traversal from, typically the {@linkplain FST#getRootNode() root node}.
   *
   * @return Returns a unique integer assigned to the input sequence in the automaton (reflecting
   * the number of that sequence in the input used to build the automaton). Returns a negative
   * integer if the input sequence was not part of the input from which the automaton was created.
   * The type of mismatch is a constant defined in {@link MatchResult}.
   */
  public int perfectHash(byte[] sequence, int start, int length, int node) {
    assert _FST.getFlags().contains(FSTFlags.NUMBERS) : "FST not built with NUMBERS option.";
    assert length > 0 : "Must be a non-empty sequence.";

    int hash = 0;
    final int end = start + length - 1;

    int seqIndex = start;
    byte label = sequence[seqIndex];

    // Seek through the current node's labels, looking for 'label', update hash.
    for (int arc = _FST.getFirstArc(node); arc != 0; ) {
      if (_FST.getArcLabel(arc) == label) {
        if (_FST.isArcFinal(arc)) {
          if (seqIndex == end) {
            return hash;
          }

          hash++;
        }

        if (_FST.isArcTerminal(arc)) {
          /* The automaton contains a prefix of the input sequence. */
          return AUTOMATON_HAS_PREFIX;
        }

        // The sequence is a prefix of one of the sequences stored in the automaton.
        if (seqIndex == end) {
          return SEQUENCE_IS_A_PREFIX;
        }

        // Make a transition along the arc, go the target node's first arc.
        arc = _FST.getFirstArc(_FST.getEndNode(arc));
        label = sequence[++seqIndex];
        continue;
      } else {
        if (_FST.isArcFinal(arc)) {
          hash++;
        }
        if (!_FST.isArcTerminal(arc)) {
          hash += _FST.getRightLanguageCount(_FST.getEndNode(arc));
        }
      }

      arc = _FST.getNextArc(arc);
    }

    if (seqIndex > start) {
      return AUTOMATON_HAS_PREFIX;
    } else {
      // Labels of this node ended without a match on the sequence.
      // Perfect hash does not exist.
      return NO_MATCH;
    }
  }

  /**
   * @param sequence The byte sequence to calculate perfect hash for. 
   * @see #perfectHash(byte[], int, int, int)
   * @return Returns a unique integer assigned to the input sequence in the automaton (reflecting
   * the number of that sequence in the input used to build the automaton). Returns a negative
   * integer if the input sequence was not part of the input from which the automaton was created.
   * The type of mismatch is a constant defined in {@link MatchResult}.
   */
  public int perfectHash(byte[] sequence) {
    return perfectHash(sequence, 0, sequence.length, _FST.getRootNode());
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
      reuse.reset(NO_MATCH, start, node);
      return reuse;
    }

    final FST FST = this._FST;
    final int end = start + length;
    for (int i = start; i < end; i++) {
      final int arc = FST.getArc(node, sequence[i]);
      if (arc != 0) {
        if (i + 1 == end && FST.isArcFinal(arc)) {
          /* The automaton has an exact match of the input sequence. */
          reuse.reset(EXACT_MATCH, i, node);
          return reuse;
        }

        if (FST.isArcTerminal(arc)) {
          /* The automaton contains a prefix of the input sequence. */
          reuse.reset(AUTOMATON_HAS_PREFIX, i + 1, node);
          return reuse;
        }

        // Make a transition along the arc.
        node = FST.getEndNode(arc);
      } else {
        if (i > start) {
          reuse.reset(AUTOMATON_HAS_PREFIX, i, node);
        } else {
          reuse.reset(NO_MATCH, i, node);
        }
        return reuse;
      }
    }

    /* The sequence is a prefix of at least one sequence in the automaton. */
    reuse.reset(SEQUENCE_IS_A_PREFIX, 0, node);
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
    return match(sequence, _FST.getRootNode());
  }
}