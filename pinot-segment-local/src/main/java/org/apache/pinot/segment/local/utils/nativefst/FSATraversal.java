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
 * generic FSA.
 */
public final class FSATraversal {
	/**
	 * Target automaton.
	 */
	private final FSA fsa;

	/**
	 * Traversals of the given FSA.
	 * 
	 * @param fsa The target automaton for traversals. 
	 */
	public FSATraversal(FSA fsa) {
		this.fsa = fsa;
	}

	/**
	 * Calculate perfect hash for a given input sequence of bytes. The perfect hash requires
	 * that {@link FSA} is built with {@link FSAFlags#NUMBERS} and corresponds to the sequential
	 * order of input sequences used at automaton construction time.

	 * @param sequence The byte sequence to calculate perfect hash for. 
	 * @param start Start index in the sequence array. 
	 * @param length Length of the byte sequence, must be at least 1.
	 * @param node The node to start traversal from, typically the {@linkplain FSA#getRootNode() root node}.
	 * 
	 * @return Returns a unique integer assigned to the input sequence in the automaton (reflecting
	 * the number of that sequence in the input used to build the automaton). Returns a negative
	 * integer if the input sequence was not part of the input from which the automaton was created.
	 * The type of mismatch is a constant defined in {@link MatchResult}.
	 */
	public int perfectHash(byte[] sequence, int start, int length, int node) {
		assert fsa.getFlags().contains(FSAFlags.NUMBERS) : "FSA not built with NUMBERS option.";
		assert length > 0 : "Must be a non-empty sequence.";

		int hash = 0;
		final int end = start + length - 1;

		int seqIndex = start;
		byte label = sequence[seqIndex];

		// Seek through the current node's labels, looking for 'label', update hash.
		for (int arc = fsa.getFirstArc(node); arc != 0;) {
			if (fsa.getArcLabel(arc) == label) {
				if (fsa.isArcFinal(arc)) {
					if (seqIndex == end) {
						return hash;
					}

					hash++;
				}

				if (fsa.isArcTerminal(arc)) {
					/* The automaton contains a prefix of the input sequence. */
					return AUTOMATON_HAS_PREFIX;
				}

				// The sequence is a prefix of one of the sequences stored in the automaton.
				if (seqIndex == end) {
					return SEQUENCE_IS_A_PREFIX;
				}

				// Make a transition along the arc, go the target node's first arc.
				arc = fsa.getFirstArc(fsa.getEndNode(arc));
				label = sequence[++seqIndex];
				continue;
			} else {
				if (fsa.isArcFinal(arc)) {
					hash++;
				}
				if (!fsa.isArcTerminal(arc)) {
					hash += fsa.getRightLanguageCount(fsa.getEndNode(arc));
				}
			}

			arc = fsa.getNextArc(arc);
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
		return perfectHash(sequence, 0, sequence.length, fsa.getRootNode());
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
	 * @param node The node to start traversal from, typically the {@linkplain FSA#getRootNode() root node}.  
	 * 
	 * @return The same object as <code>reuse</code>, but with updated match {@link MatchResult#kind}
	 *        and other relevant fields.
	 */
	public MatchResult match(MatchResult reuse, byte[] sequence, int start, int length, int node) {
		if (node == 0) {
		  reuse.reset(NO_MATCH, start, node);
			return reuse;
		}

		final FSA fsa = this.fsa;
		final int end = start + length;
		for (int i = start; i < end; i++) {
			final int arc = fsa.getArc(node, sequence[i]);
			if (arc != 0) {
				if (i + 1 == end && fsa.isArcFinal(arc)) {
					/* The automaton has an exact match of the input sequence. */
				  reuse.reset(EXACT_MATCH, i, node);
					return reuse;
				}

				if (fsa.isArcTerminal(arc)) {
					/* The automaton contains a prefix of the input sequence. */
				  reuse.reset(AUTOMATON_HAS_PREFIX, i + 1, node);
					return reuse;
				}

				// Make a transition along the arc.
				node = fsa.getEndNode(arc);
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
   * @param node The node to start traversal from, typically the {@linkplain FSA#getRootNode() root node}.  
   *
   * @return {@link MatchResult} with updated match {@link MatchResult#kind}.
   */
  public MatchResult match(byte[] sequence, int start, int length, int node) {
    return match(new MatchResult(), sequence, start, length, node);
  }

	/**
   * @param sequence Input sequence to look for in the automaton.
   * @param node The node to start traversal from, typically the {@linkplain FSA#getRootNode() root node}.  
   *
   * @return {@link MatchResult} with updated match {@link MatchResult#kind}.
	 */
	public MatchResult match(byte[] sequence, int node) {
		return match(sequence, 0, sequence.length, node);
	}

	/**
   * @param sequence Input sequence to look for in the automaton.
   *
   * @return {@link MatchResult} with updated match {@link MatchResult#kind}.
	 */
	public MatchResult match(byte[] sequence) {
		return match(sequence, fsa.getRootNode());
	}
}