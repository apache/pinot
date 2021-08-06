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
package org.apache.pinot.fsa;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import static org.apache.pinot.fsa.FSAFlags.FLEXIBLE;
import static org.apache.pinot.fsa.FSAFlags.NEXTBIT;
import static org.apache.pinot.fsa.FSAFlags.NUMBERS;
import static org.apache.pinot.fsa.FSAFlags.STOPBIT;


/**
 * CFSA (Compact Finite State Automaton) binary format implementation. This is a
 * slightly reorganized version of {@link FSA5} offering smaller automata size
 * at some (minor) performance penalty.
 *
 * <p><b>Note:</b> Serialize to {@link CFSA2} for new code.</p>
 *
 * <p>The encoding of automaton body is as follows.</p>
 * 
 * <pre>
 * ---- FSA header (standard)
 * Byte                            Description 
 *       +-+-+-+-+-+-+-+-+\
 *     0 | | | | | | | | | +------ '\'
 *       +-+-+-+-+-+-+-+-+/
 *       +-+-+-+-+-+-+-+-+\
 *     1 | | | | | | | | | +------ 'f'
 *       +-+-+-+-+-+-+-+-+/
 *       +-+-+-+-+-+-+-+-+\
 *     2 | | | | | | | | | +------ 's'
 *       +-+-+-+-+-+-+-+-+/
 *       +-+-+-+-+-+-+-+-+\
 *     3 | | | | | | | | | +------ 'a'
 *       +-+-+-+-+-+-+-+-+/
 *       +-+-+-+-+-+-+-+-+\
 *     4 | | | | | | | | | +------ version (fixed 0xc5)
 *       +-+-+-+-+-+-+-+-+/
 *       +-+-+-+-+-+-+-+-+\
 *     5 | | | | | | | | | +------ filler character
 *       +-+-+-+-+-+-+-+-+/
 *       +-+-+-+-+-+-+-+-+\
 *     6 | | | | | | | | | +------ annot character
 *       +-+-+-+-+-+-+-+-+/
 *       +-+-+-+-+-+-+-+-+\
 *     7 |C|C|C|C|G|G|G|G| +------ C - node data size (ctl), G - address size (gotoLength)
 *       +-+-+-+-+-+-+-+-+/
 *       +-+-+-+-+-+-+-+-+\
 *  8-32 | | | | | | | | | +------ labels mapped for type (1) of arc encoding. 
 *       : : : : : : : : : |
 *       +-+-+-+-+-+-+-+-+/
 * 
 * ---- Start of a node; only if automaton was compiled with NUMBERS option.
 * 
 * Byte
 *        +-+-+-+-+-+-+-+-+\
 *      0 | | | | | | | | | \  LSB
 *        +-+-+-+-+-+-+-+-+  +
 *      1 | | | | | | | | |  |      number of strings recognized
 *        +-+-+-+-+-+-+-+-+  +----- by the automaton starting
 *        : : : : : : : : :  |      from this node.
 *        +-+-+-+-+-+-+-+-+  +
 *  ctl-1 | | | | | | | | | /  MSB
 *        +-+-+-+-+-+-+-+-+/
 *        
 * ---- A vector of node's arcs. Conditional format, depending on flags.
 * 
 * 1) NEXT bit set, mapped arc label. 
 * 
 *                +--------------- arc's label mapped in M bits if M's field value &gt; 0
 *                | +------------- node pointed to is next
 *                | | +----------- the last arc of the node
 *         _______| | | +--------- the arc is final
 *        /       | | | |
 *       +-+-+-+-+-+-+-+-+\
 *     0 |M|M|M|M|M|1|L|F| +------ flags + (M) index of the mapped label.
 *       +-+-+-+-+-+-+-+-+/
 * 
 * 2) NEXT bit set, label separate.
 * 
 *                +--------------- arc's label stored separately (M's field is zero).
 *                | +------------- node pointed to is next
 *                | | +----------- the last arc of the node
 *                | | | +--------- the arc is final
 *                | | | |
 *       +-+-+-+-+-+-+-+-+\
 *     0 |0|0|0|0|0|1|L|F| +------ flags
 *       +-+-+-+-+-+-+-+-+/
 *       +-+-+-+-+-+-+-+-+\
 *     1 | | | | | | | | | +------ label
 *       +-+-+-+-+-+-+-+-+/
 * 
 * 3) NEXT bit not set. Full arc.
 * 
 *                  +------------- node pointed to is next
 *                  | +----------- the last arc of the node
 *                  | | +--------- the arc is final
 *                  | | |
 *       +-+-+-+-+-+-+-+-+\
 *     0 |A|A|A|A|A|0|L|F| +------ flags + (A) address field, lower bits
 *       +-+-+-+-+-+-+-+-+/
 *       +-+-+-+-+-+-+-+-+\
 *     1 | | | | | | | | | +------ label
 *       +-+-+-+-+-+-+-+-+/
 *       : : : : : : : : :       
 *       +-+-+-+-+-+-+-+-+\
 * gtl-1 |A|A|A|A|A|A|A|A| +------ address, continuation (MSB)
 *       +-+-+-+-+-+-+-+-+/
 * </pre>
 */
public final class CFSA extends FSA {
	/**
	 * Automaton header version value. 
	 */
	public static final byte VERSION = (byte) 0xC5;

	/**
	 * Bitmask indicating that an arc corresponds to the last character of a
	 * sequence available when building the automaton.
	 */
	public static final int BIT_FINAL_ARC = 1 << 0;

	/**
	 * Bitmask indicating that an arc is the last one of the node's list and the
	 * following one belongs to another node.
	 */
	public static final int BIT_LAST_ARC = 1 << 1;

	/**
	 * Bitmask indicating that the target node of this arc follows it in the
	 * compressed automaton structure (no goto field).
	 */
	public static final int BIT_TARGET_NEXT = 1 << 2;

	/**
	 * An array of bytes with the internal representation of the automaton.
	 * Please see the documentation of this class for more information on how
	 * this structure is organized.
	 */
	public byte[] arcs;

	/**
	 * The length of the node header structure (if the automaton was compiled with
	 * <code>NUMBERS</code> option). Otherwise zero.
	 */
	public final int nodeDataLength;

  /**
   * Flags for this automaton version.
   */
  private final Set<FSAFlags> flags;

  /**
   * Number of bytes each address takes in full, expanded form (goto length).
   */
	public final int gtl;

	/**
	 * Label mapping for arcs of type (1) (see class documentation). The array
	 * is indexed by mapped label's value and contains the original label.
	 */
	public final byte[] labelMapping;

	/**
	 * Creates a new automaton, reading it from a file in FSA format, version 5.
	 */
	CFSA(InputStream stream) throws IOException {
	  DataInputStream in = new DataInputStream(stream);

    // Skip legacy header fields.
    in.readByte();  // filler
    in.readByte();  // annotation
    final byte hgtl = in.readByte();

		/*
		 * Determine if the automaton was compiled with NUMBERS. If so, modify
		 * ctl and goto fields accordingly.
		 */
		flags = EnumSet.of(FLEXIBLE, STOPBIT, NEXTBIT);
		if ((hgtl & 0xf0) != 0) {
			this.nodeDataLength = (hgtl >>> 4) & 0x0f;
			this.gtl = hgtl & 0x0f;
			flags.add(NUMBERS);
		} else {
			this.nodeDataLength = 0;
			this.gtl = hgtl & 0x0f;
		}

		/*
		 * Read mapping dictionary.
		 */
		labelMapping = new byte[1 << 5];
		in.readFully(labelMapping);

		/*
		 * Read arcs' data.
		 */
		arcs = readRemaining(in);
	}

  /**
	 * Returns the start node of this automaton. May return <code>0</code> if
	 * the start node is also an end node.
	 */
	@Override
	public int getRootNode() {
        // Skip dummy node marking terminating state.
        final int epsilonNode = skipArc(getFirstArc(0));
        
        // And follow the epsilon node's first (and only) arc.
        return getDestinationNodeOffset(getFirstArc(epsilonNode));
	}

	/**
     * {@inheritDoc} 
     */
	@Override
	public final int getFirstArc(int node) {
		return nodeDataLength + node;
	}

	/**
     * {@inheritDoc} 
     */
	@Override
	public final int getNextArc(int arc) {
		if (isArcLast(arc))
			return 0;
		else
			return skipArc(arc);
	}

	/**
     * {@inheritDoc} 
     */
	@Override
	public int getArc(int node, byte label) {
		for (int arc = getFirstArc(node); arc != 0; arc = getNextArc(arc)) {
			if (getArcLabel(arc) == label)
				return arc;
		}

		// An arc labeled with "label" not found.
		return 0;
	}

	/**
     * {@inheritDoc} 
     */
	@Override
	public int getEndNode(int arc) {
		final int nodeOffset = getDestinationNodeOffset(arc);
		if (0 == nodeOffset) {
			throw new RuntimeException("This is a terminal arc [" + arc + "]");
		}
		return nodeOffset;
	}

	/**
     * {@inheritDoc} 
     */
	@Override
	public byte getArcLabel(int arc) {
		if (isNextSet(arc) && isLabelCompressed(arc)) {
			return this.labelMapping[(arcs[arc] >>> 3) & 0x1f];
		} else {
			return arcs[arc + 1];
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getRightLanguageCount(int node) {
        assert getFlags().contains(NUMBERS): "This FSA was not compiled with NUMBERS.";
	    return FSA5.decodeFromBytes(arcs, node, nodeDataLength);
    }

	/**
     * {@inheritDoc} 
     */
	@Override
	public boolean isArcFinal(int arc) {
		return (arcs[arc] & BIT_FINAL_ARC) != 0;
	}

	/**
     * {@inheritDoc} 
     */
	@Override
	public boolean isArcTerminal(int arc) {
		return (0 == getDestinationNodeOffset(arc));
	}

	/**
	 * Returns <code>true</code> if this arc has <code>NEXT</code> bit set.
	 * 
	 * @see #BIT_LAST_ARC
	 * @param arc The node's arc identifier.
	 * @return Returns true if the argument is the last arc of a node.
	 */
	public boolean isArcLast(int arc) {
		return (arcs[arc] & BIT_LAST_ARC) != 0;
	}

	/**
	 * @see #BIT_TARGET_NEXT
   * @param arc The node's arc identifier.
   * @return Returns true if {@link #BIT_TARGET_NEXT} is set for this arc.
	 */
	public boolean isNextSet(int arc) {
		return (arcs[arc] & BIT_TARGET_NEXT) != 0;
	}

	/**
   * @param arc The node's arc identifier.
   * @return Returns <code>true</code> if the label is compressed inside flags byte.
	 */
	public boolean isLabelCompressed(int arc) {
		assert isNextSet(arc) : "Only applicable to arcs with NEXT bit.";
		return (arcs[arc] & (-1 << 3)) != 0;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * <p>For this automaton version, an additional {@link FSAFlags#NUMBERS} flag
	 * may be set to indicate the automaton contains extra fields for each node.</p>
	 */
	public Set<FSAFlags> getFlags() {
	    return Collections.unmodifiableSet(flags);
	}

	/**
	 * Returns the address of the node pointed to by this arc.
	 */
	final int getDestinationNodeOffset(int arc) {
		if (isNextSet(arc)) {
			/* The destination node follows this arc in the array. */
			return skipArc(arc);
		} else {
			/*
			 * The destination node address has to be extracted from the arc's
			 * goto field.
			 */
			int r = 0;
			for (int i = gtl; --i >= 1;) {
				r = r << 8 | (arcs[arc + 1 + i] & 0xff);
			}
			r = r << 8 | (arcs[arc] & 0xff);
			return r >>> 3;
		}
	}

	/**
	 * Read the arc's layout and skip as many bytes, as needed, to skip it.
	 */
	private int skipArc(int offset) {
		if (isNextSet(offset)) {
			if (isLabelCompressed(offset)) {
				offset++;
			} else {
				offset += 1 + 1;
			}
		} else {
			offset += 1 + gtl;
		}
		return offset;
	}
}