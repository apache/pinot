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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.realtime.impl.dictionary.OffHeapMutableBytesStore;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;


/**
 * FST binary format implementation
 *
 * <p>
 * This version indicates the dictionary was built with these flags:
 * {@link FSTFlags#FLEXIBLE}, {@link FSTFlags#STOPBIT} and
 * {@link FSTFlags#NEXTBIT}. The internal representation of the FST must
 * therefore follow this description (please note this format describes only a
 * single transition (arc), not the entire dictionary file).
 *
 * <pre>
 * ---- this node header present only if automaton was compiled with NUMBERS option.
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
 * ---- remaining part of the node
 * Length of output symbols dictionary -- Integer
 * <Arc ID, Output Symbol>
 * <Arc ID, Output Symbol>
 * <Arc ID, Output Symbol>
 * .
 * .
 * .
 * <Arc ID, Output Symbol> (Length)
 *
 * Byte
 *       +-+-+-+-+-+-+-+-+\
 *     0 | | | | | | | | | +------ label
 *       +-+-+-+-+-+-+-+-+/
 *
 *                  +------------- node pointed to is next
 *                  | +----------- the last arc of the node
 *                  | | +--------- the arc is final
 *                  | | |
 *             +-----------+
 *             |    | | |  |
 *         ___+___  | | |  |
 *        /       \ | | |  |
 *       MSB           LSB |
 *        7 6 5 4 3 2 1 0  |
 *       +-+-+-+-+-+-+-+-+ |
 *     1 | | | | | | | | | \ \
 *       +-+-+-+-+-+-+-+-+  \ \  LSB
 *       +-+-+-+-+-+-+-+-+     +
 *     2 | | | | | | | | |     |
 *       +-+-+-+-+-+-+-+-+     |
 *     3 | | | | | | | | |     +----- target node address (in bytes)
 *       +-+-+-+-+-+-+-+-+     |      (not present except for the byte
 *       : : : : : : : : :     |       with flags if the node pointed to
 *       +-+-+-+-+-+-+-+-+     +       is next)
 *   gtl | | | | | | | | |    /  MSB
 *       +-+-+-+-+-+-+-+-+   /
 * gtl+1                           (gtl = gotoLength)
 * </pre>
 */
public final class ImmutableFST extends FST {
  /**
   * Default filler byte.
   */
  public final static byte DEFAULT_FILLER = '_';

  /**
   * Default annotation byte.
   */
  public final static byte DEFAULT_ANNOTATION = '+';

  /**
   * Automaton version as in the file header.
   */
  public static final byte VERSION = 5;

  /**
   * Bit indicating that an arc corresponds to the last character of a sequence
   * available when building the automaton.
   */
  public static final int BIT_FINAL_ARC = 1 << 0;

  /**
   * Bit indicating that an arc is the last one of the node's list and the
   * following one belongs to another node.
   */
  public static final int BIT_LAST_ARC = 1 << 1;

  /**
   * Bit indicating that the target node of this arc follows it in the
   * compressed automaton structure (no goto field).
   */
  public static final int BIT_TARGET_NEXT = 1 << 2;

  /**
   * An offset in the arc structure, where the address and flags field begins.
   * In version 5 of FST automata, this value is constant (1, skip label).
   */
  public final static int ADDRESS_OFFSET = 1;

  private static final int PER_BUFFER_SIZE = 16;

  /**
   * An array of bytes with the internal representation of the automaton. Please
   * see the documentation of this class for more information on how this
   * structure is organized.
   */
  //public final OffHeapMutableBytesStore _mutableBytesStore;

  public final PinotDataBuffer _pinotDataBuffer;

  public int _offset;
  /**
   * The length of the node header structure (if the automaton was compiled with
   * <code>NUMBERS</code> option). Otherwise zero.
   */
  public final int _nodeDataLength;
  /**
   * Number of bytes each address takes in full, expanded form (goto length).
   */
  public final int _gotoLength;
  /** Filler character. */
  public final byte _filler;
  /** Annotation character. */
  public final byte _annotation;
  public Map<Integer, Integer> _outputSymbols;
  /**
   * Flags for this automaton version.
   */
  private Set<FSTFlags> _flags;

  /**
   * Read and wrap a binary automaton in FST version 5.
   */
  ImmutableFST(InputStream stream, boolean hasOutputSymbols, PinotDataBufferMemoryManager memoryManager, final int fstDataSize)
      throws IOException {
    DataInputStream in = new DataInputStream(stream);

    _offset = 0;

    _filler = in.readByte();
    _annotation = in.readByte();
    final byte hgtl = in.readByte();

    //_mutableBytesStore = new OffHeapMutableBytesStore(memoryManager, "ImmutableFST");
    _pinotDataBuffer = memoryManager.allocate(fstDataSize, "ImmutableFST");

    /*
     * Determine if the automaton was compiled with NUMBERS. If so, modify
     * ctl and goto fields accordingly.
     */
    _flags = EnumSet.of(FSTFlags.FLEXIBLE, FSTFlags.STOPBIT, FSTFlags.NEXTBIT);
    if ((hgtl & 0xf0) != 0) {
      _flags.add(FSTFlags.NUMBERS);
    }

    _flags = Collections.unmodifiableSet(_flags);

    _nodeDataLength = (hgtl >>> 4) & 0x0f;
    _gotoLength = hgtl & 0x0f;

    if (hasOutputSymbols) {
      final int outputSymbolsLength = in.readInt();
      byte[] outputSymbolsBuffer = readRemaining(in, outputSymbolsLength);

      if (outputSymbolsBuffer.length > 0) {
        String outputSymbolsSerialized = new String(outputSymbolsBuffer);

        _outputSymbols = buildMap(outputSymbolsSerialized);
      }
    }

    readRemaining(in);
  }

  private void readRemaining(InputStream in)
      throws IOException {
    byte[] buffer = new byte[PER_BUFFER_SIZE];
    while ((in.read(buffer)) >= 0) {
      _pinotDataBuffer.readFrom(_offset, buffer);
      _offset = _offset + PER_BUFFER_SIZE;
      //_mutableBytesStore.add(buffer);
    }
  }

  /**
   * Returns the start node of this automaton.
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
  public int getFirstArc(int node) {
    return _nodeDataLength + node;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getNextArc(int arc) {
    if (isArcLast(arc)) {
      return 0;
    } else {
      return skipArc(arc);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getArc(int node, byte label) {
    for (int arc = getFirstArc(node); arc != 0; arc = getNextArc(arc)) {
      if (getArcLabel(arc) == label) {
        return arc;
      }
    }

    // An arc labeled with "label" not found.
    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getEndNode(int arc) {
    return getDestinationNodeOffset(arc);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte getArcLabel(int arc) {
    return getByte(arc, 0);
  }

  @Override
  public int getOutputSymbol(int arc) {
    return _outputSymbols.get(arc);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isArcFinal(int arc) {
    return (getByte(arc, ADDRESS_OFFSET) & BIT_FINAL_ARC) != 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isArcTerminal(int arc) {
    return (0 == getDestinationNodeOffset(arc));
  }

  /**
   * Returns the number encoded at the given node. The number equals the count
   * of the set of suffixes reachable from <code>node</code> (called its right
   * language).
   */
  @Override
  public int getRightLanguageCount(int node) {
    assert getFlags().contains(FSTFlags.NUMBERS) : "This FST was not compiled with NUMBERS.";
    return decodeFromBytes(node, _nodeDataLength);
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * For this automaton version, an additional {@link FSTFlags#NUMBERS} flag may
   * be set to indicate the automaton contains extra fields for each node.
   * </p>
   */
  @Override
  public Set<FSTFlags> getFlags() {
    return _flags;
  }

  /**
   * Returns <code>true</code> if this arc has <code>NEXT</code> bit set.
   *
   * @see #BIT_LAST_ARC
   * @param arc The node's arc identifier.
   * @return Returns true if the argument is the last arc of a node.
   */
  public boolean isArcLast(int arc) {
    return (getByte(arc, ADDRESS_OFFSET) & BIT_LAST_ARC) != 0;
  }

  /**
   * @see #BIT_TARGET_NEXT
   * @param arc The node's arc identifier.
   * @return Returns true if {@link #BIT_TARGET_NEXT} is set for this arc.
   */
  public boolean isNextSet(int arc) {

    return (getByte(arc, ADDRESS_OFFSET) & BIT_TARGET_NEXT) != 0;
  }

  /**
   * Returns an n-byte integer encoded in byte-packed representation.
   */
  private int decodeFromBytes(final int start, final int n) {
    int r = 0;

    for (int i = n; --i >= 0; ) {
      int seek = start + i;
      int actualArcOffset = seek >= PER_BUFFER_SIZE ? seek / PER_BUFFER_SIZE : 0;
      int bufferOffset = seek >= PER_BUFFER_SIZE ? seek - ((actualArcOffset) * PER_BUFFER_SIZE) : seek;

      //byte[] inputData = _mutableBytesStore.get(actualArcOffset);
      byte[] inputData = new byte[PER_BUFFER_SIZE];
      _pinotDataBuffer.copyTo(actualArcOffset, inputData);

      r = r << 8 | (inputData[bufferOffset] & 0xff);
    }
    return r;
  }

  /**
   * Returns the address of the node pointed to by this arc.
   */
  private int getDestinationNodeOffset(int arc) {
    if (isNextSet(arc)) {
      /* The destination node follows this arc in the array. */
      return skipArc(arc);
    } else {
      /*
       * The destination node address has to be extracted from the arc's
       * goto field.
       */
      return decodeFromBytes(arc + ADDRESS_OFFSET, _gotoLength) >>> 3;
    }
  }

  /**
   * Read the arc's layout and skip as many bytes, as needed.
   */
  private int skipArc(int offset) {
    return offset + (isNextSet(offset) ? 1 + 1   /* label + flags */ : 1 + _gotoLength /* label + flags/address */);
  }

  private byte getByte(int seek, int offset) {
    int actualArcOffset = seek >= PER_BUFFER_SIZE ? seek / PER_BUFFER_SIZE : 0;
    int bufferOffset = seek >= PER_BUFFER_SIZE ? seek - ((actualArcOffset) * PER_BUFFER_SIZE) : seek;

    byte[] retVal = new byte[PER_BUFFER_SIZE];
    //retVal = _mutableBytesStore.get(actualArcOffset);
    _pinotDataBuffer.copyTo(actualArcOffset, retVal);

    int target = bufferOffset + offset;

    if (target >= PER_BUFFER_SIZE) {
      //retVal = _mutableBytesStore.get(actualArcOffset + 1);
      _pinotDataBuffer.copyTo(actualArcOffset + 1, retVal);
      target = target - PER_BUFFER_SIZE;
    }

    return retVal[target];
  }
}
