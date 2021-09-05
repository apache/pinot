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
package org.apache.pinot.segment.local.utils.nativefst.builders;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.pinot.segment.local.utils.nativefst.FSA;
import org.apache.pinot.segment.local.utils.nativefst.FSAFlags;


/**
 * An FSA with constant-size arc representation produced directly by
 * {@link FSABuilder}.
 * 
 * @see FSABuilder
 */
final class ConstantArcSizeFSA extends FSA {
  /** Size of the target address field (constant for the builder). */
  public final static int TARGET_ADDRESS_SIZE = 4;

  /** Size of the flags field (constant for the builder). */
  public final static int FLAGS_SIZE = 1;

  /** Size of the label field (constant for the builder). */
  public final static int LABEL_SIZE = 1;

  /**
   * Size of a single arc structure.
   */
  public final static int ARC_SIZE = FLAGS_SIZE + LABEL_SIZE + TARGET_ADDRESS_SIZE;

  /** Offset of the flags field inside an arc. */
  public final static int FLAGS_OFFSET = 0;

  /** Offset of the label field inside an arc. */
  public final static int LABEL_OFFSET = FLAGS_SIZE;

  /** Offset of the address field inside an arc. */
  public final static int ADDRESS_OFFSET = LABEL_OFFSET + LABEL_SIZE;

  /** A dummy address of the terminal state. */
  final static int TERMINAL_STATE = 0;

  /**
   * An arc flag indicating the target node of an arc corresponds to a final
   * state.
   */
  public final static int BIT_ARC_FINAL = 1 << 1;

  /** An arc flag indicating the arc is last within its state. */
  public final static int BIT_ARC_LAST = 1 << 0;

  /**
   * An epsilon state. The first and only arc of this state points either to the
   * root or to the terminal state, indicating an empty automaton.
   */
  private final int _epsilon;

  /**
   * FSA data, serialized as a byte array.
   */
  private final byte[] _data;

  private Map<Integer, Integer> _outputSymbols;

  /**
   * @param data
   *          FSA data. There must be no trailing bytes after the last state.
   */
  ConstantArcSizeFSA(byte[] data, int epsilon, Map<Integer, Integer> outputSymbols) {
    assert epsilon == 0 : "Epsilon is not zero?";

    this._epsilon = epsilon;
    this._data = data;
    this._outputSymbols = outputSymbols;
  }

  @Override
  public int getRootNode() {
    return getEndNode(getFirstArc(_epsilon));
  }

  @Override
  public int getFirstArc(int node) {
    return node;
  }

  @Override
  public int getArc(int node, byte label) {
    for (int arc = getFirstArc(node); arc != 0; arc = getNextArc(arc)) {
      if (getArcLabel(arc) == label)
        return arc;
    }
    return 0;
  }

  @Override
  public int getNextArc(int arc) {
    //TODO: atri
    //System.out.println("ARC IS " + arc);

    if (isArcLast(arc))
      return 0;
    return arc + ARC_SIZE;
  }

  @Override
  public byte getArcLabel(int arc) {
    return _data[arc + LABEL_OFFSET];
  }

  @Override
  public Map<Integer, Integer> getOutputSymbols() {
    return _outputSymbols;
  }

  @Override
  public int getOutputSymbol(int arc) {
    return _outputSymbols.get(arc);
  }

  /**
   * Fills the target state address of an arc.
   */
  private int getArcTarget(int arc) {
    arc += ADDRESS_OFFSET;
    return (_data[arc]           ) << 24 |
           (_data[arc + 1] & 0xff) << 16 |
           (_data[arc + 2] & 0xff) << 8  |
           (_data[arc + 3] & 0xff);
  }

  @Override
  public boolean isArcFinal(int arc) {
    return (_data[arc + FLAGS_OFFSET] & BIT_ARC_FINAL) != 0;
  }

  @Override
  public boolean isArcTerminal(int arc) {
    return getArcTarget(arc) == 0;
  }

  public boolean isArcLast(int arc) {
    return (_data[arc + FLAGS_OFFSET] & BIT_ARC_LAST) != 0;
  }

  @Override
  public int getEndNode(int arc) {
    return getArcTarget(arc);
  }

  @Override
  public Set<FSAFlags> getFlags() {
    return Collections.emptySet();
  }
}
