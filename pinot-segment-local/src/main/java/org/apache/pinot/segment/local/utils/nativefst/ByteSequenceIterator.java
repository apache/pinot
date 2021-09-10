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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * An iterator that traverses the right language of a given node (all sequences
 * reachable from a given node).
 */
public final class ByteSequenceIterator implements Iterator<ByteBuffer> {
  /**
   * Default expected depth of the recursion stack (estimated longest sequence
   * in the automaton). Buffers expand by the same value if exceeded.
   */
  private final static int EXPECTED_MAX_STATES = 15;

  /** The FST to which this iterator belongs. */
  private final FST _FST;

  /** An internal cache for the next element in the FST */
  private ByteBuffer _nextElement;

  /**
   * A buffer for the current sequence of bytes from the current node to the
   * root.
   */
  private byte[] _buffer = new byte[EXPECTED_MAX_STATES];

  /** Reusable byte buffer wrapper around {@link #_buffer}. */
  private ByteBuffer _bufferWrapper = ByteBuffer.wrap(_buffer);

  /** An arc stack for DFS when processing the automaton. */
  private int[] _arcs = new int[EXPECTED_MAX_STATES];

  /** Current processing depth in {@link #_arcs}. */
  private int _position;

  /**
   * Create an instance of the iterator for a given node.
   * @param FST The automaton to iterate over.
   * @param node The starting node's identifier (can be the {@link FST#getRootNode()}).
   */
  public ByteSequenceIterator(FST FST, int node) {
    this._FST = FST;

    if (FST.getFirstArc(node) != 0) {
      restartFrom(node);
    }
  }

  /**
   * Restart walking from <code>node</code>. Allows iterator reuse.
   *
   * @param node Restart the iterator from <code>node</code>.
   * @return Returns <code>this</code> for call chaining.
   */
  public ByteSequenceIterator restartFrom(int node) {
    _position = 0;
    _bufferWrapper.clear();
    _nextElement = null;

    pushNode(node);
    return this;
  }

  /** Returns <code>true</code> if there are still elements in this iterator. */
  @Override
  public boolean hasNext() {
    if (_nextElement == null) {
      _nextElement = advance();
    }

    return _nextElement != null;
  }

  /**
   * @return Returns a {@link ByteBuffer} with the sequence corresponding to the
   *         next final state in the automaton.
   */
  @Override
  public ByteBuffer next() {
    if (_nextElement != null) {
      final ByteBuffer cache = _nextElement;
      _nextElement = null;
      return cache;
    } else {
      final ByteBuffer cache = advance();
      if (cache == null) {
        throw new NoSuchElementException();
      }
      return cache;
    }
  }

  /**
   * Advances to the next available final state.
   */
  private final ByteBuffer advance() {
    if (_position == 0) {
      return null;
    }

    while (_position > 0) {
      final int lastIndex = _position - 1;
      final int arc = _arcs[lastIndex];

      if (arc == 0) {
        // Remove the current node from the queue.
        _position--;
        continue;
      }

      // Go to the next arc, but leave it on the stack
      // so that we keep the recursion depth level accurate.
      _arcs[lastIndex] = _FST.getNextArc(arc);

      // Expand buffer if needed.
      final int bufferLength = this._buffer.length;
      if (lastIndex >= bufferLength) {
        this._buffer = Arrays.copyOf(_buffer, bufferLength + EXPECTED_MAX_STATES);
        this._bufferWrapper = ByteBuffer.wrap(_buffer);
      }
      _buffer[lastIndex] = _FST.getArcLabel(arc);

      if (!_FST.isArcTerminal(arc)) {
        // Recursively descend into the arc's node.
        pushNode(_FST.getEndNode(arc));
      }

      if (_FST.isArcFinal(arc)) {
        _bufferWrapper.clear();
        _bufferWrapper.limit(lastIndex + 1);
        return _bufferWrapper;
      }
    }

    return null;
  }

  /**
   * Not implemented in this iterator.
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("Read-only iterator.");
  }

  /**
   * Descends to a given node, adds its arcs to the stack to be traversed.
   */
  private void pushNode(int node) {
    // Expand buffers if needed.
    if (_position == _arcs.length) {
      _arcs = Arrays.copyOf(_arcs, _arcs.length + EXPECTED_MAX_STATES);
    }

    _arcs[_position++] = _FST.getFirstArc(node);
  }
}