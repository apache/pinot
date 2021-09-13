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

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pinot.segment.local.utils.nativefst.ConstantArcSizeFST;
import org.apache.pinot.segment.local.utils.nativefst.FST;


/**
 * Fast, memory-conservative finite state transducer builder, returning an
 * in-memory {@link FST} that is a tradeoff between construction speed and
 * memory consumption. Use serializers to compress the returned automaton into
 * more compact form.
 *
 * @see FSTSerializer
 */
public final class FSTBuilder {
  /**
   * A comparator comparing full byte arrays. Unsigned byte comparisons ('C'-locale).
   */
  public static final Comparator<byte[]> LEXICAL_ORDERING = new Comparator<byte[]>() {
    public int compare(byte[] o1, byte[] o2) {
      return FSTBuilder.compare(o1, 0, o1.length, o2, 0, o2.length);
    }
  };
  /** A megabyte. */
  private final static int MB = 1024 * 1024;

  /**
   * Internal serialized FST buffer expand ratio.
   */
  private final static int BUFFER_GROWTH_SIZE = 5 * MB;

  /**
   * Maximum number of labels from a single state.
   */
  private final static int MAX_LABELS = 256;
  /**
   * Internal serialized FST buffer expand ratio.
   */
  private final int _bufferGrowthSize;
  private byte[] _serialized = new byte[0];
  private Map<Integer, Integer> _outputSymbols = new HashMap<>();
  /**
   * Number of bytes already taken in {@link #_serialized}. Start from 1 to keep
   * 0 a sentinel value (for the hash set and final state).
   */
  private int _size;
  /**
   * States on the "active path" (still mutable). Values are addresses of each
   * state's first arc.
   */
  private int[] _activePath = new int[0];
  /**
   * Current length of the active path.
   */
  private int _activePathLen;
  /**
   * The next offset at which an arc will be added to the given state on
   * {@link #_activePath}.
   */
  private int[] _nextArcOffset = new int[0];
  /**
   * Root state. If negative, the automaton has been built already and cannot be
   * extended.
   */
  private int _root;
  /**
   * An epsilon state. The first and only arc of this state points either to the
   * root or to the terminal state, indicating an empty automaton.
   */
  private int _epsilon;
  /**
   * Hash set of state addresses in {@link #_serialized}, hashed by
   * {@link #hash(int, int)}. Zero reserved for an unoccupied slot.
   */
  private int[] _hashSet = new int[2];
  /**
   * Number of entries currently stored in {@link #_hashSet}.
   */
  private int _hashSize = 0;
  /**
   * Previous sequence added to the automaton in {@link #add(byte[], int, int, int)}.
   * Used in assertions only.
   */
  private byte[] _previous;
  /**
   * Information about the automaton and its compilation.
   */
  private TreeMap<InfoEntry, Object> _info;
  /**
   * {@link #_previous} sequence's length, used in assertions only.
   */
  private int _previousLength;
  /** Number of serialization buffer reallocations. */
  private int _serializationBufferReallocations;

  /** */
  public FSTBuilder() {
    this(BUFFER_GROWTH_SIZE);
  }

  /**
   * @param bufferGrowthSize Buffer growth size (in bytes) when constructing the automaton.
   */
  public FSTBuilder(int bufferGrowthSize) {
    this._bufferGrowthSize = Math.max(bufferGrowthSize, ConstantArcSizeFST.ARC_SIZE * MAX_LABELS);

    // Allocate epsilon state.
    _epsilon = allocateState(1);
    _serialized[_epsilon + ConstantArcSizeFST.FLAGS_OFFSET] |= ConstantArcSizeFST.BIT_ARC_LAST;

    // Allocate root, with an initial empty set of output arcs.
    expandActivePath(1);
    _root = _activePath[0];
  }

  public static FST buildFST(SortedMap<String, Integer> input) {

    FSTBuilder fstbuilder = new FSTBuilder();

    for (Map.Entry<String, Integer> entry : input.entrySet()) {
      fstbuilder.add(entry.getKey().getBytes(), 0, entry.getKey().length(), entry.getValue().intValue());
    }

    return fstbuilder.complete();
  }

  /**
   * Build a minimal, deterministic automaton from a sorted list of byte
   * sequences.
   *
   * @param input Input sequences to build automaton from.
   * @return Returns the automaton encoding all input sequences.
   */
  public static FST build(byte[][] input, int[] outputSymbols) {
    final FSTBuilder builder = new FSTBuilder();

    int i = 0;
    for (byte[] chs : input) {
      builder.add(chs, 0, chs.length, i < outputSymbols.length ? outputSymbols[i] : -1);
      ++i;
    }

    return builder.complete();
  }

  /**
   * Build a minimal, deterministic automaton from an iterable list of byte
   * sequences.
   *
   * @param input Input sequences to build automaton from.
   * @return Returns the automaton encoding all input sequences.
   */
  public static FST build(Iterable<byte[]> input, int[] outputSymbols) {
    final FSTBuilder builder = new FSTBuilder();

    int i = 0;

    for (byte[] chs : input) {
      builder.add(chs, 0, chs.length, i < outputSymbols.length ? outputSymbols[i] : -1);
      ++i;
    }

    return builder.complete();
  }

  /**
   * Lexicographic order of input sequences. By default, consistent with the "C"
   * sort (absolute value of bytes, 0-255).
   */
  private static int compare(byte[] s1, int start1, int lens1, byte[] s2, int start2, int lens2) {
    final int max = Math.min(lens1, lens2);

    for (int i = 0; i < max; i++) {
      final byte c1 = s1[start1++];
      final byte c2 = s2[start2++];
      if (c1 != c2) {
        return (c1 & 0xff) - (c2 & 0xff);
      }
    }

    return lens1 - lens2;
  }

  /**
   * Add a single sequence of bytes to the FST. The input must be
   * lexicographically greater than any previously added sequence.
   *
   * @param sequence The array holding input sequence of bytes.
   * @param start Starting offset (inclusive)
   * @param len Length of the input sequence (at least 1 byte).
   */
  public void add(byte[] sequence, int start, int len, int outputSymbol) {
    assert _serialized != null : "Automaton already built.";
    assert _previous == null || len == 0 || compare(_previous, 0, _previousLength, sequence, start, len) <= 0
        : "Input must be sorted: " + Arrays.toString(Arrays.copyOf(_previous, _previousLength)) + " >= " + Arrays
            .toString(Arrays.copyOfRange(sequence, start, len));
    assert setPrevious(sequence, start, len);

    // Determine common prefix length.
    final int commonPrefix = commonPrefix(sequence, start, len);

    // Make room for extra states on active path, if needed.
    expandActivePath(len);

    // Freeze all the states after the common prefix.
    for (int i = _activePathLen - 1; i > commonPrefix; i--) {
      final int frozenState = freezeState(i);
      setArcTarget(_nextArcOffset[i - 1] - ConstantArcSizeFST.ARC_SIZE, frozenState);
      _nextArcOffset[i] = _activePath[i];
    }

    int prevArc = -1;

    // Create arcs to new suffix states.
    for (int i = commonPrefix + 1, j = start + commonPrefix; i <= len; i++) {
      final int p = _nextArcOffset[i - 1];

      _serialized[p + ConstantArcSizeFST.FLAGS_OFFSET] = (byte) (i == len ? ConstantArcSizeFST.BIT_ARC_FINAL : 0);
      _serialized[p + ConstantArcSizeFST.LABEL_OFFSET] = sequence[j++];
      setArcTarget(p, i == len ? ConstantArcSizeFST.TERMINAL_STATE : _activePath[i]);

      _nextArcOffset[i - 1] = p + ConstantArcSizeFST.ARC_SIZE;

      prevArc = p;
    }

    if (prevArc != -1) {
      _outputSymbols.put(prevArc, outputSymbol);
    }

    // Save last sequence's length so that we don't need to calculate it again.
    this._activePathLen = len;
  }

  /**
   * @return Finalizes the construction of the automaton and returns it.
   */
  public FST complete() {
    add(new byte[0], 0, 0, -1);

    if (_nextArcOffset[0] - _activePath[0] == 0) {
      // An empty FST.
      setArcTarget(_epsilon, ConstantArcSizeFST.TERMINAL_STATE);
    } else {
      // An automaton with at least a single arc from root.
      _root = freezeState(0);
      setArcTarget(_epsilon, _root);
    }

    _info = new TreeMap<>();
    _info.put(InfoEntry.SERIALIZATION_BUFFER_SIZE, _serialized.length);
    _info.put(InfoEntry.SERIALIZATION_BUFFER_REALLOCATIONS, _serializationBufferReallocations);
    _info.put(InfoEntry.CONSTANT_ARC_AUTOMATON_SIZE, _size);
    _info.put(InfoEntry.MAX_ACTIVE_PATH_LENGTH, _activePath.length);
    _info.put(InfoEntry.STATE_REGISTRY_TABLE_SLOTS, _hashSet.length);
    _info.put(InfoEntry.STATE_REGISTRY_SIZE, _hashSize);
    _info.put(InfoEntry.ESTIMATED_MEMORY_CONSUMPTION_MB,
        (this._serialized.length + this._hashSet.length * 4) / (double) MB);

    final FST fst = new ConstantArcSizeFST(Arrays.copyOf(this._serialized, this._size), _epsilon, _outputSymbols);
    this._serialized = null;
    this._hashSet = null;

    return fst;
  }

  /** Is this arc the state's last? */
  private boolean isArcLast(int arc) {
    return (_serialized[arc + ConstantArcSizeFST.FLAGS_OFFSET] & ConstantArcSizeFST.BIT_ARC_LAST) != 0;
  }

  /** Is this arc final? */
  private boolean isArcFinal(int arc) {
    return (_serialized[arc + ConstantArcSizeFST.FLAGS_OFFSET] & ConstantArcSizeFST.BIT_ARC_FINAL) != 0;
  }

  /** Get label's arc. */
  private byte getArcLabel(int arc) {
    return _serialized[arc + ConstantArcSizeFST.LABEL_OFFSET];
  }

  /**
   * Fills the target state address of an arc.
   */
  private void setArcTarget(int arc, int state) {
    arc += ConstantArcSizeFST.ADDRESS_OFFSET + ConstantArcSizeFST.TARGET_ADDRESS_SIZE;
    for (int i = 0; i < ConstantArcSizeFST.TARGET_ADDRESS_SIZE; i++) {
      _serialized[--arc] = (byte) state;
      state >>>= 8;
    }
  }

  /**
   * Returns the address of an arc.
   */
  private int getArcTarget(int arc) {
    arc += ConstantArcSizeFST.ADDRESS_OFFSET;
    return (_serialized[arc]) << 24 | (_serialized[arc + 1] & 0xff) << 16 | (_serialized[arc + 2] & 0xff) << 8 | (
        _serialized[arc + 3] & 0xff);
  }

  /**
   * @return The number of common prefix characters with the previous sequence.
   */
  private int commonPrefix(byte[] sequence, int start, int len) {
    // Empty root state case.
    final int max = Math.min(len, _activePathLen);
    int i;
    for (i = 0; i < max; i++) {
      final int lastArc = _nextArcOffset[i] - ConstantArcSizeFST.ARC_SIZE;
      if (sequence[start++] != getArcLabel(lastArc)) {
        break;
      }
    }

    return i;
  }

  /**
   * Freeze a state: try to find an equivalent state in the interned states
   * dictionary first, if found, return it, otherwise, serialize the mutable
   * state at <code>activePathIndex</code> and return it.
   */
  private int freezeState(final int activePathIndex) {
    final int start = _activePath[activePathIndex];
    final int end = _nextArcOffset[activePathIndex];
    final int len = end - start;

    // Set the last arc flag on the current active path's state.
    _serialized[end - ConstantArcSizeFST.ARC_SIZE + ConstantArcSizeFST.FLAGS_OFFSET] |= ConstantArcSizeFST.BIT_ARC_LAST;

    // Try to locate a state with an identical content in the hash set.
    int state = 0;

    if (!equivalent(state, start, len)) {
      state = serialize(activePathIndex);
      if (++_hashSize > _hashSet.length / 2) {
        expandAndRehash();
      }

      replaceOutputSymbol(start, state);
    }
    return state;
  }

  /**
   * Replace older offset with new frozen state in output symbols map
   */
  private void replaceOutputSymbol(int target, int state) {

    if (!_outputSymbols.containsKey(target)) {
      return;
    }

    int outputSymbol = _outputSymbols.get(target);

    _outputSymbols.put(state, outputSymbol);
    _outputSymbols.remove(target);
  }

  /**
   * Reallocate and rehash the hash set.
   */
  private void expandAndRehash() {
    final int[] newHashSet = new int[_hashSet.length * 2];
    final int bucketMask = (newHashSet.length - 1);

    for (int j = 0; j < _hashSet.length; j++) {
      final int state = _hashSet[j];
      if (state > 0) {
        int slot = hash(state, stateLength(state)) & bucketMask;
        for (int i = 0; newHashSet[slot] > 0; ) {
          slot = (slot + (++i)) & bucketMask;
        }
        newHashSet[slot] = state;
      }
    }
    this._hashSet = newHashSet;
  }

  /**
   * The total length of the serialized state data (all arcs).
   */
  private int stateLength(int state) {
    int arc = state;
    while (!isArcLast(arc)) {
      arc += ConstantArcSizeFST.ARC_SIZE;
    }
    return arc - state + ConstantArcSizeFST.ARC_SIZE;
  }

  /**
   * Return <code>true</code> if two regions in {@link #_serialized} are
   * identical.
   */
  private boolean equivalent(int start1, int start2, int len) {
    if (start1 + len > _size || start2 + len > _size) {
      return false;
    }

    while (len-- > 0) {
      if (_serialized[start1++] != _serialized[start2++]) {
        return false;
      }
    }

    return true;
  }

  /**
   * Serialize a given state on the active path.
   */
  private int serialize(final int activePathIndex) {
    expandBuffers();

    final int newState = _size;
    final int start = _activePath[activePathIndex];
    final int len = _nextArcOffset[activePathIndex] - start;

    if (len > ConstantArcSizeFST.ARC_SIZE) {
      assert len % ConstantArcSizeFST.ARC_SIZE == 0;

      int i = 0;
      int j = 0;

      while (i < len) {
        Integer currentOutputSymbol =
            _outputSymbols.get(_activePath[activePathIndex] + (j * ConstantArcSizeFST.ARC_SIZE));

        if (currentOutputSymbol != null) {
          _outputSymbols.put((newState + (j * ConstantArcSizeFST.ARC_SIZE)),
              _outputSymbols.get(_activePath[activePathIndex] + (j * ConstantArcSizeFST.ARC_SIZE)));
        }

        i = i + ConstantArcSizeFST.ARC_SIZE;
        j++;
      }
    }

    System.arraycopy(_serialized, start, _serialized, newState, len);

    _size += len;
    return newState;
  }

  /**
   * Hash code of a fragment of {@link #_serialized} array.
   */
  private int hash(int start, int byteCount) {
    assert byteCount % ConstantArcSizeFST.ARC_SIZE == 0 : "Not an arc multiply?";

    int h = 0;
    for (int arcs = byteCount / ConstantArcSizeFST.ARC_SIZE; --arcs >= 0; start += ConstantArcSizeFST.ARC_SIZE) {
      h = 17 * h + getArcLabel(start);
      h = 17 * h + getArcTarget(start);
      if (isArcFinal(start)) {
        h += 17;
      }
    }

    return h;
  }

  /**
   * Append a new mutable state to the active path.
   */
  private void expandActivePath(int size) {
    if (_activePath.length < size) {
      final int p = _activePath.length;
      _activePath = Arrays.copyOf(_activePath, size);
      _nextArcOffset = Arrays.copyOf(_nextArcOffset, size);

      for (int i = p; i < size; i++) {
        int newState = allocateState(/* assume max labels count */MAX_LABELS);
        _nextArcOffset[i] = newState;
        _activePath[i] = newState;
      }
    }
  }

  /**
   * Expand internal buffers for the next state.
   */
  private void expandBuffers() {
    if (this._serialized.length < _size + ConstantArcSizeFST.ARC_SIZE * MAX_LABELS) {
      _serialized = Arrays.copyOf(_serialized, _serialized.length + _bufferGrowthSize);
      _serializationBufferReallocations++;
    }
  }

  /**
   * Allocate space for a state with the given number of outgoing labels.
   *
   * @return state offset
   */
  private int allocateState(int labels) {
    expandBuffers();
    final int state = _size;
    _size += labels * ConstantArcSizeFST.ARC_SIZE;
    return state;
  }

  /**
   * Copy <code>current</code> into an internal buffer.
   */
  private boolean setPrevious(byte[] sequence, int start, int length) {
    if (_previous == null || _previous.length < length) {
      _previous = new byte[length];
    }

    System.arraycopy(sequence, start, _previous, 0, length);
    _previousLength = length;
    return true;
  }

  /**
   * Debug and information constants.
   *
   * @see FSTBuilder#getInfo()
   */
  public enum InfoEntry {
    SERIALIZATION_BUFFER_SIZE("Serialization buffer size"),
    SERIALIZATION_BUFFER_REALLOCATIONS("Serialization buffer reallocs"),
    CONSTANT_ARC_AUTOMATON_SIZE("Constant arc FST size"),
    MAX_ACTIVE_PATH_LENGTH("Max active path"),
    STATE_REGISTRY_TABLE_SLOTS("Registry hash slots"),
    STATE_REGISTRY_SIZE("Registry hash entries"),
    ESTIMATED_MEMORY_CONSUMPTION_MB("Estimated mem consumption (MB)");

    private final String _stringified;

    InfoEntry(String stringified) {
      this._stringified = stringified;
    }

    @Override
    public String toString() {
      return _stringified;
    }
  }
}
