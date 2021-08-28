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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pinot.segment.local.utils.nativefst.FSA;
import org.apache.pinot.segment.local.utils.nativefst.FSA5;


/**
 * Fast, memory-conservative finite state automaton builder, returning an
 * in-memory {@link FSA} that is a tradeoff between construction speed and
 * memory consumption. Use serializers to compress the returned automaton into
 * more compact form.
 * 
 * @see FSASerializer
 */
public final class FSABuilder {
  /**
   * Debug and information constants.
   * 
   * @see FSABuilder#getInfo()
   */
  public enum InfoEntry {
    SERIALIZATION_BUFFER_SIZE("Serialization buffer size"),
    SERIALIZATION_BUFFER_REALLOCATIONS("Serialization buffer reallocs"), 
    CONSTANT_ARC_AUTOMATON_SIZE("Constant arc FSA size"), 
    MAX_ACTIVE_PATH_LENGTH("Max active path"), 
    STATE_REGISTRY_TABLE_SLOTS("Registry hash slots"), 
    STATE_REGISTRY_SIZE("Registry hash entries"), 
    ESTIMATED_MEMORY_CONSUMPTION_MB("Estimated mem consumption (MB)");

    private final String stringified;

    InfoEntry(String stringified) {
      this.stringified = stringified;
    }

    @Override
    public String toString() {
      return stringified;
    }
  }

  /** A megabyte. */
  private final static int MB = 1024 * 1024;

  /**
   * Internal serialized FSA buffer expand ratio.
   */
  private final static int BUFFER_GROWTH_SIZE = 5 * MB;

  /**
   * Maximum number of labels from a single state.
   */
  private final static int MAX_LABELS = 256;

  /**
   * A comparator comparing full byte arrays. Unsigned byte comparisons ('C'-locale).
   */
  public static final Comparator<byte[]> LEXICAL_ORDERING = new Comparator<byte[]>() {
    public int compare(byte[] o1, byte[] o2) {
      return FSABuilder.compare(o1, 0, o1.length, o2, 0, o2.length);
    }
  };

  /**
   * Internal serialized FSA buffer expand ratio.
   */
  private final int bufferGrowthSize;

  private byte[] serialized = new byte[0];

  private Map<Integer, Integer> outputSymbols = new HashMap<>();

  /**
   * Number of bytes already taken in {@link #serialized}. Start from 1 to keep
   * 0 a sentinel value (for the hash set and final state).
   */
  private int size;

  /**
   * States on the "active path" (still mutable). Values are addresses of each
   * state's first arc.
   */
  private int[] activePath = new int[0];

  /**
   * Current length of the active path.
   */
  private int activePathLen;

  /**
   * The next offset at which an arc will be added to the given state on
   * {@link #activePath}.
   */
  private int[] nextArcOffset = new int[0];

  /**
   * Root state. If negative, the automaton has been built already and cannot be
   * extended.
   */
  private int root;

  /**
   * An epsilon state. The first and only arc of this state points either to the
   * root or to the terminal state, indicating an empty automaton.
   */
  private int epsilon;

  /**
   * Hash set of state addresses in {@link #serialized}, hashed by
   * {@link #hash(int, int)}. Zero reserved for an unoccupied slot.
   */
  private int[] hashSet = new int[2];

  /**
   * Number of entries currently stored in {@link #hashSet}.
   */
  private int hashSize = 0;

  /**
   * Previous sequence added to the automaton in {@link #add(byte[], int, int, int)}.
   * Used in assertions only.
   */
  private byte[] previous;

  /**
   * Information about the automaton and its compilation.
   */
  private TreeMap<InfoEntry, Object> info;

  /**
   * {@link #previous} sequence's length, used in assertions only.
   */
  private int previousLength;

  /** */
  public FSABuilder() {
    this(BUFFER_GROWTH_SIZE);
  }

  /**
   * @param bufferGrowthSize Buffer growth size (in bytes) when constructing the automaton.
   */
  public FSABuilder(int bufferGrowthSize) {
    this.bufferGrowthSize = Math.max(bufferGrowthSize, ConstantArcSizeFSA.ARC_SIZE * MAX_LABELS);

    // Allocate epsilon state.
    epsilon = allocateState(1);
    serialized[epsilon + ConstantArcSizeFSA.FLAGS_OFFSET] |= ConstantArcSizeFSA.BIT_ARC_LAST;

    // Allocate root, with an initial empty set of output arcs.
    expandActivePath(1);
    root = activePath[0];
  }

  public static FSA buildFSA(SortedMap<String, Integer> input) {

    FSABuilder fsaBuilder = new FSABuilder();

    for (Map.Entry<String, Integer> entry : input.entrySet()) {
      fsaBuilder.add(entry.getKey().getBytes(), 0, entry.getKey().length(), entry.getValue().intValue());
    }

    return fsaBuilder.complete();
  }

  /**
   * Add a single sequence of bytes to the FSA. The input must be
   * lexicographically greater than any previously added sequence.
   * 
   * @param sequence The array holding input sequence of bytes. 
   * @param start Starting offset (inclusive)
   * @param len Length of the input sequence (at least 1 byte).
   */
  public void add(byte[] sequence, int start, int len, int outputSymbol) {
    assert serialized != null : "Automaton already built.";
    assert previous == null || len == 0 || compare(previous, 0, previousLength, sequence, start, len) <= 0 : "Input must be sorted: "
        + Arrays.toString(Arrays.copyOf(previous, previousLength))
        + " >= "
        + Arrays.toString(Arrays.copyOfRange(sequence, start, len));
    assert setPrevious(sequence, start, len);

    // Determine common prefix length.
    final int commonPrefix = commonPrefix(sequence, start, len);

    // Make room for extra states on active path, if needed.
    expandActivePath(len);

    // Freeze all the states after the common prefix.
    for (int i = activePathLen - 1; i > commonPrefix; i--) {
      final int frozenState = freezeState(i);
      setArcTarget(nextArcOffset[i - 1] - ConstantArcSizeFSA.ARC_SIZE, frozenState);
      nextArcOffset[i] = activePath[i];
    }

    int prevArc = -1;

    // Create arcs to new suffix states.
    for (int i = commonPrefix + 1, j = start + commonPrefix; i <= len; i++) {
      final int p = nextArcOffset[i - 1];

      //TODO: atri
      //System.out.println("CURRENT OFFSET " + p);

      serialized[p + ConstantArcSizeFSA.FLAGS_OFFSET] = (byte) (i == len ? ConstantArcSizeFSA.BIT_ARC_FINAL : 0);
      serialized[p + ConstantArcSizeFSA.LABEL_OFFSET] = sequence[j++];
      setArcTarget(p, i == len ? ConstantArcSizeFSA.TERMINAL_STATE : activePath[i]);

      //TODO: atri
      //System.out.println("PUTTING CHAR " + (char) sequence[j - 1] + " " + "at " + p);
      int foo = i == len ? ConstantArcSizeFSA.TERMINAL_STATE : activePath[i];
      //System.out.println("ARC PUTTING for " + p + " for symbol " + (char) sequence[j - 1] + " and target " + foo);

      //TODO: atri
      //System.out.println("CURRENT ARC " + p + " target " + foo);
      //System.out.println("CURRENT ONE " + i + " value of seq " + j + " p " + p);

      //TODO: atri
      //System.out.println("PUTTING SYMBOL " + outputSymbol + " FOR " + i);
      nextArcOffset[i - 1] = p + ConstantArcSizeFSA.ARC_SIZE;

      prevArc = p;
    }

    if (prevArc != -1) {
      outputSymbols.put(prevArc, outputSymbol);
    }
    
    // Save last sequence's length so that we don't need to calculate it again.
    this.activePathLen = len;
  }

  /** Number of serialization buffer reallocations. */
  private int serializationBufferReallocations;

  /**
   * @return Finalizes the construction of the automaton and returns it.
   */
  public FSA complete() {
    add(new byte[0], 0, 0, -1);

    if (nextArcOffset[0] - activePath[0] == 0) {
      // An empty FSA.
      setArcTarget(epsilon, ConstantArcSizeFSA.TERMINAL_STATE);
    } else {
      // An automaton with at least a single arc from root.
      root = freezeState(0);
      setArcTarget(epsilon, root);
    }

    info = new TreeMap<InfoEntry, Object>();
    info.put(InfoEntry.SERIALIZATION_BUFFER_SIZE, serialized.length);
    info.put(InfoEntry.SERIALIZATION_BUFFER_REALLOCATIONS, serializationBufferReallocations);
    info.put(InfoEntry.CONSTANT_ARC_AUTOMATON_SIZE, size);
    info.put(InfoEntry.MAX_ACTIVE_PATH_LENGTH, activePath.length);
    info.put(InfoEntry.STATE_REGISTRY_TABLE_SLOTS, hashSet.length);
    info.put(InfoEntry.STATE_REGISTRY_SIZE, hashSize);
    info.put(InfoEntry.ESTIMATED_MEMORY_CONSUMPTION_MB, 
        (this.serialized.length + this.hashSet.length * 4) / (double) MB);

    /**
     * Two phase FST build operation -- build a constant arc size FST out of the current data, then
     * iterate through the FST and optimize for actual arc sizes and build a compact FST.
     */
    final FSA fsa = new ConstantArcSizeFSA(Arrays.copyOf(this.serialized, this.size), epsilon, outputSymbols);
    this.serialized = null;
    this.hashSet = null;

    try {
      final byte[] fsaData =
          new FSA5Serializer().withNumbers().serialize(fsa, new ByteArrayOutputStream()).toByteArray();

      return FSA.read(new ByteArrayInputStream(fsaData), FSA5.class, true);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }

    //TODO: atri
    //System.out.println("MAP IS " + outputSymbols.toString());
  }

  /**
   * Build a minimal, deterministic automaton from a sorted list of byte
   * sequences.
   * 
   * @param input Input sequences to build automaton from. 
   * @return Returns the automaton encoding all input sequences.
   */
  public static FSA build(byte[][] input, int[] outputSymbols) {
    final FSABuilder builder = new FSABuilder();

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
  public static FSA build(Iterable<byte[]> input, int[] outputSymbols) {
    final FSABuilder builder = new FSABuilder();

    int i = 0;

    for (byte[] chs : input) {
      builder.add(chs, 0, chs.length, i < outputSymbols.length ? outputSymbols[i] : -1);
      ++i;
    }

    return builder.complete();
  }

  /**
   * @return Returns various statistics concerning the FSA and its compilation.
   * @see InfoEntry
   */
  public Map<InfoEntry, Object> getInfo() {
    return info;
  }

  /** Is this arc the state's last? */
  private boolean isArcLast(int arc) {
    return (serialized[arc + ConstantArcSizeFSA.FLAGS_OFFSET] & ConstantArcSizeFSA.BIT_ARC_LAST) != 0;
  }

  /** Is this arc final? */
  private boolean isArcFinal(int arc) {
    return (serialized[arc + ConstantArcSizeFSA.FLAGS_OFFSET] & ConstantArcSizeFSA.BIT_ARC_FINAL) != 0;
  }

  /** Get label's arc. */
  private byte getArcLabel(int arc) {
    return serialized[arc + ConstantArcSizeFSA.LABEL_OFFSET];
  }

  /**
   * Fills the target state address of an arc.
   */
  private void setArcTarget(int arc, int state) {
    arc += ConstantArcSizeFSA.ADDRESS_OFFSET + ConstantArcSizeFSA.TARGET_ADDRESS_SIZE;
    for (int i = 0; i < ConstantArcSizeFSA.TARGET_ADDRESS_SIZE; i++) {
      serialized[--arc] = (byte) state;
      state >>>= 8;
    }
  }

  /**
   * Returns the address of an arc.
   */
  private int getArcTarget(int arc) {
    arc += ConstantArcSizeFSA.ADDRESS_OFFSET;
    return (serialized[arc]           ) << 24 | 
           (serialized[arc + 1] & 0xff) << 16 | 
           (serialized[arc + 2] & 0xff) << 8  |
           (serialized[arc + 3] & 0xff);
  }

  /**
   * @return The number of common prefix characters with the previous sequence.
   */
  private int commonPrefix(byte[] sequence, int start, int len) {
    // Empty root state case.
    final int max = Math.min(len, activePathLen);
    int i;
    for (i = 0; i < max; i++) {
      final int lastArc = nextArcOffset[i] - ConstantArcSizeFSA.ARC_SIZE;
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
    final int start = activePath[activePathIndex];
    final int end = nextArcOffset[activePathIndex];
    final int len = end - start;

    // Set the last arc flag on the current active path's state.
    serialized[end - ConstantArcSizeFSA.ARC_SIZE + ConstantArcSizeFSA.FLAGS_OFFSET] |= ConstantArcSizeFSA.BIT_ARC_LAST;

    // Try to locate a state with an identical content in the hash set.
    final int bucketMask = (hashSet.length - 1);
    int slot = hash(start, len) & bucketMask;
    for (int i = 0;;) {
      int state = hashSet[slot];
      if (state == 0) {
        state = hashSet[slot] = serialize(activePathIndex);
        if (++hashSize > hashSet.length / 2)
          expandAndRehash();

        replaceOutputSymbol(activePathIndex, state);

        //TODO: atri
        //System.out.println("Previos state "  + activePath[activePathIndex] + " new state " + state);
        return state;
      } else if (equivalent(state, start, len)) {
        replaceOutputSymbol(activePathIndex, state);

        //TODO: atri
        //System.out.println("Previos state "  + activePath[activePathIndex] + " new state " + state);
        return state;
      }

      slot = (slot + (++i)) & bucketMask;
    }
  }

  /**
   * Replace older offset with new frozen state in output symbols map
   */
  private void replaceOutputSymbol(int activePathIndex, int state) {

    //TODO: atri
    //System.out.println("KEY CAME IN " + activePath[activePathIndex]);
    if (!outputSymbols.containsKey(activePath[activePathIndex])) {
      //TODO: atri
      //System.out.println("NOT FOUND " +  activePath[activePathIndex]);
      return;
    }
    //TODO: atri
    //System.out.println("value is " + activePath[activePathIndex]);
    //System.out.println("CURRENT VAL " + outputSymbols);

    int outputSymbol = outputSymbols.get(activePath[activePathIndex]);
    outputSymbols.put(state, outputSymbol);
    outputSymbols.remove(activePath[activePathIndex]);
  }

  /**
   * Reallocate and rehash the hash set.
   */
  private void expandAndRehash() {
    final int[] newHashSet = new int[hashSet.length * 2];
    final int bucketMask = (newHashSet.length - 1);

    for (int j = 0; j < hashSet.length; j++) {
      final int state = hashSet[j];
      if (state > 0) {
        int slot = hash(state, stateLength(state)) & bucketMask;
        for (int i = 0; newHashSet[slot] > 0;) {
          slot = (slot + (++i)) & bucketMask;
        }
        newHashSet[slot] = state;
      }
    }
    this.hashSet = newHashSet;
  }

  /**
   * The total length of the serialized state data (all arcs).
   */
  private int stateLength(int state) {
    int arc = state;
    while (!isArcLast(arc)) {
      arc += ConstantArcSizeFSA.ARC_SIZE;
    }
    return arc - state + ConstantArcSizeFSA.ARC_SIZE;
  }

  /**
   * Return <code>true</code> if two regions in {@link #serialized} are
   * identical.
   */
  private boolean equivalent(int start1, int start2, int len) {
    if (start1 + len > size || start2 + len > size)
      return false;

    while (len-- > 0)
      if (serialized[start1++] != serialized[start2++])
        return false;

    return true;
  }

  /**
   * Serialize a given state on the active path.
   */
  private int serialize(final int activePathIndex) {
    expandBuffers();

    final int newState = size;
    final int start = activePath[activePathIndex];
    final int len = nextArcOffset[activePathIndex] - start;

    if (len > ConstantArcSizeFSA.ARC_SIZE) {
      assert len % ConstantArcSizeFSA.ARC_SIZE == 0;

      int i = 0;
      int j = 0;

      while (i < len) {
        //TODO: atri
        //System.out.println("IS COND1 " + (newState + (j *ConstantArcSizeFSA.ARC_SIZE)) + " " + (activePath[activePathIndex] + ConstantArcSizeFSA.ARC_SIZE));

        Integer currentOutputSymbol = outputSymbols.get(activePath[activePathIndex] + (j * ConstantArcSizeFSA.ARC_SIZE));

        if (currentOutputSymbol != null) {
          outputSymbols.put((newState + (j * ConstantArcSizeFSA.ARC_SIZE)), outputSymbols.get(activePath[activePathIndex] + (j * ConstantArcSizeFSA.ARC_SIZE)));
        }

        i = i + ConstantArcSizeFSA.ARC_SIZE;
        j++;
      }
    }

    System.arraycopy(serialized, start, serialized, newState, len);

    //TODO: atri
    //System.out.println("NEW LABEL " + (char) serialized[newState + 1]);

    size += len;
    return newState;
  }

  /**
   * Hash code of a fragment of {@link #serialized} array.
   */
  private int hash(int start, int byteCount) {
    assert byteCount % ConstantArcSizeFSA.ARC_SIZE == 0 : "Not an arc multiply?";

    int h = 0;
    for (int arcs = byteCount / ConstantArcSizeFSA.ARC_SIZE; --arcs >= 0; start += ConstantArcSizeFSA.ARC_SIZE) {
      h = 17 * h + getArcLabel(start);
      h = 17 * h + getArcTarget(start);
      if (isArcFinal(start))
        h += 17;
    }

    return h;
  }

  /**
   * Append a new mutable state to the active path.
   */
  private void expandActivePath(int size) {
    if (activePath.length < size) {
      final int p = activePath.length;
      activePath = Arrays.copyOf(activePath, size);
      nextArcOffset = Arrays.copyOf(nextArcOffset, size);

      for (int i = p; i < size; i++) {
        nextArcOffset[i] = activePath[i] = allocateState(/* assume max labels count */MAX_LABELS);
      }
    }
  }

  /**
   * Expand internal buffers for the next state.
   */
  private void expandBuffers() {
    if (this.serialized.length < size + ConstantArcSizeFSA.ARC_SIZE * MAX_LABELS) {
      serialized = Arrays.copyOf(serialized, serialized.length + bufferGrowthSize);
      serializationBufferReallocations++;
    }
  }

  /**
   * Allocate space for a state with the given number of outgoing labels.
   * 
   * @return state offset
   */
  private int allocateState(int labels) {
    expandBuffers();
    final int state = size;
    size += labels * ConstantArcSizeFSA.ARC_SIZE;
    return state;
  }

  /**
   * Copy <code>current</code> into an internal buffer.
   */
  private boolean setPrevious(byte[] sequence, int start, int length) {
    if (previous == null || previous.length < length) {
      previous = new byte[length];
    }

    System.arraycopy(sequence, start, previous, 0, length);
    previousLength = length;
    return true;
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
      if (c1 != c2)
        return (c1 & 0xff) - (c2 & 0xff);
    }

    return lens1 - lens2;
  }
}
